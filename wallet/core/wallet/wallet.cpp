// Copyright 2018 The Beam Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "wallet_common.h"

namespace beam::wallet
{
    // @param address as string
    // Returns whether the address is a valid SBBS address i.e. a point on an ellyptic curve or a valid token
    bool CheckReceiverAddress(const std::string& addr)
    {
        auto p = ParseParameters(addr);
        return !!p;
    }

    TxParameters ProcessReceiverAddress(const TxParameters& parameters, IWalletDB::Ptr walletDB)
    {
        const auto& peerAddr = parameters.GetParameter<WalletID>(TxParameterID::PeerAddr);
        if (!peerAddr)
            return parameters;

        // if there is no parameter default behaviour is to save address or update it, for historical reasons
        auto savePeerAddressParam = parameters.GetParameter<bool>(TxParameterID::SavePeerAddress);
        bool savePeerAddress = !savePeerAddressParam || *savePeerAddressParam == true;

        auto receiverAddr = walletDB->getAddress(*peerAddr);
        if (receiverAddr)
        {
            if (receiverAddr->isOwn() && receiverAddr->isExpired())
            {
                BEAM_LOG_ERROR() << "Can't send to the expired address.";
                throw ReceiverAddressExpiredException();
            }

            if (savePeerAddress)
            {
                // update address comment if changed
                if (auto message = parameters.GetParameter<ByteBuffer>(TxParameterID::Message); message)
                {
                    auto messageStr = std::string(message->begin(), message->end());
                    if (messageStr != receiverAddr->m_label)
                    {
                        receiverAddr->m_label = messageStr;
                        walletDB->saveAddress(*receiverAddr);
                    }
                }
            }

            TxParameters temp{ parameters };
            temp.SetParameter(TxParameterID::IsSelfTx, receiverAddr->isOwn());
            return temp;
        }

        if (savePeerAddress)
        {
            WalletAddress address;
            address.m_BbsAddr = *peerAddr;
            address.m_createTime = getTimestamp();
            if (auto message = parameters.GetParameter<ByteBuffer>(TxParameterID::Message); message)
            {
                address.m_label = std::string(message->begin(), message->end());
            }
            if (auto ep = parameters.GetParameter<PeerID>(TxParameterID::PeerEndpoint); ep)
            {
                address.m_Endpoint = *ep;
            }
            walletDB->saveAddress(address);
        }

        TxParameters temp{parameters};
        temp.SetParameter(TxParameterID::IsSelfTx, false);
        return temp;
    }

    Wallet::Wallet(IWalletDB::Ptr walletDB, TxCompletedAction&& action, UpdateCompletedAction&& updateCompleted)
        : m_WalletDB{ walletDB }
        , m_TxCompletedAction{ std::move(action) }
        , m_UpdateCompleted{ std::move(updateCompleted) }
        , m_LastSyncTotal(0)
        , m_OwnedNodesOnline(0)
    {
        assert(walletDB);
        _myThread = std::this_thread::get_id();

        // the only default type of transaction
        RegisterTransactionType(TxType::Simple, make_unique<SimpleTransaction::Creator>(m_WalletDB));
        RegisterTransactionType(TxType::Contract, make_unique<ContractTransaction::Creator>(m_WalletDB));
        m_IsTreasuryHandled = storage::isTreasuryHandled(*m_WalletDB);
        m_Extra.m_ShieldedOutputs = m_WalletDB->get_ShieldedOuts();
    }

    Wallet::~Wallet()
    {
        CleanupNetwork();
    }

    void Wallet::CleanupNetwork()
    {
        // clear all requests
#define THE_MACRO(type) \
                while (!m_Pending##type.empty()) \
                    DeleteReq(*m_Pending##type.begin());

        WalletFlyClientRequests_All(THE_MACRO)
#undef THE_MACRO

        m_MessageEndpoints.clear();
        m_NodeEndpoint = nullptr;

        // save voucher request manager state
        auto buf = m_VoucherManager.SaveState();
        if (buf.empty())
            m_WalletDB->removeVarRaw(VoucherManager::Ser::s_szVarName);
        else
            m_WalletDB->setVarRaw(VoucherManager::Ser::s_szVarName, &buf.front(), buf.size());
    }

    // Fly client implementation
    void Wallet::get_Kdf(Key::IKdf::Ptr& pKdf)
    {
        pKdf = m_WalletDB->get_MasterKdf();
    }

    void Wallet::get_OwnerKdf(Key::IPKdf::Ptr& ownerKdf)
    {
        ownerKdf = m_WalletDB->get_OwnerKdf();
    }

    // Implementation of the FlyClient protocol method
    // @id : PeerID - peer id of the node
    // bUp : bool - flag indicating that node is online
    void Wallet::OnOwnedNode(const PeerID& id, bool bUp)
    {
        if (bUp)
        {
            if (!m_OwnedNodesOnline++) // on first connection to the node
            {
                AbortBodiesRequests();
                ResetCommitmentsCache();
                RequestEvents(); // maybe time to refresh UTXOs
            }
        }
        else
        {
            assert(m_OwnedNodesOnline); // check that m_OwnedNodesOnline is positive number
            if (!--m_OwnedNodesOnline)
            {
                AbortEvents();
            }
        }

        for (const auto sub : m_subscribers)
        {
            sub->onOwnedNode(id, bUp);
        }
    }

    Block::SystemState::IHistory& Wallet::get_History()
    {
        return m_WalletDB->get_History();
    }

    void Wallet::SetNodeEndpoint(proto::FlyClient::INetwork::Ptr nodeEndpoint)
    {
        m_NodeEndpoint = std::move(nodeEndpoint);
    }

    proto::FlyClient::INetwork::Ptr  Wallet::GetNodeEndpoint() const
    {
        return m_NodeEndpoint;
    }

    void Wallet::AddMessageEndpoint(IWalletMessageEndpoint::Ptr endpoint)
    {
        m_MessageEndpoints.insert(endpoint);
    }

    void Wallet::Subscribe(IWalletObserver* observer)
    {
        assert(!std::ranges::contains(m_subscribers, observer));
        m_subscribers.push_back(observer);
        m_WalletDB->Subscribe(observer);
    }

    void Wallet::Unsubscribe(IWalletObserver* observer)
    {
        auto it = std::find(m_subscribers.begin(), m_subscribers.end(), observer);
        assert(it != m_subscribers.end());
        m_subscribers.erase(it);
        m_WalletDB->Unsubscribe(observer);
    }

    void Wallet::Subscribe(ISimpleSwapHandler* handler)
    {
        if (m_ssHandler)
        {
            assert(false);
            throw std::runtime_error("Can be only one SimpleSwap handler");
        }
        m_ssHandler = handler;
    }

    void Wallet::Unsubscribe(ISimpleSwapHandler* handler)
    {
        if (m_ssHandler != handler)
        {
            assert(false);
            throw std::runtime_error("Unexpected SimpleSwap unsubscribe");
        }
        m_ssHandler = nullptr;
    }

    bool Wallet::IsConnectedToOwnNode() const
    {
        return m_OwnedNodesOnline > 0;
    }

    bool Wallet::CanDetectCoins() const
    {
        return m_OwnedNodesOnline || m_IsBodyRequestsEnabled;
    }

    void Wallet::EnableBodyRequests(bool value)
    {
        m_IsBodyRequestsEnabled = value;
    }

    void Wallet::assertThread() const
    {
        if (_myThread != std::this_thread::get_id())
        {
            assert(false);
            throw std::runtime_error("Wallet accessed from wrong thread");
        }
    }
}
