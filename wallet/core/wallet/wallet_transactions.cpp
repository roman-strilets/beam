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
    namespace
    {
        bool ApplyTransactionParameters(BaseTransaction::Ptr tx, const PackedTxParameters& parameters, bool isInternalSource)
        {
            bool txChanged = false;
            SubTxID subTxID = kDefaultSubTxID;
            for (const auto& p : parameters)
            {
                if (p.first == TxParameterID::SubTxIndex)
                {
                    // change subTxID
                    Deserializer d;
                    d.reset(p.second.data(), p.second.size());
                    d& subTxID;
                    continue;
                }

                if (!isInternalSource && !tx->IsTxParameterExternalSettable(p.first, subTxID))
                {
                    BEAM_LOG_WARNING() << tx->GetTxID() << "Attempt to set internal tx parameter: " << static_cast<int>(p.first);
                    continue;
                }

                txChanged |= tx->GetWalletDB()->setTxParameter(tx->GetTxID(), subTxID, p.first, p.second, true, isInternalSource);
            }
            return txChanged;
        }
    }

    void Wallet::RegisterTransactionType(TxType type, BaseTransaction::Creator::Ptr creator)
    {
        m_TxCreators[type] = std::move(creator);
    }

    TxID Wallet::StartTransaction(const TxParameters& parameters)
    {
        auto tx = ConstructTransactionFromParameters(parameters);
        if (!tx)
        {
            throw FailToStartNewTransactionException();
        }
        ProcessTransaction(tx);
        return *parameters.GetTxID();
    }

    void Wallet::ProcessTransaction(wallet::BaseTransaction::Ptr tx)
    {
        MakeTransactionActive(tx);
        UpdateTransaction(tx->GetTxID());
    }

    void Wallet::ResumeTransaction(const TxDescription& tx)
    {
        if (tx.canResume() && !m_ActiveTransactions.contains(tx.m_txId))
        {
            auto t = ConstructTransaction(tx.m_txId, tx.m_txType);
            if (t)
            {
                MakeTransactionActive(t);
                UpdateOnSynced(t);
            }
        }
    }

    void Wallet::VisitActiveTransaction(const TxVisitor& visitor)
    {
        for(const auto& it: m_ActiveTransactions)
        {
            visitor(it.first, it.second);
        }
    }

    void Wallet::ResumeAllTransactions()
    {
        // restore being-received vouchers
        {
            ByteBuffer buf;
            m_WalletDB->getBlob(VoucherManager::Ser::s_szVarName, buf);
            m_VoucherManager.LoadState(buf);
        }

        InitWidgetRunner();

        auto func = [this](const auto& tx)
        {
            ResumeTransaction(tx);
            return true;
        };
        TxListFilter filter;
        m_WalletDB->visitTx(func, filter);

        m_VoucherManager.CheckTimer();
    }

    size_t Wallet::GetUnsafeActiveTransactionsCount() const
    {
        return std::count_if(m_ActiveTransactions.begin(), m_ActiveTransactions.end(), [](const auto& p)
            {
                return p.second && !p.second->IsInSafety();
            });
    }

    void Wallet::on_tx_completed(const TxID& id)
    {
        // Note: the passed TxID is (most probably) the member of the transaction, 
        // which we, most probably, are going to erase from the map, which can potentially delete it.
        // Make sure we either copy the txID, or prolong the lifetime of the tx.
        TxID txID = id; // copy
        BaseTransaction::Ptr pGuard;

        auto it = m_ActiveTransactions.find(txID);
        if (it != m_ActiveTransactions.end())
        {
            pGuard.swap(it->second);
            m_ActiveTransactions.erase(it);
            m_NextTipTransactionToUpdate.erase(pGuard);

            pGuard->FreeResources();
        }

        if (m_TxCompletedAction)
        {
            m_TxCompletedAction(txID);
        }
    }

    void Wallet::on_tx_failed(const TxID& txID)
    {
        on_tx_completed(txID);
    }

    // Implementation of the INegotiatorGateway::UpdateOnNextTip
    void Wallet::UpdateOnNextTip(const TxID& txID)
    {
        auto it = m_ActiveTransactions.find(txID);
        if (it != m_ActiveTransactions.end())
        {
            UpdateOnNextTip(it->second);
        }
    }

    bool Wallet::CanCancelTransaction(const TxID& txId) const
    {
        if (auto it = m_ActiveTransactions.find(txId); it != m_ActiveTransactions.end())
        {
            return it->second->CanCancel();
        }
        return false;
    }

    void Wallet::CancelTransaction(const TxID& txId)
    {
        BEAM_LOG_INFO() << txId << " Canceling tx";

        if (auto it = m_ActiveTransactions.find(txId); it != m_ActiveTransactions.end())
        {
            it->second->Cancel();
        }
        else
        {
            BEAM_LOG_WARNING() << "Transaction already inactive";
        }
    }

    void Wallet::DeleteTransaction(const TxID& txId)
    {
        BEAM_LOG_INFO() << "deleting tx " << txId;
        if (auto it = m_ActiveTransactions.find(txId); it == m_ActiveTransactions.end())
        {
            m_WalletDB->deleteTx(txId);
        }
        else
        {
            BEAM_LOG_WARNING() << "Cannot delete running transaction";
        }
    }

    void Wallet::UpdateTransaction(const TxID& txID)
    {
        auto it = m_ActiveTransactions.find(txID);
        if (it != m_ActiveTransactions.end())
        {
            UpdateTransaction(it->second);
        }
        else
        {
            BEAM_LOG_DEBUG() << txID << " Unexpected event";
        }
    }

    void Wallet::UpdateTransaction(BaseTransaction::Ptr tx)
    {
        DoInSyncedWallet([this, tx]() { UpdateActiveTransaction(tx); });
    }

    void Wallet::UpdateActiveTransaction(BaseTransaction::Ptr tx)
    {
        auto it = m_ActiveTransactions.find(tx->GetTxID());
        if (it != m_ActiveTransactions.end())
        {
            tx->Update();
        }
    }

    void Wallet::UpdateOnSynced(BaseTransaction::Ptr tx)
    {
        m_SyncActionsQueue.push([this, tx]()
        {
             UpdateActiveTransaction(tx);
        });
    }

    void Wallet::UpdateOnNextTip(BaseTransaction::Ptr tx)
    {
        m_NextTipTransactionToUpdate.insert(tx);
    }

    void Wallet::OnTransactionMsg(const WalletID& myID, const SetTxParameter& msg)
    {
        //
        // Extend Auto addresses
        //
        if (auto addr = m_WalletDB->getAddress(myID); addr)
        {
            if (!addr->isPermanent())
            {
                addr->setExpirationStatus(WalletAddress::ExpirationStatus::Auto);
                m_WalletDB->saveAddress(*addr);
            }
        }

        //
        // Process transaction request
        //
        auto it = m_ActiveTransactions.find(msg.m_TxID);
        if (it != m_ActiveTransactions.end())
        {
            const auto& pTx = it->second;

            if (pTx->GetType() != msg.m_Type)
            {
                BEAM_LOG_WARNING() << msg.m_TxID << " Parameters for invalid tx type";
                return;
            }

            WalletID peerID;
            if (pTx->GetParameter(TxParameterID::PeerAddr, peerID))
            {
                if (peerID != msg.m_From)
                    return; // if we already have PeerID, we should ignore messages from others
            }
            else
            {
                pTx->SetParameter(TxParameterID::PeerAddr, msg.m_From, false);
            }

            if (ApplyTransactionParameters(pTx, msg.m_Parameters, false))
                UpdateTransaction(pTx);

            return;
        }

        {
            TxType type = TxType::Simple;
            if (storage::getTxParameter(*m_WalletDB, msg.m_TxID, TxParameterID::TransactionType, type)) {
                // This request has already been processed; a different type here comes from the peer, not from us
                return;
            }
        }

        if (msg.m_Type == TxType::AtomicSwap)
        {
            // we don't create swap from SBBS message
            return;
        }

        // GetParameter reads any sub-tx, but only default sub-tx values reach the tx; Simple and DEX txs never carry another one
        for (const auto& p : msg.m_Parameters)
        {
            if (p.first == TxParameterID::SubTxIndex)
            {
                BEAM_LOG_WARNING() << msg.m_TxID << " Incoming tx with sub-tx parameters rejected";
                return;
            }
        }

        bool isSender = false;
        if (!msg.GetParameter(TxParameterID::IsSender, isSender))
        {
            // we don't accept txs without IsSender
            return;
        }

        if (isSender)
        {
            // TxParameterID::IsSender should always be false if it is coming from outside
            BEAM_LOG_WARNING() << msg.m_TxID << " Incoming tx that makes us the sender rejected";
            return;
        }

        if (msg.m_Type == TxType::DexSimpleSwap)
        {
            if (m_ssHandler == nullptr)
            {
                BEAM_LOG_WARNING() << "DexSimpleSwap tx is received but feature is disabled. " << "TxID is " << msg.m_TxID;
                return;
            }
        }

        if (msg.m_Type == TxType::Simple || msg.m_Type == TxType::DexSimpleSwap)
        {
            if (msg.m_Type == TxType::DexSimpleSwap)
            {
                if (!m_ssHandler->acceptIncomingDexSS(msg))
                {
                    // TODO:DEX create tx and fail tx with rejected reason to make the peer not wait
                    BEAM_LOG_INFO() << "Incoming DexSimpleSwap rejected. "
                               << "DexOrderID [" << msg.GetParameterOrDefault<DexOrderID>(TxParameterID::ExternalDexOrderID) << "] "
                               << "TxID " << msg.m_TxID;
                    return;
                }
            }

            auto pTx = ConstructTransaction(msg.m_TxID, msg.m_Type);
            if (!pTx)
            {
                return;
            }

            pTx->SetParameter(TxParameterID::TransactionType, msg.m_Type, true);
            pTx->SetParameter(TxParameterID::CreateTime, getTimestamp(),true);
            pTx->SetParameter(TxParameterID::MyAddr, myID, false);
            pTx->SetParameter(TxParameterID::PeerAddr, msg.m_From, false);
            pTx->SetParameter(TxParameterID::IsInitiator, false, false);
            pTx->SetParameter(TxParameterID::IsSender, false, false); // set locally, peers can't set it
            pTx->SetParameter(TxParameterID::Status, TxStatus::Pending, true);

            auto address = m_WalletDB->getAddress(myID);
            if (address.is_initialized())
            {
                ByteBuffer message(address->m_label.begin(), address->m_label.end());
                pTx->SetParameter(TxParameterID::Message, message);
            }

            MakeTransactionActive(pTx);
            ApplyTransactionParameters(pTx, msg.m_Parameters, false);
            UpdateTransaction(pTx);

            if (msg.m_Type == TxType::DexSimpleSwap)
            {
                m_ssHandler->onDexTxCreated(msg, pTx);
                BEAM_LOG_INFO() << "Incoming DexSimpleSwap accepted. TxID is " << msg.m_TxID;
            }
        }
        else
        {
            BEAM_LOG_WARNING() << "Unsupported TX Type requested via SBBS: "
                          << " type: " << static_cast<unsigned int>(msg.m_Type)
                          << " txid: " << msg.m_TxID;
            return;
        }
    }

    BaseTransaction::Ptr Wallet::ConstructTransaction(const TxID& id, TxType type)
    {
        auto it = m_TxCreators.find(type);
        if (it == m_TxCreators.end())
        {
            BEAM_LOG_WARNING() << id << " Unsupported type of transaction: " << static_cast<int>(type);
            return wallet::BaseTransaction::Ptr();
        }

        return it->second->Create(BaseTransaction::TxContext(*this, *this, id));
    }

    BaseTransaction::Ptr Wallet::ConstructTransactionFromParameters(const TxParameters& parameters)
    {
        auto type = parameters.GetParameter<TxType>(TxParameterID::TransactionType);
        if (!type)
        {
            return BaseTransaction::Ptr();
        }

        auto it = m_TxCreators.find(*type);
        if (it == m_TxCreators.end())
        {
            BEAM_LOG_ERROR() << *parameters.GetTxID() << " Unsupported type of transaction: " << static_cast<int>(*type);
            return BaseTransaction::Ptr();
        }

        auto completedParameters = it->second->CheckAndCompleteParameters(parameters);

        auto newTx = it->second->Create(BaseTransaction::TxContext(*this, *this, *parameters.GetTxID()));
        ApplyTransactionParameters(newTx, completedParameters.Pack(), true);
        return newTx;
    }

    void Wallet::MakeTransactionActive(BaseTransaction::Ptr tx)
    {
        m_ActiveTransactions.emplace(tx->GetTxID(), tx);
    }

    void Wallet::markAppNotificationAsRead(const TxID& id)
    {
        auto it = m_ActiveTransactions.find(id);
        if (m_ActiveTransactions.end() == it)
        {
            return;
        }

        if (!storage::setTxParameter(*m_WalletDB, id, TxParameterID::IsContractNotificationMarkedAsRead, true, true))
            BEAM_LOG_ERROR() << "Can't mark application notification as read.";
    }
}
