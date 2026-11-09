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
    // Implementation of the INegotiatorGateway::send_tx_params
    void Wallet::send_tx_params(const WalletID& peerID, const SetTxParameter& msg)
    {
        for (auto& endpoint : m_MessageEndpoints)
        {
            if ((endpoint.get() != m_pBlockedForTxs) || IsSpecialMsg(msg))
                endpoint->Send(peerID, msg);
        }
    }

    void Wallet::Listen(const WalletID& wid, const ECC::Scalar::Native& sk, IHandler* pH)
    {
        for (auto& p : m_MessageEndpoints)
            p->Listen(wid, sk, pH);
    }

    void Wallet::Unlisten(const WalletID& wid, IHandler* pH)
    {
        for (auto& p : m_MessageEndpoints)
            p->Unlisten(wid, pH);
    }

    void Wallet::Send(const WalletID& peerID, const Blob& b)
    {
        for (auto& p : m_MessageEndpoints)
            Cast::Down<IRawCommGateway>(*p).Send(peerID, b);
    }

    void Wallet::SendSpecialMsg(const WalletID& peerID, SetTxParameter& msg)
    {
        memset0(&msg.m_TxID.front(), msg.m_TxID.size());
        send_tx_params(peerID, msg);
    }

    void Wallet::OnSpecialMsg(const WalletID& myID, const SetTxParameter& msg)
    {
        switch (msg.m_Type)
        {
        case TxType::VoucherRequest:
            {
                BEAM_LOG_DEBUG() << "Got voucher request!";
                auto pKeyKeeper = m_WalletDB->get_KeyKeeper();
                if (!pKeyKeeper                                         // We can generate the ticket with OwnerKey, but can't sign it.
                 || !CanDetectCoins())                                  // The wallet has no ability to recognize received shielded coin
                {
                    BEAM_LOG_ERROR() << "Cannot send voucher" << TRACE(pKeyKeeper) << TRACE(CanDetectCoins());
                    FailVoucherRequest(msg.m_From, myID);
                    return; 
                }

                uint32_t nCount = 0;
                msg.GetParameter((TxParameterID) 0, nCount);

                if (!nCount)
                {
                    BEAM_LOG_ERROR() << "Cannot send voucher" << TRACE(nCount);
                    FailVoucherRequest(msg.m_From, myID);
                    return; //?!
                }

                auto address = m_WalletDB->getAddress(myID);
                if (!address.is_initialized() || !address->m_OwnID)
                    return;

                auto res = GenerateVoucherList(pKeyKeeper, address->m_OwnID, nCount);
                BEAM_LOG_DEBUG() << "Generated voucher list, size: " << res.size();
                SetTxParameter msgOut;
                msgOut.m_Type = TxType::VoucherResponse;
                msgOut.m_From = myID;
                msgOut.AddParameter(TxParameterID::ShieldedVoucherList, res);

                SendSpecialMsg(msg.m_From, msgOut);

            }
            break;

        case TxType::VoucherResponse:
            {
                std::vector<ShieldedTxo::Voucher> res;
                msg.GetParameter(TxParameterID::ShieldedVoucherList, res);
                if (res.empty())
                {
                    BEAM_LOG_WARNING() << "Received an empty voucher list";
                    FailTxWaitingForVouchers(msg.m_From);
                    return;
                }

                auto address = m_WalletDB->getAddress(msg.m_From);
                if (!address.is_initialized())
                {
                    BEAM_LOG_WARNING() << "Received vouchers for unknown address: " << beam::to_string(msg.m_From);
                    FailTxWaitingForVouchers(msg.m_From);
                    return;
                }

                if (!IsValidVoucherList(res, address->m_Endpoint))
                {
                    BEAM_LOG_WARNING() << "Invalid voucher list received";
                    FailTxWaitingForVouchers(msg.m_From);
                    return;
                }

                OnVouchersFrom(*address, myID, std::move(res));

            }
            break;

            case TxType::InstantSbbsMessage:
            {
                auto receiver = m_WalletDB->getAddress(myID);
                if (!receiver.is_initialized() || !receiver->m_OwnID)
                    return;

                size_t message_size = 0;
                msg.GetParameter((TxParameterID) 0, message_size);

                Timestamp timestamp;
                msg.GetParameter((TxParameterID) 1, timestamp);

                ByteBuffer message_bb;
                message_bb.reserve(message_size);
                msg.GetParameter((TxParameterID) 2, message_bb);
                std::string message(message_bb.begin(), message_bb.end());

                m_WalletDB->storeIM(timestamp, msg.m_From, myID, message, true, false);
            }
            break;

        default: // suppress watning
            break;
        }
    }

    bool Wallet::IsSpecialMsg(const SetTxParameter& msg)
    {
        return memis0(&msg.m_TxID.front(), msg.m_TxID.size());
    }

    void Wallet::OnWalletMessage(const WalletID& myID, const SetTxParameter& msg, IWalletMessageEndpoint* pFrom)
    {
        if (IsSpecialMsg(msg))
        {
            // special command/request
            try
            {
                OnSpecialMsg(myID, msg);
            }
            catch (const std::exception& exc)
            {
                BEAM_LOG_WARNING() << "Special msg failed: " << exc.what();
            }
        }
        else
        {
            if (!(pFrom && (pFrom == m_pBlockedForTxs)))
                OnTransactionMsg(myID, msg);
        }
    }

    void Wallet::ProcessStoredMessages()
    {
        if (m_MessageEndpoints.empty() || m_StoredMessagesProcessed)
        {
            return;
        }
        auto messages = m_WalletDB->getWalletMessages();
        for (auto& message : messages)
        {
            // Every endpoint gets its own copy: moving would hand the second and later
            // endpoints an empty buffer.
            for (auto& endpoint : m_MessageEndpoints)
            {
                endpoint->SendRawMessage(message.m_PeerID, ByteBuffer(message.m_Message));
            }
            m_WalletDB->deleteWalletMessage(message.m_ID);
        }
        m_StoredMessagesProcessed = true;
    }

    void Wallet::sendInstantSbbsMessage(beam::Timestamp timestamp, const WalletID& peerID, const WalletID& myID, ByteBuffer&& message)
    {
        SetTxParameter msg;
        msg.m_From = myID;
        msg.m_Type = TxType::InstantSbbsMessage;

        msg.AddParameter((TxParameterID) 0, message.size());
        msg.AddParameter((TxParameterID) 1, timestamp);
        msg.AddParameter((TxParameterID) 2, message);

        SendSpecialMsg(peerID, msg);
    }
}
