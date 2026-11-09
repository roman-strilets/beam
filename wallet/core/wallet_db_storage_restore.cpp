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

#include "wallet_db_common.h"

namespace beam::wallet
{
    namespace storage
    {
        namespace
        {
            Timestamp RestoreCreationTime(const Block::SystemState::Full& tip, Height confirmHeight)
            {
                Timestamp ts = tip.m_TimeStamp;
                auto hTip = tip.get_Height();
                if (hTip > confirmHeight)
                {
                    auto delta = (hTip - confirmHeight);
                    ts -= delta * Rules::get().DA.get_Target_s();
                }
                else if (hTip < confirmHeight)
                {
                    auto delta = confirmHeight - hTip;
                    ts += delta * Rules::get().DA.get_Target_s();
                }
                return ts;
            }

            bool IsOwnSelfPushTx(const IWalletDB& db, const TxID& txID)
            {
                TxType type = TxType::Simple;
                bool isSelfTx = false;
                return
                    getTxParameter(db, txID, TxParameterID::TransactionType, type) &&
                    (TxType::PushTransaction == type) &&
                    getTxParameter(db, txID, TxParameterID::IsSelfTx, isSelfTx) &&
                    isSelfTx;
            }

            // The TxID packed into a shielded coin is chosen by its sender. It's used only if it names our own
            // self-push tx (such a TxID never leaves this wallet), otherwise the receive record gets an ID derived from the coin.
            TxID get_ShieldedCoinTxID(const IWalletDB& db, const ShieldedCoin& coin)
            {
                if (coin.m_createTxId)
                    return *coin.m_createTxId; // assigned locally

                TxID txID;
                const auto* message = ShieldedTxo::User::ToPackedMessage(coin.m_CoinID.m_User);
                std::copy_n(message->m_TxID.m_pData, txID.size(), txID.begin());
                if (IsOwnSelfPushTx(db, txID))
                    return txID;

                const auto& key = coin.m_CoinID.m_Key;
                ECC::Hash::Value hv;
                ECC::Hash::Processor()
                    << "shielded.rcv.txid"
                    << key.m_kSerG
                    << key.m_nIdx
                    << key.m_IsCreatedByViewer
                    >> hv;

                std::copy_n(hv.m_pData, txID.size(), txID.begin());
                return txID;
            }
        }

        void restoreTransactionFromShieldedCoin(IWalletDB& db, ShieldedCoin& coin, INegotiatorGateway& gateway)
        {
            // add virtual transaction for receiver
            beam::Block::SystemState::Full tip;
            db.get_History().get_Tip(tip);
            storage::DeduceStatus(db, coin, tip.get_Height());

            if (coin.m_Status != ShieldedCoin::Status::Available &&
                coin.m_Status != ShieldedCoin::Status::Maturing &&
                coin.m_Status != ShieldedCoin::Status::Spent)
            {
                return;
            }

            TxID txID = get_ShieldedCoinTxID(db, coin);
            if (!coin.m_createTxId)
            {
                coin.m_createTxId = txID;
                db.saveShieldedCoin(coin);
            }

            if (db.getTx(txID))
            {
                // our own push tx, or the receive record made earlier. Nothing from the coin may modify it
                TxType type = TxType::Simple;
                if (getTxParameter(db, txID, TxParameterID::TransactionType, type) && (TxType::PushTransaction == type))
                    storage::setTxParameter(db, txID, TxParameterID::KernelProofHeight, coin.m_confirmHeight, true);
                return;
            }

            // A new receive record. Only the sender endpoint and the advisory max-privacy hint are taken from the coin
            const auto* message = ShieldedTxo::User::ToPackedMessage(coin.m_CoinID.m_User);

            TxAddressType addressType = TxAddressType::Offline;
            if (message->m_MaxPrivacyMinAnonymitySet)
            {
                addressType = TxAddressType::MaxPrivacy;
            }
            else if (!coin.m_CoinID.m_Key.m_IsCreatedByViewer)
            {
                addressType = TxAddressType::PublicOffline;
            }

            {
                auto params = CreateTransactionParameters(TxType::PushTransaction, txID)
                    .SetParameter(TxParameterID::AddressType, addressType)
                    .SetParameter(TxParameterID::IsSelfTx, false)
                    .SetParameter(TxParameterID::Status, TxStatus::Completed)
                    .SetParameter(TxParameterID::Amount, coin.m_CoinID.m_Value)
                    .SetParameter(TxParameterID::AssetID, coin.m_CoinID.m_AssetID)
                    .SetParameter(TxParameterID::IsSender, false)
                    .SetParameter(TxParameterID::CreateTime, RestoreCreationTime(tip, coin.m_confirmHeight))
                    .SetParameter(TxParameterID::KernelID, Merkle::Hash(Zero));

                if (coin.m_CoinID.m_User.m_Sender != Zero) // This is optional. Sender may prefer to send anonymously
                    params.SetParameter(TxParameterID::PeerEndpoint, coin.m_CoinID.m_User.m_Sender);

                const auto assetId = coin.m_CoinID.m_AssetID;
                if (assetId != Asset::s_BeamID)
                {
                    if (const auto oinfo = db.findAsset(assetId))
                    {
                        WalletAsset info(*oinfo);
                        if (info.IsExpired(db))
                        {
                            gateway.confirm_asset(txID, assetId);
                        }
                        else
                        {
                            params.SetParameter(TxParameterID::AssetInfoFull, static_cast<Asset::Full>(*oinfo))
                                    .SetParameter(TxParameterID::AssetConfirmedHeight, oinfo->m_RefreshHeight);
                        }
                    }
                    else
                    {
                        gateway.confirm_asset(txID, assetId);
                    }
                }

                if (message->m_MaxPrivacyMinAnonymitySet)
                {
                    params.SetParameter(TxParameterID::MaxPrivacyMinAnonimitySet, message->m_MaxPrivacyMinAnonymitySet);
                }

                const auto packed = params.Pack();
                for (const auto& p : packed)
                {
                    storage::setTxParameter(db, *params.GetTxID(), p.first, p.second, true);
                }
            }
        }
    }
}
