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
    namespace
    {
        constexpr char kIsTreasuryHandled[] = "IsTreasuryHandled";
        constexpr char kNeedToRequestBodies[] = "NeedToRequestBodies";
        constexpr char s_szNextEvt[] = "NextUtxoEvent"; // any event, not just UTXO. The name is for historical reasons

        Height DeduceTxProofHeightImpl(const IWalletDB& walletDB, const TxID& txID, TxType type)
        {
            Height height = 0;

            if (type == TxType::AssetInfo)
            {
                storage::getTxParameter(walletDB, txID, TxParameterID::AssetConfirmedHeight, height);
            }
            else
            {
                storage::getTxParameter(walletDB, txID, TxParameterID::KernelProofHeight, height);
            }

            return height;
        }
    }

    namespace storage
    {
        bool getTxParameter(const IWalletDB& db, const TxID& txID, SubTxID subTxID, TxParameterID paramID, ECC::Scalar::Native& value)
        {
            ECC::Scalar s;
            if (getTxParameter(db, txID, subTxID, paramID, s))
            {
                value.Import(s);
                return true;
            }
            return false;
        }

        bool getTxParameter(const IWalletDB& db, const TxID& txID, SubTxID subTxID, TxParameterID paramID, ByteBuffer& value)
        {
            return db.getTxParameter(txID, subTxID, paramID, value);
        }

        bool getTxParameter(const IWalletDB& db, const TxID& txID, TxParameterID paramID, ByteBuffer& value)
        {
            return getTxParameter(db, txID, kDefaultSubTxID, paramID, value);
        }

        bool getTxParameter(const IWalletDB& db, const TxID& txID, TxParameterID paramID, ECC::Scalar::Native& value)
        {
            return getTxParameter(db, txID, kDefaultSubTxID, paramID, value);
        }

        bool setTxParameter(IWalletDB& db, const TxID& txID, SubTxID subTxID, TxParameterID paramID,
            const ECC::Scalar::Native& value, bool shouldNotifyAboutChanges)
        {
            ECC::Scalar s;
            value.Export(s);
            return setTxParameter(db, txID, subTxID, paramID, s, shouldNotifyAboutChanges);
        }

        bool setTxParameter(IWalletDB& db, const TxID& txID, SubTxID subTxID, TxParameterID paramID,
            const ByteBuffer& value, bool shouldNotifyAboutChanges)
        {
            return db.setTxParameter(txID, subTxID, paramID, value, shouldNotifyAboutChanges);
        }

        bool setTxParameter(IWalletDB& db, const TxID& txID, TxParameterID paramID, const ECC::Scalar::Native& value, bool shouldNotifyAboutChanges)
        {
            return setTxParameter(db, txID, kDefaultSubTxID, paramID, value, shouldNotifyAboutChanges);
        }

        bool setTxParameter(IWalletDB& db, const TxID& txID, TxParameterID paramID, const ByteBuffer& value, bool shouldNotifyAboutChanges)
        {
            return setTxParameter(db, txID, kDefaultSubTxID, paramID, value, shouldNotifyAboutChanges);
        }

        bool changeAddressExpiration(IWalletDB& walletDB, const WalletID& walletID, WalletAddress::ExpirationStatus status)
        {
            if (walletID != Zero)
            {
                auto address = walletDB.getAddress(walletID);

                if (!address.is_initialized())
                {
                    BEAM_LOG_INFO() << "Address " << to_string(walletID) << "is absent in wallet";
                    return false;
                }

                address->setExpirationStatus(status);
                walletDB.saveAddress(*address);
            }
            else
            {
                for (auto& address : walletDB.getAddresses(true))
                {
                    address.setExpirationStatus(status);
                    walletDB.saveAddress(address);
                }
            }
            return true;
        }

        bool Totals::AssetTotals::IsNZ() const
        {
            return Avail != Zero || Maturing != Zero || MaturingShielded != Zero || Incoming != Zero ||
                   ReceivingChange != Zero || ReceivingIncoming != Zero || Outgoing != Zero ||
                   OutgoingShielded != Zero || AvailShielded != Zero || IncomingShielded != Zero;
        }

        Totals::Totals()
        {
            allTotals[Zero] = AssetTotals();
        }

        Totals::Totals(IWalletDB& db, bool nzOnly)
        {
            allTotals[Zero] = AssetTotals();
            Init(db, nzOnly);
        }

        void Totals::Init(IWalletDB& walletDB, bool nzOnly)
        {
            auto getTotalsRef = [this](Asset::ID assetId) -> AssetTotals& {
                if (allTotals.find(assetId) == allTotals.end()) {
                    allTotals[assetId] = AssetTotals();
                    allTotals[assetId].AssetId = assetId;
                }
                return allTotals[assetId];
            };

            walletDB.visitCoins([getTotalsRef] (const Coin& c) -> bool
            {
                auto& totals = getTotalsRef(c.m_ID.m_AssetID);
                totals.MinCoinHeightMW = totals.MinCoinHeightMW == 0 ? c.m_confirmHeight :
                                         std::min(c.m_confirmHeight, totals.MinCoinHeightMW);

                const auto value = MultiWord::From(c.m_ID.m_Value);
                switch (c.m_status)
                {
                case Coin::Status::Available:
                    totals.Avail   += value;
                    totals.Unspent += value;
                    switch (c.m_ID.m_Type)
                    {
                    case Key::Type::Coinbase:
                        assert(!c.isAsset());
                        totals.AvailCoinbase += value;
                        break;
                    case Key::Type::Comission:
                        assert(!c.isAsset());
                        totals.AvailFee += value;
                        break;
                    default: // suppress warning
                        break;
                    }
                    break;

                case Coin::Status::Maturing:
                    totals.Maturing += value;
                    totals.Unspent += value;
                    break;

                case Coin::Status::Incoming:
                    totals.Incoming += value;
                    if (c.m_ID.m_Type == Key::Type::Change)
                    {
                        totals.ReceivingChange += value;
                    }
                    else
                    {
                        totals.ReceivingIncoming += value;
                    }
                    break;

                case Coin::Status::Outgoing:
                    totals.Outgoing += value;
                    break;

                case Coin::Status::Unavailable:
                    totals.Unavail += value;
                    break;

                default: // suppress warning
                    break;
                }

                switch (c.m_ID.m_Type)
                {
                case Key::Type::Coinbase:
                    assert(!c.isAsset());
                    totals.Coinbase += value;
                    break;
                case Key::Type::Comission:
                    assert(!c.isAsset());
                    totals.Fee += value;
                    break;
                default: // suppress warning
                    break;
                }
                return true;
            });

            walletDB.visitShieldedCoins([getTotalsRef](const ShieldedCoin& c) -> bool {
                auto& totals = getTotalsRef(c.m_CoinID.m_AssetID);
                totals.MinCoinHeightShielded = totals.MinCoinHeightShielded == 0 ? c.m_confirmHeight :
                                               std::min(c.m_confirmHeight, totals.MinCoinHeightShielded);

                const auto value = MultiWord::From(c.m_CoinID.m_Value);
                switch(c.m_Status) {
                    case ShieldedCoin::Status::Available:
                        totals.AvailShielded += value;
                        totals.UnspentShielded += value;
                        break;
                    case ShieldedCoin::Status::Maturing:
                        totals.MaturingShielded += value;
                        totals.UnspentShielded += value;
                        break;
                    case ShieldedCoin::Status::Unavailable:
                        totals.UnavailShielded += value;
                        break;
                    case ShieldedCoin::Status::Outgoing:
                        totals.OutgoingShielded += value;
                        break;
                    case ShieldedCoin::Status::Incoming:
                        totals.IncomingShielded += value;
                        break;
                    case ShieldedCoin::Status::Spent:
                        break; // this is not necessary
                    default:
                        assert(false); // should never happen
                }
                return true;
            });

             walletDB.visitAssets([this](const WalletAsset& asset) -> bool {
                // we also add owned assets to totals even if there are no coins for owned assets
                if(!HasTotals(asset.m_ID) && asset.m_IsOwned)
                {
                    allTotals[asset.m_ID] = AssetTotals();
                    allTotals[asset.m_ID].AssetId = asset.m_ID;
                }
                return true;
             });

             for (auto iter = allTotals.begin(); iter != allTotals.end();)
             {
                 // We ALWAYS count beam as NZ-asset
                 if (iter->first == Asset::s_BeamID || iter->second.IsNZ())
                 {
                     assetsNZ.insert(iter->first);
                     ++iter;
                 }
                 else
                 {
                    if (nzOnly)
                    {
                        iter = allTotals.erase(iter);
                    }
                    else
                    {
                        ++iter;
                    }
                 }
             }
        }

        Totals::AssetTotals Totals::GetTotals(Asset::ID assetId) const
        {
            if(allTotals.find(assetId) == allTotals.end())
            {
                AssetTotals result;
                result.AssetId = assetId;
                return result;
            }
            return allTotals[assetId];
        }

        const std::set<Asset::ID>& Totals::GetAssetsNZ() const
        {
            return assetsNZ;
        }

        const std::map<Asset::ID, Totals::AssetTotals>& Totals::GetAllTotals() const
        {
            return allTotals;
        }

        bool Totals::HasTotals(Asset::ID assetId) const
        {
            return allTotals.find(assetId) != allTotals.end();
        }

        bool IsOngoingTx(const IWalletDB& walletDB, const boost::optional<TxID>& txID)
        {
            if (!txID)
                return false;

            TxStatus txStatus;
            if (getTxParameter(walletDB, txID.get(), TxParameterID::Status, txStatus))
            {
                switch (txStatus)
                {
                case TxStatus::Canceled:
                case TxStatus::Failed:
                case TxStatus::Completed:
                    break;

                default:
                    return true;
                }
            }

            return false;
        }

        bool IsConsumeTx(const IWalletDB& walletDB, const boost::optional<TxID>& txID)
        {
            if (!txID) return false;

            TxType txType;
            if (getTxParameter(walletDB, txID.get(), TxParameterID::TransactionType, txType))
            {
                return txType == TxType::AssetConsume;
            }

            return false;
        }

        void DeduceStatus(const IWalletDB& walletDB, Coin& c, Height hTop)
        {
            if (c.m_spentHeight != MaxHeight)
            {
                if (c.isAsset() && IsConsumeTx(walletDB, c.m_spentTxId))
                {
                    c.m_status = Coin::Status::Consumed;
                }
                else
                {
                    c.m_status = Coin::Status::Spent;
                }
                return;
            }

            if (c.m_confirmHeight != MaxHeight)
            {
                uint32_t minConfirmations = 0;
                if (c.m_createTxId &&
                    getTxParameter(walletDB, c.m_createTxId.get(), TxParameterID::MinConfirmations, minConfirmations))
                {
                    c.setOffset(minConfirmations);
                }
                
                if (hTop < c.m_maturity + minConfirmations)
                {
                    c.m_status = Coin::Status::Maturing;
                    return;
                }

                if (IsOngoingTx(walletDB, c.m_spentTxId))
                {
                    c.m_status = Coin::Status::Outgoing;
                    return;
                }

                c.m_status = Coin::Status::Available;
                return;
            }

            if (IsOngoingTx(walletDB, c.m_createTxId))
            {
                c.m_status = Coin::Status::Incoming;
                return;
            }

            c.m_status = Coin::Status::Unavailable;
        }

        void DeduceStatus(const IWalletDB& walletDB, ShieldedCoin& c, Height hTop)
        {
            if (c.m_spentHeight != MaxHeight)
            {
                if (c.isAsset() && IsConsumeTx(walletDB, c.m_spentTxId))
                {
                    c.m_Status = ShieldedCoin::Status::Consumed;
                }
                else
                {
                    c.m_Status = ShieldedCoin::Status::Spent;
                }
                return;
            }

            if (c.m_confirmHeight != MaxHeight)
            {
                const auto* packedMessage = ShieldedTxo::User::ToPackedMessage(c.m_CoinID.m_User);
                uint32_t mpAnonymitySet = packedMessage->m_MaxPrivacyMinAnonymitySet;
                if (mpAnonymitySet)
                {
                    Height timeLimit = walletDB.get_MaxPrivacyLockTimeLimitHours();

                    if (!timeLimit || c.m_confirmHeight + timeLimit * 60 > hTop)
                    {
                        ShieldedCoin::UnlinkStatus unlinkStatus;
                        unlinkStatus.Init(c, walletDB.get_ShieldedOuts());
                        if (unlinkStatus.m_Progress < 100 * mpAnonymitySet / beam::MaxPrivacyAnonimitySetFractionsCount)
                        {
                            c.m_Status = ShieldedCoin::Status::Maturing;
                            return;
                        }
                    }
                }

                if (hTop < c.m_confirmHeight)
                {
                    c.m_Status = ShieldedCoin::Status::Maturing;
                    return;
                }

                if(storage::IsOngoingTx(walletDB, c.m_spentTxId))
                {
                    c.m_Status = ShieldedCoin::Status::Outgoing;
                    return;
                }

                c.m_Status = ShieldedCoin::Status::Available;
                return;
            }

            if (storage::IsOngoingTx(walletDB, c.m_createTxId))
            {
                c.m_Status = ShieldedCoin::Status::Incoming;
                return;
            }

            c.m_Status = ShieldedCoin::Status::Unavailable;
        }

        Height DeduceTxProofHeight(const IWalletDB& walletDB, const TxDescription &tx)
        {
            return DeduceTxProofHeightImpl(walletDB, tx.m_txId, tx.m_txType);
        }

        Height DeduceTxDisplayHeight(const IWalletDB& walletDB, const TxDescription &tx)
        {
            auto height = DeduceTxProofHeight(walletDB, tx);
            if (height == 0)
            {
                storage::getTxParameter(walletDB, tx.m_txId, TxParameterID::KernelUnconfirmedHeight, height);
                if (height == 0)
                {
                    storage::getTxParameter(walletDB, tx.m_txId, TxParameterID::AssetUnconfirmedHeight, height);
                    if (height == 0)
                    {
                        storage::getTxParameter(walletDB, tx.m_txId, TxParameterID::MinHeight, height);
                    }
                }
            }
            return height;
        }

        bool isTreasuryHandled(const IWalletDB& db)
        {
            bool res = false;
            storage::getVar(db, kIsTreasuryHandled, res);
            return res;
        }

        void setTreasuryHandled(IWalletDB& db,  bool value)
        {
            storage::setVar(db, kIsTreasuryHandled, value);
        }

        bool needToRequestBodies(const IWalletDB& db)
        {
            bool res = false;
            storage::getVar(db, kNeedToRequestBodies, res);
            return res;
        }

        void setNeedToRequestBodies(IWalletDB& db, bool value)
        {
            storage::setVar(db, kNeedToRequestBodies, value);
        }

        Height getNextEventHeight(const IWalletDB& db)
        {
            uintBigFor<Height>::Type var;
            if (!storage::getVar(db, s_szNextEvt, var))
                return 0;

            Height h;
            var.Export(h);
            return h;
        }

        void setNextEventHeight(IWalletDB& db, Height value)
        {
            storage::setVar(db, s_szNextEvt, uintBigFor<Height>::Type(value));
        }

        void SaveVouchers(IWalletDB& walletDB, const ShieldedVoucherList& vouchers, const WalletID& walletID)
        {
            try
            {
                for (const auto& v : vouchers)
                {
                    walletDB.saveVoucher(v, walletID, true);
                }
            }
            catch (const DatabaseException&)
            {
                // probably, we are trying to insert an existing voucher, ingnore
            }
        }

        namespace
        {
            void LogSqliteError(void* pArg, int iErrCode, const char* zMsg)
            {
                BEAM_LOG_ERROR() << "(" << iErrCode << ") " << zMsg;
            }
        }

        void HookErrors()
        {
            sqlite3_config(SQLITE_CONFIG_LOG, LogSqliteError, nullptr);
        }

        bool isMyAddress(
            const std::vector<WalletAddress>& myAddresses, const WalletID& wid)
        {
            auto myAddrIt = std::find_if(
                myAddresses.begin(),
                myAddresses.end(),
                [&wid] (const WalletAddress& addr)
                {
                    return wid == addr.m_BbsAddr;
                });
            return myAddrIt != myAddresses.end();
        }
    }
}
