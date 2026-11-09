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
        struct VoucherFlags
        {
            static constexpr uint16_t Default  = 0;
            static constexpr uint16_t Preserve = 1;
            static constexpr uint16_t Deleted  = 2;
        };
    }

    struct WalletDB::ShieldedStatusCtx
    {
        Height m_hTip;

        ShieldedStatusCtx(const WalletDB& db)
        {
            m_hTip = db.getCurrentHeight();
        }
    };

    void WalletDB::visitShieldedCoins(std::function<bool(const ShieldedCoin& info)> func) const
    {
        ShieldedStatusCtx ssc(*this);

        sqlite::Statement stm(this, "SELECT " SHIELDED_COIN_FIELDS " FROM " SHIELDED_COINS_NAME " ORDER BY ID;");
        while (stm.step())
        {
            ShieldedCoin coin;

            int colIdx = 0;
            ENUM_SHIELDED_COIN_FIELDS(STM_GET_LIST, NOSEP, coin);

            storage::DeduceStatus(*this, coin, ssc.m_hTip);
            if (!func(coin))
                break;
        }
    }

    void WalletDB::visitShieldedCoinsUnspent(const std::function<bool(const ShieldedCoin& info)>& func) const
    {
        ShieldedStatusCtx ssc(*this);

        sqlite::Statement stm(this, "SELECT " SHIELDED_COIN_FIELDS " FROM " SHIELDED_COINS_NAME " WHERE spentHeight <0 ORDER BY ID;");
        while (stm.step())
        {
            ShieldedCoin coin;

            int colIdx = 0;
            ENUM_SHIELDED_COIN_FIELDS(STM_GET_LIST, NOSEP, coin);

            storage::DeduceStatus(*this, coin, ssc.m_hTip);
            if (!func(coin))
                break;
        }
    }

    std::vector<ShieldedCoin> WalletDB::getAllShieldedCoins() const
    {
        std::vector<ShieldedCoin> coins;
        visitShieldedCoins([&coins] (const ShieldedCoin& coin) -> bool {
            coins.push_back(coin);
            return true;
        });
        return coins;
    }

    std::vector<ShieldedCoin> WalletDB::getShieldedCoins(Asset::ID assetId) const
    {
        std::vector<ShieldedCoin> coins;
        visitShieldedCoins([&coins, assetId] (const ShieldedCoin& coin) -> bool {
            if (coin.m_CoinID.m_AssetID == assetId)
            {
                coins.push_back(coin);
            }
            return true;
        });
        return coins;
    }

    boost::optional<ShieldedCoin> WalletDB::getShieldedCoin(const TxID& txId) const
    {
        sqlite::Statement stm(this, "SELECT " SHIELDED_COIN_FIELDS " FROM " SHIELDED_COINS_NAME " WHERE createTxId=?1 OR spentTxId=?1;");
        stm.bind(1, txId);

        if (stm.step())
        {
            ShieldedStatusCtx ssc(*this);
            ShieldedCoin coin;
            int colIdx = 0;
            ENUM_SHIELDED_COIN_FIELDS(STM_GET_LIST, NOSEP, coin);
            storage::DeduceStatus(*this, coin, ssc.m_hTip);
            return coin;
        }

        return {};
    }

    boost::optional<ShieldedCoin> WalletDB::getShieldedCoin(TxoID id) const
    {
        sqlite::Statement stm(this, "SELECT " SHIELDED_COIN_FIELDS " FROM " SHIELDED_COINS_NAME " WHERE ID = ?;");
        stm.bind(1, id);

        if (stm.step())
        {
            ShieldedStatusCtx ssc(*this);
            ShieldedCoin coin;
            int colIdx = 0;
            ENUM_SHIELDED_COIN_FIELDS(STM_GET_LIST, NOSEP, coin);
            storage::DeduceStatus(*this, coin, ssc.m_hTip);
            return coin;
        }

        return {};
    }

    boost::optional<ShieldedCoin> WalletDB::getShieldedCoin(const ShieldedTxo::BaseKey& key) const
    {
        sqlite::Statement stm(this, "SELECT " SHIELDED_COIN_FIELDS " FROM " SHIELDED_COINS_NAME " WHERE Key = ?;");
        stm.bind(1, key);

        if (stm.step())
        {
            ShieldedStatusCtx ssc(*this);
            ShieldedCoin coin;
            int colIdx = 0;
            ENUM_SHIELDED_COIN_FIELDS(STM_GET_LIST, NOSEP, coin);
            storage::DeduceStatus(*this, coin, ssc.m_hTip);
            return coin;
        }
        return {};
    }

    void WalletDB::clearShieldedCoins()
    {
        sqlite::Statement stm(this, "DELETE FROM " SHIELDED_COINS_NAME ";");
        stm.step();
        notifyShieldedCoinsChanged(ChangeAction::Reset, {});
    }

    void WalletDB::saveShieldedCoin(const ShieldedCoin& shieldedCoin)
    {
        saveShieldedCoinRaw(shieldedCoin);
    }

    void WalletDB::rollbackConfirmedShieldedUtxo(Height minHeight)
    {
        ShieldedStatusCtx ssc(*this);
        vector<ShieldedCoin> changedCoins;

        // 1. Undo 'spent' where necessary
        const char* req = "SELECT " ENUM_SHIELDED_COIN_FIELDS(LIST, COMMA, ) " FROM " SHIELDED_COINS_NAME " WHERE spentHeight > ?1;";
        sqlite::Statement stm(this, req);
        stm.bind(1, minHeight);

        while (stm.step())
        {
            ShieldedCoin& coin = changedCoins.emplace_back();
            int colIdx = 0;
            ENUM_SHIELDED_COIN_FIELDS(STM_GET_LIST, NOSEP, coin);

            coin.m_spentHeight = MaxHeight;
            updateShieldedCoinRaw(coin);

            storage::DeduceStatus(*this, coin, ssc.m_hTip);
            changedCoins.push_back(std::move(coin));
        }

        if (!changedCoins.empty())
        {
            notifyShieldedCoinsChanged(ChangeAction::Updated, changedCoins);
            changedCoins.clear();
        }

        // 2. For unspent - remove confirmHeight where applicable
        visitShieldedCoinsUnspent([&changedCoins, minHeight](const ShieldedCoin& coin) -> bool {
            if ((MaxHeight != coin.m_confirmHeight) && (coin.m_confirmHeight > minHeight))
                changedCoins.push_back(coin);
            return true;
            });

        if (!changedCoins.empty())
        {
            for (size_t i = 0; i < changedCoins.size(); i++)
            {
                ShieldedCoin& coin = changedCoins[i];
                coin.m_confirmHeight = MaxHeight;
                updateShieldedCoinRaw(coin);
                storage::DeduceStatus(*this, coin, ssc.m_hTip);
            }

            // TODO: erase coins completely with "Unavail" status
            notifyShieldedCoinsChanged(ChangeAction::Updated, changedCoins);
        }
    }

    void WalletDB::insertShieldedCoinRaw(const ShieldedCoin& coin)
    {
        const char* req = "INSERT INTO " SHIELDED_COINS_NAME " (" ENUM_SHIELDED_COIN_FIELDS(LIST, COMMA, ) ") VALUES(" ENUM_SHIELDED_COIN_FIELDS(BIND_LIST, COMMA, ) ");";
        sqlite::Statement stm(this, req);
        int colIdx = 0;
       
        ENUM_SHIELDED_COIN_FIELDS(STM_BIND_LIST, NOSEP, coin);
        stm.step();
    }

    bool WalletDB::updateShieldedCoinRaw(const ShieldedCoin& coin)
    {
        const char* req = "UPDATE " SHIELDED_COINS_NAME " SET " ENUM_SHIELDED_COIN_FIELDS(SET_LIST, COMMA, ) "WHERE Key = ?;";
        sqlite::Statement stm(this, req);
        int colIdx = 0;

        ENUM_SHIELDED_COIN_FIELDS(STM_BIND_LIST, NOSEP, coin);
        stm.bind(++colIdx, coin.m_CoinID.m_Key);
        stm.step();

        return sqlite3_changes(_db) > 0;
    }

    void WalletDB::saveShieldedCoinRaw(const ShieldedCoin& coin)
    {
        std::vector<ShieldedCoin> v = { coin };
        storage::DeduceStatus(*this, v[0], getCurrentHeight());
        if (!updateShieldedCoinRaw(coin))
        {
            insertShieldedCoinRaw(coin);
            notifyShieldedCoinsChanged(ChangeAction::Added, v);
            return;
        }
        notifyShieldedCoinsChanged(ChangeAction::Updated, v);
    }

    void WalletDB::restoreShieldedCoinsSpentByTx(const TxID& txId)
    {
        std::vector<ShieldedTxo::BaseKey> updatedRows;
        {
            const char* req = "SELECT Key FROM " SHIELDED_COINS_NAME " WHERE spentTxId=?1;";
            sqlite::Statement stm(this, req);
            stm.bind(1, txId);
            while (stm.step())
            {
                ShieldedTxo::BaseKey& key = updatedRows.emplace_back();
                stm.get(0, key);
            }
        }
        if (!updatedRows.empty())
        {
            {
                const char* req = "UPDATE " SHIELDED_COINS_NAME " SET spentTxId=NULL WHERE spentTxId=?1;";
                sqlite::Statement stm(this, req);
                stm.bind(1, txId);
                stm.step();
            }

            vector<ShieldedCoin> updatedCoins;
            updatedCoins.reserve(updatedRows.size());

            auto currentHeight = getCurrentHeight();
            for (const auto& key : updatedRows)
            {
                auto& coin = updatedCoins.emplace_back(getShieldedCoin(key).value());
                storage::DeduceStatus(*this, coin, currentHeight);
            }

            notifyShieldedCoinsChanged(ChangeAction::Updated, updatedCoins);
        }
    }

    void WalletDB::DeleteShieldedCoin(const ShieldedTxo::BaseKey& key)
    {
        const char* req = "DELETE FROM " SHIELDED_COINS_NAME " WHERE Key=?1;";
        sqlite::Statement stm(this, req);
        stm.bind(1, key);
        stm.step();
    }

    void WalletDB::deleteShieldedCoinsCreatedByTx(const TxID& txId)
    {
        ShieldedStatusCtx ssc(*this);
        std::vector<ShieldedCoin> deletedItems;
        {
            const char* req = "SELECT " ENUM_SHIELDED_COIN_FIELDS(LIST, COMMA, ) " FROM " SHIELDED_COINS_NAME " WHERE createTxId=?1 AND confirmHeight=?2;";
            sqlite::Statement stm(this, req);
            stm.bind(1, txId);
            stm.bind(2, MaxHeight);
            while (stm.step())
            {
                ShieldedCoin& coin = deletedItems.emplace_back();
                int colIdx = 0;
                ENUM_SHIELDED_COIN_FIELDS(STM_GET_LIST, NOSEP, coin);
                storage::DeduceStatus(*this, coin, ssc.m_hTip);
                DeleteShieldedCoin(coin.m_CoinID.m_Key);
            }
        }

        if (!deletedItems.empty())
            notifyShieldedCoinsChanged(ChangeAction::Removed, deletedItems);
    }

    boost::optional<ShieldedTxo::Voucher> WalletDB::grabVoucher(const WalletID& peerID)
    {
        int f = VoucherFlags::Deleted;
        boost::optional<ShieldedTxo::Voucher> res;
        sqlite::Statement stm(this, "SELECT rowid, Voucher, Flags FROM " VOUCHERS_NAME " WHERE WalletID=?1 AND (Flags&?2)!=?2 ;");
        stm.bind(1, peerID);
        stm.bind(2, f);

        if (stm.step())
        {
            res.emplace();
            int rowID = 0;
            stm.get(0, rowID);
            stm.get(1, *res);
            stm.get(2, f);
            
            if ((f & VoucherFlags::Preserve) == 0)
            {
                sqlite::Statement delStm(this, "DELETE FROM " VOUCHERS_NAME " WHERE rowid=?1;");
                delStm.bind(1, rowID);
                delStm.step();
            }
            else
            {
                f |= VoucherFlags::Deleted;
                sqlite::Statement stm2(this, "UPDATE " VOUCHERS_NAME  " SET Flags=?2 WHERE rowid=?1;");
                stm2.bind(1, rowID);
                stm2.bind(2, f);
                stm2.step();
            }
        }
        return res;
    }

    void WalletDB::saveVoucher(const ShieldedTxo::Voucher& v, const WalletID& walletID, bool preserveOnGrab)
    {
        uint16_t flags = (preserveOnGrab) ? VoucherFlags::Preserve : VoucherFlags::Default;
        sqlite::Statement stm(this, "INSERT INTO " VOUCHERS_NAME " (" ENUM_VOUCHERS_FIELDS(LIST, COMMA, ) ") VALUES(?,?,?);");
        stm.bind(1, walletID);
        stm.bind(2, v);
        stm.bind(3, flags);
        stm.step();
    }

    size_t WalletDB::getVoucherCount(const WalletID& peerID) const
    {
        int res = 0;
        uint16_t f = VoucherFlags::Deleted | VoucherFlags::Preserve;
        sqlite::Statement stm(this, "SELECT COUNT(Voucher) FROM " VOUCHERS_NAME " WHERE WalletID=?1 AND (Flags & ?2)!=?2;");
        stm.bind(1, peerID);
        stm.bind(2, f);
        if (stm.step())
        {
            stm.get(0, res);
        }
        return size_t(res);
    }
}
