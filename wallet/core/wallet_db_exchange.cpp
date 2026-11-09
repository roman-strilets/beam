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
    std::vector<std::pair<ByteBuffer, bool>> WalletDB::loadDexOffers()
    {
        sqlite::Statement countStm(this, "SELECT COUNT(*) FROM " DEX_OFFERS_NAME ";");
        countStm.step();
        
        uint64_t count;
        countStm.get(0, count);

        std::vector<std::pair<ByteBuffer, bool>> offers;
        offers.reserve(count);

        const char* selectReq = "SELECT * FROM " DEX_OFFERS_NAME ";";
        sqlite::Statement stm(this, selectReq);


        while (stm.step())
        {
            ByteBuffer buffer;
            stm.get(1, buffer);
            bool isMine = false;
            stm.get(2, isMine);

            offers.emplace_back(buffer, isMine);
        }

        return offers;
    }

#ifdef BEAM_ASSET_SWAP_SUPPORT
    void WalletDB::saveDexOffer(const DexOrderID& offerId, const ByteBuffer& offer, bool isMine)
    {
        const auto& idStr = offerId.to_string();
        BEAM_LOG_DEBUG() << "Save offer: " << idStr;
        const char* selectReq = "SELECT * FROM " DEX_OFFERS_NAME " WHERE id=?1;";
        sqlite::Statement stm2(this, selectReq);
        stm2.bind(1, idStr);

        if (stm2.step())
        {
            const char* updateReq = "UPDATE " DEX_OFFERS_NAME " SET data=?2, isMine=?3 WHERE id=?1;";
            sqlite::Statement stm(this, updateReq);

            stm.bind(1, idStr);
            stm.bind(2, offer);
            stm.bind(3, isMine);

            stm.step();
        }
        else
        {
            const char* insertReq = "INSERT INTO " DEX_OFFERS_NAME " (" ENUM_DEX_OFFERS_FIELDS(LIST, COMMA, ) ") VALUES(" ENUM_DEX_OFFERS_FIELDS(BIND_LIST, COMMA, ) ");";
            sqlite::Statement stm(this, insertReq);

            stm.bind(1, idStr);
            stm.bind(2, offer);
            stm.bind(3, isMine);

            stm.step();
        }
    }

    void WalletDB::dropDexOffer(const DexOrderID& offerId)
    {
        const auto& idStr = offerId.to_string();
        BEAM_LOG_DEBUG() << "Delete offer: " << idStr;
        const char* dropReq = "DELETE FROM " DEX_OFFERS_NAME " WHERE id=?1;";
        sqlite::Statement stm(this, dropReq);
        stm.bind(1, idStr);

        stm.step();
    }
#endif  // BEAM_ASSET_SWAP_SUPPORT

    std::vector<ExchangeRate> WalletDB::getLatestExchangeRates() const
    {
        ExchangeRates res;

        const char* req = "SELECT * FROM " EXCHANGE_RATES_NAME " ORDER BY updateTime DESC;";
        sqlite::Statement stm(this, req);

        while (stm.step())
        {
            auto& rate = res.emplace_back();
            int colIdx = 0;
            ENUM_EXCHANGE_RATES_FIELDS(STM_GET_LIST, NOSEP, rate);
        }

        return res;
    }

    void WalletDB::saveExchangeRate(const ExchangeRate& rate)
    {
        const char* selectReq = "SELECT * FROM " EXCHANGE_RATES_NAME " WHERE cfrom=?1 AND cto=?2;";
        sqlite::Statement selectStm(this, selectReq);
        selectStm.bind(1, rate.m_from.m_value);
        selectStm.bind(2, rate.m_to.m_value);

        if (selectStm.step())
        {
            const char* updateReq = "UPDATE " EXCHANGE_RATES_NAME " SET rate=?1, updateTime=?2 WHERE cfrom=?3 AND cto=?4;";
            sqlite::Statement updateStm(this, updateReq);

            updateStm.bind(1, rate.m_rate);
            updateStm.bind(2, rate.m_updateTime);
            updateStm.bind(3, rate.m_from.m_value);
            updateStm.bind(4, rate.m_to.m_value);
            updateStm.step();
        }
        else
        {
            const char* insertReq = "INSERT INTO " EXCHANGE_RATES_NAME " (" ENUM_EXCHANGE_RATES_FIELDS(LIST, COMMA, ) ") VALUES(" ENUM_EXCHANGE_RATES_FIELDS(BIND_LIST, COMMA, ) ");";
            sqlite::Statement stm(this, insertReq);
            int colIdx = 0;
            ENUM_EXCHANGE_RATES_FIELDS(STM_BIND_LIST, NOSEP, rate);
            stm.step();
        }
    }

    boost::optional<ExchangeRateAtPoint> WalletDB::getExchangeRateNearPoint(const Currency& from, const Currency& to, uint64_t maxHeight) const
    {
        const char* req = "SELECT * FROM " EXCHANGE_RATES_HISTORY_NAME " WHERE cfrom=?1 AND cto=?2 AND height<=?3 ORDER BY updateTime DESC LIMIT 1;";
        sqlite::Statement stm(this, req);
        stm.bind(1, from.m_value);
        stm.bind(2, to.m_value);
        stm.bind(3, maxHeight);

        while (stm.step())
        {
            int colIdx = 0;
            ExchangeRateAtPoint rate;
            ENUM_EXCHANGE_RATES_HISTORY_FIELDS(STM_GET_LIST, NOSEP, rate);
            return rate;
        }

        return boost::none;
    }

    ExchangeRatesHistory WalletDB::getExchangeRatesHistory(uint64_t startHeight, uint64_t endHeight) const
    {
        ExchangeRatesHistory res;

        const char* req = "SELECT * FROM " EXCHANGE_RATES_HISTORY_NAME " WHERE height<=?1 AND height>=?2 ORDER BY updateTime DESC;";
        sqlite::Statement stm(this, req);
        stm.bind(1, endHeight);
        stm.bind(2, startHeight);

        while (stm.step())
        {
            auto& rate = res.emplace_back();
            int colIdx = 0;
            ENUM_EXCHANGE_RATES_HISTORY_FIELDS(STM_GET_LIST, NOSEP, rate);
        }

        return res;
    }

    void WalletDB::saveExchangeRate(const ExchangeRateAtPoint& rate)
    {
        const char* insertReq = "INSERT INTO " EXCHANGE_RATES_HISTORY_NAME " (" ENUM_EXCHANGE_RATES_HISTORY_FIELDS(LIST, COMMA, ) ") VALUES(" ENUM_EXCHANGE_RATES_HISTORY_FIELDS(BIND_LIST, COMMA, ) ");";
        sqlite::Statement stm(this, insertReq);
        int colIdx = 0;
        ENUM_EXCHANGE_RATES_HISTORY_FIELDS(STM_BIND_LIST, NOSEP, rate);
        stm.step();
    }
}
