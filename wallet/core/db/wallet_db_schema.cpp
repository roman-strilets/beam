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
        const char* Version = "Version";
        const int BusyTimeoutMs = 5000;

        const int DbVersion   = 39;
        const int DbVersion38 = 38;
        const int DbVersion37 = 37;
        const int DbVersion36 = 36;
        const int DbVersion35 = 35;
        const int DbVersion34 = 34;
        const int DbVersion33 = 33;
        const int DbVersion32 = 32;
        const int DbVersion31 = 31;
        const int DbVersion30 = 30;
        const int DbVersion29 = 29;
        const int DbVersion28 = 28;
        const int DbVersion27 = 27;
        const int DbVersion26 = 26;
        const int DbVersion25 = 25;
        const int DbVersion24 = 24;
        const int DbVersion23 = 23;
        const int DbVersion22 = 22;
        const int DbVersion21 = 21;
        const int DbVersion20 = 20;
        const int DbVersion19 = 19;
        const int DbVersion18 = 18;
        const int DbVersion17 = 17;
        const int DbVersion16 = 16;
        const int DbVersion15 = 15;
        const int DbVersion14 = 14;
        const int DbVersion13 = 13;
        const int DbVersion12 = 12;
        const int DbVersion11 = 11;
        const int DbVersion10 = 10;

        void enterKey(sqlite3 * db, const SecString& password)
        {
            if (password.size() > static_cast<size_t>(numeric_limits<int>::max()))
            {
                throwIfError(SQLITE_TOOBIG, db);
            }
            int ret = sqlite3_key(db, password.data(), static_cast<int>(password.size()));
            throwIfError(ret, db);
        }
    }

    bool WalletDB::isInitialized(const string& path)
    {
#ifdef WIN32
        return fs::exists(Utf8toUtf16(path.c_str()));
#else
        return fs::exists(path);
#endif
    }

    namespace
    {
        void OpenAndMigrateIfNeeded(const string& path, sqlite3** db, const SecString& password);
    }

    bool WalletDB::isValidPassword(const std::string& path, const SecString& password)
    {
        try 
        {
            sqlite3* db = nullptr;
            OpenAndMigrateIfNeeded(path, &db, password);
            return true;
        }
        catch (...)
        {

        }
        return false;
    }

    namespace
    {
        bool IsTableCreated(const WalletDB* db, const char* tableName)
        {
            std::string req = "SELECT name FROM sqlite_master WHERE type='table' AND name='";
            req += tableName;
            req += "';";

            sqlite::Statement stm(db, req.c_str(), false);
            return stm.step();
        }

        void CreateStorageTable(sqlite3* db)
        {
            const char* req = "CREATE TABLE " STORAGE_NAME " (" ENUM_ALL_STORAGE_FIELDS(LIST_WITH_TYPES, COMMA, ) ");"
                "CREATE UNIQUE INDEX CoinIndex ON " STORAGE_NAME "(" ENUM_STORAGE_ID(LIST, COMMA, )  ");"
                "CREATE INDEX ConfirmIndex ON " STORAGE_NAME"(confirmHeight);";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateAppDataTable(sqlite3* db)
        {
            const char* req = "CREATE TABLE " TblAppData " (" TblAppData_Name " BLOB NOT NULL, " TblAppData_Key " BLOB NOT NULL, " TblAppData_Val " BLOB NOT NULL);"
                "CREATE UNIQUE INDEX " TblAppData "_Idx ON " TblAppData "(" TblAppData_Name "," TblAppData_Key  ");";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateWalletMessageTable(sqlite3* db)
        {
            {
                const char* req = "CREATE TABLE IF NOT EXISTS " WALLET_MESSAGE_NAME " (" ENUM_WALLET_MESSAGE_FIELDS(LIST_WITH_TYPES, COMMA, ) ");";
                int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
                throwIfError(ret, db);
            }
            {
                const char* req = "CREATE TABLE IF NOT EXISTS " INCOMING_WALLET_MESSAGE_NAME " (" ENUM_INCOMING_WALLET_MESSAGE_FIELDS(LIST_WITH_TYPES, COMMA, ) ");";
                int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
                throwIfError(ret, db);
            }
        }

        void CreatePrivateVariablesTable(sqlite3* db)
        {
            const char* req = "CREATE TABLE " PRIVATE_VARIABLES_NAME " (" ENUM_VARIABLES_FIELDS(LIST_WITH_TYPES, COMMA, ) ");";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }
    
        void CreateAddressesTable(sqlite3* db, const std::string& tableName)
        {
            const std::string req = "CREATE TABLE " + tableName + " (" ENUM_ADDRESS_FIELDS(LIST_WITH_TYPES, COMMA, ) ") WITHOUT ROWID;"
                              "CREATE INDEX " + tableName + "WalletIDIndex ON " + tableName + "(WalletID);";
            int ret = sqlite3_exec(db, req.c_str(), nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateVariablesTable(sqlite3* db)
        {
            const char* req = "CREATE TABLE " VARIABLES_NAME " (" ENUM_VARIABLES_FIELDS(LIST_WITH_TYPES, COMMA, ) ");";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateTxParamsIndex(sqlite3* db)
        {
            const char* req = "CREATE INDEX IF NOT EXISTS TxParamsIndex ON " TX_PARAMS_NAME " (txID,subTxID,paramID);";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateTxParamsTable(sqlite3* db)
        {
            const char* req = "CREATE TABLE " TX_PARAMS_NAME " (" ENUM_TX_PARAMS_FIELDS(LIST_WITH_TYPES, COMMA, ) ", PRIMARY KEY (txID, subTxID, paramID)) WITHOUT ROWID;";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
            CreateTxParamsIndex(db);
        }

        void CreateStatesTable(sqlite3* db)
        {
            const char* req = "CREATE TABLE [" TblStates "] ("
                "[" TblStates_Height    "] INTEGER NOT NULL PRIMARY KEY,"
                "[" TblStates_Hdr        "] BLOB NOT NULL)";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateLaserAddressTable(sqlite3* db)
        {
            CreateAddressesTable(db, LASER_ADDRESSES_NAME);
        }

        void CreateLaserTables(sqlite3* db)
        {
            const char* req = "CREATE TABLE " LASER_CHANNELS_NAME " (" ENUM_LASER_CHANNEL_FIELDS(LIST_WITH_TYPES, COMMA, ) ") WITHOUT ROWID;";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);

            CreateLaserAddressTable(db);

            BEAM_LOG_DEBUG() << "Create laser tables";
        }

        void CreateAssetsTable(sqlite3* db)
        {
            assert(db != nullptr);
            const char* req = "CREATE TABLE " ASSETS_NAME " (" ENUM_ASSET_FIELDS(LIST_WITH_TYPES, COMMA, ) ") WITHOUT ROWID;"
                              "CREATE UNIQUE INDEX OwnerIndex ON " ASSETS_NAME "(Owner);"
                              "CREATE INDEX RefreshHeightIndex ON " ASSETS_NAME "(RefreshHeight);";
            const auto ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateEventsTable(sqlite3* db)
        {
            assert(db != nullptr);
            const char* req = "CREATE TABLE " EVENTS_NAME " (" ENUM_EVENTS_FIELDS(LIST_WITH_TYPES, COMMA, ) ");"
                "CREATE INDEX EventsIndex ON " EVENTS_NAME "(Height, Body);"
                "CREATE INDEX EventsKeyIndex ON " EVENTS_NAME "(Key);";
            const auto ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateTxSummaryTable(sqlite3* db)
        {
            assert(db != nullptr);
#define MACRO(id, type) "," #id " INTEGER" 
            const char* req = "CREATE TABLE " TX_SUMMARY_NAME " (TxID BLOB NOT NULL PRIMARY KEY " ENUM_TX_SUMMARY_FIELDS(MACRO) ") WITHOUT ROWID;"
#undef MACRO

#ifndef STR
#   define STR(s) #s 
#endif

#define MACRO(id, type) "CREATE INDEX " STR(id##Index) " ON " TX_SUMMARY_NAME "(" #id ");"
            ENUM_TX_SUMMARY_FIELDS(MACRO)
#undef MACRO
#undef STR
                ;
            const auto ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void MigrateAssetsFrom20(sqlite3* db)
        {
            assert(db != nullptr);

            // assets table changed: value from INTEGER to BLOB
            // move old data to temp table
            {
                const char* req = "ALTER TABLE " ASSETS_NAME " RENAME TO " ASSETS_NAME "_del;"
                                  "DROP INDEX OwnerIndex;"
                                  "DROP INDEX RefreshHeightIndex;";
                int ret = sqlite3_exec(db, req, NULL, NULL, NULL);
                throwIfError(ret, db);
            }

            // create new table
            CreateAssetsTable(db);

            // migration
            {
                const char* req = "INSERT INTO " ASSETS_NAME " (" ENUM_ASSET_FIELDS_0(LIST, COMMA, ) ") SELECT " ENUM_ASSET_FIELDS_0(LIST, COMMA, ) " FROM " ASSETS_NAME "_del;";
                int ret = sqlite3_exec(db, req, NULL, NULL, NULL);
                throwIfError(ret, db);
            }

            // remove tmp table
            {
                const char* req = "DROP TABLE " ASSETS_NAME "_del;";
                int ret = sqlite3_exec(db, req, NULL, NULL, NULL);
                throwIfError(ret, db);
            }
        }

        void CreateNotificationsTable(sqlite3* db)
        {
            const char* req = "CREATE TABLE " NOTIFICATIONS_NAME " (" ENUM_NOTIFICATION_FIELDS(LIST_WITH_TYPES, COMMA, ) ") WITHOUT ROWID;";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateExchangeRatesTable29(sqlite3* db)
        {
            const char* req = "CREATE TABLE " EXCHANGE_RATES_NAME " (" ENUM_EXCHANGE_RATES_FIELDS29(LIST_WITH_TYPES, COMMA, ) ", PRIMARY KEY (currency, unit)) WITHOUT ROWID;";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateExchangeRatesTable(sqlite3* db)
        {
            const char* req = "CREATE TABLE " EXCHANGE_RATES_NAME " (" ENUM_EXCHANGE_RATES_FIELDS(LIST_WITH_TYPES, COMMA, ) ", PRIMARY KEY (cfrom, cto)) WITHOUT ROWID;";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateVerificationTable(sqlite3* db)
        {
            const char* req = "CREATE TABLE " VERIFICATION_NAME " (" ENUM_VERIFICATION_FIELDS(LIST_WITH_TYPES, COMMA, ) ", PRIMARY KEY (assetID, updateTime)) WITHOUT ROWID;";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateExchangeRatesHistoryTable29(sqlite3* db)
        {
            const char* req = "CREATE TABLE " EXCHANGE_RATES_HISTORY_NAME " (" ENUM_EXCHANGE_RATES_HISTORY_FIELDS29(LIST_WITH_TYPES, COMMA, ) ", PRIMARY KEY (currency, unit, updateTime)) WITHOUT ROWID;";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateExchangeRatesHistoryTable(sqlite3* db)
        {
            const char* req = "CREATE TABLE " EXCHANGE_RATES_HISTORY_NAME " (" ENUM_EXCHANGE_RATES_HISTORY_FIELDS(LIST_WITH_TYPES, COMMA, ) ", PRIMARY KEY (cfrom, cto, updateTime)) WITHOUT ROWID;";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void AddAddressIdentityColumn(const WalletDB* walletDB, sqlite3* db)
        {
            const char* req = "ALTER TABLE " ADDRESSES_NAME " ADD Identity BLOB NULL;";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);

            const char* req_laser_addr_identity_exist =
                "SELECT COUNT(*) AS CNTREC FROM pragma_table_info('" LASER_ADDRESSES_NAME "') WHERE name='Identity';";
            int laser_addr_identity_exist = 0;
            for (sqlite::Statement stm(walletDB, req_laser_addr_identity_exist); stm.step();)
            {
                stm.get(0, laser_addr_identity_exist);
            }

            if (!laser_addr_identity_exist)
            {
                const char* req_laser = "ALTER TABLE " LASER_ADDRESSES_NAME " ADD Identity BLOB NULL;";
                ret = sqlite3_exec(db, req_laser, nullptr, nullptr, nullptr);
                throwIfError(ret, db);
            }
        }

        void CreateShieldedCoinsTable(sqlite3* db)
        {
            const char* req = "CREATE TABLE " SHIELDED_COINS_NAME " (" ENUM_SHIELDED_COIN_FIELDS(LIST_WITH_TYPES, COMMA, ) ") WITHOUT ROWID;";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateShieldedCoinsTableIndex(sqlite3* db)
        {
            const char* req = "CREATE INDEX " SHIELDED_COINS_NAME "SpendIdx ON " SHIELDED_COINS_NAME "(spentHeight);";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateVouchersTable(sqlite3* db)
        {
            const char* req = "CREATE TABLE " VOUCHERS_NAME " (" ENUM_VOUCHERS_FIELDS(LIST_WITH_TYPES, COMMA, ) ");"
                "CREATE INDEX WalletIDIndex ON " VOUCHERS_NAME "(WalletID);";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        bool VouchersHasFlags(const WalletDB* db)
        {
            sqlite::Statement stm(db, "SELECT* FROM PRAGMA_table_info('Vouchers') WHERE name = 'Flags';");
            return stm.step();
        }

        void AddVouchersFlagsColumn(sqlite3* db)
        {
            const char* req = "ALTER TABLE " VOUCHERS_NAME " ADD Flags INTEGER NULL;";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void CreateDexOffersTable(sqlite3* db)
        {
            const char* req = "CREATE TABLE " DEX_OFFERS_NAME " (" ENUM_DEX_OFFERS_FIELDS(LIST_WITH_TYPES, COMMA, ) ") WITHOUT ROWID;";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void MigrateAddressesFrom24(WalletDB* walletDB, sqlite3* db, const std::string& tableName)
        {
            // move old data to temp table
            if (!IsTableCreated(walletDB, (tableName + "_del").c_str()))
            {
                const std::string req = "ALTER TABLE " + tableName + " RENAME TO " + tableName + "_del;"
                                        "DROP INDEX IF EXISTS " + tableName + "WalletIDIndex;";
                int ret = sqlite3_exec(db, req.c_str(), NULL, NULL, NULL);
                throwIfError(ret, db);
            }

            // create new table
            CreateAddressesTable(db, tableName);

            bool isLaser = tableName == LASER_ADDRESSES_NAME;
            // migration
            {
                sqlite::Statement stm((const WalletDB*)walletDB, ("SELECT * FROM " + tableName + "_del;").c_str());

                while (stm.step())
                {
                    WalletAddress addr;
                    stm.get(0, addr.m_BbsAddr);
                    stm.get(1, addr.m_label);
                    stm.get(2, addr.m_category);
                    stm.get(3, addr.m_createTime);
                    stm.get(4, addr.m_duration);
                    stm.get(5, addr.m_OwnID);
                    stm.get(6, addr.m_Endpoint);

                    if (addr.m_BbsAddr != Zero)
                    {
                        walletDB->setDefaultToken(addr);
                        walletDB->saveAddress(addr, isLaser);
                    }
                }
            }

            // remove tmp table
            {
                const std::string req = "DROP TABLE " + tableName + "_del;";
                int ret = sqlite3_exec(db, req.c_str(), NULL, NULL, NULL);
                throwIfError(ret, db);
            }
        }

        void MigrateAddressesFrom24(WalletDB* walletDB, sqlite3* db)
        {
            MigrateAddressesFrom24(walletDB, db, ADDRESSES_NAME);
            MigrateAddressesFrom24(walletDB, db, LASER_ADDRESSES_NAME);
        }

        void MigrateTransactionsFrom26(WalletDB* walletDB)
        {
            sqlite::Statement stm((const WalletDB*)walletDB, "SELECT txID, value FROM " TX_PARAMS_NAME " WHERE subTxID=?1 AND paramID=?2;");
            stm.bind(1, kDefaultSubTxID);
            stm.bind(2, TxParameterID::OriginalToken);
            while (stm.step())
            {
                TxID txID;
                std::string originalAddress;
                ByteBuffer buf;
                stm.get(0, txID);
                stm.get(1, buf);
                if (fromByteBuffer(buf, originalAddress))
                {
                    auto addressType = GetAddressType(originalAddress);
                    storage::setTxParameter(*walletDB, txID, TxParameterID::AddressType, addressType, false);
                }
            }
        }

        struct ExchangeRate29
        {
            enum class CurrType: uint32_t
            {};

            CurrType    m_from = CurrType(0);
            CurrType    m_to   = CurrType(0);
            Amount      m_rate = 0;
            Timestamp   m_updateTime = 0;
            SERIALIZE(m_from, m_to, m_rate, m_updateTime);
        };

        Currency exchangeCurr29to30(ExchangeRate29::CurrType curr29)
        {
            switch(static_cast<uint32_t>(curr29)) {
                case 0: return  Currency::BEAM();
                case 1: return  Currency::BTC();
                case 2: return  Currency::LTC();
                case 3: return  Currency::QTUM();
                case 4: return  Currency::USD();
                case 5: return  Currency::DOGE();
                case 6: return  Currency::DASH();
                case 7: return  Currency::ETH();
                case 8: return  Currency::DAI();
                case 9: return  Currency::USDT();
                case 10: return Currency::WBTC();
                case 11: return Currency::BCH();
                default: return Currency::UNKNOWN();
            }
        }

        void MigrateRatesFrom29 (WalletDB* walletDB, sqlite3* db)
        {
            if (!IsTableCreated(walletDB, EXCHANGE_RATES_NAME "_del"))
            {
                const char* req = "ALTER TABLE " EXCHANGE_RATES_NAME " RENAME TO " EXCHANGE_RATES_NAME "_del;";
                int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
                throwIfError(ret, db);
            }

            if (!IsTableCreated(walletDB, EXCHANGE_RATES_NAME))
            {
                CreateExchangeRatesTable(db);
            }

            {
                const char* req = "SELECT currency, unit, rate, updateTime FROM " EXCHANGE_RATES_NAME "_del;";
                for (sqlite::Statement stm(walletDB, req); stm.step();)
                {
                    ExchangeRate rate;

                    ExchangeRate29::CurrType from29;
                    ExchangeRate29::CurrType to29;

                    stm.get(0, from29);
                    stm.get(1, to29);
                    stm.get(2, rate.m_rate);
                    stm.get(3, rate.m_updateTime);

                    rate.m_from = exchangeCurr29to30(from29);
                    rate.m_to   = exchangeCurr29to30(to29);

                    walletDB->saveExchangeRate(rate);
                }
            }

            {
                const char* req = "DROP TABLE " EXCHANGE_RATES_NAME "_del;";
                int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
                throwIfError(ret, db);
            }
        }

        void MigrateRatesHistoryFrom29 (WalletDB* walletDB, sqlite3* db)
        {
            if (!IsTableCreated(walletDB, EXCHANGE_RATES_HISTORY_NAME "_del"))
            {
                const char* req = "ALTER TABLE " EXCHANGE_RATES_HISTORY_NAME " RENAME TO " EXCHANGE_RATES_HISTORY_NAME "_del;";
                int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
                throwIfError(ret, db);
            }

            if (!IsTableCreated(walletDB, EXCHANGE_RATES_HISTORY_NAME))
            {
                CreateExchangeRatesHistoryTable(db);
            }

            {
                const char* req = "SELECT currency, unit, rate, updateTime, height FROM " EXCHANGE_RATES_HISTORY_NAME "_del;";
                for (sqlite::Statement stm(walletDB, req); stm.step();)
                {
                    ExchangeRateAtPoint rate;

                    ExchangeRate29::CurrType from29;
                    ExchangeRate29::CurrType to29;

                    stm.get(0, from29);
                    stm.get(1, to29);
                    stm.get(2, rate.m_rate);
                    stm.get(3, rate.m_updateTime);
                    stm.get(4, rate.m_height);

                    rate.m_from = exchangeCurr29to30(from29);
                    rate.m_to   = exchangeCurr29to30(to29);

                    walletDB->saveExchangeRate(rate);
                }
            }

            {
                const char* req = "DROP TABLE " EXCHANGE_RATES_HISTORY_NAME "_del;";
                int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
                throwIfError(ret, db);
            }
        }

        void MigrateTxRatesFrom29to30(WalletDB* walletDB, sqlite3* db)
        {
            struct UpdateInfo
            {
                TxID txID = TxID();
                uint64_t subTxID = kDefaultSubTxID;
                std::vector<ExchangeRate> rates;
            };

            std::vector<UpdateInfo> updates;

            sqlite::Statement stm((const WalletDB*)walletDB, "SELECT txID, subTxID, value FROM " TX_PARAMS_NAME " WHERE paramID=?1;");
            stm.bind(1, TxParameterID::ExchangeRates);
            while (stm.step())
            {
                UpdateInfo updateInfo;
                ByteBuffer buffer;

                stm.get(0, updateInfo.txID);
                stm.get(1, updateInfo.subTxID);
                stm.get(2, buffer);

                std::vector<ExchangeRate29> rates29;
                if (fromByteBuffer(buffer, rates29))
                {
                    for (const auto& rate29: rates29)
                    {
                        ExchangeRate rate30;
                        rate30.m_from       = exchangeCurr29to30(rate29.m_from);
                        rate30.m_to         = exchangeCurr29to30(rate29.m_to);
                        rate30.m_rate       = rate29.m_rate;
                        rate30.m_updateTime = rate29.m_updateTime;
                        updateInfo.rates.push_back(rate30);
                    }
                    updates.push_back(updateInfo);
                }
                else
                {
                    throw std::runtime_error("MigrateTxRatesFrom29to30 - failed to read v29 rates vector");
                }
            }

            for (const auto& updateInfo: updates)
            {
                storage::setTxParameter(*walletDB, updateInfo.txID, static_cast<SubTxID>(updateInfo.subTxID), TxParameterID::ExchangeRates, updateInfo.rates, false);
                BEAM_LOG_INFO() << "Rates convert for " << updateInfo.txID << ", rcnt: " << updateInfo.rates.size();
            }
        }

        void MigrateTransactionsFrom30(WalletDB* walletDB, sqlite3* db)
        {
            walletDB->visitTx([&](const TxDescription& tx) -> bool {
                if(tx.GetTxID().is_initialized())
                {
                    TxAddressType addressType = TxAddressType::Unknown;
                    tx.GetParameter(TxParameterID::AddressType, addressType);
                    if (addressType == TxAddressType::Unknown)
                    {
                        addressType = GetAddressType(tx);
                        storage::setTxParameter(*walletDB, *tx.GetTxID(), TxParameterID::AddressType, addressType, false);
                    }
                }
                else
                {
                    BEAM_LOG_WARNING() << "MigrateTransactionsFrom30: Unexpected tx without TxID";
                    assert(false);
                }
                return true;
            }, TxListFilter());
        }

        void MigrateFrom31(WalletDB* walletDB, sqlite3* db)
        {
            if (!IsTableCreated(walletDB, VERIFICATION_NAME))
            {
                CreateVerificationTable(db);
            }
        }

        void MigrateFrom32_After20(sqlite3* db)
        {
            assert(db != nullptr);

            // assets table changed: added Deposit column
            // move old data to temp table
            {
                const char* req = "ALTER TABLE " ASSETS_NAME " ADD COLUMN Deposit INTEGER;";
                int ret = sqlite3_exec(db, req, NULL, NULL, NULL);
                throwIfError(ret, db);
            }
        }

        void OpenAndMigrateIfNeeded(const string& path, sqlite3** db, const SecString& password)
        {
            int ret = sqlite3_open_v2(path.c_str(), db, SQLITE_OPEN_READWRITE, nullptr);
            throwIfError(ret, *db);
            enterKey(*db, password);
            // try to decrypt
            auto checkKey = [&]() {return sqlite3_exec(*db, "SELECT count(*) FROM sqlite_master;", NULL, NULL, NULL); };
            ret = checkKey();
            if (ret != SQLITE_OK)
            {
                BEAM_LOG_INFO() << "Applying PRAGMA cipher_migrate...";
                ret = sqlite3_close(*db);
                throwIfError(ret, *db);
                ret = sqlite3_open_v2(path.c_str(), db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
                throwIfError(ret, *db);
                enterKey(*db, password);
                ret = sqlite3_exec(*db, "PRAGMA cipher_migrate; ", nullptr, nullptr, nullptr);
                throwIfError(ret, *db);
                ret = checkKey();
                throwIfError(ret, *db);
            }
        }

        bool MoveSeedToPrivateVariables(WalletDB& db)
        {
            ECC::NoLeak<ECC::Hash::Value> seed;
            if (!storage::getVar(db, WalletSeed, seed.V))
            {
                assert(false && "there is no seed for walletDB");
                BEAM_LOG_ERROR() << "there is no seed for walletDB";
                return false;
            }
            db.setPrivateVarRaw(WalletSeed, &seed.V, sizeof(seed.V));

            {
                sqlite::Statement stm(&db, "DELETE FROM " VARIABLES_NAME " WHERE name=?1;");
                stm.bind(1, WalletSeed);
                stm.step();
            }

            return true;
        }

        bool DropPrivateVariablesFromPublicDatabase(WalletDB& db)
        {
            {
                sqlite::Statement stm(&db, "SELECT name FROM sqlite_master WHERE type='table' AND name='" PRIVATE_VARIABLES_NAME "';");

                if (!stm.step())
                {
                    return true; // there is nothing to drop
                }
            }

            // ensure that we have  master key in private database
            {
                ECC::NoLeak<ECC::Hash::Value> seed; // seed from public db
                if (GetPrivateVarRaw(db, WalletSeed, &seed.V, sizeof(ECC::Hash::Value), false))
                {
                    ECC::NoLeak<ECC::Hash::Value> seed2; // seed from private db
                    if (GetPrivateVarRaw(db, WalletSeed, &seed2.V, sizeof(ECC::Hash::Value), true))
                    {
                        if (seed.V != seed2.V)
                        {
                            BEAM_LOG_ERROR() << "Public database has different master key. Please check your \'wallet.db\' and \'wallet.db.private\'";
                            return false;
                        }
                    }
                    else
                    {
                        db.setPrivateVarRaw(WalletSeed, &seed.V, sizeof(seed.V));
                    }
                }
            }

            sqlite::Statement dropStm(&db, "DROP TABLE " PRIVATE_VARIABLES_NAME ";");
            dropStm.step();
            return true;
        }

        void CreateIMTables(sqlite3* db)
        {
            const char* req = "CREATE TABLE " IM_NAME " (" ENUM_IM_FIELDS(LIST_WITH_TYPES, COMMA, ) ");"
                "CREATE UNIQUE INDEX " IM_NAME "_timestamp_counterpart on " IM_NAME " ( timestamp, counterpart ) ;"
                "insert into SQLITE_SEQUENCE (name, seq) VALUES ('" IM_NAME "', 0);";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }

        void DeleteIMTables(sqlite3* db)
        {
            const char* req = "DROP TABLE IF EXISTS " IM_NAME ";";
            int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, db);
        }
    }

    void WalletDB::createTables(sqlite3* db, sqlite3* privateDb)
    {
        CreateStorageTable(db);
        CreateWalletMessageTable(db);
        CreatePrivateVariablesTable(privateDb);
        CreateVariablesTable(db);
        CreateAddressesTable(db, ADDRESSES_NAME);
        CreateTxParamsTable(db);
        CreateStatesTable(db);
        CreateLaserTables(db);
        CreateAssetsTable(db);
        CreateShieldedCoinsTable(db);
        CreateShieldedCoinsTableIndex(db);
        CreateNotificationsTable(db);
        CreateExchangeRatesTable(db);
        CreateVouchersTable(db);
        CreateExchangeRatesHistoryTable(db);
        CreateEventsTable(db);
        CreateTxSummaryTable(db);
        CreateVerificationTable(db);
        CreateDexOffersTable(db);
        CreateIMTables(db);
        CreateAppDataTable(db);
    }

    std::shared_ptr<WalletDB> WalletDB::initBase(const string& path, const SecString& password, bool separateDBForPrivateData)
    {
        if (isInitialized(path))
        {
            BEAM_LOG_ERROR() << path << " already exists.";
            throw DatabaseException("Database already exists");
        }

        sqlite3* db = nullptr;
        {
            int ret = sqlite3_open_v2(path.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
            throwIfError(ret, db);
        }

        sqlite3* sdb = db;

        if (separateDBForPrivateData)
        {
            int ret = sqlite3_open_v2((path+".private").c_str(), &sdb, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
            throwIfError(ret, sdb);
            enterKey(sdb, password);
        }

        enterKey(db, password);
        auto walletDB = make_shared<WalletDB>(db, sdb);
        walletDB->onPrepareToModify();
        createTables(walletDB->_db, walletDB->m_PrivateDB);

        storage::setVar(*walletDB, Version, DbVersion);

        return walletDB;

    }

    IWalletDB::Ptr WalletDB::open(const string& path, const SecString& password)
    {
        return open(path, password, nullptr);
    }

    IWalletDB::Ptr WalletDB::open(const string& path, const SecString& password, const IPrivateKeyKeeper2::Ptr& pKeyKeeper)
    {
        if (!isInitialized(path))
        {
            BEAM_LOG_ERROR() << path << " not found, please init the wallet before.";
            throw DatabaseNotFoundException();
        }

        sqlite3* db = nullptr;
        OpenAndMigrateIfNeeded(path, &db, password);
        sqlite3* sdb = db;
        string privatePath = path + ".private";
        bool separateDBForPrivateData = isInitialized(privatePath);
        if (separateDBForPrivateData)
        {
            OpenAndMigrateIfNeeded(privatePath, &sdb, password);
        }

        auto walletDB = make_shared<WalletDB>(db, sdb);
        {
            int ret = sqlite3_busy_timeout(walletDB->_db, BusyTimeoutMs);
            throwIfError(ret, walletDB->_db);
        }
        {
            walletDB->InitKeys(pKeyKeeper);

            int version = 0;
            storage::getVar(*walletDB, Version, version);

            // start new transaction
            walletDB->onPrepareToModify();
            // migration
            try
            {
                switch (version)
                {
                case DbVersion10:
                case DbVersion11:
                case DbVersion12:
                {
                    BEAM_LOG_INFO() << "Converting DB from format 10-11";

                    // storage table changes: removed [status], [createHeight], [lockedHeight], added [spentHeight]
                    // sqlite doesn't support column removal. So instead we'll rename this table, select the data, and insert it to the new table
                    //
                    // The missing data, [spentHeight] - can only be deduced strictly if the UTXO has a reference to the spending tx. Otherwise we'll have to put a dummy spentHeight.
                    // In case of a rollback there's a chance (albeit small) we won't notice the UTXO un-spent status. But in case of such a problem this should be fixed by the "UTXO rescan".

                    if (!IsTableCreated(walletDB.get(), STORAGE_NAME "_del"))
                    {
                        const char* req =
                            "ALTER TABLE " STORAGE_NAME " RENAME TO " STORAGE_NAME "_del;"
                            "DROP INDEX CoinIndex;"
                            "DROP INDEX ConfirmIndex;";

                        int ret = sqlite3_exec(walletDB->_db, req, NULL, NULL, NULL);
                        throwIfError(ret, walletDB->_db);
                    }

                    if (!IsTableCreated(walletDB.get(), STORAGE_NAME))
                    {
                        CreateStorageTable(walletDB->_db);
                    }

                    {
                        const char* req = "SELECT * FROM " STORAGE_NAME "_del;";

                        for (sqlite::Statement stm(walletDB.get(), req); stm.step(); )
                        {
                            Coin coin;
                            stm.get(0, coin.m_ID.m_Type);
                            stm.get(1, coin.m_ID.m_SubIdx);
                            stm.get(2, coin.m_ID.m_Idx);
                            stm.get(3, coin.m_ID.m_Value);

                            uint32_t status = 0;
                            stm.get(4, status);

                            stm.get(5, coin.m_maturity);
                            // createHeight - skip
                            stm.get(7, coin.m_confirmHeight);
                            // lockedHeight - skip
                            stm.get(9, coin.m_createTxId);
                            stm.get(10, coin.m_spentTxId);
                            stm.get(11, coin.m_OBSOLETTEsessionId);

                            if (Coin::Status::Spent == static_cast<Coin::Status>(status))
                            {
                                // try to guess the spentHeight
                                coin.m_spentHeight = coin.m_maturity; // init guess

                                if (coin.m_spentTxId)
                                {
                                    // we cannot use getTxParameter since it uses newer db scheme
                                    //storage::getTxParameter(*walletDB, coin.m_spentTxId.get(), TxParameterID::KernelProofHeight, coin.m_spentHeight);
                                    sqlite::Statement stm2(walletDB.get(), "SELECT value FROM " TX_PARAMS_NAME " WHERE txID=?1 AND paramID=?2;");
                                    stm2.bind(1, coin.m_spentTxId.get());
                                    stm2.bind(2, TxParameterID::KernelProofHeight);

                                    if (stm2.step())
                                    {
                                        ByteBuffer buf;
                                        stm2.get(0, buf);
                                        Height h = 0;
                                        if (fromByteBuffer(buf, h))
                                        {
                                            coin.m_spentHeight = h;
                                        }
                                    }
                                }
                            }

                            walletDB->saveCoin(coin);
                        }
                    }

                    {
                        const char* req = "DROP TABLE " STORAGE_NAME "_del;";
                        int ret = sqlite3_exec(walletDB->_db, req, NULL, NULL, NULL);
                        throwIfError(ret, walletDB->_db);
                    }
                }

                // no break;

                case DbVersion13:
                    BEAM_LOG_INFO() << "Converting DB to format 13...";

                    CreateWalletMessageTable(walletDB->_db);
                    CreatePrivateVariablesTable(walletDB->m_PrivateDB);

                    if (!MoveSeedToPrivateVariables(*walletDB))
                    {
                        throw DatabaseException("failed to move seed to private valiables");
                    }

                case DbVersion14:
                {
                    BEAM_LOG_INFO() << "Converting DB from format 14...";

                    // tx_params table changed: added new column [subTxID]
                    // move old data to temp table
                    {
                        const char* req = "ALTER TABLE " TX_PARAMS_NAME " RENAME TO " TX_PARAMS_NAME "_del;";
                        int ret = sqlite3_exec(walletDB->_db, req, NULL, NULL, NULL);
                        throwIfError(ret, walletDB->_db);
                    }

                    // create new table
                    CreateTxParamsTable(walletDB->_db);

                    // migration
                    {
                        const char* req = "INSERT INTO " TX_PARAMS_NAME " (" ENUM_TX_PARAMS_FIELDS(LIST, COMMA, ) ") SELECT \"txID\", ?1 as \"subTxID\", \"paramID\", \"value\" FROM " TX_PARAMS_NAME "_del;";
                        sqlite::Statement stm(walletDB.get(), req);
                        stm.bind(1, kDefaultSubTxID);
                        stm.step();
                    }

                    // remove tmp table
                    {
                        const char* req = "DROP TABLE " TX_PARAMS_NAME "_del;";
                        int ret = sqlite3_exec(walletDB->_db, req, NULL, NULL, NULL);
                        throwIfError(ret, walletDB->_db);
                    }
                }
                // no break;

                case DbVersion15:
                    BEAM_LOG_INFO() << "Converting DB from format 15...";

                    // originally there was a coin migration, because "assetId" column was added.
                    // We now postpone this migration till 18-19, where the "assetId" column was moved into an index.

                    // no break; 
                case DbVersion16:
                    BEAM_LOG_INFO() << "Converting DB from format 16...";
                    CreateLaserAddressTable(walletDB->_db);

                    // no break;
                case DbVersion17:
                    BEAM_LOG_INFO() << "Converting DB from format 17...";
                    CreateAssetsTable(walletDB->_db);
                    // no break

                case DbVersion18:
                    BEAM_LOG_INFO() << "Converting DB from format 18...";
                    walletDB->MigrateCoins();
                    CreateNotificationsTable(walletDB->_db);
                    CreateExchangeRatesTable29(walletDB->_db);
                    AddAddressIdentityColumn(walletDB.get(), walletDB->_db);
                    // no break

                case DbVersion19:
                    BEAM_LOG_INFO() << "Converting DB from format 19...";
                    CreateShieldedCoinsTable(walletDB->_db);
                    // no break

                case DbVersion20:
                    BEAM_LOG_INFO() << "Converting DB from format 20...";
                    MigrateAssetsFrom20(walletDB->_db);
                    // no break

                case DbVersion21:
                    BEAM_LOG_INFO() << "Converting DB from format 21...";
                    CreateVouchersTable(db);
                    // no break

                case DbVersion22:
                    CreateShieldedCoinsTableIndex(db);
                    // no break

                case DbVersion23:
                    BEAM_LOG_INFO() << "Converting DB from format 23...";
                    if (!VouchersHasFlags(walletDB.get()))
                    {
                        AddVouchersFlagsColumn(db);
                    }
                    // no break

                case DbVersion24:
                    BEAM_LOG_INFO() << "Converting DB from format 24...";
                    MigrateAddressesFrom24(walletDB.get(), walletDB->_db);
                    // no break

                case DbVersion25:
                    BEAM_LOG_INFO() << "Converting DB from format 25...";
                    CreateExchangeRatesHistoryTable29(walletDB->_db);
                    // no break

                case DbVersion26:
                    BEAM_LOG_INFO() << "Converting DB from format 26...";
                    MigrateTransactionsFrom26(walletDB.get());
                    // no break

                case DbVersion27:
                    BEAM_LOG_INFO() << "Converting DB from format 27...";
                    CreateTxSummaryTable(walletDB->_db);
                    walletDB->FillTxSummaryTable();
                    // no break

                case DbVersion28:
                    BEAM_LOG_INFO() << "Converting DB from format 28...";
                    CreateEventsTable(walletDB->_db);
                    // no break

                case DbVersion29:
                    BEAM_LOG_INFO() << "Converting DB from format 29...";
                    MigrateRatesFrom29(walletDB.get(), walletDB->_db);
                    MigrateRatesHistoryFrom29(walletDB.get(), walletDB->_db);
                    MigrateTxRatesFrom29to30(walletDB.get(), walletDB->_db);
                    // no break

                case DbVersion30:
                    BEAM_LOG_INFO() << "Converting DB from format 30...";
                    MigrateTransactionsFrom30(walletDB.get(), walletDB->_db);
                    // no break

                case DbVersion31:
                    BEAM_LOG_INFO() << "Converting DB from format 31...";
                    MigrateFrom31(walletDB.get(), walletDB->_db);
                    // no break

                case DbVersion32:
                    BEAM_LOG_INFO() << "Converting DB from format 32...";
                    if (version > DbVersion20)
                        MigrateFrom32_After20(walletDB->_db);

                case DbVersion33:
                    BEAM_LOG_INFO() << "Converting DB from format 33...";
                    CreateDexOffersTable(walletDB->_db);
                    // no break

                case DbVersion34:
                    BEAM_LOG_INFO() << "Converting DB from format 34...";
                    CreateIMTables(walletDB->_db);
                    // no break

                case DbVersion35:
                    BEAM_LOG_INFO() << "Converting DB from format 35...";
                    walletDB->DeleteNonceAddresses();
                    // no break

                case DbVersion36:
                    BEAM_LOG_INFO() << "Converting DB from format 36...";
                    DeleteIMTables(walletDB->_db);
                    CreateIMTables(walletDB->_db);
                    // no break

                case DbVersion37:
                    BEAM_LOG_INFO() << "Converting DB from format 37...";

                    CreateTxParamsIndex(walletDB->_db);
                    CreateAppDataTable(walletDB->_db);

                    // no break

                case DbVersion38:
                    BEAM_LOG_INFO() << "Converting DB from format 38...";

                    if (version > DbVersion20)
                    {
                        const char* req = "DROP TABLE IF EXISTS " LASER_CHANNELS_NAME ";";
                        int ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
                        throwIfError(ret, db);

                        req = "DROP TABLE IF EXISTS " LASER_ADDRESSES_NAME ";";
                        ret = sqlite3_exec(db, req, nullptr, nullptr, nullptr);
                        throwIfError(ret, db);

                        CreateLaserTables(walletDB->_db);
                    }

                    storage::setVar(*walletDB, Version, DbVersion);

                case DbVersion:
                    // drop private variables from public database for cold wallet
                    if (separateDBForPrivateData && !DropPrivateVariablesFromPublicDatabase(*walletDB))
                    {
                        throw DatabaseException("failed to drop private variables from public database");
                    }
                    break; // ok

                default:
                {
                    BEAM_LOG_DEBUG() << "Invalid DB version: " << version << ". Expected: " << DbVersion;
                    throw InvalidDatabaseVersionException();
                }
                }

                walletDB->flushDB();
            }
            catch (std::runtime_error& err)
            {
                BEAM_LOG_ERROR() << "Database migration failed: " << err.what();
                walletDB->rollbackDB();
                throw DatabaseMigrationException();
            }
        }
        {
            const char* req = "SELECT name FROM sqlite_master WHERE type='table' AND name='" STORAGE_NAME "';";
            int ret = sqlite3_exec(walletDB->_db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, walletDB->_db);
        }

        {
            const char* req = "SELECT " STORAGE_FIELDS " FROM " STORAGE_NAME ";";
            int ret = sqlite3_exec(walletDB->_db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, walletDB->_db);
        }

        {
            const char* req = "SELECT " VARIABLES_FIELDS " FROM " VARIABLES_NAME ";";
            int ret = sqlite3_exec(walletDB->_db, req, nullptr, nullptr, nullptr);
            throwIfError(ret, walletDB->_db);
        }

        walletDB->getVarRaw(COIN_CONFIRMATIONS_COUNT, &walletDB->m_coinConfirmationsOffset, sizeof(uint32_t));
        walletDB->m_Initialized = true;
        return static_pointer_cast<IWalletDB>(walletDB);
    }

    void WalletDB::MigrateCoins()
    {
        // migrate coins
        if (!IsTableCreated(this, STORAGE_NAME "_del"))
        {
            const char* req =
                "ALTER TABLE " STORAGE_NAME " RENAME TO " STORAGE_NAME "_del;"
                "DROP INDEX CoinIndex;"
                "DROP INDEX ConfirmIndex;";

            int ret = sqlite3_exec(_db, req, NULL, NULL, NULL);
            throwIfError(ret, _db);
        }

        if (!IsTableCreated(this, STORAGE_NAME))
        {
            CreateStorageTable(_db);
        }

        {
            const char* req = "SELECT * FROM " STORAGE_NAME "_del;";
            for (sqlite::Statement stm(this, req); stm.step();)
            {
                Coin coin;
                stm.get(0, coin.m_ID.m_Type);
                stm.get(1, coin.m_ID.m_SubIdx);
                stm.get(2, coin.m_ID.m_Idx);
                stm.get(3, coin.m_ID.m_Value);
                stm.get(4, coin.m_maturity);
                stm.get(5, coin.m_confirmHeight);
                stm.get(6, coin.m_spentHeight);
                stm.get(7, coin.m_createTxId);
                stm.get(8, coin.m_spentTxId);
                stm.get(9, coin.m_OBSOLETTEsessionId);

                saveCoin(coin);
            }
        }

        {
            const char* req = "DROP TABLE " STORAGE_NAME "_del;";
            int ret = sqlite3_exec(_db, req, NULL, NULL, NULL);
            throwIfError(ret, _db);
        }
    }
}
