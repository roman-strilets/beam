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

#pragma once

// Definitions shared by all parts of WalletDB implementation (wallet_db*.cpp).
// Must be the first include of each part. Not intended for use outside of them.

#include "wallet_db.h"

#include "utility/logger.h"
#include "utility/helpers.h"
#include "sqlite/sqlite3.h"
#include "core/storage/block_rw.h"
#include "wallet/core/common/common.h"
#include "wallet/core/common/common_utils.h"
#include <sstream>
#include <boost/functional/hash.hpp>
#ifndef __EMSCRIPTEN__
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;
#else
#include <emscripten.h>
#include <emscripten/threading.h>
#include <filesystem>
namespace fs = std::filesystem;
#endif
#include <core/consensus/block_crypt.h>
#include <core/consensus/shielded.h>
#include "nlohmann/json.hpp"
#include "utility/std_extension.h"
#include "keykeeper/local_private_key_keeper.h"
#include "wallet/core/common/strings_resources.h"
#include "core/base/uintBig.h"
#include "utility/test_helpers.h"
#include <queue>
#include <unordered_map>
#include <boost/algorithm/string.hpp>

#include "wallet/core/tx/base_transaction.h"
#include "node/processor/processor.h"
#include "wallet/core/common/strings_resources.h"
#include <boost/format.hpp>

#define NOSEP
#define COMMA ", "
#define AND " AND "


#define ENUM_STORAGE_ID(each, sep, obj) \
    each(Type,           ID.m_Type,     INTEGER NOT NULL, obj) sep \
    each(SubKey,         ID.m_SubIdx,   INTEGER NOT NULL, obj) sep \
    each(Number,         ID.m_Idx,      INTEGER NOT NULL, obj) sep \
    each(amount,         ID.m_Value,    INTEGER NOT NULL, obj) sep \
    each(assetId,        ID.m_AssetID,  INTEGER, obj)

#define ENUM_STORAGE_FIELDS(each, sep, obj) \
    each(maturity,       maturity,            INTEGER NOT NULL, obj) sep \
    each(confirmHeight,  confirmHeight,       INTEGER, obj) sep \
    each(spentHeight,    spentHeight,         INTEGER, obj) sep \
    each(createTxId,     createTxId,          BLOB, obj) sep \
    each(spentTxId,      spentTxId,           BLOB, obj) sep \
    each(sessionId,      OBSOLETTEsessionId,  INTEGER NOT NULL, obj)

#define ENUM_ALL_STORAGE_FIELDS(each, sep, obj) \
    ENUM_STORAGE_ID(each, sep, obj) sep \
    ENUM_STORAGE_FIELDS(each, sep, obj)

#define LIST(name, member, type, obj) #name
#define LIST_WITH_TYPES(name, member, type, obj) #name " " #type

#define STM_BIND_LIST(name, member, type, obj) stm.bind(++colIdx, obj .m_ ## member);
#define STM_GET_LIST(name, member, type, obj) stm.get(colIdx++, obj .m_ ## member);

#define BIND_LIST(name, member, type, obj) "?"
#define SET_LIST(name, member, type, obj) #name "=?"

#define STORAGE_FIELDS ENUM_ALL_STORAGE_FIELDS(LIST, COMMA, )

#define STORAGE_WHERE_ID " WHERE " ENUM_STORAGE_ID(SET_LIST, AND, )
#define STORAGE_BIND_ID(obj) ENUM_STORAGE_ID(STM_BIND_LIST, NOSEP, obj)

#define STORAGE_NAME "storage"
#define VARIABLES_NAME "variables"
#define ADDRESSES_NAME "addresses"
#define TX_PARAMS_NAME "txparams"
#define PRIVATE_VARIABLES_NAME "PrivateVariables"
#define WALLET_MESSAGE_NAME "WalletMessages"
#define INCOMING_WALLET_MESSAGE_NAME "IncomingWalletMessages"
#define LASER_CHANNELS_NAME "LaserChannels"
#define LASER_ADDRESSES_NAME "LaserAddresses"
#define ASSETS_NAME "Assets"
#define SHIELDED_COINS_NAME "ShieldedCoins"
#define NOTIFICATIONS_NAME "notifications"
#define EXCHANGE_RATES_NAME "exchangeRates"
#define EXCHANGE_RATES_HISTORY_NAME "exchangeRatesHistory"
#define VERIFICATION_NAME "assetsVerification"
#define VOUCHERS_NAME "vouchers"
#define COIN_CONFIRMATIONS_COUNT "confirmations_count"
#define EVENTS_NAME "events"
#define TX_SUMMARY_NAME "tx_summary"
#define DEX_OFFERS_NAME "dex_offers"
#define IM_NAME "IM"
#define LAST_READ_IM_ID "LastReadIMId"

#define TblAppData "appdata"
#define TblAppData_Name "name"
#define TblAppData_Key "key"
#define TblAppData_Val "val"

#define ENUM_VARIABLES_FIELDS(each, sep, obj) \
    each(name,  name,  TEXT UNIQUE, obj) sep \
    each(value, value, BLOB, obj)

#define VARIABLES_FIELDS ENUM_VARIABLES_FIELDS(LIST, COMMA, )

#define ENUM_ADDRESS_FIELDS(each, sep, obj) \
    each(walletID,       BbsAddr,        BLOB, obj) sep \
    each(label,          label,          TEXT NOT NULL, obj) sep \
    each(category,       category,       TEXT, obj) sep \
    each(createTime,     createTime,     INTEGER, obj) sep \
    each(duration,       duration,       INTEGER, obj) sep \
    each(OwnID,          OwnID,          INTEGER NOT NULL, obj) sep \
    each(Identity,       Endpoint,       BLOB, obj) sep \
    each(Address,        Token,        TEXT NOT NULL PRIMARY KEY, obj) 

#define ENUM_TX_PARAMS_FIELDS(each, sep, obj) \
    each(txID,           txID,           BLOB NOT NULL , obj) sep \
    each(subTxID,        subTxID,        INTEGER NOT NULL , obj) sep \
    each(paramID,        paramID,        INTEGER NOT NULL , obj) sep \
    each(value,          value,          BLOB, obj)

#define ENUM_WALLET_MESSAGE_FIELDS(each, sep, obj) \
    each(ID,  ID,  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, obj) sep \
    each(PeerID, PeerID,   BLOB, obj) sep \
    each(Message, Message, BLOB, obj)

#define ENUM_INCOMING_WALLET_MESSAGE_FIELDS(each, sep, obj) \
    each(ID,  ID,  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, obj) sep \
    each(Channel, Channel, INTEGER, obj) sep \
    each(Message, Message, BLOB, obj)

#define TblStates            "States"
#define TblStates_Height     "Height"
#define TblStates_Hdr        "State"

#define ENUM_LASER_CHANNEL_FIELDS(each, sep, obj) \
    each(chID,             chID,             BLOB NOT NULL PRIMARY KEY, obj) sep \
    each(bbsID,            bbsID,            INTEGER NOT NULL, obj) sep \
    each(trgWID,           trgWID,           BLOB NOT NULL, obj) sep \
    each(state,            state,            INTEGER NOT NULL, obj) sep \
    each(fee,              fee,              INTEGER, obj) sep \
    each(locktime,         locktime,         INTEGER, obj) sep \
    each(amountMy,         amountMy,         INTEGER, obj) sep \
    each(amountTrg,        amountTrg,        INTEGER, obj) sep \
    each(amountCurrentMy,  amountCurrentMy,  INTEGER, obj) sep \
    each(amountCurrentTrg, amountCurrentTrg, INTEGER, obj) sep \
    each(lockHeight,       lockHeight,       INTEGER, obj) sep \
    each(bbsTimestamp,     bbsTimestamp,     INTEGER, obj) sep \
    each(data,             data,             BLOB, obj)

#define LASER_CHANNEL_FIELDS ENUM_LASER_CHANNEL_FIELDS(LIST, COMMA, )

#define ENUM_ASSET_FIELDS_0(each, sep, obj) \
    each(ID,             ID,            INTEGER NOT NULL PRIMARY KEY,  obj) sep \
    each(Value,          Value,         BLOB,              obj) sep \
    each(Owner,          Owner,         BLOB NOT NULL,     obj) sep \
    each(LockHeight,     LockHeight,    INTEGER,           obj) sep \
    each(Metadata,       Metadata,      BLOB,              obj) sep \
    each(RefreshHeight,  RefreshHeight, INTEGER NOT NULL,  obj) sep \
    each(IsOwned,        IsOwned,       INTEGER,           obj)

#define ENUM_ASSET_FIELDS(each, sep, obj) \
    ENUM_ASSET_FIELDS_0(each, sep, obj) sep \
    each(Deposit,        Deposit,       INTEGER,           obj)

#define ENUM_SHIELDED_COIN_FIELDS(each, sep, obj) \
    each(Key,                   CoinID.m_Key,         BLOB NOT NULL PRIMARY KEY, obj) sep \
    each(User,                  CoinID.m_User,        BLOB, obj) sep \
    each(ID,                    TxoID,                INTEGER NOT NULL, obj) sep \
    each(assetID,               CoinID.m_AssetID,     INTEGER, obj) sep \
    each(value,                 CoinID.m_Value,       INTEGER NOT NULL, obj) sep \
    each(confirmHeight,         confirmHeight,        INTEGER, obj) sep \
    each(spentHeight,           spentHeight,          INTEGER, obj) sep \
    each(createTxId,            createTxId,           BLOB, obj) sep \
    each(spentTxId,             spentTxId,            BLOB, obj)

#define SHIELDED_COIN_FIELDS ENUM_SHIELDED_COIN_FIELDS(LIST, COMMA, )

#define ENUM_NOTIFICATION_FIELDS(each, sep, obj) \
    each(ID,            ID,             BLOB NOT NULL PRIMARY KEY, obj) sep \
    each(type,          type,           INTEGER,            obj) sep \
    each(state,         state,          INTEGER,            obj) sep \
    each(createTime,    createTime,     INTEGER,            obj) sep \
    each(content,       content,        BLOB NOT NULL,      obj)

#define ENUM_VERIFICATION_FIELDS(each, sep, obj) \
    each(assetID,       assetID,        INTEGER,  obj) sep \
    each(verified,      verified,       INTEGER,  obj) sep \
    each(icon,          icon,           TEXT,     obj) sep \
    each(color,         color,          TEXT,     obj) sep \
    each(updateTime,    updateTime,     INTEGER,  obj)

#define ENUM_EXCHANGE_RATES_FIELDS29(each, sep, obj) \
    each(currency,      currency,       INTEGER,            obj) sep \
    each(unit,          unit,           INTEGER,            obj) sep \
    each(rate,          rate,           INTEGER,            obj) sep \
    each(updateTime,    updateTime,     INTEGER,            obj)

#define ENUM_EXCHANGE_RATES_FIELDS(each, sep, obj) \
    each(cfrom,         from.m_value,   TEXT NOT NULL,      obj) sep \
    each(cto,           to.m_value,     TEXT NOT NULL,      obj) sep \
    each(rate,          rate,           INTEGER,            obj) sep \
    each(updateTime,    updateTime,     INTEGER,            obj)

#define ENUM_EXCHANGE_RATES_HISTORY_FIELDS29(each, sep, obj) \
    each(height,        height,         INTEGER,            obj) sep \
    each(currency,      currency,       INTEGER,            obj) sep \
    each(unit,          unit,           INTEGER,            obj) sep \
    each(rate,          rate,           INTEGER,            obj) sep \
    each(updateTime,    updateTime,     INTEGER,            obj)

#define ENUM_EXCHANGE_RATES_HISTORY_FIELDS(each, sep, obj) \
    each(height,        height,         INTEGER,            obj) sep \
    each(cfrom,         from.m_value,   TEXT NOT NULL,      obj) sep \
    each(cto,           to.m_value,     TEXT NOT NULL,      obj) sep \
    each(rate,          rate,           INTEGER,            obj) sep \
    each(updateTime,    updateTime,     INTEGER,            obj)

#define ENUM_VOUCHERS_FIELDS(each, sep, obj) \
    each(WalletID,  WalletID,  BLOB NOT NULL, obj) sep \
    each(Voucher,   Voucher,   BLOB NOT NULL UNIQUE, obj) sep \
    each(Flags,     Flags,     INTEGER, obj)


#define ENUM_EVENTS_FIELDS(each, sep, obj) \
    each(Height,  Height,  INTEGER NOT NULL, obj) sep \
    each(Body,    Body,   BLOB NOT NULL, obj) sep \
    each(Key,     Key,    BLOB NOT NULL, obj)

#define ENUM_DEX_OFFERS_FIELDS(each, sep, obj) \
    each(id,               id,             TEXT NOT NULL PRIMARY KEY, obj) sep \
    each(data,             data,           BLOB, obj) sep \
    each(isMine,           isMine,         INTEGER, obj)

#define ENUM_TX_SUMMARY_FIELDS(each) \
    each(CreateTime, Timestamp) \
    BEAM_TX_LIST_FILTER_MAP(each)

#define ENUM_IM_FIELDS(each, sep, obj) \
    each(id,          id,             INTEGER PRIMARY KEY AUTOINCREMENT, obj) sep \
    each(timestamp,   timestamp,      INTEGER NOT NULL, obj) sep \
    each(counterpart, counterpart,    BLOB NOT NULL,    obj) sep \
    each(mySbbs,      mySbbs,         BLOB NOT NULL,    obj) sep \
    each(message,     message,        BLOB NOT NULL,    obj) sep \
    each(is_income,   is_income,      INTEGER,    obj) sep \
    each(is_read,     is_read,        INTEGER,    obj)

//namespace std
//{
//    template<>
//    struct hash<pair<beam::Amount, beam::Amount>>
//    {
//        typedef pair<beam::Amount, beam::Amount> argument_type;
//        typedef std::size_t result_type;
//
//        result_type operator()(const argument_type& a) const noexcept
//        {
//            return boost::hash<argument_type>()(a);
//        }
//    };
//}

namespace beam::wallet
{
    using namespace std;

    inline void throwIfError(int res, sqlite3* db)
    {
        if (res == SQLITE_OK)
        {
            return;
        }
        stringstream ss;
        ss << "sqlite error code=" << res << ", " << sqlite3_errmsg(db);
        if (res == SQLITE_NOTADB)
        {
            throw FileIsNotDatabaseException();
        }
        throw DatabaseException(ss.str());
    }

    namespace
    {
        const char* const WalletSeed = "WalletSeed";

        template<typename T>
        void deserialize(T& value, const ByteBuffer& blob)
        {
            fromByteBuffer(blob, value);
        }
    }

    namespace sqlite
    {
        struct Statement
        {
            Statement(const WalletDB* db, const char* sql, bool privateDB = false)
                : _walletDB(nullptr)
                , _db(privateDB ? db->m_PrivateDB : db->_db)
                , _stm(nullptr)
            {
                int ret = sqlite3_prepare_v2(_db, sql, -1, &_stm, nullptr);
                throwIfError(ret, _db);
            }

            Statement(WalletDB* db, const char* sql, bool privateDB = false)
                : _walletDB(db)
                , _db(privateDB ? db->m_PrivateDB : db->_db)
                , _stm(nullptr)
            {
                if (_walletDB)
                {
                    _walletDB->onPrepareToModify();
                }
                int ret = sqlite3_prepare_v2(_db, sql, -1, &_stm, nullptr);
                throwIfError(ret, _db);
            }

            void Reset()
            {
                sqlite3_clear_bindings(_stm);
                sqlite3_reset(_stm);
            }

            void bind(int col, int val)
            {
                int ret = sqlite3_bind_int(_stm, col, val);
                throwIfError(ret, _db);
            }

            void bind(int col, Key::Type val)
            {
                int ret = sqlite3_bind_int(_stm, col, val);
                throwIfError(ret, _db);
            }

            void bind(int col, uint64_t val)
            {
                int ret = sqlite3_bind_int64(_stm, col, val);
                throwIfError(ret, _db);
            }

            void bind(int col, uint32_t val)
            {
                int ret = sqlite3_bind_int(_stm, col, val);
                throwIfError(ret, _db);
            }
            
            template<typename EnumType, typename = EnumTypesOnly<EnumType>>
            void bind(int col, EnumType type)
            {
                bind(col, underlying_cast(type));
            }

            void bind(int col, const TxID& id)
            {
                bind(col, id.data(), id.size());
            }

            void bind(int col, const boost::optional<TxID>& id)
            {
                if (id.is_initialized())
                {
                    bind(col, *id);
                }
                else
                {
                    bind(col, nullptr, 0);
                }
            }

            void bind(int col, const ECC::Hash::Value& hash)
            {
                bind(col, hash.m_pData, hash.nBytes);
            }

            void bind(int col, const WalletID& x)
            {
                bind(col, &x, sizeof(x));
            }

            void bind(int col, const io::Address& address)
            {
                bind(col, address.u64());
            }

            void bind(int col, const ByteBuffer& m)
            {
                bind(col, m.data(), m.size());
            }

            void bind(int col, const Blob& b)
            {
                // According to our convention empty blob is NOT NULL, it should be an empty BLOB field.
                // During initialization from buffer, if the buffer size is 0 - the x.p is left uninitialized.
                //
                // In sqlite code if x.p is NULL - it would treat the field as NULL, rather than an empty blob.
                // And if the uninitialized x.p is occasionally NULL - we get wrong behavior.
                //
                // Hence - we work this around, use `this`, as an arbitrary non-NULL pointer
                const void* pPtr = b.n ? b.p : this;
                bind(col, pPtr, b.n);
            }

            template<uint32_t nBytes_>
            void bind(int col, const uintBig_t<nBytes_>& data)
            {
                bind(col, data.m_pData, static_cast<size_t>(nBytes_));
            }

            void bind(int col, const void* blob, size_t size)
            {
                if (size > static_cast<size_t>(numeric_limits<int32_t>::max()))// 0x7fffffff
                {
                    throwIfError(SQLITE_TOOBIG, _db);
                }
                int ret = sqlite3_bind_blob(_stm, col, blob, static_cast<int>(size), nullptr);
                throwIfError(ret, _db);
            }

            void bind(int col, const Block::SystemState::Full& s)
            {
                bind(col, &s, sizeof(s));
            }

            void bind(int col, const ShieldedTxo::BaseKey& x)
            {
                const auto& b = _buffers.emplace_back(toByteBuffer(x));
                bind(col, b);
            }

            void bind(int col, const ShieldedTxo::Voucher& x)
            {
                const auto& b = _buffers.emplace_back(toByteBuffer(x));
                bind(col, b);
            }

            void bind(int col, const ShieldedTxo::User& x)
            {
                bind(col, &x, sizeof(x));
            }

            void bind(int col, const char* val)
            {
                int ret = sqlite3_bind_text(_stm, col, val, -1, nullptr);
                throwIfError(ret, _db);
            }

            void bind(int col, const string& val) // utf-8
            {
                int ret = sqlite3_bind_text(_stm, col, val.data(), -1, nullptr);
                throwIfError(ret, _db);
            }

            void bind(int col, TxParameterID val)
            {
                int ret = sqlite3_bind_int(_stm, col, static_cast<int>(val));
                throwIfError(ret, _db);
            }

            void bind(int col, const ECC::Scalar& scalar)
            {
                const auto& b = _buffers.emplace_back(toByteBuffer(scalar));
                bind(col, b);
            }

            void bind(int col, const ECC::Point& point)
            {
                const auto& b = _buffers.emplace_back(toByteBuffer(point));
                bind(col, b);
            }

            bool step()
            {
                int n = _walletDB ? sqlite3_total_changes(_db) : 0;
                int ret = sqlite3_step(_stm);
                if (_walletDB && sqlite3_total_changes(_db) != n)
                {
                    _walletDB->onModified();
                }
                switch (ret)
                {
                case SQLITE_ROW: return true;   // has another row ready continue
                case SQLITE_DONE: return false; // has finished executing stop;
                default:
                    throwIfError(ret, _db);
                    return false; // and stop
                }
            }

            void get(int col, uint64_t& val)
            {
                val = sqlite3_column_int64(_stm, col);
            }

            void get(int col, uint32_t& val)
            {
                val = sqlite3_column_int(_stm, col);
            }

            void get(int col, int& val)
            {
                val = sqlite3_column_int(_stm, col);
            }

            template<typename EnumType, typename = EnumTypesOnly<EnumType>>
            void get(int col, EnumType& enumValue)
            {
                UnderlyingType<EnumType> temp;
                get(col, temp);
                enumValue = static_cast<EnumType>(temp);
            }

            void get(int col, bool& val)
            {
                val = sqlite3_column_int(_stm, col) == 0 ? false : true;
            }

            void get(int col, TxID& id)
            {
                getBlobStrict(col, static_cast<void*>(id.data()), static_cast<int>(id.size()));
            }

            void get(int col, Block::SystemState::Full& s)
            {
                // read/write as a blob, skip serialization
                getBlobStrict(col, &s, sizeof(s));
            }

            void get(int col, ShieldedTxo::BaseKey& x)
            {
                ByteBuffer b;
                get(col, b);
                fromByteBuffer(b, x);
            }

            void get(int col, ShieldedTxo::Voucher& x)
            {
                ByteBuffer b;
                get(col, b);
                fromByteBuffer(b, x);
            }

            void get(int col, ShieldedTxo::User& x)
            {
                // read/write as a blob, skip serialization
                getBlobStrict(col, &x, sizeof(x));
            }

            void get(int col, boost::optional<TxID>& id)
            {
                TxID val;
                if (getBlobSafe(col, &val, sizeof(val)))
                    id = val;
            }

            void get(int col, ECC::Hash::Value& hash)
            {
                getBlobStrict(col, hash.m_pData, hash.nBytes);
            }

            void get(int col, PeerID& x)
            {
                if (!getBlobSafe(col, &x, sizeof(x)))
                {
                    x = Zero;
                }
            }

            void get(int col, WalletID& x)
            {
                getBlobStrict(col, &x, sizeof(x));
            }

            void get(int col, io::Address& address)
            {
                uint64_t t = 0;;
                get(col, t);
                address = io::Address::from_u64(t);
            }
            void get(int col, ByteBuffer& b)
            {
                b.clear();

                int size = sqlite3_column_bytes(_stm, col);
                if (size > 0)
                {
                    b.resize(size);
                    memcpy(&b.front(), sqlite3_column_blob(_stm, col), size);
                }
            }

            void get(int col, ECC::Scalar& scalar)
            {
                ByteBuffer b;
                get(col, b);
                fromByteBuffer(b, scalar);
            }

            void get(int col, ECC::Point& point)
            {
                ByteBuffer b;
                get(col, b);
                fromByteBuffer(b, point);
            }

            template<uint32_t nBytes_>
            void get(int col, uintBig_t<nBytes_>& data)
            {
                uint32_t size = sqlite3_column_bytes(_stm, col);
                if (size == nBytes_)
                    memcpy(data.m_pData, sqlite3_column_blob(_stm, col), size);
                else
                    throw std::runtime_error("wrong blob size");
            }

            bool getBlobSafe(int col, void* blob, int size)
            {
                if (sqlite3_column_bytes(_stm, col) != size)
                    return false;

                memcpy(blob, sqlite3_column_blob(_stm, col), size);
                return true;
            }

            void getBlobStrict(int col, void* blob, int size)
            {
                if (!getBlobSafe(col, blob, size))
                    throw std::runtime_error("wdb corruption");
            }

            void get(int col, Key::Type& type)
            {
                type = sqlite3_column_int(_stm, col);
            }

            void get(int col, string& str) // utf-8
            {
                int size = sqlite3_column_bytes(_stm, col);
                if (size > 0)
                {
                    const unsigned char* data = sqlite3_column_text(_stm, col);
                    str.assign(reinterpret_cast<const string::value_type*>(data));
                }
            }

            const char* retrieveSQL()
            {
                return sqlite3_expanded_sql(_stm);
            }

            bool IsNull(int col)
            {
                return SQLITE_NULL == sqlite3_column_type(_stm, col);
            }

            ~Statement()
            {
                sqlite3_finalize(_stm);
            }
        private:
            WalletDB* _walletDB;
            sqlite3 * _db;
            sqlite3_stmt* _stm;
            std::vector<ByteBuffer> _buffers;
        };

        struct Transaction
        {
            Transaction(sqlite3* db)
                : _db(db)
                , _commited(false)
                , _rollbacked(false)
            {
                begin();
            }

            ~Transaction()
            {
                if (!_commited && !_rollbacked)
                    rollback();
            }

            void begin()
            {
                int ret = sqlite3_exec(_db, "BEGIN EXCLUSIVE;", nullptr, nullptr, nullptr);
                throwIfError(ret, _db);
            }

            bool commit()
            {
                int ret = sqlite3_exec(_db, "COMMIT;", nullptr, nullptr, nullptr);

                _commited = (ret == SQLITE_OK);
                return _commited;
            }

            void rollback() noexcept
            {
                int ret = sqlite3_exec(_db, "ROLLBACK;", nullptr, nullptr, nullptr);
                _rollbacked = (ret == SQLITE_OK);
            }
        private:
            sqlite3 * _db;
            bool _commited;
            bool _rollbacked;
        };
    }

    namespace
    {
        inline bool GetPrivateVarRaw(const WalletDB& db, const char* name, void* data, int size, bool privateDb)
        {
            {
                sqlite::Statement stm(&db, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '" PRIVATE_VARIABLES_NAME "';", privateDb);
                if (!stm.step())
                {
                    return false; // public database
                }
            }

            {
                const char* req = "SELECT value FROM " PRIVATE_VARIABLES_NAME " WHERE name=?1;";

                sqlite::Statement stm(&db, req, privateDb);
                stm.bind(1, name);

                return stm.step() && stm.getBlobSafe(0, data, size);
            }
        }
    }
}
