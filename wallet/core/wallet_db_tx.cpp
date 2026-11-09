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
    void WalletDB::visitTx(std::function<bool(const TxDescription&)> func, const TxListFilter& filter) const
    {
        helpers::StopWatch sw;
        sw.start();

        std::string query = "SELECT TxID FROM " TX_SUMMARY_NAME;
        std::vector<std::string> parts;
        std::string whereParams;

#define MACRO(id, type) \
        if (filter.m_##id) \
        { \
            std::string q(#id "="); \
            q.append(std::to_string((int)*filter.m_##id)); \
            parts.push_back(std::move(q)); \
        } 
        BEAM_TX_LIST_NORMAL_PARAM_MAP(MACRO)
        
        if (!parts.empty())
        {
            whereParams.append(boost::join(parts, " AND "));
            parts.clear();
        }

        BEAM_TX_LIST_HEIGHT_MAP(MACRO)
#undef MACRO
        if (!parts.empty())
        {
            if (whereParams.empty())
            {
                whereParams.append(" (");
            }
            else
            {
                whereParams.append(" AND (");
            }
            whereParams.append(boost::join(parts, " OR "))
                       .append(")");
        }

        if (!whereParams.empty())
        {
            query.append(" WHERE ");
            query.append(whereParams);
        }
        
        query.append(" ORDER BY CreateTime DESC");

        sqlite::Statement stm(this, query.c_str());
        sqlite::Statement stm2(this, "SELECT * FROM " TX_PARAMS_NAME " WHERE txID=?1;");
        TxID txID;
        while (stm.step())
        {
            stm.get(0, txID);

            auto t = getTxImpl(txID, stm2);
            if (t.is_initialized())
            {
                if (!func(*t))
                {
                    break;
                }
            }
        }

        sw.stop();
        BEAM_LOG_DEBUG() << "visitTx elapsed time: " << sw.milliseconds() << " ms";
    }

    vector<TxDescription> WalletDB::getTxHistory(wallet::TxType txType, uint64_t start, int count) const
    {
        // TODO this is temporary solution
        int txCount = getTxCount(txType);

        vector<TxDescription> res;
        if (txCount > 0)
        {
            res.reserve(static_cast<size_t>(min(txCount, count)));
            std::string req = "SELECT DISTINCT txID FROM " TX_PARAMS_NAME;

            if (txType != wallet::TxType::ALL)
            {
                req += " WHERE paramID = ?3 AND value = ?4";
            }

            req += " LIMIT ?1 OFFSET ?2 ;";

            sqlite::Statement stm(this, req.c_str());
            stm.bind(1, count);
            stm.bind(2, start);

            ByteBuffer typeBlob;
            if (txType != wallet::TxType::ALL)
            {
                stm.bind(3, wallet::TxParameterID::TransactionType);
                typeBlob = toByteBuffer(txType);
                stm.bind(4, typeBlob);
            }

            const char* gtParamReq = "SELECT * FROM " TX_PARAMS_NAME " WHERE txID=?1;";
            sqlite::Statement stm2(this, gtParamReq);

            while (stm.step())
            {
                TxID txID;
                stm.get(0, txID);
                auto t = getTxImpl(txID, stm2);
                if (t.is_initialized())
                {
                    res.emplace_back(*t);
                }
            }
            sort(res.begin(), res.end(), [](const auto& left, const auto& right) {return left.m_createTime > right.m_createTime; });
        }

        return res;
    }

    int WalletDB::getTxCount(wallet::TxType txType) const
    {
        int txCount = 0;

        std::string req = "SELECT COUNT(DISTINCT txID) FROM " TX_PARAMS_NAME " WHERE paramID = ?1";
        req += (txType != wallet::TxType::ALL) ? " AND value = ?2 ;" : " ;";

        sqlite::Statement stm(this, req.c_str());
        stm.bind(1, wallet::TxParameterID::TransactionType);

        ByteBuffer typeBlob;
        if (txType != wallet::TxType::ALL)
        {
            typeBlob = toByteBuffer(txType);
            stm.bind(2, typeBlob);
        }

        stm.step();
        stm.get(0, txCount);


        return txCount;
    }

    boost::optional<TxDescription> WalletDB::getTx(const TxID& txId) const
    {
        // load only simple TX that supported by TxDescription
        const char* req = "SELECT * FROM " TX_PARAMS_NAME " WHERE txID=?1;";
        sqlite::Statement stm(this, req);
        return getTxImpl(txId, stm);
    }

    boost::optional<TxDescription> WalletDB::getTxImpl(const TxID& txId, sqlite::Statement& stm) const
    {
        stm.Reset();
        stm.bind(1, txId);

        TxDescription txDescription(txId);
        struct Set : public std::priority_queue<TxParameterID, std::vector<TxParameterID>, std::greater<TxParameterID> >
        {
            container_type::const_iterator begin() const noexcept { return c.begin(); }
            container_type::const_iterator end() const noexcept { return c.end(); }
        } gottenParams;

        while (stm.step())
        {
            TxParameter parameter = {};
            int colIdx = 0;
            ENUM_TX_PARAMS_FIELDS(STM_GET_LIST, NOSEP, parameter);
            auto parameterID = static_cast<TxParameterID>(parameter.m_paramID);

            txDescription.SetParameter(parameterID, std::move(parameter.m_value), static_cast<SubTxID>(parameter.m_subTxID));

            if (parameter.m_subTxID == kDefaultSubTxID)
            {
                gottenParams.emplace(parameterID);
            }
        }

        txDescription.fillFromTxParameters(txDescription);

        if (std::includes(gottenParams.begin(), gottenParams.end(), m_mandatoryTxParams.begin(), m_mandatoryTxParams.end()))
        {
            return txDescription;
        }

        return boost::optional<TxDescription>{};
    }

    void WalletDB::saveTx(const TxDescription& p)
    {
        ChangeAction action = ChangeAction::Added;

        storage::setTxParameter(*this, p.m_txId, TxParameterID::TransactionType, p.m_txType, false);
        storage::setTxParameter(*this, p.m_txId, TxParameterID::Amount, p.m_amount, false);
        storage::setTxParameter(*this, p.m_txId, TxParameterID::Fee, p.m_fee, false);
        storage::setTxParameter(*this, p.m_txId, TxParameterID::AssetID, p.m_assetId, false);
        storage::setTxParameter(*this, p.m_txId, TxParameterID::AssetMetadata, p.m_assetMeta, false);
        if (p.m_minHeight)
        {
            storage::setTxParameter(*this, p.m_txId, TxParameterID::MinHeight, p.m_minHeight, false);
        }
        storage::setTxParameter(*this, p.m_txId, TxParameterID::PeerAddr, p.m_peerAddr, false);
        if (p.m_myAddr != Zero)
        {
            storage::setTxParameter(*this, p.m_txId, TxParameterID::MyAddr, p.m_myAddr, false);
        }
        storage::setTxParameter(*this, p.m_txId, TxParameterID::Message, p.m_message, false);
        storage::setTxParameter(*this, p.m_txId, TxParameterID::CreateTime, p.m_createTime, false);
        storage::setTxParameter(*this, p.m_txId, TxParameterID::ModifyTime, p.m_modifyTime, false);
        storage::setTxParameter(*this, p.m_txId, TxParameterID::IsSender, p.m_sender, false);
        storage::setTxParameter(*this, p.m_txId, TxParameterID::Status, p.m_status, false);
        storage::setTxParameter(*this, p.m_txId, TxParameterID::IsSelfTx, p.m_selfTx, false);

        // notify only when full TX saved
        notifyTransactionChanged(action, {p});
    }

    void WalletDB::deleteTx(const TxID& txId)
    {
        auto tx = getTx(txId);
        if (tx.is_initialized())
        {
            // we left one record about tx type in order to avoid re-launching of deleted transaction
            const char* req = "DELETE FROM " TX_PARAMS_NAME " WHERE txID=?1 AND paramID!=?2;";
            sqlite::Statement stm(this, req);

            stm.bind(1, txId);
            stm.bind(2, TxParameterID::TransactionType);

            stm.step();
            deleteParametersFromCache(txId);
            notifyTransactionChanged(ChangeAction::Removed, { *tx });
        }

        {
            sqlite::Statement stm(this, "DELETE FROM " TX_SUMMARY_NAME " WHERE txID=?1;");
            stm.bind(1, txId);
            stm.step();
        }
    }

    void WalletDB::restoreCoinsSpentByTx(const TxID& txId)
    {
        std::vector<int> updatedRows;
        {
            const char* req = "SELECT rowid FROM " STORAGE_NAME " WHERE spentTxId=?1;";
            sqlite::Statement stm(this, req);
            stm.bind(1, txId);
            while(stm.step())
            {
                int& coin = updatedRows.emplace_back();
                stm.get(0, coin);
            }
        }
        if (!updatedRows.empty())
        {
            {
                const char* req = "UPDATE " STORAGE_NAME " SET spentTxId=NULL WHERE spentTxId=?1;";
                sqlite::Statement stm(this, req);
                stm.bind(1, txId);
                stm.step();
            }
            notifyCoinsChanged(ChangeAction::Updated, getCoinsByRowIDs(updatedRows));
        }
    }

    void WalletDB::deleteCoinsCreatedByTx(const TxID& txId)
    {
        std::vector<Coin> deletedItems;
        {
            const char* req = "SELECT " ENUM_ALL_STORAGE_FIELDS(LIST, COMMA, ) " FROM " STORAGE_NAME " WHERE createTxId=?1 AND confirmHeight=?2;";
            sqlite::Statement stm(this, req);
            stm.bind(1, txId);
            stm.bind(2, MaxHeight);
            while (stm.step())
            {
                Coin& coin = deletedItems.emplace_back();
                int colIdx = 0;
                ENUM_ALL_STORAGE_FIELDS(STM_GET_LIST, NOSEP, coin);
            }
        }
        if (!deletedItems.empty())
        {
            const char* req = "DELETE FROM " STORAGE_NAME " WHERE createTxId=?1 AND confirmHeight=?2;";
            sqlite::Statement stm(this, req);
            stm.bind(1, txId);
            stm.bind(2, MaxHeight);
            stm.step();

            notifyCoinsChanged(ChangeAction::Removed, deletedItems);
        }
    }

    bool WalletDB::setTxParameter(const TxID& txID, SubTxID subTxID, TxParameterID paramID, const ByteBuffer& blob, bool shouldNotifyAboutChanges, bool allowModify /* = true */)
    {
        if (auto txIter = m_TxParametersCache.find(txID); txIter != m_TxParametersCache.end())
        {
            if (auto subTxIter = txIter->second.find(subTxID); subTxIter != txIter->second.end())
            {
                if (auto pit = subTxIter->second.find(paramID); pit != subTxIter->second.end())
                {
                    if (pit->second && blob == *(pit->second))
                    {
                        return false;
                    }
                }
            }
        }

        bool hasTx = hasTransaction(txID);
        {
            sqlite::Statement stm(this, "SELECT * FROM " TX_PARAMS_NAME " WHERE txID=?1 AND subTxID=?2 AND paramID=?3;");

            stm.bind(1, txID);
            stm.bind(2, subTxID);
            stm.bind(3, paramID);

            if (stm.step())
            {
                // already set
                if (!allowModify)
                    return false;

                sqlite::Statement stm2(this, "UPDATE " TX_PARAMS_NAME  " SET value = ?4 WHERE txID = ?1 AND subTxID=?2 AND paramID = ?3;");
                stm2.bind(1, txID);
                stm2.bind(2, subTxID);
                stm2.bind(3, paramID);
                stm2.bind(4, blob);
                stm2.step();

                if (shouldNotifyAboutChanges)
                {
                    auto tx = getTx(txID);
                    if (tx.is_initialized())
                    {
                        notifyTransactionChanged(ChangeAction::Updated, { *tx });
                    }
                }

                insertParameterToCache(txID, subTxID, paramID, blob);
                OnTxSummaryParam(txID, subTxID, paramID, &blob);
                return true;
            }
        }
        
        sqlite::Statement stm(this, "INSERT INTO " TX_PARAMS_NAME " (" ENUM_TX_PARAMS_FIELDS(LIST, COMMA, ) ") VALUES(" ENUM_TX_PARAMS_FIELDS(BIND_LIST, COMMA, ) ");");
        TxParameter parameter;
        parameter.m_txID = txID;
        parameter.m_subTxID = subTxID;
        parameter.m_paramID = static_cast<int>(paramID);
        parameter.m_value = blob;
        int colIdx = 0;
        ENUM_TX_PARAMS_FIELDS(STM_BIND_LIST, NOSEP, parameter);
        stm.step();
        if (shouldNotifyAboutChanges)
        {
            auto tx = getTx(txID);
            if (tx.is_initialized())
            {
                notifyTransactionChanged(hasTx ? ChangeAction::Updated : ChangeAction::Added, { *tx });
            }
        }

        insertParameterToCache(txID, subTxID, paramID, blob);
        OnTxSummaryParam(txID, subTxID, paramID, &blob);
        return true;
    }

    template<typename T>
    void WalletDB::FillTxSummaryTableParam(const char* szField, TxParameterID paramID)
    {
        sqlite::Statement stm(this, "SELECT txID, value FROM " TX_PARAMS_NAME " WHERE paramID=?1 AND subTxID=?2;");
        stm.bind(1, paramID);
        stm.bind(2, kDefaultSubTxID);

        ByteBuffer buf;

        while (stm.step())
        {
            TxID txID;
            stm.get(0, txID);
            stm.get(1, buf);

            OnTxSummaryParam<T>(txID, szField, &buf);
            buf.clear();
        }
    }

    void WalletDB::FillTxSummaryTable()
    {
        int ret = sqlite3_exec(_db, "CREATE INDEX IF NOT EXISTS TxParamsValueIndex ON " TX_PARAMS_NAME " (paramID,subTxID);", nullptr, nullptr, nullptr);
        throwIfError(ret, _db);

#define THE_MACRO(id, type) FillTxSummaryTableParam<type>(#id, TxParameterID::id);
        ENUM_TX_SUMMARY_FIELDS(THE_MACRO)
#undef THE_MACRO

        ret = sqlite3_exec(_db, "DROP INDEX TxParamsValueIndex", nullptr, nullptr, nullptr);
        throwIfError(ret, _db);
    }


    void WalletDB::OnTxSummaryParam(const TxID& txID, SubTxID subTxID, TxParameterID paramID, const ByteBuffer* pBlob)
    {
        if (kDefaultSubTxID != subTxID)
            return;

        switch (paramID)
        {
#define THE_MACRO(id, type) case TxParameterID::id: OnTxSummaryParam<type>(txID, #id, pBlob); break;
        ENUM_TX_SUMMARY_FIELDS(THE_MACRO)

        default:
            // suppress warning
            break;
#undef THE_MACRO
        }
    }

    template<typename T>
    void WalletDB::OnTxSummaryParam(const TxID& txID, const char* szName, const ByteBuffer* pBlob)
    {
        char szQuery[0x100];
        snprintf(szQuery, _countof(szQuery), "UPDATE " TX_SUMMARY_NAME " SET %s=?1 WHERE TxID=?2", szName);

        T val = {};
        {
            sqlite::Statement stm(this, szQuery);
            stm.bind(2, txID);

            if (pBlob)
            {
                fromByteBuffer(*pBlob, val);
                stm.bind(1, val);
            }
            else
            {
                stm.bind(1, nullptr);
            }

            stm.step();
        }
        
        if (!sqlite3_changes(_db) && pBlob)
        {
            snprintf(szQuery, _countof(szQuery), "INSERT INTO " TX_SUMMARY_NAME " (TxID,%s) VALUES(?1,?2)", szName);

            sqlite::Statement stm(this, szQuery);
            stm.bind(1, txID);
            stm.bind(2, val);

            stm.step();
        }
    }

    bool WalletDB::delTxParameter(const TxID& txID, SubTxID subTxID, TxParameterID paramID)
    {
        if (auto txIter = m_TxParametersCache.find(txID); txIter != m_TxParametersCache.end())
        {
            if (auto subTxIter = txIter->second.find(subTxID); subTxIter != txIter->second.end())
            {
                if (auto pit = subTxIter->second.find(paramID); pit != subTxIter->second.end())
                {
                    subTxIter->second.erase(pit);
                }
            }
        }

        sqlite::Statement stm(this, "DELETE FROM " TX_PARAMS_NAME " WHERE txID=?1 AND subTxID=?2 AND paramID=?3;");

        stm.bind(1, txID);
        stm.bind(2, subTxID);
        stm.bind(3, paramID);

        stm.step();

        OnTxSummaryParam(txID, subTxID, paramID, nullptr);

        return true;
    }

    bool WalletDB::getTxParameter(const TxID& txID, SubTxID subTxID, TxParameterID paramID, ByteBuffer& blob) const
    {
        if (auto txIter = m_TxParametersCache.find(txID); txIter != m_TxParametersCache.end())
        {
            if (auto subTxIter = txIter->second.find(subTxID); subTxIter != txIter->second.end())
            {
                if (auto pit = subTxIter->second.find(paramID); pit != subTxIter->second.end())
                {
                    if (pit->second)
                    {
                        blob = *(pit->second);
                        return true;
                    }
                    return false;
                }
            }
        }

        sqlite::Statement stm(this, "SELECT value FROM " TX_PARAMS_NAME " WHERE txID=?1 AND subTxID=?2 AND paramID=?3;");
        return getTxParameterImpl(txID, subTxID, paramID, blob, stm);
    }

    bool WalletDB::getTxParameterImpl(const TxID& txID, SubTxID subTxID, TxParameterID paramID, ByteBuffer& blob, sqlite::Statement& stm) const
    {
        stm.Reset();
        stm.bind(1, txID);
        stm.bind(2, subTxID);
        stm.bind(3, paramID);

        if (stm.step())
        {
            stm.get(0, blob);
            insertParameterToCache(txID, subTxID, paramID, blob);
            return true;
        }
        insertParameterToCache(txID, subTxID, paramID, boost::optional<ByteBuffer>());
        return false;
    }

    std::vector<TxParameter> WalletDB::getAllTxParameters() const
    {
        sqlite::Statement stm(this, "SELECT * FROM " TX_PARAMS_NAME ";");
        std::vector<TxParameter> res;
        while (stm.step())
        {
            auto& p = res.emplace_back();
            int colIdx = 0;
            ENUM_TX_PARAMS_FIELDS(STM_GET_LIST, NOSEP, p);
            insertParameterToCache(
				p.m_txID,
				static_cast<SubTxID>(p.m_subTxID),
				static_cast<TxParameterID>(p.m_paramID),
				p.m_value);
        }
        return res;
    }

    bool IWalletDB::get_EffectiveEndpointEx(const TxID& txID, SubTxID subTxID, PeerID& res, TxParameterID par1, TxParameterID par2) const
    {
        if (!storage::getTxParameter(*this, txID, par1, res))
        {
            WalletID wid;
            if (!storage::getTxParameter(*this, txID, par2, wid))
                return false;

            res = wid.m_Pk;
        }
        return true;
    }

    bool IWalletDB::get_EffectiveEndpointPeer(const TxID& txID, SubTxID subTxID, PeerID& res) const
    {
        return get_EffectiveEndpointEx(txID, subTxID, res, TxParameterID::PeerEndpoint, TxParameterID::PeerAddr);
    }

    bool IWalletDB::get_EffectiveEndpointMy(const TxID& txID, SubTxID subTxID, PeerID& res) const
    {
        return get_EffectiveEndpointEx(txID, subTxID, res, TxParameterID::MyEndpoint, TxParameterID::MyAddr);
    }

    void WalletDB::insertParameterToCache(const TxID& txID, SubTxID subTxID, TxParameterID paramID, const boost::optional<ByteBuffer>& blob) const
    {
        m_TxParametersCache[txID][subTxID][paramID] = blob;
    }

    void WalletDB::deleteParametersFromCache(const TxID& txID)
    {
        m_TxParametersCache.erase(txID);
    }

    bool WalletDB::hasTransaction(const TxID& txID) const
    {
        ByteBuffer blob;
        for (const auto& paramID : m_mandatoryTxParams)
        {
            if (!getTxParameter(txID, kDefaultSubTxID, paramID, blob))
            {
                return false;
            }
        }
        return true;
    }
}
