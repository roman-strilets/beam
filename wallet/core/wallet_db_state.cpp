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
        const char* SystemStateIDName = "SystemStateID";
        const char* LastUpdateTimeName = "LastUpdateTime";
        const char* kStateSummaryShieldedOutsDBPath = "StateSummaryShieldedOuts";
        const char* kStateSummaryAssetsMaxDBPath = "StateSummaryAidMax";
        const char* kMaxPrivacyLockTimeLimitHours = "MaxPrivacyLockTimeLimitHours";
        const uint8_t kDefaultMaxPrivacyLockTimeLimitHours = 72;
    }

    TxoID IWalletDB::get_ShieldedOuts() const
    {
        TxoID ret = 0;
        storage::getVar(*this, kStateSummaryShieldedOutsDBPath, ret);
        return ret;
    }

    void IWalletDB::set_ShieldedOuts(TxoID val)
    {
        storage::setVar(*this, kStateSummaryShieldedOutsDBPath, val);
    }

    Asset::ID IWalletDB::get_AidMax() const
    {
        Asset::ID ret = 0;
        storage::getVar(*this, kStateSummaryAssetsMaxDBPath, ret);
        return ret;
    }

    void IWalletDB::set_AidMax(Asset::ID val)
    {
        storage::setVar(*this, kStateSummaryAssetsMaxDBPath, val);
    }


    uint8_t IWalletDB::get_MaxPrivacyLockTimeLimitHours() const
    {
        uint8_t ret = kDefaultMaxPrivacyLockTimeLimitHours;
        storage::getVar(*this, kMaxPrivacyLockTimeLimitHours, ret);
        return ret;
    }

    void IWalletDB::set_MaxPrivacyLockTimeLimitHours(uint8_t val)
    {
        storage::setVar(*this, kMaxPrivacyLockTimeLimitHours, val);
    }

    void WalletDB::setVarRaw(const char* name, const void* data, size_t size)
    {
        const char* req = "INSERT or REPLACE INTO " VARIABLES_NAME " (" VARIABLES_FIELDS ") VALUES(?1, ?2);";

        sqlite::Statement stm(this, req);

        stm.bind(1, name);
        stm.bind(2, data, size);

        stm.step();
    }

    bool WalletDB::getVarRaw(const char* name, void* data, int size) const
    {
        const char* req = "SELECT value FROM " VARIABLES_NAME " WHERE name=?1;";

        sqlite::Statement stm(this, req);
        stm.bind(1, name);

        return
            stm.step() &&
            stm.getBlobSafe(0, data, size);
    }

    void WalletDB::removeVarRaw(const char* name)
    {
        const char* req = "DELETE FROM " VARIABLES_NAME " WHERE name=?1;";
        sqlite::Statement stm(this, req);

        stm.bind(1, name);
        stm.step();
    }

    void WalletDB::setPrivateVarRaw(const char* name, const void* data, size_t size)
    {
        const char* req = "INSERT or REPLACE INTO " PRIVATE_VARIABLES_NAME " (" VARIABLES_FIELDS ") VALUES(?1, ?2);";

        sqlite::Statement stm(this, req, true);

        stm.bind(1, name);
        stm.bind(2, data, size);

        stm.step();
    }

    bool WalletDB::getPrivateVarRaw(const char* name, void* data, int size) const
    {
        return GetPrivateVarRaw(*this, name, data, size, true);
    }

    bool WalletDB::getBlob(const char* name, ByteBuffer& var) const
    {
        const char* req = "SELECT value FROM " VARIABLES_NAME " WHERE name=?1;";

        sqlite::Statement stm(this, req);
        stm.bind(1, name);
        if (stm.step())
        {
            stm.get(0, var);
            return true;
        }
        return false;
    }

    Timestamp WalletDB::getLastUpdateTime() const
    {
        Timestamp timestamp = {};
        
        if (storage::getVar(*this, LastUpdateTimeName, timestamp))
        {
            return timestamp;
        }
        return 0;
    }

    void WalletDB::setSystemStateID(const HeightHash& stateID)
    {
        storage::setVar(*this, SystemStateIDName, stateID);
        storage::setVar(*this, LastUpdateTimeName, getTimestamp());
        notifySystemStateChanged(stateID);
    }

    bool WalletDB::getSystemStateID(HeightHash& stateID) const
    {
        return storage::getVar(*this, SystemStateIDName, stateID);
    }

    Height WalletDB::getCurrentHeight() const
    {
        HeightHash id = {};
        if (getSystemStateID(id))
        {
            return id.m_Height;
        }
        return 0;
    }

    void WalletDB::insertEvent(Height h, const Blob& body, const Blob& key)
    {
        sqlite::Statement stm(this, "INSERT INTO " EVENTS_NAME "(" ENUM_EVENTS_FIELDS(LIST, COMMA, ) ") VALUES(" ENUM_EVENTS_FIELDS(BIND_LIST, COMMA, ) ");");
        stm.bind(1, h);
        stm.bind(2, body);
        stm.bind(3, key);
        stm.step();
    }

    void WalletDB::deleteEventsFrom(Height h)
    {
        sqlite::Statement stm(this, "DELETE FROM " EVENTS_NAME " WHERE Height >= ?1");
        stm.bind(1, h);
        stm.step();
    }

    void WalletDB::visitEvents(Height min, const Blob& key, std::function<bool(Height, ByteBuffer&&)>&& func) const
    {
        sqlite::Statement stm(this, "SELECT * FROM " EVENTS_NAME " WHERE Height >= ?1 AND Key == ?2");
        stm.bind(1, min);
        stm.bind(2, key);
        while (stm.step())
        {
            Height h = 0;
            ByteBuffer body;
            stm.get(0, h);
            stm.get(1, body);
            if (!func(h, std::move(body)))
                break;
        }
    }

    void WalletDB::visitEvents(Height min, std::function<bool(Height, ByteBuffer&&)>&& func) const
    {
        sqlite::Statement stm(this, "SELECT * FROM " EVENTS_NAME " WHERE Height >= ?1");
        stm.bind(1, min);
        while (stm.step())
        {
            Height h = 0;
            ByteBuffer body;
            stm.get(0, h);
            stm.get(1, body);
            if (!func(h, std::move(body)))
                break;
        }
    }

    Block::SystemState::IHistory& WalletDB::get_History()
    {
        return m_History;
    }

    void WalletDB::ShrinkHistory()
    {
        Block::SystemState::Full s;
        if (m_History.get_Tip(s))
        {
            Height hTip = s.get_Height();
            Height h0 = 0;

            if (Rules::get().IsConstantSpan())
            {
                const Height hMaxBacklog = Rules::get().MaxRollback * 2; // can actually be more
                if (hTip > hMaxBacklog)
                    h0 = hTip - hMaxBacklog;
            }
            else
            {
                // make sure we leave at least some most recent headers.
                struct Walker :public Block::SystemState::IHistory::IWalker
                {
                    uint32_t m_Count = 3;
                    Height m_h0 = 0;

                    bool OnState(const Block::SystemState::Full& s) override
                    {
                        if (--m_Count)
                            return true;

                        m_h0 = s.get_Height();
                        return false;
                    }
                } w;

                if (!m_History.Enum(w, &hTip))
                    h0 = w.m_h0;

            }

            if (h0)
            {
                const char* req = "DELETE FROM " TblStates " WHERE " TblStates_Height "<=?";
                sqlite::Statement stm(this, req);
                stm.bind(1, h0);
                stm.step();

            }
        }
    }

    bool WalletDB::History::Enum(IWalker& w, const Height* pBelow)
    {
        const char* req = pBelow ?
            "SELECT " TblStates_Hdr " FROM " TblStates " WHERE " TblStates_Height "<? ORDER BY " TblStates_Height " DESC;" :
            "SELECT " TblStates_Hdr " FROM " TblStates " ORDER BY " TblStates_Height " DESC;";

        sqlite::Statement stm(&get_ParentObj(), req);

        if (pBelow)
            stm.bind(1, *pBelow);

        while (stm.step())
        {
            Block::SystemState::Full s;
            stm.get(0, s);

            if (!w.OnState(s))
                return false;
        }

        return true;
    }

    bool WalletDB::History::get_At(Block::SystemState::Full& s, Height h)
    {
        const char* req = "SELECT " TblStates_Hdr " FROM " TblStates " WHERE " TblStates_Height "=?";

        sqlite::Statement stm(&get_ParentObj(), req);
        stm.bind(1, h);

        if (!stm.step())
            return false;

        stm.get(0, s);
        return true;
    }

    void WalletDB::History::AddStates(const Block::SystemState::Full* pS, size_t nCount)
    {
        const char* req = "INSERT OR REPLACE INTO " TblStates " (" TblStates_Height "," TblStates_Hdr ") VALUES(?,?)";
        sqlite::Statement stm(&get_ParentObj(), req);

        for (size_t i = 0; i < nCount; i++)
        {
            if (i)
                stm.Reset();

            stm.bind(1, pS[i].get_Height());
            stm.bind(2, pS[i]);
            stm.step();
        }
    }

    void WalletDB::History::DeleteFrom(Height h)
    {
        const char* req = "DELETE FROM " TblStates " WHERE " TblStates_Height ">=?";
        sqlite::Statement stm(&get_ParentObj(), req);
        stm.bind(1, h);
        stm.step();
    }

    bool WalletDB::get_AppData(const Blob& name, const Blob& key, ByteBuffer& res)
    {
        const char* req = "SELECT " TblAppData_Val " FROM " TblAppData " WHERE " TblAppData_Name "=? AND " TblAppData_Key "=?";

        sqlite::Statement stm(this, req);
        stm.bind(1, name);
        stm.bind(2, key);

        if (!stm.step())
            return false;

        stm.get(0, res);
        return true;

    }

    void WalletDB::set_AppData(const Blob& name, const Blob& key, const Blob* pRes)
    {
        if (pRes)
        {
            const char* req = "INSERT or REPLACE INTO " TblAppData " (" TblAppData_Name "," TblAppData_Key "," TblAppData_Val ") VALUES(?,?,?);";

            sqlite::Statement stm(this, req);

            stm.bind(1, name);
            stm.bind(2, key);
            stm.bind(3, *pRes);

            stm.step();
        }
        else
        {
            const char* req = "DELETE FROM " TblAppData " WHERE " TblAppData_Name "=? AND " TblAppData_Key "=?";
            sqlite::Statement stm(this, req);
            stm.bind(1, name);
            stm.bind(2, key);
            stm.step();
        }
    }

    void WalletDB::ClearAppData(const Blob& name)
    {
        const char* req = "DELETE FROM " TblAppData " WHERE " TblAppData_Name "=?";
        sqlite::Statement stm(this, req);
        stm.bind(1, name);
        stm.step();
    }
}
