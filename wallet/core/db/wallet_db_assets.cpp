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
    void WalletDB::saveAsset(const Asset::Full &info, Height refreshHeight)
    {
        refreshHeight = refreshHeight ? refreshHeight : getCurrentHeight();
        assert(info.m_LockHeight <= refreshHeight);

        const char* find = "SELECT * FROM " ASSETS_NAME " WHERE ID=?1;";
        sqlite::Statement stmFind(this, find);
        stmFind.bind(1, info.m_ID);

        const char* update = "UPDATE " ASSETS_NAME " SET Value=?2, Owner=?3, LockHeight=?4, Metadata=?5, RefreshHeight=?6, Deposit=?7 WHERE ID=?1;";
        const char* insert = "INSERT INTO " ASSETS_NAME " (" ENUM_ASSET_FIELDS(LIST, COMMA, ) ") VALUES(" ENUM_ASSET_FIELDS(BIND_LIST, COMMA, ) ");";
        const bool  found  = stmFind.step();

        sqlite::Statement stm(this, found ? update: insert);
        stm.bind(1, info.m_ID);
        stm.bind(2, info.m_Value);
        stm.bind(3, info.m_Owner);
        stm.bind(4, info.m_LockHeight);
        stm.bind(5, info.m_Metadata.m_Value);
        stm.bind(6, refreshHeight);

        if (found)
            stm.bind(7, info.m_Deposit);
        else
        {
            // By default we do not mark new assets as owned
            // For owned assets this value is set to the owner index by the 'AssetRegister' transaction
            stm.bind(7, 0);
            stm.bind(8, info.m_Deposit);
        }

        stm.step();
        notifyAssetChanged(found ? ChangeAction::Updated : ChangeAction::Added, info.m_ID);
    }

    void WalletDB::markAssetOwned(const Asset::ID assetId)
    {
        const char* update = "UPDATE " ASSETS_NAME " SET IsOwned=?2 WHERE ID=?1;";
        sqlite::Statement stmUpdate(this, update);
        stmUpdate.bind(1, assetId);
        stmUpdate.bind(2, 1);
        stmUpdate.step();
        notifyAssetChanged(ChangeAction::Updated, assetId);
    }

    void WalletDB::dropAsset(const PeerID& ownerId)
    {
        if(const auto oasset = findAsset(ownerId))
        {
            const auto& info = *oasset;
            dropAsset(info.m_ID);
        }
    }

    void WalletDB::dropAsset(const Asset::ID assetId)
    {
        const char* deleteReq = "DELETE FROM " ASSETS_NAME " WHERE ID=?1;";
        sqlite::Statement stm(this, deleteReq);
        stm.bind(1, assetId);
        stm.step();
        notifyAssetChanged(ChangeAction::Removed, assetId);
    }

    void WalletDB::visitAssets(std::function<bool(const WalletAsset& info)> visitor) const
    {
        const char* select = "SELECT * FROM " ASSETS_NAME " ORDER BY ID;";
        sqlite::Statement stm(this, select);

        while (stm.step())
        {
            WalletAsset asset;

            stm.get(0, asset.m_ID);
            stm.get(1, asset.m_Value);
            stm.get(2, asset.m_Owner);
            stm.get(3, asset.m_LockHeight);
            stm.get(4, asset.m_Metadata.m_Value);
            asset.m_Metadata.UpdateHash();
            stm.get(5, asset.m_RefreshHeight);
            assert(asset.m_RefreshHeight != 0);
            stm.get(6, asset.m_IsOwned);

            if (!visitor(asset))
            {
                break;
            }
        }
    }

    boost::optional<WalletAsset> WalletDB::findAsset(Asset::ID assetId) const
    {
        const char* find = "SELECT * FROM " ASSETS_NAME " WHERE ID=?1;";
        sqlite::Statement stmFind(this, find);
        stmFind.bind(1, assetId);
        if (!stmFind.step())
        {
            return boost::optional<WalletAsset>();
        }

        WalletAsset asset;

        stmFind.get(0, asset.m_ID);
        assert(asset.m_ID == assetId);

        stmFind.get(1, asset.m_Value);
        stmFind.get(2, asset.m_Owner);
        stmFind.get(3, asset.m_LockHeight);
        stmFind.get(4, asset.m_Metadata.m_Value);
        asset.m_Metadata.UpdateHash();
        stmFind.get(5, asset.m_RefreshHeight);
        assert(asset.m_RefreshHeight != 0);
        stmFind.get(6, asset.m_IsOwned);

        if (stmFind.IsNull(7))
            asset.m_Deposit = Rules::get().CA.DepositForList2; // assume it's legacy
        else
            stmFind.get(7, asset.m_Deposit);

        return asset;
    }

    boost::optional<WalletAsset> WalletDB::findAsset(const PeerID& ownerID) const
    {
        const char* find = "SELECT ID FROM " ASSETS_NAME " WHERE Owner=?1;";
        sqlite::Statement stmFind(this, find);
        stmFind.bind(1, ownerID);

        if (!stmFind.step())
        {
            return boost::optional<WalletAsset>();
        }

        Asset::ID assetID = Asset::s_InvalidID;
        stmFind.get(0, assetID);

        return findAsset(assetID);
    }

    void WalletDB::rollbackAssets(Height minHeight)
    {
        std::set<Asset::ID> changed;
        std::set<Asset::ID> dropped;

        {
            const char* findDrop = "SELECT ID FROM " ASSETS_NAME " WHERE LockHeight>?1;";
            sqlite::Statement stmFind(this, findDrop);
            stmFind.bind(1, minHeight);

            while (stmFind.step())
            {
                Asset::ID assetID = Asset::s_InvalidID;
                stmFind.get(0, assetID);
                dropped.insert(assetID);
            }
        }

        {
            const char* findUpdate = "SELECT ID FROM " ASSETS_NAME " WHERE RefreshHeight > ?1;";
            sqlite::Statement stmFind(this, findUpdate);
            stmFind.bind(1, minHeight);

            while (stmFind.step())
            {
                Asset::ID assetID = Asset::s_InvalidID;
                stmFind.get(0, assetID);
                changed.insert(assetID);
            }
        }

        const char* drop = "DELETE FROM " ASSETS_NAME " WHERE LockHeight>?1;";
        sqlite::Statement stmDrop(this, drop);
        stmDrop.bind(1, minHeight);
        stmDrop.step();

        const char* update = "UPDATE " ASSETS_NAME " SET RefreshHeight=?1 WHERE RefreshHeight > ?1;";
        sqlite::Statement stmUpdate(this, update);
        stmUpdate.bind(1, minHeight);
        stmUpdate.step();

        for(auto assetId: dropped)
        {
            notifyAssetChanged(ChangeAction::Removed, assetId);
        }

        for(auto assetId: changed)
        {
            notifyAssetChanged(ChangeAction::Updated, assetId);
        }
    }

    void WalletDB::saveVerification(const VerificationInfo& vi)
    {
        const char* selectReq = "SELECT * FROM " VERIFICATION_NAME " WHERE assetID=?1";
        sqlite::Statement selectStm(this, selectReq);
        selectStm.bind(1, vi.m_assetID);

        if (selectStm.step())
        {
            const char* updateReq = "UPDATE " VERIFICATION_NAME " SET verified=?2, icon=?3, color=?4, updateTime=?5 WHERE assetID=?1;";
            sqlite::Statement updateStm(this, updateReq);

            updateStm.bind(1, vi.m_assetID);
            updateStm.bind(2, vi.m_verified);
            updateStm.bind(3, vi.m_icon);
            updateStm.bind(4, vi.m_color);
            updateStm.bind(5, vi.m_updateTime);
            updateStm.step();
        }
        else
        {
            const char* insertReq = "INSERT INTO " VERIFICATION_NAME " (" ENUM_VERIFICATION_FIELDS(LIST, COMMA, ) ") VALUES(" ENUM_VERIFICATION_FIELDS(BIND_LIST, COMMA, ) ");";
            sqlite::Statement stm(this, insertReq);
            int colIdx = 0;
            ENUM_VERIFICATION_FIELDS(STM_BIND_LIST, NOSEP, vi);
            stm.step();
        }
    }

    std::vector<VerificationInfo> WalletDB::getVerification() const
    {
        std::vector<VerificationInfo> res;

        const char* req = "SELECT * FROM " VERIFICATION_NAME " ORDER BY assetID ASC;";
        sqlite::Statement stm(this, req);

        while (stm.step())
        {
            auto& vi = res.emplace_back();
            int colIdx = 0;
            ENUM_VERIFICATION_FIELDS(STM_GET_LIST, NOSEP, vi);
        }

        return res;
    }
}
