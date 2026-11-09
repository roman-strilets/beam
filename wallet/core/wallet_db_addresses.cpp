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
    void IWalletDB::get_SbbsPeerID(ECC::Scalar::Native& sk, PeerID& pid, uint64_t ownID) const
    {
        Key::IKdf::Ptr pKdfSbbs = get_SbbsKdf();
        if (!pKdfSbbs)
            throw CannotGenerateSecretException();

        ECC::Hash::Value hv;
        Key::ID(ownID, Key::Type::Bbs).get_Hash(hv);

        pKdfSbbs->DeriveKey(sk, hv);
        pid.FromSk(sk);
    }

    void IWalletDB::get_SbbsWalletID(ECC::Scalar::Native& sk, WalletID& wid, uint64_t ownID)
    {
        get_SbbsPeerID(sk, wid.m_Pk, ownID);
        wid.SetChannelFromPk();
    }

    void IWalletDB::get_SbbsWalletID(WalletID& wid, uint64_t ownID)
    {
        ECC::Scalar::Native sk;
        get_SbbsWalletID(sk, wid, ownID);
    }

    bool IWalletDB::ValidateSbbsWalletID(const WalletID& wid, uint64_t ownID)
    {
        WalletID wid2;
        get_SbbsWalletID(wid2, ownID);
        return wid == wid2;
    }

    void IWalletDB::createAddress(WalletAddress& addr)
    {
        addr.m_createTime = beam::getTimestamp();
        addr.m_OwnID = AllocateKidRange(1);

        get_SbbsWalletID(addr.m_BbsAddr, addr.m_OwnID);
        get_Endpoint(addr.m_Endpoint, addr.m_OwnID);
        setDefaultToken(addr);

        BEAM_LOG_INFO() << boost::format(kWalletAddrNewGenerated) % addr.m_Token;

        if (addr.m_Endpoint != Zero) {
            BEAM_LOG_INFO() << boost::format(kAddrNewGeneratedEndpoint) % beam::to_base58(addr.m_Endpoint);
        }

        if (!addr.m_label.empty()) {
            BEAM_LOG_INFO() << boost::format(kAddrNewGeneratedLabel) % addr.m_label;
        }
    }

    void IWalletDB::get_Endpoint(PeerID& pid, uint64_t ownID) const
    {
        ECC::Hash::Value hv;
        Key::ID(ownID, Key::Type::EndPoint).get_Hash(hv);
        ECC::Point::Native pt;
        get_OwnerKdf()->DerivePKeyG(pt, hv);
        pid = ECC::Point(pt).m_X;
    }

    namespace
    {
        void loadAddress(const IWalletDB* db, sqlite::Statement& stm, WalletAddress& address)
        {
            int colIdx = 0;
            ENUM_ADDRESS_FIELDS(STM_GET_LIST, NOSEP, address);
            if (address.isOwn() && address.m_Endpoint == Zero)
                db->get_Endpoint(address.m_Endpoint, address.m_OwnID);
        }
    }

    boost::optional<WalletAddress> WalletDB::getAddressByToken(const std::string& token, bool isLaser) const
    {
        const std::string addrTableName = isLaser ? LASER_ADDRESSES_NAME : ADDRESSES_NAME;
        auto req = "SELECT * FROM " + addrTableName + " WHERE Address=?1;";

        sqlite::Statement stm(this, req.c_str());
        stm.bind(1, token);

        if (stm.step())
        {
            WalletAddress address = {};
            loadAddress(this, stm, address);

            return address;
        }

        return boost::optional<WalletAddress>();
    }

    boost::optional<WalletAddress> WalletDB::getAddress(const WalletID& id, bool isLaser) const
    {
        const std::string addrTableName = isLaser ? LASER_ADDRESSES_NAME : ADDRESSES_NAME;
        auto req = "SELECT * FROM " + addrTableName + " WHERE walletID=?1;";

        sqlite::Statement stm(this, req.c_str());
        stm.bind(1, id);

        if (stm.step())
        {
            WalletAddress address = {};
            loadAddress(this, stm, address);

            return address;
        }

        return boost::optional<WalletAddress>();
    }

    std::vector<WalletAddress> WalletDB::getAddresses(bool own, bool isLaser) const
    {
        const std::string addrTableName =
            isLaser ? LASER_ADDRESSES_NAME : ADDRESSES_NAME;
        vector<WalletAddress> res;
        auto req = "SELECT * FROM " + addrTableName + " ORDER BY createTime DESC;";
        sqlite::Statement stm(this, req.c_str());

        while (stm.step())
        {
            auto& a = res.emplace_back();
            loadAddress(this, stm, a);

            if (a.isOwn() != own)
                res.pop_back(); // akward, but ok
        }
        return res;
    }

    void IWalletDB::setDefaultToken(WalletAddress& wa)
    {
        if (get_MasterKdf())
            wa.setDefaultToken();
        else
            // remote key keeper is incompatible with bbs-style addresses.
            wa.m_Token = GenerateRegularNewToken(wa, 0, 0, "");
    }

    void WalletAddress::setDefaultToken()
    {
        // Current decision: use 'old'-style addresses by default
        assert(m_BbsAddr != Zero);
        m_Token = to_string(m_BbsAddr);
    }

    bool WalletDB::IsSuitableDefaultAddr(const WalletAddress& addr)
    {
        if (!addr.isOwn())
            return false;
        if (!addr.isPermanent())
            return false;

        return TokenType::RegularNewStyle == GetTokenType(addr.m_Token);
    }

    void WalletDB::getDefaultAddressAlways(WalletAddress& res)
    {
        if (m_widDefaultAddr)
        {
            auto pAddr = getAddress(*m_widDefaultAddr);
            if (pAddr)
            {
                res = std::move(*pAddr);
                return;
            }

            m_widDefaultAddr.reset();
        }

        uint32_t iBest = static_cast<uint32_t>(-1);

        auto vec = getAddresses(true);
        for (uint32_t i = 0; i < vec.size(); i++)
        {
            const auto& addr = vec[i];
            if (!IsSuitableDefaultAddr(addr))
                continue;

            if (iBest < vec.size())
            {
                // prefer newer address
                if (vec[iBest].m_OwnID >= addr.m_OwnID)
                    continue;
            }

            iBest = i;
        }

        if (iBest < vec.size())
        {
            res = std::move(vec[iBest]);
            m_widDefaultAddr.reset(res.m_BbsAddr);
        }
        else
        {
            createAddress(res);
            res.m_label = kDefaultAddrLabel;
            res.setExpirationStatus(WalletAddress::ExpirationStatus::Never);
            res.m_Token = GenerateRegularNewToken(res, 0, 0, "");
            saveAddress(res);

            m_widDefaultAddr.reset(res.m_BbsAddr);
        }

    }

    void WalletDB::saveAddress(const WalletAddress& addr, bool isLaser)
    {
        const std::string addrTableName = isLaser ? LASER_ADDRESSES_NAME : ADDRESSES_NAME;

        WalletAddress address = addr;
        if (address.m_Token.empty())
            setDefaultToken(address);

        if (m_widDefaultAddr && IsSuitableDefaultAddr(addr))
            m_widDefaultAddr.reset();

        auto selectByToken = "SELECT * FROM " + addrTableName + " WHERE address=?1;";
        sqlite::Statement stmToken(this, selectByToken.c_str());
        stmToken.bind(1, address.m_Token);

        if (stmToken.step())
        {
            {
                auto updateReq = "UPDATE " + addrTableName + " SET label=?2, category=?3, duration=?4, createTime=?5, walletID=?6, OwnID=?7, Identity=?8 WHERE address=?1;";
                sqlite::Statement stm(this, updateReq.c_str());

                stm.bind(1, address.m_Token);
                stm.bind(2, address.m_label);
                stm.bind(3, address.m_category);
                stm.bind(4, address.m_duration);
                stm.bind(5, address.m_createTime);
                stm.bind(6, address.m_BbsAddr);
                stm.bind(7, address.m_OwnID);
                stm.bind(8, address.m_Endpoint);
                stm.step();
            }

            if (!isLaser)
            {
                notifyAddressChanged(ChangeAction::Updated, { address });
            }

            return;
        }

        // This check is intentional, it skips MaxPrivacy addresses
        if (address.m_BbsAddr.m_Pk != Zero)
        {
            auto selectByWid = "SELECT * FROM " + addrTableName + " WHERE walletID=?1;";
            sqlite::Statement stmWid(this, selectByWid.c_str());
            stmWid.bind(1, address.m_BbsAddr);

            if (stmWid.step())
            {
                {
                    auto updateReq = "UPDATE " + addrTableName + " SET address=?1, label=?2, category=?3, duration=?4, createTime=?5, OwnID=?7, Identity=?8 WHERE walletID=?6;";
                    sqlite::Statement stm(this, updateReq.c_str());

                    stm.bind(1, address.m_Token);
                    stm.bind(2, address.m_label);
                    stm.bind(3, address.m_category);
                    stm.bind(4, address.m_duration);
                    stm.bind(5, address.m_createTime);
                    stm.bind(6, address.m_BbsAddr);
                    stm.bind(7, address.m_OwnID);
                    stm.bind(8, address.m_Endpoint);
                    stm.step();
                }

                if (!isLaser)
                {
                    notifyAddressChanged(ChangeAction::Updated, {address});
                }

                return;
            }
        }

        {
            auto insertReq = "INSERT INTO " + addrTableName + " (" ENUM_ADDRESS_FIELDS(LIST, COMMA, ) ") VALUES(" ENUM_ADDRESS_FIELDS(BIND_LIST, COMMA, ) ");";
            sqlite::Statement stm(this, insertReq.c_str());
            int colIdx = 0;
            ENUM_ADDRESS_FIELDS(STM_BIND_LIST, NOSEP, address);
            stm.step();
        }

        if (!isLaser)
        {
            notifyAddressChanged(ChangeAction::Added, { address });
        }
    }

    void WalletDB::generateAndSaveDefaultAddress()
    {
        WalletAddress address;
        createAddress(address);
        address.m_label = kDefaultAddrLabel;
        address.setExpirationStatus(WalletAddress::ExpirationStatus::Never);
        saveAddress(address);
        BEAM_LOG_DEBUG() << "Default address: " << address.m_Token;
    }

    void WalletDB::deleteAddressByToken(const std::string& addr, bool isLaser)
    {
        auto address = getAddressByToken(addr, isLaser);
        if (address)
        {
            const std::string addrTableName =
                isLaser ? LASER_ADDRESSES_NAME : ADDRESSES_NAME;

            auto req = "DELETE FROM " + addrTableName + " WHERE Address=?1;";
            sqlite::Statement stm(this, req.c_str());

            stm.bind(1, addr);
            stm.step();

            if (m_widDefaultAddr && address->m_BbsAddr == *m_widDefaultAddr)
                m_widDefaultAddr.reset();

            if (!isLaser)
            {
                notifyAddressChanged(ChangeAction::Removed, { *address });
            }
        }
    }

    void WalletDB::deleteAddress(const WalletID& id, bool isLaser)
    {
        auto address = getAddress(id, isLaser);
        assert(address.is_initialized());

        if (address)
        {
            const std::string addrTableName = isLaser ? LASER_ADDRESSES_NAME : ADDRESSES_NAME;
            const auto req = "DELETE FROM " + addrTableName + " WHERE walletID=?1;";
            sqlite::Statement stm(this, req.c_str());

            stm.bind(1, id);
            stm.step();

            if (m_widDefaultAddr && address->m_BbsAddr == *m_widDefaultAddr)
                m_widDefaultAddr.reset();

            if (!isLaser)
            {
                notifyAddressChanged(ChangeAction::Removed, {*address});
            }
        }
    }

    void WalletDB::DeleteNonceAddresses()
    {
        // Historically all send txs used 'nonce' address, i.e. addresses created specifically for it.
        std::map<uint64_t, uint32_t> addrUse;

        sqlite::Statement stm(this, "SELECT TxID FROM " TX_SUMMARY_NAME);
        sqlite::Statement stm2(this, "SELECT * FROM " TX_PARAMS_NAME " WHERE txID=?1;");
        TxID txID;
        while (stm.step())
        {
            stm.get(0, txID);

            auto t = getTxImpl(txID, stm2);
            if (!t.is_initialized())
                break;

            auto& txDesc = *t;

            uint64_t ownID = 0;
            if (!txDesc.GetParameter(TxParameterID::MyAddressID, ownID) || !ownID)
                continue;

            if (!txDesc.m_sender)
                continue;

            auto it = addrUse.find(ownID);
            if (addrUse.end() == it)
                addrUse[ownID] = 0;
            else
                it->second++; // for more safety: if addr is used more than once - it's not nonce
        }

        auto vAddrs = getAddresses(true);
        for (auto& addr : vAddrs)
        {
            auto it = addrUse.find(addr.m_OwnID);
            if (addrUse.end() == it)
                continue;
            if (it->second)
                continue; // not a nonce, used in multiple txs

            deleteAddress(addr.m_BbsAddr);
            BEAM_LOG_INFO() << "Removed nonce addr: " << addr.m_Token;

        }
    }
}
