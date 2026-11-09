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
        const char* OwnerKey = "OwnerKey";
        const char* HwwKind = "Hww";
    }

    void WalletDB::storeOwnerKey()
    {
        ECC::NoLeak<ECC::HKdfPub::Packed> packedOwnerKey;
        assert(m_pKdfOwner->ExportP(nullptr) == sizeof(packedOwnerKey));
        m_pKdfOwner->ExportP(&packedOwnerKey);

        storage::setVar(*this, OwnerKey, packedOwnerKey.V);
    }

    struct WalletDB::LocalKeyKeeper
        :public LocalPrivateKeyKeeperStd
    {
        using LocalPrivateKeyKeeperStd::LocalPrivateKeyKeeperStd;

        struct UsedSlots
        {
            static const char s_szDbName[];

            State::UsedMap m_Used;

            template <typename Archive>
            void serialize(Archive& ar)
            {
                ar & m_Used;
            }

            void Load(WalletDB& db)
            {
                ByteBuffer buf;
                if (db.getBlob(s_szDbName, buf))
                {
                    Deserializer der;
                    der.reset(buf);
                    der & *this;
                }
            }

            void Save(WalletDB& db)
            {
                Serializer ser;
                ser & *this;

                db.setVarRaw(s_szDbName, ser.buffer().first, ser.buffer().second);
            }
        };

        class UsedSlotsWrk
            :public UsedSlots
        {
            LocalKeyKeeper* m_pLocal;

            void SwapSafe()
            {
                if (m_pLocal)
                    m_pLocal->m_State.m_Used.swap(m_Used);
            }

        public:

            UsedSlotsWrk(LocalKeyKeeper* pLocal)
                :m_pLocal(pLocal)
            {
                SwapSafe();
            }

            ~UsedSlotsWrk()
            {
                SwapSafe();
            }
        };

    };

    const char WalletDB::LocalKeyKeeper::UsedSlots::s_szDbName[] = "KeyKeeperSlots";

    void WalletDB::FromMaster(const ECC::uintBig& seed)
    {
        ECC::HKdf::Create(m_pKdfMaster, seed);
        FromMaster();
    }

    void WalletDB::FromMaster()
    {
        assert(m_pKdfMaster);
        m_pKdfOwner = m_pKdfMaster;
        m_pKdfSbbs = m_pKdfMaster;

        if (!m_pKeyKeeper)
        {
            m_pKeyKeeper = std::make_shared<LocalKeyKeeper>(m_pKdfMaster);
            m_pLocalKeyKeeper = &Cast::Up<LocalKeyKeeper>(*m_pKeyKeeper);
        }

        UpdateLocalSlots();
    }

    void WalletDB::FromKeyKeeper()
    {
        assert(!m_pKdfMaster);

        if (!m_pKeyKeeper)
            throw std::runtime_error("Key keeper required");

        IPrivateKeyKeeper2::Method::get_Kdf m;
        m.m_Type = IPrivateKeyKeeper2::KdfType::Root;
        m_pKeyKeeper->InvokeSync(m);

        if (m.m_pKdf)
        {
            // trusted mode.
            m_pKdfMaster = std::move(m.m_pKdf);
            FromMaster();
            return;
        }

        if (!m.m_pPKdf)
            throw std::runtime_error("can't get owner key");

        m_pKdfOwner = std::move(m.m_pPKdf);

        // trustless mode. create SBBS Kdf from a child PKdf. It won't be directly accessible from the owner key
        m.m_Type = IPrivateKeyKeeper2::KdfType::Sbbs;
        m_pKeyKeeper->InvokeSync(m);

        if (!m.m_pPKdf)
            throw std::runtime_error("can't get sbbs key");

        ECC::Scalar::Native sk;
        m.m_pPKdf->DerivePKey(sk, Zero);

        ECC::NoLeak<ECC::Scalar> s;
        s.V = sk;

        ECC::HKdf::Create(m_pKdfSbbs, s.V.m_Value);

        UpdateLocalSlots();

    }

    void WalletDB::UpdateLocalSlots()
    {
        assert(m_pKeyKeeper);

        IPrivateKeyKeeper2::Method::get_NumSlots m = { 0 };
        m.m_Count = 0;
        if ((IPrivateKeyKeeper2::Status::Success != m_pKeyKeeper->InvokeSync(m)) || !m.m_Count)
            throw std::runtime_error("key keeper no slots");

        m_KeyKeeperSlots = m.m_Count;

        LocalKeyKeeper::UsedSlots us;
        bool bKeep = false;

        try
        {
            us.Load(*this);
            if (us.m_Used.empty() || (us.m_Used.rbegin()->first < m.m_Count))
                bKeep = true;
        }
        catch (...)
        {
        }

        if (!bKeep)
        {
            us.m_Used.clear();
            us.Save(*this);
        }

        if (m_pLocalKeyKeeper)
        {
            ECC::GenRandom(m_pLocalKeyKeeper->m_State.m_hvLast);

            if (bKeep)
                // restore used slots
                m_pLocalKeyKeeper->m_State.m_Used.swap(us.m_Used);
        }
    }

    IWalletDB::Ptr WalletDB::initNoKeeper(const std::string& path, const SecString& password, bool separateDBForPrivateData)
    {
        return initBase(path, password, separateDBForPrivateData);
    }

    IWalletDB::Ptr WalletDB::init(const string& path, const SecString& password, const ECC::NoLeak<ECC::uintBig>& secretKey, bool separateDBForPrivateData)
    {
        std::shared_ptr<WalletDB> walletDB = initBase(path, password, separateDBForPrivateData);
        if (walletDB)
        {
            walletDB->FromMaster(secretKey.V);

            walletDB->setPrivateVarRaw(WalletSeed, &secretKey.V, sizeof(secretKey.V)); // store master key
            walletDB->storeOwnerKey(); // store owner key (public)

            walletDB->flushDB();
            walletDB->m_Initialized = true;
        }

        return walletDB;
    }

    IWalletDB::Ptr WalletDB::init(const string& path, const SecString& password, const IPrivateKeyKeeper2::Ptr& pKeyKeeper, bool separateDBForPrivateData)
    {
        std::shared_ptr<WalletDB> walletDB = initBase(path, password, separateDBForPrivateData);
        if (walletDB)
        {
            walletDB->m_pKeyKeeper = pKeyKeeper;
            walletDB->FromKeyKeeper();

            walletDB->storeOwnerKey(); // store owner key (public)

            walletDB->flushDB();
            walletDB->m_Initialized = true;
        }

        return walletDB;
    }

    void WalletDB::InitKeys(const IPrivateKeyKeeper2::Ptr& pKeyKeeper)
    {
        ECC::NoLeak<ECC::Hash::Value> seed;
        if (getPrivateVarRaw(WalletSeed, &seed.V, sizeof(seed.V)))
        {
            FromMaster(seed.V);
            return;
        }

        ECC::NoLeak<ECC::HKdfPub::Packed> packedOwnerKey;
        bool bHadOwnerKey = storage::getVar(*this, OwnerKey, packedOwnerKey.V);

        m_pKeyKeeper = pKeyKeeper;
        if (!m_pKeyKeeper)
        {
            uint32_t iKind = 0;
            if (storage::getBlobVar(*this, HwwKind, iKind) && iKind)
                throw std::runtime_error("Hardware wallets are no longer supported");
        }

        if (m_pKeyKeeper)
        {
            FromKeyKeeper();

            if (bHadOwnerKey)
            {
                // consistency check. Make sure there's an agreement w.r.t. stored owner key
                ECC::NoLeak<ECC::HKdfPub::Packed> keyCurrent;
                m_pKdfOwner->ExportP(&keyCurrent);

                if (memcmp(&packedOwnerKey, &keyCurrent, sizeof(keyCurrent)))
                    throw std::runtime_error("Key keeper is different");
            }
            else
                storeOwnerKey();

            return;
        }
        
        if (bHadOwnerKey)
        {
            // Read-only wallet.
            m_pKdfOwner = std::make_shared<ECC::HKdfPub>();
            Cast::Up<ECC::HKdfPub>(*m_pKdfOwner).Import(packedOwnerKey.V);
        }
        else // Headless wallet. Generate dummy owner kdf
        {
            ECC::HKdf kdf;
            ECC::Scalar::Native sk;
            ECC::Scalar s;
            sk.GenRandomNnz();
            sk.Export(s);
            kdf.Generate(s.m_Value);
            auto pubKdf = std::make_shared<ECC::HKdfPub>();
            pubKdf->GenerateFrom(kdf);
            m_pKdfOwner = pubKdf;
        }
    }

    WalletDB::WalletDB(sqlite3* db)
        : WalletDB(db, db)
    {

    }

    WalletDB::WalletDB(sqlite3* db, sqlite3* sdb)
        : _db(db)
        , m_PrivateDB(sdb)
        , m_IsFlushPending(false)
        , m_mandatoryTxParams{
            TxParameterID::TransactionType,
            TxParameterID::CreateTime}
    {
        if (m_Initialized && !storage::getVar(*this, LAST_READ_IM_ID, m_lastReadIMId))
            m_lastReadIMId = 0;
    }

    WalletDB::~WalletDB()
    {
        if (_db)
        {
            if (m_DbTransaction)
            {
                try
                {
                    m_DbTransaction->commit();
                }
                catch (const runtime_error& ex)
                {
                    BEAM_LOG_ERROR() << "Wallet DB Commit failed: " << ex.what();
                }
                m_DbTransaction.reset();
            }
            BEAM_VERIFY(SQLITE_OK == sqlite3_close(_db));
            if (m_PrivateDB && _db != m_PrivateDB)
            {
                BEAM_VERIFY(SQLITE_OK == sqlite3_close(m_PrivateDB));
                m_PrivateDB = nullptr;
            }
            _db = nullptr;
        }
        
    }

    Key::IKdf::Ptr WalletDB::get_MasterKdf() const
    {
        return m_pKdfMaster;
    }

    Key::IPKdf::Ptr WalletDB::get_OwnerKdf() const
    {
        return m_pKdfOwner;
    }

    Key::IKdf::Ptr WalletDB::get_SbbsKdf() const
    {
        return m_pKdfSbbs;
    }

    IPrivateKeyKeeper2::Ptr WalletDB::get_KeyKeeper() const
    {
        return m_pKeyKeeper;
    }

    IPrivateKeyKeeper2::Slot::Type WalletDB::SlotAllocate()
    {
        IPrivateKeyKeeper2::Slot::Type iSlot = IPrivateKeyKeeper2::Slot::Invalid;

        if (m_pKeyKeeper)
        {
            LocalKeyKeeper::UsedSlotsWrk us(m_pLocalKeyKeeper);
            if (!m_pLocalKeyKeeper)
                us.Load(*this);

            assert(us.m_Used.size() <= m_KeyKeeperSlots);
            if (us.m_Used.size() < m_KeyKeeperSlots)
            {
                if (us.m_Used.empty())
                    iSlot = 0;
                else
                {
                    iSlot = us.m_Used.rbegin()->first + 1;
                    if (iSlot >= m_KeyKeeperSlots)
                    {
                        // find free from the beginning
                        iSlot = 0;
                        for (LocalKeyKeeper::State::UsedMap::iterator it = us.m_Used.begin(); it->first == iSlot; ++it)
                            iSlot++;
                    }
                }

                ECC::Hash::Value& hv = us.m_Used[iSlot];
                if (m_pLocalKeyKeeper)
                    m_pLocalKeyKeeper->m_State.Regenerate(hv);
                else
                    hv = Zero;
            }

            us.Save(*this);
        }

        return iSlot;
    }

    void WalletDB::SlotFree(IPrivateKeyKeeper2::Slot::Type iSlot)
    {
        if (m_pKeyKeeper && (IPrivateKeyKeeper2::Slot::Invalid != iSlot))
        {
            LocalKeyKeeper::UsedSlotsWrk us(m_pLocalKeyKeeper);
            if (!m_pLocalKeyKeeper)
                us.Load(*this);

            LocalKeyKeeper::State::UsedMap::iterator it = us.m_Used.find(iSlot);
            if (us.m_Used.end() != it)
            {
                us.m_Used.erase(it);
                us.Save(*this);
            }
        }
    }

    void WalletDB::Subscribe(IWalletDbObserver* observer)
    {
        if (std::find(m_subscribers.begin(), m_subscribers.end(), observer) == m_subscribers.end())
        {
            m_subscribers.push_back(observer);
        }
    }

    void WalletDB::Unsubscribe(IWalletDbObserver* observer)
    {
        auto it = std::find(m_subscribers.begin(), m_subscribers.end(), observer);

        assert(it != m_subscribers.end());

        m_subscribers.erase(it);
    }

    void WalletDB::changePassword(const SecString& password)
    {
        int ret = sqlite3_rekey(_db, password.data(), static_cast<int>(password.size()));
        throwIfError(ret, _db);
    }

    void WalletDB::flushDB()
    {
        if (m_IsFlushPending)
        {
            assert(m_FlushTimer);
            m_FlushTimer->cancel();
        }

        onFlushTimer();
    }

    void WalletDB::rollbackDB()
    {
        if (m_IsFlushPending)
        {
            assert(m_FlushTimer);
            m_FlushTimer->cancel();
            m_IsFlushPending = false;
        }
        if (m_DbTransaction)
        {
            m_DbTransaction->rollback();
            m_DbTransaction.reset();
        }
    }

    void WalletDB::onModified()
    {
        if (!m_Initialized) // wallet db is opening or initializing, there could be no reactor to run timer
        {
            //onFlushTimer();
        }
        else if (!m_IsFlushPending)
        {
            if (!m_FlushTimer)
            {
                m_FlushTimer = io::Timer::create(io::Reactor::get_Current());
            }
            m_FlushTimer->start(50, false, BIND_THIS_MEMFN(onFlushTimer));
            m_IsFlushPending = true;
        }
    }

    void WalletDB::onFlushTimer()
    {
        m_IsFlushPending = false;
        if (m_DbTransaction)
        {
            m_DbTransaction->commit();
            m_DbTransaction.reset();

#ifdef __EMSCRIPTEN__
            if (get_MasterKdf()) // dont do sync for headless wallet
            {
                MAIN_THREAD_ASYNC_EM_ASM(
                    FS.syncfs(false, function() {});
                );
            }
#endif
        }
    }

    void WalletDB::onPrepareToModify()
    {
        if (!m_DbTransaction)
        {
            m_DbTransaction.reset(new sqlite::Transaction(_db));
        }
    }

    void WalletDB::notifyCoinsChanged(ChangeAction action, const vector<Coin>& items)
    {
        if (items.empty() && action != ChangeAction::Reset)
            return;

        for (const auto sub : m_subscribers)
        {
            sub->onCoinsChanged(action, items);
        }
    }

    void WalletDB::notifyTransactionChanged(ChangeAction action, const vector<TxDescription>& items)
    {
        if (items.empty() && action != ChangeAction::Reset)
            return;

        for (const auto sub : m_subscribers)
        {
            sub->onTransactionChanged(action, items);
        }
    }

    void WalletDB::notifySystemStateChanged(const HeightHash& stateID)
    {
        for (const auto sub : m_subscribers) sub->onSystemStateChanged(stateID);
    }

    void WalletDB::notifyAssetChanged(ChangeAction action, Asset::ID assetID)
    {
        for (const auto sub : m_subscribers) sub->onAssetChanged(action, assetID);
    }

    void WalletDB::notifyAddressChanged(ChangeAction action, const vector<WalletAddress>& items)
    {
        if (items.empty() && action != ChangeAction::Reset)
            return;

        for (const auto sub : m_subscribers)
        {
            sub->onAddressChanged(action, items);
        }
    }

    void WalletDB::notifyShieldedCoinsChanged(ChangeAction action, const std::vector<ShieldedCoin>& items)
    {
        if (items.empty() && action != ChangeAction::Reset)
            return;

        for (const auto sub : m_subscribers)
        {
            sub->onShieldedCoinsChanged(action, items);
        }
    }
}
