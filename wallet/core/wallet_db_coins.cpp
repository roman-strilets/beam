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
        struct CoinSelector
        {
            typedef std::vector<Coin> Coins;
            typedef std::vector<size_t> Indexes;

            using Result = pair<Amount, Indexes>;

            const Coins& m_Coins; // input coins must be in ascending order, without zeroes
            
            CoinSelector(const Coins& coins)
                :m_Coins(coins)
            {
            }

            static constexpr uint32_t s_Factor = 16;
            static constexpr uint64_t s_NoOverflowSrc = uint64_t(-1) / s_Factor;

            struct Partial
            {
                static constexpr Amount s_Inf = Amount(-1);

                struct Link {
                    size_t m_iNext; // 1-based, to distinguish "NULL" pointers
                    size_t m_iElement;
                };

                std::vector<Link> m_vLinks;

                struct Slot {
                    size_t m_iTop;
                    Amount m_Sum;
                };

                Slot m_pSlots[s_Factor + 1];
                Amount m_Goal;

                void Reset()
                {
                    m_vLinks.clear();
                    ZeroObject(m_pSlots);
                }

                uint32_t get_Slot_Fast(Amount v) const
                {
                    uint64_t val = uint64_t(v) * s_Factor;
                    assert(val / s_Factor == v); // overflow should not happen
                    val /= m_Goal;

                    return (val < s_Factor) ?
                        static_cast<uint32_t>(val) :
                        s_Factor;
                }

                uint32_t get_Slot_Big(Amount v) const
                {
                    // Use slower 'robust' arithmetics, guaranteed not to overflow
                    MultiWord::NumberForType<uint32_t>::Type val;
                    val.SetDiv(MultiWord::From(v) * MultiWord::From(s_Factor), MultiWord::From(m_Goal));

                    auto res = val.get_Element<uint32_t, 0>();
                    return std::min(res, s_Factor);
                }

                uint32_t get_Slot(Amount v) const
                {
                    if (v <= s_NoOverflowSrc)
                    {
                        uint32_t res = get_Slot_Fast(v);
                        //assert(get_Slot_Big(v) == res);
                        return res;
                    }

                    return get_Slot_Big(v);
                }

                void Append(Slot& rDst, Amount v, size_t i0)
                {
                    m_vLinks.emplace_back();
                    m_vLinks.back().m_iElement = i0;
                    m_vLinks.back().m_iNext = rDst.m_iTop;

                    rDst.m_iTop = m_vLinks.size();
                    rDst.m_Sum += v;
                }

                bool IsBetter(Amount v, uint32_t iDst) const
                {
                    const Slot& rDst = m_pSlots[iDst];
                    return (s_Factor == iDst) ?
                        (!rDst.m_Sum || (v < rDst.m_Sum)) :
                        (v > rDst.m_Sum);
                }

                void AddItem(Amount v, size_t i0)
                {
                    // try combining first. Go from higher to lower, to make sure we don't process a slot which already contains this item
                    for (uint32_t iSrc = s_Factor; iSrc--; )
                    {
                        Slot& rSrc = m_pSlots[iSrc];
                        if (!rSrc.m_Sum)
                            continue;

                        Amount v2 = rSrc.m_Sum + v;
                        uint32_t iDst = get_Slot(v2);

                        if (!IsBetter(v2, iDst))
                            continue;

                        Slot& rDst = m_pSlots[iDst];

                        // improve
                        if (iSrc != iDst)
                            rDst = rSrc; // copy

                        Append(rDst, v, i0);
                    }

                    // try as-is
                    uint32_t iDst = get_Slot(v);
                    if (IsBetter(v, iDst))
                    {
                        Slot& rDst = m_pSlots[iDst];
                        ZeroObject(rDst);
                        Append(rDst, v, i0);
                    }
                }
            };

            void SolveOnce(Partial& part, Amount goal, size_t iEnd)
            {
                assert((goal > 0) && (iEnd <= m_Coins.size()));
                part.Reset();
                part.m_Goal = goal;

                for (size_t i = iEnd; i--; )
                    part.AddItem(m_Coins[i].m_ID.m_Value, i);
            }

            Result Select(Amount amount)
            {
                Partial part;
                size_t iEnd = m_Coins.size();

                Amount nOvershootPrev = Amount(-1);

                Result res;
                for (res.first = 0; (res.first < amount) && iEnd; )
                {
                    Amount goal = amount - res.first;
                    SolveOnce(part, goal, iEnd);

                    Partial::Slot& r1 = part.m_pSlots[s_Factor];
                    // reverse list direction
                    size_t iPrev = 0;
                    for (size_t i = r1.m_iTop; i; )
                    {
                        Partial::Link& link = part.m_vLinks[i - 1];
                        size_t iNext = link.m_iNext;
                        link.m_iNext = iPrev;
                        iPrev = i;
                        i = iNext;
                    }
                    r1.m_iTop = iPrev;

                    if (r1.m_Sum < goal)
                    {
                        // no solution
                        assert(!r1.m_Sum && !res.first);

                        // return the maximum we have
                        uint32_t iSlot = s_Factor - 1;
                        for ( ; iSlot > 0; iSlot--)
                            if (part.m_pSlots[iSlot].m_Sum)
                                break;

                        res.first = part.m_pSlots[iSlot].m_Sum;

                        for (size_t iLink = part.m_pSlots[iSlot].m_iTop; iLink; )
                        {
                            const Partial::Link& link = part.m_vLinks[iLink - 1];
                            iLink = link.m_iNext;

                            assert(link.m_iElement < iEnd);
                            res.second.push_back(link.m_iElement);
                        }

                        return res;
                    }

                    Amount nOvershoot = r1.m_Sum - goal;
                    bool bShouldRetry = (nOvershoot < nOvershootPrev);
                    nOvershootPrev = nOvershoot;

                    for (size_t iLink = r1.m_iTop; iLink; )
                    {
                        const Partial::Link& link = part.m_vLinks[iLink - 1];
                        iLink = link.m_iNext;

                        assert(link.m_iElement < iEnd);
                        res.second.push_back(link.m_iElement);
                        iEnd = link.m_iElement;

                        Amount v = m_Coins[link.m_iElement].m_ID.m_Value;
                        res.first += v;

                        if (bShouldRetry && (amount <= res.first + nOvershoot*2))
                            break; // leave enough window for reorgs
                    }
                }

                return res;
            }


        };
    }

    void WalletDB::selectCoins2(Height h, Amount nTrg, Asset::ID aid, std::vector<Coin>& vSelStd, std::vector<ShieldedCoin>& vSelShielded, uint32_t nMaxShielded, bool bCanReturnLess)
    {
        if (!nTrg)
            return;

        Amount nSel = 0;

        vector<ShieldedCoin::WithStatus> vShielded;
        size_t iPosShielded = 0;

        auto& fs = Transaction::FeeSettings::get(h);
        Amount feeShielded = fs.m_ShieldedInputTotal;

        if (nMaxShielded)
        {
            visitShieldedCoinsUnspent(
                [&vShielded, aid, feeShielded](const ShieldedCoin& coin) -> bool
                {
                    if ((ShieldedCoin::Status::Available == coin.m_Status) && (coin.m_CoinID.m_AssetID == aid))
                    {
                        // skip dust
                        bool bDust = !aid && (coin.m_CoinID.m_Value <= feeShielded);
                        if (!bDust)
                            vShielded.emplace_back().first = coin;
                    }
                    return true;
                }
            );

            if (!vShielded.empty())
            {
                TxoID nOuts = get_ShieldedOuts();
                for (auto& x : vShielded)
                    x.second.Init(x.first, nOuts);

                ShieldedCoin::Sort(vShielded);
            }

            for (; iPosShielded < vShielded.size(); iPosShielded++)
            {
                const auto& x = vShielded[iPosShielded];
                int n = x.second.get_SpendPriority();

                if (n < 0) // don't spend unless have to
                    break;

                if (!n && (nSel >= nTrg)) // don't spend if amount already reached
                    break;

                if (!aid)
                    nTrg += feeShielded;

                nSel += x.first.m_CoinID.m_Value;
                vSelShielded.push_back(x.first);

                if (vSelShielded.size() == nMaxShielded)
                    break;
            }
        }

        Amount nSelStd = selectCoinsStd(nTrg, nSel, aid, vSelStd);
        nSel += nSelStd;

        if (nSel >= nTrg)
            return; // ok

        for (; (nSel < nTrg) && (iPosShielded < vShielded.size()) && (vSelShielded.size() < nMaxShielded); iPosShielded++)
        {
            if (!aid)
                nTrg += feeShielded;

            const auto& x = vShielded[iPosShielded];

            nSel += x.first.m_CoinID.m_Value;
            vSelShielded.push_back(x.first);
        }

        if (nSel > nTrg)
        {
            // re-select the std coins. Maybe less would be neeed now
            nSel -= nSelStd;
            nSelStd = selectCoinsStd(nTrg, nSel, aid, vSelStd);
            nSel += nSelStd;
        }

        if ((nSel < nTrg) && !bCanReturnLess)
        {
            // failed to select needed amount. By convention don't return anything
            vSelStd.clear();
            vSelShielded.clear();
        }
    }

    Amount WalletDB::selectCoinsStd(Amount nTrg, Amount nSel, Asset::ID aid, std::vector<Coin>& vSelStd)
    {
        Amount nSelStd = 0;
        if (nSel < nTrg)
        {
            vSelStd = selectCoinsEx(nTrg - nSel, aid, true);

            for (size_t i = 0; i < vSelStd.size(); i++)
                nSelStd += vSelStd[i].m_ID.m_Value;
        }
        else
            vSelStd.clear();

        return nSelStd;
    }

    vector<Coin> WalletDB::selectCoinsEx(Amount amount, Asset::ID assetId, bool bCanReturnLess)
    {
        vector<Coin> coins, coinsSel;
        HeightHash stateID = {};
        getSystemStateID(stateID);

        {
            const char* query = "SELECT " STORAGE_FIELDS " FROM " STORAGE_NAME " WHERE maturity>=0 AND maturity<=?1 AND spentHeight<0 ORDER BY amount ASC";

            sqlite::Statement stm(this, query);
            stm.bind(1, stateID.m_Height);

            while (stm.step())
            {
                auto& coin = coins.emplace_back();
                int colIdx = 0;
                ENUM_ALL_STORAGE_FIELDS(STM_GET_LIST, NOSEP, coin);

                storage::DeduceStatus(*this, coin, stateID.m_Height);
                if (Coin::Status::Available != coin.m_status)
                    coins.pop_back();
                else if (coin.m_ID.m_AssetID != assetId)
                    coins.pop_back();
                else
                {
                    if (coin.m_ID.m_Value >= amount)
                        break;
                }
            }
        }

        CoinSelector csel(coins);
        CoinSelector::Result res = csel.Select(amount);

        if (res.first >= amount)
        {
            coinsSel.reserve(res.second.size());

            for (size_t j = 0; j < res.second.size(); j++)
                coinsSel.push_back(std::move(coins[res.second[j]]));
        }
        else
        {
            if (bCanReturnLess)
                coinsSel.swap(coins);
        }

        return coinsSel;
    }

    std::vector<Coin> WalletDB::getNormalCoins(Asset::ID assetId) const
    {
        std::vector<Coin> coins;
        visitCoins([&coins, assetId](const Coin& coin) -> bool {
            if (coin.m_ID.m_AssetID == assetId)
            {
                coins.push_back(coin);
            }
            return true;
        });
        return coins;
    }

    std::vector<Coin> WalletDB::getAllNormalCoins() const
    {
        std::vector<Coin> coins;
        visitCoins([&coins](const Coin& coin) -> bool {
            coins.push_back(coin);
            return true;
        });
        return coins;
    }

    std::vector<Coin> WalletDB::getCoinsCreatedByTx(const TxID& txId) const
    {
        // select all coins for TxID
        sqlite::Statement stm(this, "SELECT " STORAGE_FIELDS " FROM " STORAGE_NAME " WHERE createTxID=?1 ORDER BY amount DESC;");
        stm.bind(1, txId);

        vector<Coin> coins;

        while (stm.step())
        {
            auto& coin = coins.emplace_back();
            int colIdx = 0;
            ENUM_ALL_STORAGE_FIELDS(STM_GET_LIST, NOSEP, coin);
        }

        return coins;
    }

    std::vector<Coin> WalletDB::getCoinsByTx(const TxID& txId) const
    {
        sqlite::Statement stm(this, "SELECT " STORAGE_FIELDS " FROM " STORAGE_NAME " WHERE createTxID=?1 OR spentTxID=?1;");
        stm.bind(1, txId);

        vector<Coin> coins;

        while (stm.step())
        {
            auto& coin = coins.emplace_back();
            int colIdx = 0;
            ENUM_ALL_STORAGE_FIELDS(STM_GET_LIST, NOSEP, coin);
        }

        return coins;
    }

    vector<Coin> WalletDB::getCoinsByID(const CoinIDList& ids) const
    {
        vector<Coin> coins;
        coins.reserve(ids.size());
        struct DummyWrapper {
                Coin::ID m_ID;
        };

        HeightHash stateID = {};
        getSystemStateID(stateID);

        for (const auto& cid : ids)
        {
            const char* req = "SELECT * FROM " STORAGE_NAME STORAGE_WHERE_ID;
            sqlite::Statement stm(this, req);

            static_assert(sizeof(DummyWrapper) == sizeof(cid), "");
            const DummyWrapper& wrp = reinterpret_cast<const DummyWrapper&>(cid);

            int colIdx = 0;
            STORAGE_BIND_ID(wrp);

            if (stm.step())
            {
                Coin coin;
                colIdx = 0;
                ENUM_ALL_STORAGE_FIELDS(STM_GET_LIST, NOSEP, coin);
                storage::DeduceStatus(*this, coin, stateID.m_Height);
                if (Coin::Status::Available == coin.m_status)
                {
                    coins.push_back(coin);
                }
            }
        }
        return coins;
    }

    void WalletDB::insertCoinRaw(const Coin& coin)
    {
        const char* req = "INSERT INTO " STORAGE_NAME " (" ENUM_ALL_STORAGE_FIELDS(LIST, COMMA, ) ") VALUES(" ENUM_ALL_STORAGE_FIELDS(BIND_LIST, COMMA, ) ");";
        sqlite::Statement stm(this, req);

        int colIdx = 0;
        ENUM_ALL_STORAGE_FIELDS(STM_BIND_LIST, NOSEP, coin);
        stm.step();
    }

    void WalletDB::insertNewCoin(Coin& coin)
    {
        Coin cDup;
        cDup.m_ID = coin.m_ID;
        while (findCoin(cDup))
            cDup.m_ID.m_Idx++;

        coin.m_ID.m_Idx = cDup.m_ID.m_Idx;
        insertCoinRaw(coin);
    }

    bool WalletDB::updateCoinRaw(const Coin& coin)
    {
        const char* req = "UPDATE " STORAGE_NAME " SET " ENUM_STORAGE_FIELDS(SET_LIST, COMMA, ) STORAGE_WHERE_ID  ";";
        sqlite::Statement stm(this, req);

        int colIdx = 0;
        ENUM_STORAGE_FIELDS(STM_BIND_LIST, NOSEP, coin);
        ENUM_STORAGE_ID(STM_BIND_LIST, NOSEP, coin);
        stm.step();

        return sqlite3_changes(_db) > 0;
    }

    bool WalletDB::saveCoinRaw(const Coin& coin)
    {
        if (!updateCoinRaw(coin))
        {
            insertCoinRaw(coin);
            return false; // inserted
        }
        return true; // updated
    }

    vector<Coin> WalletDB::getCoinsByRowIDs(const vector<int>& rowIDs) const
    {
        vector<Coin> coins;
        coins.reserve(rowIDs.size());
        Height h = getCurrentHeight();
        sqlite::Statement stm(this, "SELECT * FROM " STORAGE_NAME " WHERE rowid=?1");
        for (int rowid : rowIDs)
        {
            stm.bind(1, rowid);
            if (stm.step())
            {
                Coin& coin = coins.emplace_back();
                int colIdx = 0;
                ENUM_ALL_STORAGE_FIELDS(STM_GET_LIST, NOSEP, coin);

                storage::DeduceStatus(*this, coin, h);
            }
            stm.Reset();
        }
        return coins;
    }

    vector<Coin> WalletDB::getUpdatedCoins(const vector<Coin>& coins) const
    {
        vector<Coin> updatedCoins = coins;
        Height h = getCurrentHeight();
        for (Coin& coin : updatedCoins)
        {
            storage::DeduceStatus(*this, coin, h);
        }
        return updatedCoins;
    }

    Coin WalletDB::generateNewCoin(Amount amount, Asset::ID assetId)
    {
        Coin coin(amount, Key::Type::Regular, assetId);
        coin.m_ID.m_Idx = get_RandomID();

        // check for collisions
        Coin cDup;
        cDup.m_ID = coin.m_ID;
        while (findCoin(cDup))
        {
            cDup.m_ID.m_Idx++;
        }

        coin.m_ID.m_Idx = cDup.m_ID.m_Idx;
        return coin;
    }

    void WalletDB::storeCoin(Coin& coin)
    {
        coin.m_ID.m_Idx = get_RandomID();
        insertNewCoin(coin);
        notifyCoinsChanged(ChangeAction::Added, {coin});
    }

    void WalletDB::storeCoins(std::vector<Coin>& coins)
    {
        if (coins.empty())
            return;

        uint64_t nKeyIndex = get_RandomID();
        for (auto& coin : coins)
        {
            coin.m_ID.m_Idx = nKeyIndex;
            insertNewCoin(coin);
            nKeyIndex = coin.m_ID.m_Idx + 1;
        }
        notifyCoinsChanged(ChangeAction::Added, coins);
    }

    void WalletDB::saveCoin(const Coin& coin)
    {
        bool updated = saveCoinRaw(coin);
        notifyCoinsChanged(updated ? ChangeAction::Updated : ChangeAction::Added, getUpdatedCoins({coin}));
    }

    void WalletDB::saveCoins(const vector<Coin>& coins)
    {
        if (coins.empty())
            return;

        for (auto& coin : coins)
        {
            saveCoinRaw(coin);
        }

        notifyCoinsChanged(ChangeAction::Updated, getUpdatedCoins(coins));
    }

    uint64_t WalletDB::AllocateKidRange(uint64_t nCount)
    {
        // a bit akward, but ok
        static const char szName[] = "LastKid";

        uint64_t nLast;
        uintBigFor<uint64_t>::Type var;

        if (storage::getVar(*this, szName, var))
        {
            var.Export(nLast);
        }
        else
        {
            nLast = getTimestamp(); // by default initialize by current time X1M (1sec resolution) to prevent collisions after reinitialization. Should be ok if creating less than 1M keys / sec average
            nLast *= 1000000;
        }

        var = nLast + nCount;
        storage::setVar(*this, szName, var);

        return nLast;
    }

    void WalletDB::removeCoins(const vector<Coin::ID>& cids)
    {
        if (cids.empty())
        {
            return;
        }

        std::vector<Coin> removed;
        for (const auto& cid: cids)
        {
            Coin coin; coin.m_ID = cid;
            if (findCoin(coin))
            {
                removed.push_back(coin);
                removeCoinImpl(cid);
            }
        }

        notifyCoinsChanged(ChangeAction::Removed, removed);
    }

    void WalletDB::removeCoinImpl(const Coin::ID& cid)
    {
        const char* req = "DELETE FROM " STORAGE_NAME STORAGE_WHERE_ID;
        sqlite::Statement stm(this, req);

        struct DummyWrapper {
            Coin::ID m_ID;
        };

        static_assert(sizeof(DummyWrapper) == sizeof(cid), "");
        const DummyWrapper& wrp = reinterpret_cast<const DummyWrapper&>(cid);

        int colIdx = 0;
        STORAGE_BIND_ID(wrp)

        stm.step();
    }

    void WalletDB::clearCoins()
    {
        sqlite::Statement stm(this, "DELETE FROM " STORAGE_NAME ";");
        stm.step();
        notifyCoinsChanged(ChangeAction::Reset, {});
    }

    void WalletDB::setCoinConfirmationsOffset(uint32_t offset)
    {
        setVarRaw(COIN_CONFIRMATIONS_COUNT, &offset, sizeof(uint32_t));
        m_coinConfirmationsOffset = offset;
    }

    uint32_t WalletDB::getCoinConfirmationsOffset() const
    {
        return m_coinConfirmationsOffset;
    }

    bool WalletDB::findCoin(Coin& coin)
    {
        const char* req = "SELECT " ENUM_STORAGE_FIELDS(LIST, COMMA, ) " FROM " STORAGE_NAME STORAGE_WHERE_ID;
        sqlite::Statement stm(this, req);

        int colIdx = 0;
        STORAGE_BIND_ID(coin)

        if (!stm.step())
            return false;

        colIdx = 0;
        ENUM_STORAGE_FIELDS(STM_GET_LIST, NOSEP, coin);

        storage::DeduceStatus(*this, coin, getCurrentHeight());

        return true;
    }

    void WalletDB::visitCoins(function<bool(const Coin& coin)> func) const
    {
        const char* req = "SELECT " STORAGE_FIELDS " FROM " STORAGE_NAME " ORDER BY ROWID;";
        sqlite::Statement stm(this, req);

        Height h = getCurrentHeight();
        while (stm.step())
        {
            Coin coin;

            int colIdx = 0;
            ENUM_ALL_STORAGE_FIELDS(STM_GET_LIST, NOSEP, coin);

            storage::DeduceStatus(*this, coin, h);

            if (!func(coin))
                break;
        }
    }

    void WalletDB::rollbackConfirmedUtxo(Height minHeight)
    {
        // UTXOs
        vector<int> changedRows;
        {
            const char* req = "SELECT rowid FROM " STORAGE_NAME " WHERE confirmHeight > ?1 OR spentHeight > ?1;";
            sqlite::Statement stm(this, req);
            stm.bind(1, minHeight);
            while (stm.step())
            {
                int& rowid = changedRows.emplace_back();
                stm.get(0, rowid);
            }
        }
        if (!changedRows.empty())
        {
            {
                const char* req = "UPDATE " STORAGE_NAME " SET confirmHeight=?1 WHERE confirmHeight > ?2;";
                sqlite::Statement stm(this, req);
                stm.bind(1, MaxHeight);
                stm.bind(2, minHeight);
                stm.step();
            }

            {
                const char* req = "UPDATE " STORAGE_NAME " SET spentHeight=?1 WHERE spentHeight > ?2;";
                sqlite::Statement stm(this, req);
                stm.bind(1, MaxHeight);
                stm.bind(2, minHeight);
                stm.step();
            }
 
            notifyCoinsChanged(ChangeAction::Updated, getCoinsByRowIDs(changedRows));
        }
    }
}
