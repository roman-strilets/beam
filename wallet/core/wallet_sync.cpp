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

#include "wallet_common.h"

namespace beam::wallet
{
    // Rescan the blockchain for UTXOs and shielded coins
    void Wallet::Rescan()
    {
        AbortEvents();
        AbortBodiesRequests();
        // We save all Incoming coins of active transactions and
        // restore them after clearing db. This will save our outgoing & available amounts
        std::vector<Coin> ocoins;
        // do the same thing with shielded coins which are in progress
        std::vector<ShieldedCoin> shieldedCoins;
        for (const auto& tx : m_ActiveTransactions)
        {
            const auto& txocoins = m_WalletDB->getCoinsCreatedByTx(tx.first);
            ocoins.insert(ocoins.end(), txocoins.begin(), txocoins.end());
            auto shieldedCoin = m_WalletDB->getShieldedCoin(tx.first);

            // save newly created coins only to be able to accomplish the transaction,
            // all the others should be able to be restored from blockchain
            if (shieldedCoin && shieldedCoin->m_createTxId && *shieldedCoin->m_createTxId == tx.first)
            {
                shieldedCoins.push_back(*shieldedCoin);
            }
        }

        m_WalletDB->clearCoins();
        m_WalletDB->clearShieldedCoins();

        // Restore Incoming coins of active transactions
        m_WalletDB->saveCoins(ocoins);
        // Restore shielded coins
        for (const auto& sc : shieldedCoins)
        {
            m_WalletDB->saveShieldedCoin(sc);
        }

        storage::setNextEventHeight(*m_WalletDB, 0);
        m_WalletDB->deleteEventsFrom(0);
        ResetCommitmentsCache();
        SetTreasuryHandled(false);
        if (!m_OwnedNodesOnline)
        {
            storage::setNeedToRequestBodies(*m_WalletDB, true); // temporarily enable bodies requests
        }
        RequestBodies();
        RequestEvents();
    }

    bool Wallet::IsWalletInSync() const
    {
        HeightHash stateID;
        ZeroObject(stateID);
        m_WalletDB->getSystemStateID(stateID);
        if (!stateID.m_Height)
            return false;

        Block::SystemState::Full state;
        m_WalletDB->get_History().get_At(state, stateID.m_Height);
        return IsValidTimeStamp(state.m_TimeStamp);
    }

    Height Wallet::get_TipHeight() const
    {
        Block::SystemState::Full s;
        get_tip(s);
        return s.get_Height();
    }

    void Wallet::DoInSyncedWallet(OnSyncAction&& action)
    {
        bool bSynced = !SyncRemains() && IsNodeInSync();

        if (bSynced)
        {
            AsyncContextHolder holder(*this);
            action();
        }
        else
        {
            m_SyncActionsQueue.push(std::move(action));
        }
    }

    void Wallet::OnAsyncStarted()
    {
        if (m_AsyncUpdateCounter == 0)
        {
            BEAM_LOG_VERBOSE() << "Async update started!";
        }
        ++m_AsyncUpdateCounter;
    }

    void Wallet::OnAsyncFinished()
    {
        if (--m_AsyncUpdateCounter == 0)
        {
            BEAM_LOG_VERBOSE() << "Async update finished!";
            if (m_UpdateCompleted)
            {
                m_UpdateCompleted();
            }
        }
    }

    // Implementation of the INegotiatorGateway::get_tip
    bool Wallet::get_tip(Block::SystemState::Full& state) const
    {
        return m_WalletDB->get_History().get_Tip(state);
    }

    void Wallet::OnRolledBack()
    {
        Block::SystemState::Full sTip;
        m_WalletDB->get_History().get_Tip(sTip);

        HeightHash id;
        sTip.get_ID(id);
        BEAM_LOG_INFO() << "Rolled back to " << id;

        m_WalletDB->setSystemStateID(id);
        m_WalletDB->get_History().DeleteFrom(id.m_Height + 1);
        m_WalletDB->rollbackConfirmedUtxo(id.m_Height);
        m_WalletDB->rollbackConfirmedShieldedUtxo(id.m_Height);
        m_WalletDB->rollbackAssets(id.m_Height);
        m_WalletDB->deleteEventsFrom(id.m_Height + 1);

        // Rollback active transaction
        for (auto it = m_ActiveTransactions.begin(); m_ActiveTransactions.end() != it; it++)
        {
            const auto& pTx = it->second;
            if (pTx->Rollback(id.m_Height))
            {
                UpdateOnSynced(pTx);
            }
        }

        // Rollback inactive (completed or active) transactions if applicable
        auto txs = m_WalletDB->getTxHistory(TxType::ALL); // get list of ALL transactions
        for (auto& tx : txs)
        {
            // For all transactions that are not currently in the 'active' tx list
            if (m_ActiveTransactions.find(tx.m_txId) == m_ActiveTransactions.end())
            {
                // Reconstruct tx with reset parameters and add it to the active list
                auto pTx = ConstructTransaction(tx.m_txId, tx.m_txType);
                if (pTx && pTx->Rollback(id.m_Height))
                {
                    m_ActiveTransactions.emplace(tx.m_txId, pTx);
                    UpdateOnSynced(pTx);
                }
            }
        }

        Height h = GetEventsHeightNext();
        if (h > id.m_Height + 1)
        {
            SetEventsHeight(id.m_Height);
        }
    }

    void Wallet::OnDependentStateChanged()
    {
        uint32_t nCount = 0;
        const auto* pHv = get_DependentState(nCount);

        std::ostringstream os;
        os << "HFT state changed";

        for (uint32_t i = 0; i < nCount; i++)
            os << "\n\t" << pHv[i];

        BEAM_LOG_INFO() << os.str();

        for (auto it = m_ActiveTransactions.begin(); m_ActiveTransactions.end() != it; it++)
            it->second->OnDependentStateChanged();
    }

    void Wallet::HftSubscribe(bool bSubscribe)
    {
        if (bSubscribe)
        {
            if (!m_HftSubscribed)
                m_NodeEndpoint->DependentSubscribe(true);

            m_HftSubscribed++;
        }
        else
        {
            if (m_HftSubscribed)
            {
                --m_HftSubscribed;
                if (!m_HftSubscribed)
                    m_NodeEndpoint->DependentSubscribe(false);
            }
        }
    }

    const Merkle::Hash* Wallet::get_DependentState(uint32_t& nCount)
    {
        return m_NodeEndpoint->get_DependentState(nCount);
    }

    void Wallet::OnNewPeer(const PeerID& id, io::Address address)
    {
        constexpr size_t MaxPeers = 10;
        std::deque<io::Address> addresses;
        storage::getBlobVar(*m_WalletDB, FallbackPeers, addresses);
        addresses.push_back(address);
        if (addresses.size() > MaxPeers)
        {
            addresses.pop_front();
        }
        storage::setBlobVar(*m_WalletDB, FallbackPeers, addresses);
    }

    void Wallet::OnNewTip()
    {
        m_WalletDB->ShrinkHistory();

        Block::SystemState::Full sTip;
        get_tip(sTip);
        if (!sTip.m_Number.v)
            return; //?!

        HeightHash id;
        sTip.get_ID(id);
        BEAM_LOG_INFO() << "Sync up to " << id;

        if (!SyncRemains())
        {
            m_Extra.m_ShieldedOutputs = m_WalletDB->get_ShieldedOuts();
        }

        RequestBodies();
        RequestEvents();
        RequestStateSummary();

        for (auto& tx : m_NextTipTransactionToUpdate)
        {
            UpdateOnSynced(tx);
        }
        m_NextTipTransactionToUpdate.clear();

        CheckSyncDone();

        ProcessStoredMessages();

        ProcessWidget();
    }

    void Wallet::OnTipUnchanged()
    {
        BEAM_LOG_INFO() << "Tip has not been changed";

        RequestBodies();

        CheckSyncDone();

        ProcessStoredMessages();

        ProcessWidget();
    }

    size_t Wallet::SyncRemains() const
    {
        size_t val =
#define THE_MACRO(type) m_Pending##type.size() +
            REQUEST_TYPES_Sync(THE_MACRO)
#undef THE_MACRO
            0;

        return val;
    }

    size_t Wallet::GetSyncDone() const
    {
        auto val = SyncRemains();
        assert(val <= m_LastSyncTotal);
        size_t done = m_LastSyncTotal - val;
        done += m_BlocksDone;
        return done;
    }

    size_t Wallet::GetSyncTotal() const
    {
        return m_LastSyncTotal + m_RequestedBlocks;
    }

    void Wallet::CheckSyncDone()
    {
        ReportSyncProgress();

        if (SyncRemains())
            return;

        m_LastSyncTotal = 0;
        m_RequestedBlocks = 0;
        m_BlocksDone = 0;

        SaveKnownState();
    }

    void Wallet::SaveKnownState()
    {
        Block::SystemState::Full sTip;
        get_tip(sTip);

        HeightHash id;
        sTip.get_ID(id);

        m_WalletDB->setSystemStateID(id);
        BEAM_LOG_INFO() << "Current state is " << id;
        NotifySyncProgress();

        if (!IsValidTimeStamp(sTip.m_TimeStamp))
        {
            // we are not ready to process transactions
            return;
        }

        ActionQueue actionQueue;
        actionQueue.swap(m_SyncActionsQueue);

        if (!actionQueue.empty())
        {
            AsyncContextHolder async(*this);
            while (!actionQueue.empty())
            {
                auto& action = actionQueue.front();
                action();
                actionQueue.pop();
            }
        }

        std::set<beam::Asset::ID> assets;
        m_WalletDB->visitCoins([&assets] (const Coin& c) -> bool
        {
            if (c.m_ID.m_AssetID)
            {
                assets.insert(c.m_ID.m_AssetID);
            }
            return true;
        });

        m_WalletDB->visitAssets([&assets] (const WalletAsset& asset) -> bool
        {
           assets.erase(asset.m_ID);
           return true;
        });

        for (auto assetId: assets)
        {
            confirm_asset(assetId);
        }

        BEAM_LOG_DEBUG() << TRACE(IsMobileNodeEnabled()) << TRACE(m_Extra.m_ShieldedOutputs) << " Node shielded outs=" << m_WalletDB->get_ShieldedOuts();
        assert(m_Extra.m_ShieldedOutputs == m_WalletDB->get_ShieldedOuts());
        storage::setNeedToRequestBodies(*m_WalletDB, false); // disable body requests after importing recovery or rescan
    }

    void Wallet::NotifySyncProgress()
    {
        auto total = GetSyncTotal();
        auto done = GetSyncDone();
        assert(done <= total);
        for (const auto sub : m_subscribers)
        {
            sub->onSyncProgress(static_cast<int>(done), static_cast<int>(total));
        }
    }

    void Wallet::ReportSyncProgress()
    {
        if (!m_LastSyncTotal)
            return;
        auto total = GetSyncTotal();
        auto done = GetSyncDone();
        assert(done <= total);
        int p = static_cast<int>((done * 100) / total);
        BEAM_LOG_INFO() << "Synchronizing with node: " << p << "% (" << done << "/" << total << ")";

        NotifySyncProgress();
    }

    bool Wallet::IsNodeInSync() const
    {
        if (m_NodeEndpoint)
        {
            return IsWalletInSync();
        }
        return true; // to allow made air-gapped transactions
    }
}
