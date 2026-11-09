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

// Wallet status: balance getters, wallet observers and status updates.

#include "wallet_client_internal.h"

namespace beam::wallet
{
    namespace
    {
        constexpr size_t kShieldedPer24hFilterBlocksForUpdate = 144;
        constexpr size_t kShieldedCountHistoryWindowSize = kShieldedPer24hFilterSize << 1;
    }

    bool WalletStatus::HasStatus(Asset::ID assetId) const
    {
        return all.find(assetId) != all.end();
    }

    WalletStatus::AssetStatus WalletStatus::GetStatus(Asset::ID assetId) const
    {
        if(all.find(assetId) == all.end())
        {
            AssetStatus result;
            return result;
        }
        return all[assetId];
    }

    WalletStatus::AssetStatus WalletStatus::GetBeamStatus() const
    {
        return GetStatus(Asset::s_BeamID);
    }

    size_t WalletClient::getUnsafeActiveTransactionsCount() const
    {
        return m_unsafeActiveTxCount;
    }

    size_t WalletClient::getUnreadNotificationsCount() const
    {
        return m_unreadNotificationsCount;
    }

    bool WalletClient::isConnectionTrusted() const
    {
        return m_isConnectionTrusted;
    }

    bool WalletClient::isSynced() const
    {
        return m_isSynced;
    }

    beam::TxoID WalletClient::getTotalShieldedCount() const
    {
        return m_status.shieldedTotalCount;
    }

    uint8_t WalletClient::getMPLockTimeLimit() const
    {
        return m_mpLockTimeLimit;
    }

    uint32_t WalletClient::getMarurityProgress(const ShieldedCoin& coin) const
    {
        ShieldedCoin::UnlinkStatus us(coin, getTotalShieldedCount());
        const auto* packedMessage = ShieldedTxo::User::ToPackedMessage(coin.m_CoinID.m_User);
        uint32_t mpAnonymitySet = packedMessage->m_MaxPrivacyMinAnonymitySet;
        return mpAnonymitySet ? us.m_Progress * mpAnonymitySet / beam::MaxPrivacyAnonimitySetFractionsCount : us.m_Progress;
    }

    uint16_t WalletClient::getMaturityHoursLeft(const ShieldedCoin& coin) const
    {
        auto& timeLimit = m_mpLockTimeLimit;

        uint16_t hoursLeftByBlocksU = 0;
        if (timeLimit)
        {
            auto& stateID = m_status.stateID;
            auto hoursLeftByBlocks = (coin.m_confirmHeight + static_cast<uint32_t>(timeLimit) * 60 - stateID.m_Height) / 60.;
            hoursLeftByBlocksU = static_cast<uint16_t>(hoursLeftByBlocks > 1 ? floor(hoursLeftByBlocks) : ceil(hoursLeftByBlocks));
        }

        if (m_shieldedPer24h)
        {
            auto outputsAddedAfterMyCoin = getTotalShieldedCount() - coin.m_TxoID;
            const auto* packedMessage = ShieldedTxo::User::ToPackedMessage(coin.m_CoinID.m_User);
            
            auto mpAnonymitySet = packedMessage->m_MaxPrivacyMinAnonymitySet;
            auto maxWindowBacklog = mpAnonymitySet ? getRules().Shielded.MaxWindowBacklog * mpAnonymitySet / 64 : getRules().Shielded.MaxWindowBacklog;
            auto outputsLeftForMP = maxWindowBacklog - outputsAddedAfterMyCoin;
            auto hoursLeft = outputsLeftForMP / static_cast<double>(m_shieldedPer24h) * 24;
            uint16_t hoursLeftU = static_cast<uint16_t>(hoursLeft > 1 ? floor(hoursLeft) : ceil(hoursLeft));
            if (timeLimit)
            {
                hoursLeftU = std::min(hoursLeftU, hoursLeftByBlocksU);
            }
            return hoursLeftU;
        }

        return timeLimit ? hoursLeftByBlocksU : std::numeric_limits<uint16_t>::max();
    }

    const Rules& WalletClient::getRules() const
    {
        return m_rules;
    }

    std::set<beam::Asset::ID> WalletClient::getAssetsFull() const
    {
        return m_assetsFullList;
    }

    std::set<beam::Asset::ID> WalletClient::getAssetsNZ() const
    {
        std::set<beam::Asset::ID> assets;

        // always have BEAM, even if zero
        assets.insert(Asset::s_BeamID);

        for (const auto& status : m_status.all)
        {
            const auto& totals = status.second;
            if (totals.available != Zero || totals.maturing != Zero || totals.maturingMP != Zero ||
                totals.receiving != Zero || totals.receivingChange != Zero || totals.receivingIncoming != Zero ||
                totals.sending != Zero || totals.shielded != Zero)
            {
                assets.insert(status.first);
            }
        }

        return assets;
    }

    beam::AmountBig::Number WalletClient::getAvailable(beam::Asset::ID id) const
    {
        const auto& status = m_status.GetStatus(id);

        auto result = status.available;
        result += status.shielded;

        return result;
    }

    beam::AmountBig::Number WalletClient::getAvailableRegular(beam::Asset::ID id) const
    {
        const auto& status = m_status.GetStatus(id);
        return status.available;
    }

    beam::AmountBig::Number WalletClient::getAvailableShielded(beam::Asset::ID id) const
    {
        const auto& status = m_status.GetStatus(id);
        return status.shielded;
    }

    beam::AmountBig::Number WalletClient::getReceiving(beam::Asset::ID id) const
    {
        const auto& status = m_status.GetStatus(id);
        return status.receiving;
    }

    beam::AmountBig::Number WalletClient::getReceivingIncoming(beam::Asset::ID id) const
    {
        const auto& status = m_status.GetStatus(id);
        return status.receivingIncoming;
    }

    beam::AmountBig::Number WalletClient::getMatutingMP(beam::Asset::ID id) const
    {
        const auto& status = m_status.GetStatus(id);
        return status.maturingMP;
    }

    bool WalletClient::hasShielded(beam::Asset::ID id) const
    {
        const auto& status = m_status.GetStatus(id);
        return status.shielded != Zero;
    }

    beam::AmountBig::Number WalletClient::getReceivingChange(beam::Asset::ID id) const
    {
        const auto& status = m_status.GetStatus(id);
        return status.receivingChange;
    }

    beam::AmountBig::Number WalletClient::getSending(beam::Asset::ID id) const
    {
        const auto& status = m_status.GetStatus(id);
        return status.sending;
    }

    beam::AmountBig::Number WalletClient::getMaturing(beam::Asset::ID id) const
    {
        const auto& status = m_status.GetStatus(id);
        return status.maturing;
    }

    beam::Height WalletClient::getCurrentHeight() const
    {
        return m_status.stateID.m_Height;
    }

    beam::Timestamp WalletClient::getCurrentHeightTimestamp() const
    {
        return m_status.update.lastTime;
    }

    beam::Timestamp WalletClient::getAverageBlockTime() const
    {
        return m_averageBlockTime;
    }

    beam::Timestamp WalletClient::getLastBlockTime() const
    {
        return m_lastBlockTime;
    }

    beam::HeightHash WalletClient::getCurrentStateID() const
    {
        return m_status.stateID;
    }

    /////////////////////////////////////////////
    /// IWalletClientAsync implementation, these method are called in background thread and could safelly access wallet DB

    void WalletClient::onCoinsChanged(ChangeAction action, const std::vector<Coin>& items)
    {
        m_CoinChangesCollector.CollectItems(action, items);
        scheduleBalance();
    }

    void WalletClient::scheduleBalance()
    {
        m_balanceDelayed->start(0, false, [this] () {
            updateStatus();
        });
    }

    void WalletClient::onTransactionChanged(ChangeAction action, const std::vector<TxDescription>& items)
    {
        if (action == ChangeAction::Added)
        {
            for (const auto& tx : items)
            {
                if (!memis0(&tx.m_txId.front(), tx.m_txId.size())) // not special transaction
                {
                    if (auto p = m_exchangeRateProvider.lock()) // none if SBBS is disabled
                    {
                        m_walletDB->setTxParameter(tx.m_txId,
                            kDefaultSubTxID,
                            TxParameterID::ExchangeRates,
                            toByteBuffer(p->getRates()),
                            false);
                    }
                }
            }
        }
        m_TransactionChangesCollector.CollectItems(action, items);
        updateClientTxState();
    }

    void WalletClient::onSystemStateChanged(const HeightHash& stateID)
    {
        updateStatus();
    }

    void WalletClient::onAddressChanged(ChangeAction action, const std::vector<WalletAddress>& items)
    {
        m_AddressChangesCollector.CollectItems(action, items);
    }

    void WalletClient::onShieldedCoinsChanged(ChangeAction action, const std::vector<ShieldedCoin>& coins)
    {
        // add virtual transaction for receiver
#ifdef BEAM_LELANTUS_SUPPORT
        m_ShieldedCoinChangesCollector.CollectItems(action, coins);
        scheduleBalance();
#endif // BEAM_LELANTUS_SUPPORT
    }

    void WalletClient::onSyncProgress(int done, int total)
    {
        auto w = m_wallet.lock();
        if (w)
        {
            postFunctionToClientContext([this, isSynced = ((done == total) && w->IsWalletInSync())]()
            {
                m_isSynced = isSynced;
            });
        }
        
        onSyncProgressUpdated(done, total);
    }

    void WalletClient::onOwnedNode(const PeerID& id, bool connected)
    {
        updateConnectionTrust(connected);
        onNodeConnectionChanged(isConnected());
    }

    void WalletClient::onIMSaved(Timestamp time, const WalletID& counterpart, const std::string& message, bool isIncome)
    {
        postFunctionToClientContext([this, time, counterpart, message, isIncome]()
        {
            onInstantMessage(time, counterpart, message, isIncome);
        });
    }

    void WalletClient::getWalletStatus()
    {
        onStatus(getStatus());
    }

    WalletStatus WalletClient::getStatus() const
    {
        WalletStatus status;
        storage::Totals allTotals(*m_walletDB, false);

        for(const auto& totalsPair: allTotals.GetAllTotals()) {
            const auto& info = totalsPair.second;
            WalletStatus::AssetStatus assetStatus;

            assetStatus.available         =  info.Avail;
            assetStatus.receivingIncoming =  info.ReceivingIncoming;
            assetStatus.receivingIncoming += info.IncomingShielded;
            assetStatus.receivingChange   =  info.ReceivingChange;
            assetStatus.receiving         =  info.Incoming;
            assetStatus.sending           =  info.Outgoing;
            assetStatus.sending           += info.OutgoingShielded;
            assetStatus.maturing          =  info.Maturing;
            assetStatus.maturingMP        =  info.MaturingShielded;
            assetStatus.shielded          =  info.AvailShielded;

            status.all[totalsPair.first] = assetStatus;
        }

        ZeroObject(status.stateID);
        m_walletDB->getSystemStateID(status.stateID);
        status.shieldedTotalCount = m_walletDB->get_ShieldedOuts();
        status.update.lastTime = m_walletDB->getLastUpdateTime();
        status.nzAssets = allTotals.GetAssetsNZ();

        return status;
    }

    void WalletClient::onNodeConnectionFailed(const proto::NodeConnection::DisconnectReason& reason)
    {
        // reason -> ErrorType
        if (proto::NodeConnection::DisconnectReason::ProcessingExc == reason.m_Type)
        {
            m_walletError = getWalletError(reason.m_ExceptionDetails.m_ExceptionType);
            onWalletError(*m_walletError);
            return;
        }

        if (proto::NodeConnection::DisconnectReason::Io == reason.m_Type)
        {
            m_walletError = getWalletError(reason.m_IoError);
            onWalletError(*m_walletError);
            return;
        }

        BEAM_LOG_ERROR() << "Unprocessed error: " << reason;
    }

    void WalletClient::onNodeConnectedStatusChanged(bool isNodeConnected)
    {
        if (isNodeConnected)
        {
            ++m_connectedNodesCount;
            m_isSynced = false;
        }
        else if (m_connectedNodesCount)
        {
            --m_connectedNodesCount;
        }

        onNodeConnectionChanged(isConnected());
    }

    void WalletClient::updateStatus()
    {
        auto status = getStatus();
        updateMaxPrivacyStats(status);
        updateClientState(status);
        onStatus(status);
    }

    void WalletClient::updateClientState(const WalletStatus& status)
    {
        if (auto w = m_wallet.lock(); w)
        {
            postFunctionToClientContext([this, currentHeight = m_walletDB->getCurrentHeight()
                , count = w->GetUnsafeActiveTransactionsCount()
                , status
                , limit = m_walletDB->get_MaxPrivacyLockTimeLimitHours()]()
            {
                m_status = status;
                m_currentHeight = currentHeight;
                m_unsafeActiveTxCount = count;
                m_mpLockTimeLimit = limit;
            });

            auto currentHeight = w->get_TipHeight();

            struct Walker :public Block::SystemState::IHistory::IWalker
            {
                std::vector<Block::SystemState::Full> m_vStates;
                uint32_t m_Count;

                bool OnState(const Block::SystemState::Full& s) override
                {
                    m_vStates.push_back(s);
                    return m_vStates.size() < m_Count;
                }
            } walker;

            walker.m_Count = 10;
            walker.m_vStates.reserve(10);
            Height historyHeight = currentHeight - 10;
            m_walletDB->get_History().Enum(walker, &historyHeight);

            if (walker.m_vStates.empty())
                return;

            auto oldest = walker.m_vStates[walker.m_vStates.size() - 1];
            Block::SystemState::Full curentState;
            m_walletDB->get_History().get_Tip(curentState);
            auto distance = currentHeight - oldest.get_Height();
            auto averageBlockTime = distance ? (curentState.m_TimeStamp - oldest.m_TimeStamp) / distance : 0; 
            auto lastBlockTime = curentState.m_TimeStamp;

            postFunctionToClientContext([this, averageBlockTime, lastBlockTime]()
            {
                m_averageBlockTime = averageBlockTime;
                m_lastBlockTime = lastBlockTime;
            });
        }
    }

    void WalletClient::updateMaxPrivacyStats(const WalletStatus& status)
    {
        if (!(status.stateID.m_Height % kShieldedPer24hFilterBlocksForUpdate))
        {
            updateMaxPrivacyStatsImpl(status);
        }
    }

    void WalletClient::updateMaxPrivacyStatsImpl(const WalletStatus& status)
    {
        m_shieldedCountHistoryPart.clear();

        if (status.stateID.m_Height > kShieldedPer24hFilterBlocksForUpdate * kShieldedCountHistoryWindowSize)
        {
            auto w = m_wallet.lock();
            if (!w)
            {
                return;
            }

            m_shieldedCountHistoryPart.reserve(kShieldedCountHistoryWindowSize);

            for (uint8_t i = 0; i < kShieldedCountHistoryWindowSize; ++i)
            {
                auto h = status.stateID.m_Height - (kShieldedPer24hFilterBlocksForUpdate * i);

                w->RequestShieldedOutputsAt(h, [this](Height h, TxoID count)
                {
                    m_shieldedCountHistoryPart.emplace_back(h, count);
                    if (m_shieldedCountHistoryPart.size() == kShieldedCountHistoryWindowSize)
                    {
                        for (uint8_t i = 0; i < kShieldedPer24hFilterSize; ++i)
                        {
                            if (m_shieldedCountHistoryPart[i].second)
                            {
                                double b = static_cast<double>(m_shieldedCountHistoryPart[i].second - m_shieldedCountHistoryPart[i + kShieldedPer24hFilterSize].second);
                                m_shieldedPer24hFilter->addSample(b);
                            }
                            else
                            {
                                m_shieldedPer24hFilter->addSample(0);
                            }
                        }
                        auto shieldedPer24h = static_cast<TxoID>(floor(m_shieldedPer24hFilter->getAverage() * 10));

                        postFunctionToClientContext([this, shieldedPer24h]()
                        {
                            m_shieldedPer24h = shieldedPer24h;
                        });
                    }
                });
            }
        }
    }

    void WalletClient::updateClientTxState()
    {
        if (auto w = m_wallet.lock(); w)
        {
            postFunctionToClientContext([this, count = w->GetUnsafeActiveTransactionsCount()]()
            {
                m_unsafeActiveTxCount = count;
            });
        }
    }

    void WalletClient::updateNotifications()
    {
        size_t count = m_notificationCenter->getUnreadCount(
            [this] (std::vector<Notification>::const_iterator first, std::vector<Notification>::const_iterator last)
            {
                auto currentLibVersion = getLibVersion();
                auto currentClientRevision = getClientRevision();
                return std::count_if(first, last,
                    [&currentLibVersion, &currentClientRevision](const auto& p)
                    {
                        if (p.m_state == Notification::State::Unread)
                        {
                            if (p.m_type == Notification::Type::WalletImplUpdateAvailable)
                            {
                                WalletImplVerInfo info;
                                if (fromByteBuffer(p.m_content, info) &&
                                    VersionInfo::Application::DesktopWallet == info.m_application &&
                                    (currentLibVersion < info.m_version ||
                                    (currentLibVersion == info.m_version && currentClientRevision < info.m_UIrevision)))
                                {
                                    return true;
                                }
                            }
                            if (p.m_type == Notification::Type::TransactionFailed)
                            {
                                return true;
                            }
                        }
                        return false;
                    });
            });
        postFunctionToClientContext([this, count]()
        {
            m_unreadNotificationsCount = count;
        });
    }

    void WalletClient::updateConnectionTrust(bool trustedConnected)
    {
        if (trustedConnected)
        {
            ++m_trustedConnectionCount;
        }
        else if (m_trustedConnectionCount)
        {
            --m_trustedConnectionCount;
        }

        postFunctionToClientContext([this, isTrusted = m_trustedConnectionCount > 0/* && m_trustedConnectionCount == m_connectedNodesCount*/]()
        {
            m_isConnectionTrusted = isTrusted;
        });
    }

    bool WalletClient::isConnected() const
    {
        return m_connectedNodesCount > 0;
    }
}
