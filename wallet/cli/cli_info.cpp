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

// The info command: balances, coins and transaction history.

#include "cli_internal.h"

namespace beam
{
    const char kElectrumSeparateSymbol = ' ';

    void trimAssetName(std::string& name, int maxLen)
    {
        auto len = static_cast<int>(name.length());
        if (len > maxLen)
        {
            boost::algorithm::erase_tail(name, len - maxLen);
            name.append("...");
        }
    }

    template<typename TStatusEnum>
    string getCoinStatus(TStatusEnum s)
    {
        stringstream ss;
        ss << "[";
        switch (s)
        {
        case TStatusEnum::Available:   ss << kCoinStatusAvailable; break;
        case TStatusEnum::Unavailable: ss << kCoinStatusUnavailable; break;
        case TStatusEnum::Spent:       ss << kCoinStatusSpent; break;
        case TStatusEnum::Maturing:    ss << kCoinStatusMaturing; break;
        case TStatusEnum::Outgoing:    ss << kCoinStatusOutgoing; break;
        case TStatusEnum::Incoming:    ss << kCoinStatusIncoming; break;
        case TStatusEnum::Consumed:    ss << kCoinStatusConsumed; break;
        default:
            BOOST_ASSERT_MSG(false, kErrorUnknownCoinStatus);
        }
        ss << "]";
        string str = ss.str();
        BOOST_ASSERT(str.length() <= 30);
        return str;
    }
}  // namespace beam

namespace wallet_cli
{
namespace
{
    Height ShowAssetInfo(IWalletDB::Ptr db, const storage::Totals::AssetTotals& totals)
    {
        auto isOwned  = false;
        std::string coinName = kNA;
        std::string unitName = kAmountASSET;
        std::string nthName  = kAmountAGROTH;
        std::string ownerStr = kNA;
        std::string lkHeight = kNA;
        std::string rfHeight = kNA;
        std::string emission = kNA;

        const auto info = db->findAsset(totals.AssetId);
        if (info.is_initialized())
        {
            const WalletAssetMeta &meta = WalletAssetMeta(*info);
            isOwned  = info->m_IsOwned;
            unitName = meta.GetUnitName();
            trimAssetName(unitName, 28);
            nthName  = meta.GetNthUnitName();
            trimAssetName(nthName, 28);
            ownerStr = (isOwned ? info->m_Owner.str() + "\nYou own this asset": info->m_Owner.str());
            coinName = meta.GetName() + " (" + meta.GetShortName() + ")";
            trimAssetName(coinName, 88);
            lkHeight = std::to_string(info->m_LockHeight);
            rfHeight = std::to_string(info->m_RefreshHeight);

            std::stringstream ss;

            ss << PrintableAmount(info->m_Value, true, totals.AssetId, unitName, nthName);
            emission = ss.str();
        }

        beam::AmountBig::Number available = totals.Avail;
        available += totals.AvailShielded;

        beam::AmountBig::Number unspent = totals.Unspent;
        unspent += totals.UnspentShielded;

        beam::AmountBig::Number maturing = totals.Maturing;
        maturing += totals.MaturingShielded;

        const unsigned kWidth = 26;
        cout << boost::format(kWalletAssetSummaryFormat)
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletAssetIDFormat) % totals.AssetId
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletAssetNameFormat) % coinName
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletAssetLockHeightFormat) % lkHeight
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletAssetRefreshHeightFormat) % rfHeight
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletAssetEmissionFormat) % emission
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletAssetOwnerFormat) % ownerStr
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldAvailable) % to_string(PrintableAmount(available, false, totals.AssetId, unitName, nthName))
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldMaturing) % to_string(PrintableAmount(maturing, false, totals.AssetId, unitName, nthName))
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldInProgress) % to_string(PrintableAmount(totals.Incoming, false, totals.AssetId, unitName, nthName))
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldUnavailable) % to_string(PrintableAmount(totals.Unavail, false, totals.AssetId,unitName, nthName))
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldTotalUnspent) % to_string(PrintableAmount(unspent, false, totals.AssetId, unitName, nthName));
             // % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldShielded) % to_string(PrintableAmount(totals.Shielded, false, unitName, nthName));

        auto minHeight = std::min(totals.MinCoinHeightMW, totals.MinCoinHeightShielded);
        if (!info.is_initialized())
        {
            cout << kWalletNoInfo;
        }
        else if (minHeight && info->m_LockHeight > minHeight)
        {
            cout << boost::format(kWalletUnreliableAsset) % info->m_LockHeight;
        }

        return info ? info->m_LockHeight : 0;
    }

    std::pair<std::string, std::string> GetAssetNames(IWalletDB::Ptr walletDB, Asset::ID assetId)
    {
        std::string unitName = kBEAM;
        std::string nthName  = kGROTH;

        if (assetId != Asset::s_InvalidID)
        {
            const auto info = walletDB->findAsset(assetId);
            const WalletAssetMeta &meta = info.is_initialized() ? WalletAssetMeta(*info) : WalletAssetMeta(Asset::Full());
            unitName = meta.GetUnitName();
            nthName  = meta.GetNthUnitName();
        }

        return std::make_pair(unitName, nthName);
    }

    Height GetAssetLockHeight(IWalletDB::Ptr walletDB, Asset::ID assetId)
    {
        if (assetId != Asset::s_InvalidID)
        {
            const auto info = walletDB->findAsset(assetId);
            if (info.is_initialized())
            {
                return info->m_LockHeight;
            }
        }
        return 0;
    }

    void ShowAssetTxs(const po::variables_map& vm, const IWalletDB::Ptr& walletDB, Asset::ID assetId)
    {
        const bool show = vm.count(cli::TX_HISTORY);
        if (!show) return;

        std::vector<TxDescription> txHistory;

        if (vm.count(cli::TX_HISTORY))
        {
            auto txReg     = walletDB->getTxHistory(TxType::AssetReg);
            auto txIssue   = walletDB->getTxHistory(TxType::AssetIssue);
            auto txConsume = walletDB->getTxHistory(TxType::AssetConsume);
            auto txUnreg   = walletDB->getTxHistory(TxType::AssetUnreg);
            auto txInfo    = walletDB->getTxHistory(TxType::AssetInfo);
            auto txMaxPriv = walletDB->getTxHistory(TxType::PushTransaction);
#ifdef BEAM_ASSET_SWAP_SUPPORT
            auto txAssetsSwaps = walletDB->getTxHistory(TxType::DexSimpleSwap);
#endif  // BEAM_ASSET_SWAP_SUPPORT

            if (assetId != Asset::s_InvalidID)
            {
                auto txSimple = walletDB->getTxHistory(TxType::Simple);
                txHistory.insert(txHistory.end(), txSimple.begin(), txSimple.end());
            }

            txHistory.insert(txHistory.end(), txReg.begin(), txReg.end());
            txHistory.insert(txHistory.end(), txIssue.begin(), txIssue.end());
            txHistory.insert(txHistory.end(), txConsume.begin(), txConsume.end());
            txHistory.insert(txHistory.end(), txUnreg.begin(), txUnreg.end());
            txHistory.insert(txHistory.end(), txInfo.begin(), txInfo.end());
            txHistory.insert(txHistory.end(), txMaxPriv.begin(), txMaxPriv.end());
#ifdef BEAM_ASSET_SWAP_SUPPORT
            txHistory.insert(txHistory.end(), txAssetsSwaps.begin(), txAssetsSwaps.end());
#endif  // BEAM_ASSET_SWAP_SUPPORT
        }

        std::sort(txHistory.begin(), txHistory.end(), [](const TxDescription& a, const TxDescription& b) -> bool {
            return a.m_createTime > b.m_createTime;
        });

        txHistory.erase(std::remove_if(txHistory.begin(), txHistory.end(), [&assetId](const auto& tx) {
            return tx.m_assetId != assetId;
        }), txHistory.end());

        if (txHistory.empty())
        {
            cout << kTxHistoryEmpty << endl;
        }
        else
        {
            const auto lockHeight = GetAssetLockHeight(walletDB, assetId);
            auto [unitName, nthName] = GetAssetNames(walletDB, assetId);
            if (assetId == Asset::s_InvalidID)
            {
                unitName = kAmountASSET;
                nthName = kAmountAGROTH;
            }

            boost::ignore_unused(nthName);
            trimAssetName(unitName, 7);
            const auto amountHeader = boost::format(kAssetTxHistoryColumnAmount) %  unitName;

            cout << "TRANSACTIONS" << std::endl << std::endl << std::string(120, '-') << std::endl;

            bool unreliableDisplayed = false;
            for (auto& tx : txHistory) {
                std::string direction = tx.m_sender ? kTxDirectionOut : kTxDirectionIn;
                if (tx.m_txType == TxType::AssetInfo || tx.m_txType == TxType::AssetReg || tx.m_txType == TxType::AssetUnreg ||
                    tx.m_txType == TxType::AssetIssue || tx.m_txType == TxType::AssetConsume)
                {
                    direction = tx.getTxTypeString();
                }
                else if(tx.m_selfTx)
                {
                    direction = kTxDirectionSelf;
                }

                std::string amount = to_string(PrintableAmount(tx.m_amount, true));
                if (tx.m_txType == TxType::AssetReg)
                {
                    amount += kBeamFee;
                }
                else if (tx.m_txType == TxType::AssetUnreg)
                {
                    amount += kBeamRefund;
                }
                else if (tx.m_txType == TxType::AssetInfo)
                {
                    amount = kNA;
                }

                std::string kernelId = to_string(tx.m_kernelID);
                if (tx.m_txType == TxType::AssetInfo)
                {
                    kernelId = kNA;
                }

                Height height = storage::DeduceTxDisplayHeight(*walletDB, tx);
                if (height < lockHeight && !unreliableDisplayed)
                {
                    unreliableDisplayed = true;
                    cout << kTxHistoryUnreliableTxs;
                }

                const auto tstamp = format_timestamp(kTimeStampFormat3x3, tx.m_createTime * 1000, false);
                const auto txid   = to_hex(tx.m_txId.data(), tx.m_txId.size());
                const auto token     = tx.getToken();

                cout << std::string(4, ' ') << kTxHistoryColumnDatetTime << ": " << tstamp << std::endl;
                cout << std::string(4, ' ') <<  kTxHistoryColumnHeight << ": " << static_cast<int64_t>(height) << std::endl;
#ifdef BEAM_ASSET_SWAP_SUPPORT
                if (tx.m_txType == TxType::DexSimpleSwap)
                {
                    cout << std::string(4, ' ') << "asset swap: true" << std::endl;
                }
#endif  // BEAM_ASSET_SWAP_SUPPORT
                cout << std::string(4, ' ') << kTxHistoryColumnDirection << ": " << direction << std::endl;
                cout << std::string(4, ' ') << amountHeader << ": " << amount << std::endl;
#ifdef BEAM_ASSET_SWAP_SUPPORT
                if (tx.m_txType == TxType::DexSimpleSwap)
                {
                    const auto rasset  = tx.GetParameter<Asset::ID>(TxParameterID::DexReceiveAsset);
                    const auto ramount = tx.GetParameter<Amount>(TxParameterID::DexReceiveAmount);
                    auto [unitNameSecond, nthNameSekond] = GetAssetNames(walletDB, *rasset);
                    boost::ignore_unused(nthNameSekond);
                    const auto amountSecondHeader = boost::format(kAssetTxHistoryColumnAmount) %  unitNameSecond;
                    std::string amountSecond = to_string(PrintableAmount(*ramount, true));
                    cout << std::string(4, ' ') << amountSecondHeader << ": " << amountSecond << std::endl;
                }
#endif  // BEAM_ASSET_SWAP_SUPPORT
                cout << std::string(4, ' ') <<  kTxHistoryColumnStatus << ": " << interpretStatusCliImpl(tx) << std::endl;
                cout << std::string(4, ' ') << kTxHistoryColumnId << ": " << txid << std::endl;
                if (!kernelId.empty())
                    cout << std::string(4, ' ') <<  kTxHistoryColumnKernelId << ": " << kernelId << std::endl;
                if (!token.empty())
                    cout << std::string(4, ' ') << kTxAddress << ": " << token << std::endl;
                cout << std::string(120, '-') << std::endl;
            }
        }
    }

    void ShowAssetsInfo(const po::variables_map& vm, IWalletDB::Ptr walletDB)
    {
        if (!walletDB)
        {
            walletDB = OpenDataBase(vm);
        }

        auto showAssetId = Asset::s_InvalidID;
        if (vm.count(cli::ASSET_ID))
        {
            showAssetId = vm[cli::ASSET_ID].as<Positive<uint32_t>>().value;
        }

        HeightHash stateID = {};
        walletDB->getSystemStateID(stateID);
        storage::Totals totals(*walletDB, false);

        const auto displayAsset = [&vm, &walletDB](const storage::Totals::AssetTotals &totals) {
            cout << endl;
            ShowAssetInfo(walletDB, totals);

            if (vm.count(cli::UTXO_LIST))
            {
                ShowAssetCoins(walletDB, totals.AssetId);
            }

            ShowAssetTxs(vm, walletDB, totals.AssetId);
        };

        if (showAssetId != Asset::s_InvalidID)
        {
            displayAsset(totals.GetTotals(showAssetId));
        }
        else
        {
            bool assetDisplayed = false;
            for (auto it : totals.GetAllTotals())
            {
                const auto assetId = it.second.AssetId;
                if (assetId != Asset::s_InvalidID)
                {
                    displayAsset(it.second);
                    assetDisplayed = true;
                }
            }

            //
            // Totals counts only coins. There might be transactions for assets
            // that do not have coins. Also there might be asset transactions
            // that have 0 asset id.
            //
            if (vm.count(cli::TX_HISTORY))
            {
                std::set<Asset::ID> noCoins;

                const auto filter = [&](const std::vector<TxDescription> &vec) -> bool {
                    bool orphans = false;
                    for (const auto &tx: vec)
                    {
                        orphans = orphans || tx.m_assetId == Asset::s_InvalidID;
                        if (!totals.HasTotals(tx.m_assetId))
                        {
                            noCoins.insert(tx.m_assetId);
                        }
                    }
                    return orphans;
                };

                filter(walletDB->getTxHistory(TxType::Simple));
                filter(walletDB->getTxHistory(TxType::PushTransaction)); // max privacy
                bool hasOrphaned = filter(walletDB->getTxHistory(TxType::AssetReg));
                hasOrphaned = filter(walletDB->getTxHistory(TxType::AssetIssue)) || hasOrphaned;
                hasOrphaned = filter(walletDB->getTxHistory(TxType::AssetConsume)) || hasOrphaned;
                hasOrphaned = filter(walletDB->getTxHistory(TxType::AssetUnreg)) || hasOrphaned;
                hasOrphaned = filter(walletDB->getTxHistory(TxType::AssetInfo)) || hasOrphaned;

                for (auto assetId: noCoins)
                {
                    cout << endl << endl;
                    displayAsset(totals.GetTotals(assetId));
                    assetDisplayed = true;
                }

                if (hasOrphaned)
                {
                    cout << endl << endl << kOrphanedAseetTxs << endl;
                    ShowAssetTxs(vm, walletDB, 0);
                }

                if (!assetDisplayed && !hasOrphaned)
                {
                    // if any asset without transaction has been displayed before
                    // 'no transactions' message has been displayed as well.
                    cout << kNoAssetTxsInWallet << endl;
                }
            }

            if (!assetDisplayed)
            {
                cout << kNoAssetsInWallet << endl;
            }
        }
    }

    void ShowBEAMInfo(const po::variables_map& vm, IWalletDB::Ptr walletDB)
    {
        if (!walletDB)
        {
            walletDB = OpenDataBase(vm);
        }

        HeightHash stateID = {};
        walletDB->getSystemStateID(stateID);
        storage::Totals totalsCalc(*walletDB, false);

        // Show info about BEAM
        const auto& totals = totalsCalc.GetBeamTotals();
        const unsigned kWidth = 26;

        auto avail    = totals.Avail;    avail    += totals.AvailShielded;
        auto unspent  = totals.Unspent;  unspent  += totals.UnspentShielded;
        auto maturing = totals.Maturing; maturing += totals.MaturingShielded;
        auto unavail  = totals.Unavail;  unavail  += totals.UnavailShielded;
        auto outgoing = totals.Outgoing; outgoing += totals.OutgoingShielded;
        auto incoming = totals.Incoming; incoming += totals.IncomingShielded;

        cout << boost::format(kWalletSummaryFormat)
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldCurHeight) % stateID.m_Height
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldCurStateID) % stateID.m_Hash
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldAvailable) % to_string(PrintableAmount(avail, false, 0))
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldMaturing) % to_string(PrintableAmount(maturing, false, 0))
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldInProgress) % to_string(PrintableAmount(incoming, false, 0))
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldUnavailable) % to_string(PrintableAmount(unavail, false, 0))
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldAvailableCoinbase) % to_string(PrintableAmount(totals.AvailCoinbase, false, 0))
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldTotalCoinbase) % to_string(PrintableAmount(totals.Coinbase, false, 0))
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldAvaliableFee) % to_string(PrintableAmount(totals.AvailFee, false, 0))
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldTotalFee) % to_string(PrintableAmount(totals.Fee, false, 0))
             % boost::io::group(left, setfill('.'), setw(kWidth), kWalletSummaryFieldTotalUnspent) % to_string(PrintableAmount(unspent, false, 0));

        if (vm.count(cli::UTXO_LIST))
        {
            ShowAssetCoins(walletDB, Zero);
        }
        else if (vm.count(cli::TX_HISTORY))
        {
            std::vector<TxDescription> txHistory;

            if (vm.count(cli::TX_HISTORY))
            {
#ifdef BEAM_ASSET_SWAP_SUPPORT
                std::array<TxType, 4> types = { TxType::Simple, TxType::PushTransaction, TxType::Contract, TxType::DexSimpleSwap };
#else
                std::array<TxType, 3> types = { TxType::Simple, TxType::PushTransaction, TxType::Contract };
#endif  // BEAM_ASSET_SWAP_SUPPORT
                for (auto type : types)
                {
                    auto v = walletDB->getTxHistory(type);
                    txHistory.insert(txHistory.end(), v.begin(), v.end());
                }
            }

            txHistory.erase(std::remove_if(txHistory.begin(), txHistory.end(), [](const auto& tx) {
                return tx.m_assetId != 0;
            }), txHistory.end());

            if (txHistory.empty())
            {
                cout << kTxHistoryEmpty << endl;
            }
            else
            {
                 std::sort(txHistory.begin(), txHistory.end(), [](const TxDescription& a, const TxDescription& b) -> bool {
                    return a.m_createTime > b.m_createTime;
                });

                cout << "TRANSACTIONS" << std::endl << std::endl << std::string(120, '-') << std::endl;

                for (auto& tx : txHistory) {
                    const auto tstamp    = format_timestamp(kTimeStampFormat3x3, tx.m_createTime * 1000, false);
                    const auto direction = tx.m_selfTx ? kTxDirectionSelf : (tx.m_sender ? kTxDirectionOut : kTxDirectionIn);
                    const auto amount    = to_string(PrintableAmount(tx.m_amount, true));
                    const auto txid      = to_hex(tx.m_txId.data(), tx.m_txId.size());
                    const auto krnid     = to_string(tx.m_kernelID);
                    const auto token     = tx.getToken();

                    cout << std::string(4, ' ') << kTxHistoryColumnDatetTime << ": " << tstamp << std::endl;
                    #ifdef BEAM_ASSET_SWAP_SUPPORT
                    if (tx.m_txType == TxType::DexSimpleSwap)
                    {
                        cout << std::string(4, ' ') << "asset swap: true" << std::endl;
                    }
#endif  // BEAM_ASSET_SWAP_SUPPORT
                    cout << std::string(4, ' ') << kTxHistoryColumnDirection << ": " << direction << std::endl;
                    cout << std::string(4, ' ') << kTxHistoryColumnAmount << ": " << amount << std::endl;
                    #ifdef BEAM_ASSET_SWAP_SUPPORT
                    if (tx.m_txType == TxType::DexSimpleSwap)
                    {
                        const auto rasset  = tx.GetParameter<Asset::ID>(TxParameterID::DexReceiveAsset);
                        const auto ramount = tx.GetParameter<Amount>(TxParameterID::DexReceiveAmount);
                        auto [unitNameSecond, nthNameSekond] = GetAssetNames(walletDB, *rasset);
                        boost::ignore_unused(nthNameSekond);
                        const auto amountSecondHeader = boost::format(kAssetTxHistoryColumnAmount) %  unitNameSecond;
                        std::string amountSecond = to_string(PrintableAmount(*ramount, true));
                        cout << std::string(4, ' ') << amountSecondHeader << ": " << amountSecond << std::endl;
                    }
#endif  // BEAM_ASSET_SWAP_SUPPORT
                    cout << std::string(4, ' ') << kTxHistoryColumnStatus << ": " << interpretStatusCliImpl(tx) << std::endl;
                    cout << std::string(4, ' ') << kTxHistoryColumnId << ": " << txid << std::endl;
                    if (!krnid.empty())
                        cout << std::string(4, ' ') << kTxHistoryColumnKernelId << ": " << krnid << std::endl;
                    if (!token.empty())
                        cout << std::string(4, ' ') << kTxAddress << ": " << token << std::endl;
                    cout << std::string(120, '-') << std::endl;
                }
            }
        }
        else
        {
            std::cout << "If you wish to see the list of UXTOs or transaction history use\n\t--" << cli::UTXO_LIST << "  or  --" << cli::TX_HISTORY << "  parameters correspondingly" << std::endl;
        }

    }
}  // namespace

    void ShowAssetCoins(const IWalletDB::Ptr& walletDB, Asset::ID assetId, boost::optional<TxID> txID)
    {
        auto [unitName, nthName] = GetAssetNames(walletDB, assetId);
        const uint8_t idWidth = assetId == Asset::s_InvalidID ? 49 : 57;
        const array<uint8_t, 6> columnWidths{{idWidth, 14, 14, 18, 20, 8}};

        const auto lockHeight = GetAssetLockHeight(walletDB, assetId);
        std::vector<boost::any> reliable;
        std::vector<boost::any> unreliable;

        auto isSkipedByTxID = [&](const auto& txID, const auto& createID, const auto& spentID)
        {
            if (!txID)
            {
                return false;
            }
            if (createID && *createID == *txID)
            {
                return false;
            }
            if (spentID && *spentID == *txID)
            {
                return false;
            }
            return true;
        };

        walletDB->visitCoins([&](const Coin& c)->bool {
            if (isSkipedByTxID(txID, c.m_createTxId, c.m_spentTxId))
            {
                // skip
                return true;
            }
            if (c.m_ID.m_AssetID == assetId)
            {
                if (c.m_confirmHeight < lockHeight)
                {
                    unreliable.emplace_back(c);
                }
                else
                {
                    reliable.emplace_back(c);
                }
            }
            return true;
        });

        walletDB->visitShieldedCoins([&](const ShieldedCoin& c)->bool {
            if (isSkipedByTxID(txID, c.m_createTxId, c.m_spentTxId))
            {
                // skip
                return true;
            }
            if (c.m_CoinID.m_AssetID == assetId)
            {
                if (c.m_confirmHeight < lockHeight)
                {
                    unreliable.emplace_back(c);
                }
                else
                {
                    reliable.emplace_back(c);
                }
            }
            return true;
        });

        auto getSortHeight = [&](boost::any& ca) -> Height {
            if (ca.type() == typeid(Coin))
            {
                const auto& c = boost::any_cast<Coin>(ca);
                return c.get_Maturity();
            }
            if(ca.type() == typeid(ShieldedCoin))
            {
                const auto &c = boost::any_cast<ShieldedCoin>(ca);
                return c.m_confirmHeight;
            }
            assert(false);
            return MaxHeight;
        };

        std::sort(reliable.begin(), reliable.end(), [&](boost::any& a, boost::any& b) {
            return getSortHeight(a) < getSortHeight(b);
        });

        const auto displayCoins = [&](const std::vector<boost::any>& coins) {
            if (coins.empty())
            {
                return;
            }
            for(const auto& ca: coins) {
                Amount     value = 0;
                std::string coinId;
                std::string coinStatus;
                std::string coinMaturity;
                std::string coinType;

                if (ca.type() == typeid(Coin))
                {
                    const auto& c = boost::any_cast<Coin>(ca);
                    value        = c.m_ID.m_Value;
                    coinId       = c.toStringID();
                    coinStatus   = getCoinStatus(c.m_status);
                    coinType     = FourCC::Text(c.m_ID.m_Type);
                    coinMaturity = c.IsMaturityValid() ? std::to_string(c.get_Maturity()) : "-";
                }
                else if(ca.type() == typeid(ShieldedCoin))
                {
                    const auto& c = boost::any_cast<ShieldedCoin>(ca);
                    value         = c.m_CoinID.m_Value;
                    coinId        = c.m_TxoID == ShieldedCoin::kTxoInvalidID ? "--" : std::to_string(c.m_TxoID);
                    coinStatus    = getCoinStatus(c.m_Status);
                    coinType      = "shld";
                    coinMaturity  = c.IsMaturityValid() ? std::to_string(c.get_Maturity()) : "-";
                }
                else
                {
                    assert(false); // this should never happen
                    continue;
                }

                cout << boost::format(kCoinsTableFormat)
                    % boost::io::group(right,setw(columnWidths[0]),  coinId)
                    % boost::io::group(right,setw(columnWidths[1]), value / Rules::Coin)
                    % boost::io::group(right,setw(columnWidths[2]), value % Rules::Coin)
                    % boost::io::group(left, setw(columnWidths[3]),  coinMaturity)
                    % boost::io::group(left, setw(columnWidths[4]),  coinStatus)
                    % boost::io::group(left, setw(columnWidths[5]),  coinType)
                  << std::endl;
            }
        };

        const bool hasCoins = !(reliable.empty() && unreliable.empty());
        if (hasCoins)
        {
            trimAssetName(unitName, 12);
            trimAssetName(nthName, 12);
            cout << boost::format(kCoinsTableHeadFormat)
                     % boost::io::group(right,setw(columnWidths[0]), kCoinColumnId)
                     % boost::io::group(right,setw(columnWidths[1]), unitName)
                     % boost::io::group(right,setw(columnWidths[2]), nthName)
                     % boost::io::group(left, setw(columnWidths[3]), kCoinColumnMaturity)
                     % boost::io::group(left, setw(columnWidths[4]), kCoinColumnStatus)
                     % boost::io::group(left, setw(columnWidths[5]), kCoinColumnType)
                  << std::endl;

            displayCoins(reliable);
            if (!unreliable.empty())
            {
                cout << kTxHistoryUnreliableCoins;
                displayCoins(unreliable);
            }
        }
        else
        {
            cout << kNoCoins;
        }

        cout << endl;
    }

    int ShowWalletInfo(const po::variables_map& vm)
    {
        //
        // If asset info is requested we show only it
        // Otherwise we display both BEAM & ASSETS
        //
        const bool assetsOnly = vm.count(cli::ASSET_ID) != 0;
        IWalletDB::Ptr wdb = OpenDataBase(vm);

        if (assetsOnly)
        {
            ShowAssetsInfo(vm, wdb);
        }
        else
        {
            ShowBEAMInfo(vm, wdb);
            ShowAssetsInfo(vm, wdb);
        }

        return 0;
    }
}  // namespace wallet_cli
