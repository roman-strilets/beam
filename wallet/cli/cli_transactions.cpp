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

// DoWalletFunc, which runs a command against a live wallet, and the transaction commands built on it.

#include "cli_internal.h"

namespace wallet_cli
{
namespace
{
    // The CLI runs one command per process against one wallet, so a single
    // process-wide handle to the manual-transport endpoint is safe. Mirrors
    // WalletClient's m_slatepackEndpoint weak_ptr member.
    std::weak_ptr<beam::wallet::SlatepackEndpoint> g_cliSlatepack;

    std::shared_ptr<IWalletMessageEndpoint> MaybeCreateSbbsNetwork(const po::variables_map& vm, IWalletMessageConsumer& trg, const proto::FlyClient::INetwork::Ptr& net, const IWalletDB::Ptr& db)
    {
        if (!vm[cli::WALLET_BBS_ENABLE].as<bool>())
            return {};

        return make_shared<WalletNetworkViaBbs>(trg, net, db);
    }
}  // namespace

    // Emit a produced/stored Slatepack: print it, or write it to a file when --save is given
    // (bare --save -> <wallet_dir>/<txid>.slatepack, --save=<dir>, or --save=<file>).
    void OutputSlatepack(const po::variables_map& vm, const TxID& txID, const std::string& armored)
    {
        if (vm.count(cli::SLATEPACK_SAVE))
        {
            namespace fs = boost::filesystem;
            const std::string opt = vm[cli::SLATEPACK_SAVE].as<std::string>();
            const std::string name = beam::to_string(txID) + ".slatepack";
            fs::path out;
            if (opt.empty())                    // bare --save -> wallet dir
                out = fs::path(vm[cli::WALLET_STORAGE].as<std::string>()).parent_path() / name;
            else if (fs::is_directory(opt))     // --save=<dir>
                out = fs::path(opt) / name;
            else                                // --save=<file>
                out = fs::path(opt);
            std::ofstream f(out.string(), std::ios::binary | std::ios::trunc);
            f << armored;
            f.close();
            if (f.fail())
            {
                BEAM_LOG_ERROR() << "Cannot write Slatepack to " << fs::absolute(out).string();
                std::cout << armored << std::endl; // don't lose it: fall back to stdout
                return;
            }
            std::cout << "Saved Slatepack to " << fs::absolute(out).string() << std::endl;
        }
        else
        {
            std::cout << armored << std::endl;
        }
    }

#ifdef BEAM_LASER_SUPPORT

    int HandleLaser(const po::variables_map& vm)
    {
        auto walletDB = OpenDataBase(vm);
        auto laser =
            std::make_unique<laser::Mediator>(walletDB);

        if (vm.count(cli::LASER_LIST))
        {
            LaserShow(walletDB);
            return 0;
        }

        if (vm.count(cli::LASER_DELETE))
        {
            if (LaserDelete(laser, vm))
            {
                LaserShow(walletDB);
                return 0;
            }
            return -1;
        }

        auto nnet = CreateNetwork(*laser, vm);
        if (!nnet)
        {
            return -1;
        }

        struct EmptyMsgConsumer :public IWalletMessageConsumer {
            void OnWalletMessage(const WalletID&, const SetTxParameter&, IWalletMessageEndpoint*) override {}
        } msgConsumer;

        auto wnet = MaybeCreateSbbsNetwork(vm, msgConsumer, nnet, walletDB);
        if (!wnet)
            return -1;

        laser->SetNetwork(nnet, *wnet);
        laser->ListenClosedChannelsWithPossibleRollback();

        LaserObserver laserObserver(walletDB, vm);
        laser->AddObserver(&laserObserver);

        if (ProcessLaser(laser, walletDB, vm))
        {
            io::Reactor::get_Current().run();
            return 0;
        }

        return -1;
    }

#endif  // BEAM_LASER_SUPPORT

    int DoWalletFunc(
        const po::variables_map& vm
        , std::function<int(const po::variables_map&, Wallet::Ptr, IWalletDB::Ptr, boost::optional<TxID>&)> func
#ifdef BEAM_ASSET_SWAP_SUPPORT
        , std::function<int(const po::variables_map&, Wallet::Ptr, IWalletDB::Ptr, DexBoard::Ptr)> func2
#endif  // BEAM_ASSET_SWAP_SUPPORT
        )
    {
        BEAM_LOG_INFO() << kStartMessage;
        auto walletDB = OpenDataBase(vm);

        bool isServer = vm[cli::COMMAND].as<string>() == cli::LISTEN || vm.count(cli::LISTEN);

        boost::optional<TxID> currentTxID;
        auto onTxCompleteAction = [&currentTxID](const TxID& txID)
        {
            if (currentTxID.is_initialized() && (currentTxID.get() == txID))
                io::Reactor::get_Current().stop();
        };

        auto txCompletedAction = isServer ? Wallet::TxCompletedAction() : onTxCompleteAction;

        if (vm[cli::COMMAND].as<string>() == cli::ASSET_INFO || vm.count(cli::ASSET_INFO))
        {
            txCompletedAction = [&onTxCompleteAction, &walletDB](const TxID& txID) {
                auto tx = walletDB->getTx(txID);
                if (tx)
                {
                    const TxDescription& desc = *tx;
                    const auto info = walletDB->findAsset(desc.m_assetId);
                    Amount maxTxAmount = std::numeric_limits<AmountSigned>::max();
                    if (AmountBig::get_Hi(info->m_Value) || AmountBig::get_Lo(info->m_Value) > maxTxAmount)
                    {
                        auto maxTxValue = PrintableAmount(maxTxAmount, true, info->m_ID);
                        cout << "Warning. Total amount of asset would be larger that can be sent in one transaction "
                            << maxTxValue << ". You would be forced to send using several transactions."
                            << endl;
                    }
                }

                onTxCompleteAction(txID);
            };
        }

        auto wallet = std::make_shared<Wallet>(walletDB,
            std::move(txCompletedAction),
            Wallet::UpdateCompletedAction());
        {
            wallet::AsyncContextHolder holder(*wallet);

#ifdef BEAM_LELANTUS_SUPPORT
            lelantus::RegisterCreators(*wallet, walletDB);
#endif

#ifdef BEAM_CONFIDENTIAL_ASSETS_SUPPORT
            if (Rules::get().CA.Enabled && wallet::g_AssetsEnabled)
            {
                RegisterAllAssetCreators(*wallet);
#ifdef BEAM_ASSET_SWAP_SUPPORT
                wallet->RegisterTransactionType(
                    TxType::DexSimpleSwap, std::make_shared<DexTransaction::Creator>(walletDB));
#endif  // BEAM_ASSET_SWAP_SUPPORT
            }
#endif  // BEAM_CONFIDENTIAL_ASSETS_SUPPORT
            if (vm.count(cli::REQUEST_BODIES) && vm[cli::REQUEST_BODIES].as<bool>())
            {
                wallet->EnableBodyRequests(true);
            }
            auto nnet = CreateNetwork(*wallet, vm);
            if (!nnet)
            {
                return -1;
            }

            auto wnet = MaybeCreateSbbsNetwork(vm, *wallet, nnet, walletDB);
            if (wnet)
            {
                wallet->AddMessageEndpoint(wnet);
                if (!vm[cli::WALLET_BBS_ENABLE_TXS].as<bool>())
                    wallet->m_pBlockedForTxs = wnet.get();
            }

            // Manual (Slatepack) transport: prints the produced pack, or saves it to a
            // file when --save is given, then stops the reactor. Only fires for
            // ManualTransport-flagged txs, so normal sends are unaffected.
            auto slatepackEndpoint = make_shared<SlatepackEndpoint>(*wallet, walletDB,
                [&vm](const TxID& txID, const std::string& armored)
                {
                    OutputSlatepack(vm, txID, armored);
                    io::Reactor::get_Current().stop();
                });
            wallet->AddMessageEndpoint(slatepackEndpoint);
            g_cliSlatepack = slatepackEndpoint;

            wallet->SetNodeEndpoint(nnet);

            wallet->ResumeAllTransactions();

#ifdef BEAM_ASSET_SWAP_SUPPORT
            AssetsSwapCliHandler assetsSwapHandler;
            if (wnet)
                assetsSwapHandler.init(wallet, walletDB, nnet, *wnet);
#endif  // BEAM_ASSET_SWAP_SUPPORT

            int res = func(vm, wallet, walletDB, currentTxID);
            if (res != 0)
            {
                return res;
            }
#ifdef BEAM_ASSET_SWAP_SUPPORT
            if (wnet)
            {
                res = func2(vm, wallet, walletDB, assetsSwapHandler.getDex());
                if (res != 0)
                {
                    return res;
                }
            }
#endif  // BEAM_ASSET_SWAP_SUPPORT
        }
        io::Reactor::get_Current().run();
        return 0;
    }

    int Send(const po::variables_map& vm)
    {
        return DoWalletFunc(vm, [](auto&& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
            {
                io::Address receiverAddr;
                Asset::ID assetId = Asset::s_BeamID;
                Amount amount = 0;
                Amount fee = 0;
                WalletID receiverWalletID(Zero);

                if (!LoadBaseParamsForTX(vm, *wallet, assetId, amount, fee, receiverWalletID))
                {
                    return -1;
                }

                if (assetId != Asset::s_BeamID)
                {
                    CheckAssetsAllowed(vm);
                }

                auto params = CreateSimpleTransactionParameters();
                LoadReceiverParams(vm, params);

                params
                    .SetParameter(TxParameterID::Amount, amount)
                    // fee for shielded inputs included automatically
                    .SetParameter(TxParameterID::Fee, fee)
                    .SetParameter(TxParameterID::AssetID, assetId)
                    .SetParameter(TxParameterID::PreselectedCoins, GetPreselectedCoinIDs(vm));

                if (vm.count(cli::SLATEPACK) && vm[cli::SLATEPACK].template as<bool>())
                    params.SetParameter(TxParameterID::ManualTransport, true);

                currentTxID = wallet->StartTransaction(params);
                return 0;
            });
    }

    int Slatepack(const po::variables_map& vm)
    {
        return DoWalletFunc(vm, [](auto&& vm, auto&& wallet, auto&& walletDB, auto& currentTxID) -> int
            {
                std::string armored, readError;
                if (!read_slatepack(armored, vm, readError))
                {
                    std::cout << "No Slatepack provided: " << readError << std::endl;
                    return -1;
                }

                auto ep = g_cliSlatepack.lock();
                if (!ep)
                {
                    std::cout << "Slatepack transport not available." << std::endl;
                    return -1;
                }

                slatepack::Error error = slatepack::Error::None;
                SlatepackEndpoint::ImportInfo info;
                if (!ep->Preview(armored, error, info))
                {
                    std::cout << "Cannot import Slatepack: " << slatepack::ErrorToString(error) << std::endl;
                    return -1;
                }

                std::cout << (info.m_IsSend ? "You are sending " : "You are receiving ")
                          << PrintableAmount(info.m_Amount, true, info.m_AssetID)
                          << (info.m_IsSend ? " to " : " from ")
                          << (info.m_IsSend ? info.m_AddressTo : info.m_AddressFrom) << "\n"
                          << "Fee: " << PrintableAmount(info.m_Fee, true, Asset::s_BeamID) << "\n"
                          << "Transaction: " << info.m_TxID << std::endl;

                // --yes lets a script drive this. Without it we prompt - and a prompt that cannot
                // be answered (stdin already consumed by a pasted pack, or a pipe at EOF) is a
                // decline, never a silent accept.
                if (vm.count(cli::SLATEPACK_YES) && vm[cli::SLATEPACK_YES].template as<bool>())
                {
                    std::cout << "Proceeding (--" << cli::SLATEPACK_YES << ")." << std::endl;
                }
                else
                {
                    std::cout << "Proceed? (y/n)" << std::endl;
                    std::string answer;
                    if (!(std::cin >> answer) || (answer != "y" && answer != "Y"))
                    {
                        ep->CancelPending(info.m_TxID);
                        std::cout << "Cancelled." << std::endl;
                        return -1; // non-zero: skips DoWalletFunc's reactor.run(), exits cleanly
                    }
                }

                // Track this tx so onTxCompleteAction stops the reactor when it finalizes
                // (the reply case is stopped earlier by the outgoing handler instead).
                auto txIdVec = from_hex(info.m_TxID);
                if (txIdVec.size() >= 16)
                {
                    TxID t;
                    std::copy_n(txIdVec.begin(), 16, t.begin());
                    currentTxID = t;
                }

                if (!ep->Commit(info.m_TxID, error))
                {
                    std::cout << "Cannot process Slatepack: " << slatepack::ErrorToString(error) << std::endl;
                    return -1;
                }
                return 0; // reactor runs: outgoing handler (reply) or tx-completed (finalize) stops it
            });
    }

    int Listen(const po::variables_map& vm)
    {
        return DoWalletFunc(vm, [](auto&& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
            {
                return 0;
            });
    }

    int Rescan(const po::variables_map& vm)
    {
        return DoWalletFunc(vm, [](auto&& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
            {
                wallet->Rescan();
                return 0;
            });
    }

    int DeleteTransaction(const po::variables_map& vm)
    {
        auto txId = GetTxID(vm);
        if (!txId)
        {
            return -1;
        }

        auto walletDB = OpenDataBase(vm);
        auto tx = walletDB->getTx(*txId);
        if (tx)
        {
            BEAM_LOG_INFO() << "deleting tx " << *txId;
            if (tx->canDelete())
            {
                walletDB->deleteTx(*txId);
                return 0;
            }
            BEAM_LOG_ERROR() << kErrorTxStatusInvalid;
            return -1;
        }

        BEAM_LOG_ERROR() << kErrorTxIdUnknown;
        return -1;
    }

    int CancelTransaction(const po::variables_map& vm)
    {
        auto txId = GetTxID(vm);
        if (!txId)
        {
            return -1;
        }

        return DoWalletFunc(vm, [&txId](auto&& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
            {
                auto tx = walletDB->getTx(*txId);
                if (tx)
                {
                    if (wallet->CanCancelTransaction(*txId))
                    {
                        currentTxID = *txId;
                        wallet->CancelTransaction(*txId);
                        return 0;
                    }
                    BEAM_LOG_ERROR() << kErrorCancelTxInInvalidStatus << interpretStatusCliImpl(*tx);
                    return -1;
                }
                BEAM_LOG_ERROR() << kErrorTxIdUnknown;
                return -1;
            });
    }
}  // namespace wallet_cli
