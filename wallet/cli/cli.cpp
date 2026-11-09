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

#include "cli_internal.h"

using namespace wallet_cli;

namespace
{
    using CommandFunc = int (*)(const po::variables_map&);
    struct Command
    {
        std::string name;
        CommandFunc handler;
        std::string description;
    };

    // simple formating
    void PrintParagraph(std::stringstream& ss, const std::string& text, size_t start, size_t end)
    {
        for (size_t s = ss.str().size(); s < start; ++s)
        {
            ss.put(' ');
        }
        size_t textPos = 0, linePos = start;
        bool skipSpaces = true;
        for (size_t i = 0; i < text.size(); ++i)
        {
            if (isspace(text[i]))
            {
                if(skipSpaces)
                {
                    for (; isspace(text[textPos]) && textPos != i; ++textPos);
                    skipSpaces = false;
                }

                for (; textPos != i; ++textPos)
                {
                    ss.put(text[textPos]);
                }
            }

            if (linePos == end)
            {
                ss.put('\n');
                {
                    size_t j = start;
                    while (j--)
                    {
                        ss.put(' ');
                    }
                }
                linePos = start;
                skipSpaces = true;
            }
            else
            {
                ++linePos;
            }
        }

        if (skipSpaces)
        {
            for (; isspace(text[textPos]) && textPos != text.size(); ++textPos);
            skipSpaces = false;
        }

        for (; textPos != text.size(); ++textPos)
        {
            ss.put(text[textPos]);
        }
    }

    void printHelp(const Command* begin, const Command* end, const po::options_description& options)
    {
        if (begin != end)
        {
            cout << "\nUSAGE: " << APP_NAME << " <command> [options] [configuration rules]\n\n";
            cout << "COMMANDS:\n";
            for (auto it = begin; it != end; ++it)
            {
                std::stringstream ss;
                ss << "  " << it->name;
                PrintParagraph(ss, it->description, 40, 80);
                ss << '\n';
                cout << ss.str();
            }
            cout << std::endl;
        }
        cout << options << std::endl;
    }
}  // namespace

io::Reactor::Ptr reactor;

static const unsigned LOG_ROTATION_PERIOD_SEC = 3*60*60; // 3 hours

thread_local const beam::Rules* beam::Rules::s_pInstance = nullptr;

int main(int argc, char* argv[])
{
    beam::Crash::InstallHandler(NULL);

    beam::Rules r;
    beam::Rules::Scope scopeRules(r);

    const Command commands[] =
    {
        {cli::INIT,               InitWallet,                       "initialize new wallet database with a new seed phrase"},
        {cli::RESTORE,            RestoreWallet,                    "restore wallet database from a seed phrase provided by the user"},
        {cli::SEND,               Send,                             "send BEAM"},
        {cli::SLATEPACK,          Slatepack,                        "import a Slatepack: review a received one (produces the reply) or finalize a reply"},
        {cli::SLATEPACK_EXPORT,   ExportSlatepack,                  "re-print the stored outgoing Slatepack for a transaction (--tx_id; --save optional)"},
        {cli::SHADER_INVOKE,      ShaderInvoke,                     "Invoke a wallet-side shader"},
        {cli::SHADER_WIDGET,      ShaderWidget,                     "Set the wallet widget shader"},
        {cli::LISTEN,             Listen,                           "listen to the node (the wallet won't close till halted"},
        {cli::TREASURY,           HandleTreasury,                   "process treasury"},
        {cli::INFO,               ShowWalletInfo,                   "print information about wallet balance and transactions"},
        {cli::EXPORT_MINER_KEY,   ExportMinerKey,                   "export miner key to pass to a mining node"},
        {cli::EXPORT_OWNER_KEY,   ExportOwnerKey,                   "export owner key to allow a node to monitor owned UTXO on the blockchain"},
        {cli::NEW_ADDRESS,        CreateNewAddress,                 "generate new SBBS address"},
        {cli::CANCEL_TX,          CancelTransaction,                "cancel transaction by ID"},
        {cli::DELETE_TX,          DeleteTransaction,                "delete transaction by ID"},
        {cli::CHANGE_ADDRESS_EXPIRATION, ChangeAddressExpiration,   "change SBBS address expiration time"},
        {cli::TX_DETAILS,         TxDetails,                        "print details of the transaction with given ID"},
        {cli::PAYMENT_PROOF_EXPORT, ExportPaymentProof,             "export payment proof by transaction ID"},
        {cli::PAYMENT_PROOF_VERIFY, VerifyPaymentProof,             "verify payment proof"},
        {cli::GENERATE_PHRASE,      GeneratePhrase,                 "generate new seed phrase"},
        {cli::WALLET_ADDRESS_LIST,  ShowAddressList,                "print addresses"},
        {cli::WALLET_ADDRESS_VERIFY, VerifyAddress,                 "verify your Endpoint on the attached HW wallet"},
        {cli::WALLET_RESCAN,        Rescan,                         "rescan the blockchain for owned UTXO (works only with node configured with an owner key)"},
        {cli::EXPORT_DATA,          ExportWalletData,               "export wallet data (UTXO, transactions, addresses) to a JSON file"},
        {cli::IMPORT_DATA,          ImportWalletData,               "import wallet data from a JSON file"},
        {cli::BLOCK_DETAILS,        ShowBlockDetails,               "print information about specified block"},
        {cli::IMPORT_RECOVERY,      ImportRecovery,                 "import block data from recovery file"},
        {cli::GET_ADDRESS,            GetAddress,                   "generate transaction address for a specific receiver (identifiable by SBBS address or wallet's signature)"},
        {cli::SET_CONFIRMATIONS_COUNT, SetConfirmationsCount,       "set count of confirmations before you can't spend coin"},
        {cli::GET_CONFIRMATIONS_COUNT, GetConfirmationsCount,       "get count of confirmations before you can't spend coin"},
#ifdef BEAM_LASER_SUPPORT
        {cli::LASER,                HandleLaser,                    "laser beam command"},
#endif  // BEAM_LASER_SUPPORT
        {cli::ASSET_ISSUE,          IssueAsset,                     "issue new confidential asset"},
        {cli::ASSET_CONSUME,        ConsumeAsset,                   "consume (burn) an existing confidential asset"},
        {cli::ASSET_REGISTER,       RegisterAsset,                  "register new asset with the blockchain"},
        {cli::ASSET_UNREGISTER,     UnregisterAsset,                "unregister asset from the blockchain"},
        {cli::ASSET_INFO,           GetAssetInfo,                   "print confidential asset information from a node"},
#ifdef BEAM_ASSET_SWAP_SUPPORT
        {cli::ASSETS_SWAP_LIST,     AssetsSwapList,                 "view available assets swap list"},
        {cli::ASSETS_SWAP_CREATE,   AssetSwapCreate,                "create asset swap"},
        {cli::ASSETS_SWAP_CANCEL,   AssetSwapCancel,                "cancel asset swap"},
        {cli::ASSETS_SWAP_ACCEPT,   AssetSwapAccept,                "accept asset swap"},
#endif  // BEAM_ASSET_SWAP_SUPPORT
    };

    try
    {
        auto [options, visibleOptions] = createOptionsDescription(GENERAL_OPTIONS | WALLET_OPTIONS, kDefaultConfigFile);

        po::variables_map vm;
        try
        {
            vm = getOptions(argc, argv, options, r, true);
        }
        catch (const po::invalid_option_value& e)
        {
            cout << e.what() << std::endl;
            return 0;
        }
        catch (const NonnegativeOptionException& e)
        {
            cout << e.what() << std::endl;
            return 0;
        }
        catch (const PositiveOptionException& e)
        {
            cout << e.what() << std::endl;
            return 0;
        }
        catch (const po::error& e)
        {
            cout << e.what() << std::endl;

            return 0;
        }

        if (vm.contains(cli::HELP))
        {
            printHelp(begin(commands), end(commands), visibleOptions);

            return 0;
        }

        if (vm.contains(cli::VERSION))
        {
            cout << PROJECT_VERSION << endl;
            return 0;
        }

        if (vm.contains(cli::GIT_COMMIT_HASH))
        {
            cout << GIT_COMMIT_HASH << endl;
            return 0;
        }

        int logLevel = getLogLevel(cli::LOG_LEVEL, vm, BEAM_LOG_LEVEL_INFO);
        int fileLogLevel = getLogLevel(cli::FILE_LOG_LEVEL, vm, BEAM_LOG_LEVEL_DEBUG);

#define LOG_FILES_DIR "logs"
#define LOG_FILES_PREFIX "wallet_"

        const auto path = boost::filesystem::system_complete(LOG_FILES_DIR);
        auto logger = beam::Logger::create(logLevel, logLevel, fileLogLevel, LOG_FILES_PREFIX, path.string());

        try
        {
            po::notify(vm);

            unsigned logCleanupPeriod = vm[cli::LOG_CLEANUP_DAYS].as<uint32_t>() * 24 * 3600;
            clean_old_logfiles(LOG_FILES_DIR, LOG_FILES_PREFIX, logCleanupPeriod);
            r.UpdateChecksum();

            // always enabled since beamX
            wallet::g_AssetsEnabled = true;

            {
                reactor = io::Reactor::create();
                io::Reactor::Scope scope(*reactor);
                io::Reactor::GracefulIntHandler gih(*reactor);
                LogRotation logRotation(*reactor, LOG_ROTATION_PERIOD_SEC, logCleanupPeriod);

                if (!vm.contains(cli::COMMAND))
                {
                    BEAM_LOG_ERROR() << kErrorCommandNotSpecified;
                    return 0;
                }

                auto command = vm[cli::COMMAND].as<string>();

                auto cit = find_if(begin(commands), end(commands), [&command](const auto& p) {return p.name == command; });
                if (cit == end(commands))
                {
                    BEAM_LOG_ERROR() << boost::format(kErrorCommandUnknown) % command;
                    return -1;
                }

                BEAM_LOG_INFO() << boost::format(kVersionInfo) % PROJECT_VERSION % BRANCH_NAME;
                BEAM_LOG_INFO() << boost::format(kRulesSignatureInfo) % Rules::get().get_SignatureStr();
                        
                return cit->handler(vm);
            }
        }
        catch (const ReceiverAddressExpiredException&)
        {
        }
        catch (const FailToStartSwapException&)
        {
        }
        catch (const FileIsNotDatabaseException&)
        {
            BEAM_LOG_ERROR() << kErrorCantOpenWallet;
            return -1;
        }
        catch (const DatabaseException & ex)
        {
            BEAM_LOG_ERROR() << ex.what();
            return -1;
        }
        catch (const po::invalid_option_value& e)
        {
            cout << e.what() << std::endl;
            return 0;
        }
        catch (const NonnegativeOptionException& e)
        {
            cout << e.what() << std::endl;
            return 0;
        }
        catch (const PositiveOptionException& e)
        {
            cout << e.what() << std::endl;
            return 0;
        }
        catch (const po::error& e)
        {
            BEAM_LOG_ERROR() << e.what();
        }
        catch (const std::runtime_error& e)
        {
            BEAM_LOG_ERROR() << e.what();
        }
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << std::endl;
    }

    return 0;
}
