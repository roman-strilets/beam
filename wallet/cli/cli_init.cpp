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

// Wallet creation commands: init, restore, seed phrase and confirmation settings.

#include "cli_internal.h"

namespace wallet_cli
{
namespace
{
    enum struct InitKind {
        GenerateSeed,
        RecoverFromSeed,
    };

    int InitDataBase(const po::variables_map& vm, InitKind kind)
    {
        BOOST_ASSERT(vm.contains(cli::WALLET_STORAGE));
        auto walletPath = vm[cli::WALLET_STORAGE].as<string>();

        if (WalletDB::isInitialized(walletPath))
        {
            bool isDirectory = false;
#ifdef WIN32
            isDirectory = boost::filesystem::is_directory(Utf8toUtf16(walletPath.c_str()));
#else
            isDirectory = boost::filesystem::is_directory(walletPath);
#endif

            if (isDirectory)
            {
                walletPath.append("/wallet.db");
            }
            else
            {
                BEAM_LOG_ERROR() << kErrorWalletAlreadyInitialized;
                return -1;
            }
        }

        BEAM_LOG_INFO() << kStartMessage;

        SecString pass = GetPassword(vm);

        if (!vm.contains(cli::PASS) && !beam::confirm_wallet_pass(pass))
        {
            BEAM_LOG_ERROR() << kErrorWalletPwdNotMatch;
            return -1;
        }

        NoLeak<uintBig> walletSeed;
        walletSeed.V = Zero;
        if (!ReadWalletSeed(walletSeed, vm, InitKind::GenerateSeed == kind))
        {
            BEAM_LOG_ERROR() << kErrorSeedPhraseFail;
            return -1;
        }

        IWalletDB::Ptr walletDB = WalletDB::init(walletPath, pass, walletSeed);

        if (walletDB)
        {
            BEAM_LOG_INFO() << kWalletCreatedMessage;
            walletDB->generateAndSaveDefaultAddress();
            return 0;
        }
        BEAM_LOG_ERROR() << kErrorWalletNotCreated;
        return -1;
    }
}  // namespace

    int InitWallet(const po::variables_map& vm)
    {
        return InitDataBase(vm, InitKind::GenerateSeed);
    }

    int RestoreWallet(const po::variables_map& vm)
    {
        return InitDataBase(vm, InitKind::RecoverFromSeed);
    }

    int SetConfirmationsCount(const po::variables_map& vm)
    {
        uint32_t confirmations_count = vm[cli::CONFIRMATIONS_COUNT].as<Nonnegative<uint32_t>>().value;
        auto walletDB = OpenDataBase(vm);
        walletDB->setCoinConfirmationsOffset(confirmations_count);
        cout << boost::format(kCoinConfirmationsCount) % confirmations_count << std::endl;
        return 0;
    }

    int GetConfirmationsCount(const po::variables_map& vm)
    {
        auto walletDB = OpenDataBase(vm);
        auto confirmations_count = walletDB->getCoinConfirmationsOffset();
        cout << boost::format(kCoinConfirmationsCount) % confirmations_count << std::endl;
        return 0;
    }

    WordList GeneratePhrase()
    {
        auto phrase = createMnemonic(getEntropy());
        BOOST_ASSERT(phrase.size() == 12);
        cout << kSeedPhraseGeneratedTitle;
        for (const auto& word : phrase)
        {
            cout << word << ';';
        }
        cout << kSeedPhraseGeneratedMessage << endl;
        return phrase;
    }

    int GeneratePhrase(const po::variables_map& vm)
    {
        GeneratePhrase();
        return 0;
    }
}  // namespace wallet_cli
