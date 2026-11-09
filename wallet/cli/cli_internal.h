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

// Internal header shared by the cli*.cpp sources of beam-wallet: the include
// list and the commands and helpers that are used across the files.
#pragma once

#include "wallet/core/common/common.h"
#include "wallet/core/net/wallet_network.h"
#include "core/base/common.h"
#include "wallet/core/common/common_utils.h"
#include "wallet/core/wallet/wallet.h"
#include "wallet/core/db/wallet_db.h"
#include "wallet/core/net/wallet_network.h"
#include "wallet/core/tx/simple_transaction.h"
#include "wallet/core/slatepack/slatepack_endpoint.h"
#include "wallet/core/keys/secstring.h"
#include "wallet/core/common/strings_resources.h"
#include "wallet/core/contracts/shaders_manager.h"

#include "wallet/transactions/assets/assets_reg_creators.h"
#include "keykeeper/local_private_key_keeper.h"
#include "core/crypto/ecc_native.h"
#include "core/consensus/serialization_adapters.h"
#include "core/consensus/treasury.h"
#include "core/storage/block_rw.h"
#include <algorithm>
#include "mnemonic/mnemonic.h"
#include "utility/string_helpers.h"
#include "version.h"

//lelantus
#include "wallet/transactions/lelantus/pull_transaction.h"
#include "wallet/transactions/lelantus/push_transaction.h"
#include "wallet/transactions/lelantus/lelantus_reg_creators.h"

#include "utility/cli/options.h"
#include "utility/log_rotation.h"
#include "utility/helpers.h"
#include "wallet/core/assets/assets_utils.h"

#ifdef BEAM_LASER_SUPPORT
#include "laser.h"
#include "wallet/laser/mediator.h"
#endif  // BEAM_LASER_SUPPORT

#include "utils.h"

#ifdef BEAM_ASSET_SWAP_SUPPORT
#include "assets_swap.h"
#endif  // BEAM_ASSET_SWAP_SUPPORT

#include <boost/assert.hpp>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <fstream>
#include <boost/algorithm/string/trim.hpp>
#include <boost/algorithm/string/erase.hpp>

#include <iomanip>
#include <iterator>
#include <future>
#include <regex>
#include <core/consensus/block_crypt.h>

using namespace std;
using namespace beam;
using namespace beam::wallet;
using namespace ECC;

namespace beam
{
    void trimAssetName(std::string& name, int maxLen = 12);
}  // namespace beam

namespace wallet_cli
{
    // cli_common.cpp
    std::string interpretStatusCliImpl(const beam::wallet::TxDescription& tx);
    void ResolveWID(PeerID& res, const std::string& s);
    IWalletDB::Ptr OpenDataBase(const po::variables_map& vm, const SecString& pass);
    IWalletDB::Ptr OpenDataBase(const po::variables_map& vm);
    SecString GetPassword(const po::variables_map& vm);
    bool ReadWalletSeed(NoLeak<uintBig>& walletSeed, const po::variables_map& vm, bool generateNew);
    boost::optional<TxID> GetTxID(const po::variables_map& vm);
    CoinIDList GetPreselectedCoinIDs(const po::variables_map& vm);
    proto::FlyClient::NetworkStd::Ptr CreateNetwork(proto::FlyClient& fc, const po::variables_map& vm);

    // cli_treasury.cpp
    int HandleTreasury(const po::variables_map& vm);

    // cli_init.cpp
    int InitWallet(const po::variables_map& vm);
    int RestoreWallet(const po::variables_map& vm);
    int SetConfirmationsCount(const po::variables_map& vm);
    int GetConfirmationsCount(const po::variables_map& vm);
    WordList GeneratePhrase();
    int GeneratePhrase(const po::variables_map& vm);

    // cli_address.cpp
    int ChangeAddressExpiration(const po::variables_map& vm);
    int GetAddress(const po::variables_map& vm);
    int ShowAddressList(const po::variables_map& vm);
    int VerifyAddress(const po::variables_map& vm);
    int CreateNewAddress(const po::variables_map& vm);

    // cli_info.cpp
    void ShowAssetCoins(const IWalletDB::Ptr& walletDB, Asset::ID assetId, boost::optional<TxID> txID = {});
    int ShowWalletInfo(const po::variables_map& vm);

    // cli_export.cpp
    int TxDetails(const po::variables_map& vm);
    int ExportPaymentProof(const po::variables_map& vm);
    int ExportSlatepack(const po::variables_map& vm);
    int VerifyPaymentProof(const po::variables_map& vm);
    int ExportMinerKey(const po::variables_map& vm);
    int ExportOwnerKey(const po::variables_map& vm);
    int ExportWalletData(const po::variables_map& vm);
    int ImportWalletData(const po::variables_map& vm);

    // cli_assets.cpp
    void CheckAssetsAllowed(const po::variables_map& vm);
    int IssueAsset(const po::variables_map& vm);
    int ConsumeAsset(const po::variables_map& vm);
    int RegisterAsset(const po::variables_map& vm);
    int UnregisterAsset(const po::variables_map& vm);
    int GetAssetInfo(const po::variables_map& vm);

    // cli_transactions.cpp
    void OutputSlatepack(const po::variables_map& vm, const TxID& txID, const std::string& armored);
    int DoWalletFunc(
        const po::variables_map& vm
        , std::function<int(const po::variables_map&, Wallet::Ptr, IWalletDB::Ptr, boost::optional<TxID>&)> func
#ifdef BEAM_ASSET_SWAP_SUPPORT
        , std::function<int(const po::variables_map&, Wallet::Ptr, IWalletDB::Ptr, DexBoard::Ptr)> func2 =
            [] (const po::variables_map& vm, Wallet::Ptr wallet, IWalletDB::Ptr walletDB, DexBoard::Ptr dex) {return 0;}
#endif  // BEAM_ASSET_SWAP_SUPPORT
        );
#ifdef BEAM_LASER_SUPPORT
    int HandleLaser(const po::variables_map& vm);
#endif  // BEAM_LASER_SUPPORT
    int Send(const po::variables_map& vm);
    int Slatepack(const po::variables_map& vm);
    int Listen(const po::variables_map& vm);
    int Rescan(const po::variables_map& vm);
    int DeleteTransaction(const po::variables_map& vm);
    int CancelTransaction(const po::variables_map& vm);

    // cli_shader.cpp
    int ShaderInvoke(const po::variables_map& vm);
    int ShaderWidget(const po::variables_map& vm);

    // cli_blocks.cpp
    int ShowBlockDetails(const po::variables_map& vm);
    int ImportRecovery(const po::variables_map& vm);

#ifdef BEAM_ASSET_SWAP_SUPPORT
    // cli_assets_swap.cpp
    int AssetsSwapList(const po::variables_map& vm);
    int AssetSwapCreate(const po::variables_map& vm);
    int AssetSwapCancel(const po::variables_map& vm);
    int AssetSwapAccept(const po::variables_map& vm);
#endif  // BEAM_ASSET_SWAP_SUPPORT
}  // namespace wallet_cli
