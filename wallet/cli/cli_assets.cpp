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

// Confidential asset commands.

#include "cli_internal.h"

namespace wallet_cli
{
namespace
{
    std::string ReadAssetMeta(const po::variables_map& vm, bool allowOld)
    {
        if(!vm.count(cli::ASSET_METADATA))
        {
            throw std::runtime_error(kErrorAssetMetadataRequired);
        }

        std::string strMeta = vm[cli::ASSET_METADATA].as<std::string>();
        if (strMeta.empty())
        {
            throw std::runtime_error(kErrorAssetMetadataRequired);
        }

        WalletAssetMeta meta(strMeta);
        if (allowOld)
        {
            if (meta.isStd_v5_0() || meta.isStd_v6_0())
            {
                return strMeta;
            }
        }
        else
        {
            if (meta.isStd())
            {
                return strMeta;
            }
        }

        throw std::runtime_error(kErrorAssetNonSTDMeta);
    }

    std::string AssetID2Meta(const po::variables_map& vm, IWalletDB::Ptr walletDB)
    {
        if(!vm.count(cli::ASSET_ID))
        {
            throw std::runtime_error(kErrorAssetIDRequired);
        }

        const Asset::ID assetID = vm[cli::ASSET_ID].as<Positive<uint32_t>>().value;
        const auto info = walletDB->findAsset(assetID);
        if(!info.is_initialized())
        {
             throw std::runtime_error(kErrorAssetNotFound);
        }

        if(!info->m_IsOwned)
        {
            throw std::runtime_error(kErrorAssetNotOwned);
        }

        std::string meta;
        info->m_Metadata.get_String(meta);

        return meta;
    }

    boost::optional<TxID> IssueConsumeAsset(bool issue, const po::variables_map& vm, Wallet& wallet, IWalletDB::Ptr walletDB)
    {
        CheckAssetsAllowed(vm);

        std::string meta;
        if(vm.count(cli::ASSET_ID))
        {
            meta = AssetID2Meta(vm, walletDB);
        }
        else if (vm.count(cli::ASSET_METADATA))
        {
            meta = ReadAssetMeta(vm, true);
        }
        else
        {
            throw std::runtime_error(kErrorAssetIdOrMetaRequired);
        }

        Amount amountGroth = 0;
        constexpr Amount limit = static_cast<Amount>(std::numeric_limits<AmountSigned>::max());
        if(!ReadAmount(vm, amountGroth, limit, true))
        {
            return boost::none;
        }

        Amount fee = 0;
        if(!ReadFee(vm, fee, wallet))
        {
            return boost::none;
        }

        auto params = CreateTransactionParameters(issue ? TxType::AssetIssue : TxType::AssetConsume)
                        .SetParameter(TxParameterID::Amount, amountGroth)
                        .SetParameter(TxParameterID::Fee, fee)
                        .SetParameter(TxParameterID::PreselectedCoins, GetPreselectedCoinIDs(vm))
                        .SetParameter(TxParameterID::AssetMetadata, meta);

        return wallet.StartTransaction(params);
    }

    boost::optional<TxID> RegisterAsset(const po::variables_map& vm, Wallet& wallet)
    {
        CheckAssetsAllowed(vm);

        const auto strMeta = ReadAssetMeta(vm, false);

        Amount fee = 0;
        if (!ReadFee(vm, fee, wallet))
        {
            return boost::none;
        }

        auto params = CreateTransactionParameters(TxType::AssetReg)
                        .SetParameter(TxParameterID::Fee, fee)
                        .SetParameter(TxParameterID::PreselectedCoins, GetPreselectedCoinIDs(vm))
                        .SetParameter(TxParameterID::AssetMetadata, strMeta);

        return wallet.StartTransaction(params);
    }

    boost::optional<TxID> UnregisterAsset(const po::variables_map& vm, Wallet& wallet, IWalletDB::Ptr walletDB)
    {
        CheckAssetsAllowed(vm);

        std::string meta;
        if(vm.count(cli::ASSET_ID))
        {
            meta = AssetID2Meta(vm, walletDB);
        }
        else if (vm.count(cli::ASSET_METADATA))
        {
            meta = ReadAssetMeta(vm, true);
        }
        else
        {
            throw std::runtime_error(kErrorAssetIdOrMetaRequired);
        }

        Amount fee = 0;
        if (!ReadFee(vm, fee, wallet))
        {
            return boost::none;
        }

        auto params = CreateTransactionParameters(TxType::AssetUnreg)
                        .SetParameter(TxParameterID::Fee, fee)
                        .SetParameter(TxParameterID::PreselectedCoins, GetPreselectedCoinIDs(vm))
                        .SetParameter(TxParameterID::AssetMetadata, meta);

        return wallet.StartTransaction(params);
    }

    TxID GetAssetInfo(const po::variables_map& vm, Wallet& wallet)
    {
        CheckAssetsAllowed(vm);

        auto params = CreateTransactionParameters(TxType::AssetInfo);
        if (vm.count(cli::ASSET_ID))
        {
            Asset::ID aid = vm[cli::ASSET_ID].as<Positive<uint32_t>>().value;
            params.SetParameter(TxParameterID::AssetID, aid);
            return wallet.StartTransaction(params);
        }

        if (vm.count(cli::ASSET_METADATA))
        {
            const auto assetMeta = ReadAssetMeta(vm, true);
            params.SetParameter(TxParameterID::AssetMetadata, assetMeta);
            return wallet.StartTransaction(params);
        }

        throw std::runtime_error(kErrorAssetIdOrMetaRequired);
    }
}  // namespace

    void CheckAssetsAllowed(const po::variables_map& vm)
    {
        TxFailureReason res = wallet::CheckAssetsEnabled(MaxHeight);
        if (TxFailureReason::Count != res)
            throw std::runtime_error(GetFailureMessage(res));
    }

    int IssueAsset(const po::variables_map& vm)
    {
        return DoWalletFunc(vm, [](auto&& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
            {
                currentTxID = IssueConsumeAsset(true, vm, *wallet, walletDB);
                return currentTxID ? 0 : -1;
            });
    }

    int ConsumeAsset(const po::variables_map& vm)
    {
        return DoWalletFunc(vm, [](auto&& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
            {
                currentTxID = IssueConsumeAsset(false, vm, *wallet, walletDB);
                return currentTxID ? 0 : -1;
            });
    }

    int RegisterAsset(const po::variables_map& vm)
    {
        return DoWalletFunc(vm, [](auto&& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
            {
                currentTxID = RegisterAsset(vm, *wallet);
                return currentTxID ? 0 : -1;
            });
    }

    int UnregisterAsset(const po::variables_map& vm)
    {
        return DoWalletFunc(vm, [](auto&& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
            {
                currentTxID = UnregisterAsset(vm, *wallet, walletDB);
                return currentTxID ? 0 : -1;
            });
    }

    int GetAssetInfo(const po::variables_map& vm)
    {
        return DoWalletFunc(vm, [](auto&& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
            {
                currentTxID = GetAssetInfo(vm, *wallet);
                return currentTxID ? 0: -1;
            });
    }
}  // namespace wallet_cli
