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

// Assets info and DEX orders.

#include "wallet_client_internal.h"

namespace beam::wallet
{
#ifdef BEAM_ASSET_SWAP_SUPPORT
    void WalletClient::loadDexOrderParams()
    {
        ByteBuffer params;
        if (m_walletDB->getBlob(ASSET_SWAP_PARAMS_NAME, params))
            onAssetSwapParamsLoaded(params);
    }

    void WalletClient::storeDexOrderParams(const ByteBuffer& params)
    {
        m_walletDB->setVarRaw(ASSET_SWAP_PARAMS_NAME, params.data(), params.size());
    }

    void WalletClient::getDexOrders()
    {
        if (auto dex = _dex.lock())
        {
            onDexOrdersChanged(ChangeAction::Reset, dex->getDexOrders());
        }
    }

    void WalletClient::getDexOrder(const DexOrderID& orderId)
    {
        if (auto dex = _dex.lock())
        {
            auto order = dex->getDexOrder(orderId);
            if (order)
            {
                onFindDexOrder(*order);
            }
        }
    }

    void WalletClient::publishDexOrder(const DexOrder& order)
    {
        if (auto dex = _dex.lock())
        {
            dex->publishOrder(order);
        }
    }

    void WalletClient::cancelDexOrder(const DexOrderID& orderId)
    {
        if (auto dex = _dex.lock())
        {
            dex->cancelDexOrder(orderId);
        }
    }
#endif  // BEAM_ASSET_SWAP_SUPPORT

    void WalletClient::loadFullAssetsList()
    {
        auto wallet = m_wallet.lock();
        if (wallet)
        {
            wallet->RequestAssetsListAt(MaxHeight, [this](std::vector<Asset::Full>&& res)
            {
                std::set<Asset::ID> assetsFullList{ Asset::s_BeamID };
                for (const auto& ai : res)
                {
                    assetsFullList.insert(ai.m_ID);
                }

                postFunctionToClientContext([this, assetsFullList]()
                {
                    m_assetsFullList = std::move(assetsFullList);
                    onFullAssetsListLoaded();
                });
            });
        }
    }

    void WalletClient::getAssetInfo(const Asset::ID assetId)
    {
        m_ainfoRequests.insert(assetId);
        m_ainfoDelayed->start(0, false, [this] () {
            processAInfo();
        });
    }

    void WalletClient::onAssetChanged(ChangeAction action, Asset::ID assetId)
    {
        m_ainfoRequests.erase(assetId);
        if(const auto oasset = m_walletDB->findAsset(assetId))
        {
            onAssetInfo(assetId, *oasset);
        }
        else
        {
            WalletAsset invalid;
            onAssetInfo(assetId, invalid);
        }
    }

    void WalletClient::processAInfo ()
    {
        if (m_ainfoRequests.empty()) {
            return;
        }

        auto reqs = m_ainfoRequests;
        for(auto assetId: reqs)
        {
            if(const auto oasset = m_walletDB->findAsset(assetId))
            {
                m_ainfoRequests.erase(assetId);
                onAssetInfo(assetId, *oasset);
            }
            else
            {
                if(auto wallet = m_wallet.lock())
                {
                    wallet->ConfirmAsset(assetId);
                }
                else
                {
                    m_ainfoRequests.erase(assetId);
                    WalletAsset invalid;
                    onAssetInfo(assetId, invalid);
                }
            }
        }
    }
}
