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

// Asset swap commands.

#include "cli_internal.h"

namespace wallet_cli
{
    int AssetsSwapList(const po::variables_map& vm)
    {
        CheckAssetsAllowed(vm);

        return DoWalletFunc(vm, [](auto&& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
        {
            return 0;
        },
        [](auto&& vm, auto&& wallet, auto&& walletDB, DexBoard::Ptr dex)
        {
            auto orders = dex->getDexOrders();

            const array<uint8_t, 10> columnWidths{ {32, 18, 13, 10, 18, 16, 10, 13, 15, 6} };
            cout << boost::format(kAssetsSwapTableHead)
                % boost::io::group(left, setw(columnWidths[0]), kAssetsSwapID)
                % boost::io::group(right, setw(columnWidths[1]), kAssetsSwapSendAmount)
                % boost::io::group(right, setw(columnWidths[2]), kAssetsSwapSendAssetID)
                % boost::io::group(left, setw(columnWidths[3]), kAssetsSwapSendAssetName)
                % boost::io::group(right, setw(columnWidths[4]), kAssetsSwapReceiveAmount)
                % boost::io::group(right, setw(columnWidths[5]), kAssetsSwapReceiveAssetID)
                % boost::io::group(left, setw(columnWidths[6]), kAssetsSwapReceiveAssetName)
                % boost::io::group(right, setw(columnWidths[7]), kAssetsSwapCreation)
                % boost::io::group(right, setw(columnWidths[8]), kAssetsSwapExpiration)
                % boost::io::group(left, setw(columnWidths[9]), kAssetsSwapIsMine)
                << std::endl;

            for (auto order : orders)
            {
                cout << boost::format(kAssetsSwapTableFormat)
                    % boost::io::group(left,setw(columnWidths[0]), order.getID().to_string())
                    % boost::io::group(right,setw(columnWidths[1]), order.getSendAmount())
                    % boost::io::group(right,setw(columnWidths[2]), order.getSendAssetId())
                    % boost::io::group(left, setw(columnWidths[3]), order.getSendAssetSName())
                    % boost::io::group(right, setw(columnWidths[4]), order.getReceiveAmount())
                    % boost::io::group(right, setw(columnWidths[5]), order.getReceiveAssetId())
                    % boost::io::group(left,setw(columnWidths[6]), order.getReceiveAssetSName())
                    % boost::io::group(right,setw(columnWidths[7]), order.getCreation())
                    % boost::io::group(right,setw(columnWidths[8]), order.getExpiration())
                    % boost::io::group(left, setw(columnWidths[9]), order.isMine() ? "true" : "false")
                  << std::endl;
            }
            return 1;
        });
    }

    int AssetSwapCreate(const po::variables_map& vm)
    {
        CheckAssetsAllowed(vm);

        auto sendAssetId = Asset::s_InvalidID;
        if (auto it = vm.find(cli::ASSETS_SWAP_SEND_ASSET_ID); it != vm.end())
        {
            sendAssetId = vm[cli::ASSETS_SWAP_SEND_ASSET_ID].as<Nonnegative<uint32_t>>().value;
        }
        else
        {
            cout << "You must provide send asset id";
            return -1;
        }

        auto receiveAssetId = Asset::s_InvalidID;
        if (auto it = vm.find(cli::ASSETS_SWAP_RECEIVE_ASSET_ID); it != vm.end())
        {
            receiveAssetId = vm[cli::ASSETS_SWAP_RECEIVE_ASSET_ID].as<Nonnegative<uint32_t>>().value;
        }
        else
        {
            cout << "You must provide receive asset id";
            return -1;
        }

        if (receiveAssetId == sendAssetId)
        {
            cout << "Send and receive assets ID can't be identical.";
            return -1;
        }

        if (auto it = vm.find(cli::ASSETS_SWAP_SEND_AMOUNT); it == vm.end())
        {
            cout << "You must provide send amount";
            return -1;
        }

        auto sendAmount = vm[cli::ASSETS_SWAP_SEND_AMOUNT].as<Positive<double>>().value;
        sendAmount *= Rules::Coin;
        Amount sendAmountGroth = static_cast<Amount>(std::round(sendAmount));

        if (auto it = vm.find(cli::ASSETS_SWAP_RECEIVE_AMOUNT); it == vm.end())
        {
            cout << "You must provide receive amount";
            return -1;
        }

        auto receiveAmount = vm[cli::ASSETS_SWAP_RECEIVE_AMOUNT].as<Positive<double>>().value;
        receiveAmount *= Rules::Coin;
        Amount receiveAmountGroth = static_cast<ECC::Amount>(std::round(receiveAmount));

        unsigned expirationMinutes = vm[cli::ASSETS_SWAP_EXPIRATION].as<uint32_t>();

        if (expirationMinutes < 30 || expirationMinutes > 720)
        {
            cout << "minutes_before_expire must be > 30 and < 720";
            return -1;
        }

        return DoWalletFunc(vm, [](auto&& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
        {
            return 0;
        },
        [sendAssetId, receiveAssetId, sendAmountGroth,
         receiveAmountGroth, expirationMinutes](auto&& vm, auto&& wallet, auto&& walletDB, DexBoard::Ptr dex)
        {
            std::string sendAssetUnitName = "BEAM";
            if (sendAssetId)
            {
                auto sendAssetInfo = walletDB->findAsset(sendAssetId);
                if (!sendAssetInfo)
                {
                    cout << "ERROR: Unknown asset id - " << cli::ASSETS_SWAP_SEND_ASSET_ID;
                }
                const auto &sendAssetMeta = WalletAssetMeta(*sendAssetInfo);
                sendAssetUnitName = sendAssetMeta.GetUnitName();
            }

            std::string receiveAssetUnitName = "BEAM";
            if (receiveAssetId)
            {
                auto receiveAssetInfo = walletDB->findAsset(receiveAssetId);
                if (!receiveAssetInfo)
                {
                    cout << "ERROR: Unknown asset id - " << cli::ASSETS_SWAP_RECEIVE_ASSET_ID;
                }
                const auto &receiveAssetMeta = WalletAssetMeta(*receiveAssetInfo);
                receiveAssetUnitName = receiveAssetMeta.GetUnitName();
            }

            uint64_t keyID = walletDB->AllocateKidRange(1);
            WalletID wid;
            walletDB->get_SbbsWalletID(wid, keyID);

            DexOrder orderObj(
                DexOrderID::generate(),
                wid,
                keyID,
                sendAssetId,
                sendAmountGroth,
                sendAssetUnitName,
                receiveAssetId,
                receiveAmountGroth,
                receiveAssetUnitName,
                expirationMinutes * 60
            );

            dex->publishOrder(orderObj);

            cout << "Order created with ID: " << orderObj.getID() << std::endl
                 << "Waiting for order appruv" << std::endl;;

            return 0;
        });
    }

    int AssetSwapCancel(const po::variables_map& vm)
    {
        CheckAssetsAllowed(vm);

        std::string offerIdStr;
        DexOrderID offerId;
        if (auto it = vm.find(cli::ASSETS_SWAP_OFFER_ID); it != vm.end())
        {
            offerIdStr = vm[cli::ASSETS_SWAP_OFFER_ID].as<string>();
            if (offerIdStr.empty())
            {
                cout << cli::ASSETS_SWAP_OFFER_ID << " is empty.";
                return -1;
            }

            if (!offerId.FromHex(offerIdStr))
            {
                cout << cli::ASSETS_SWAP_OFFER_ID << " is malformed";
                return -1;
            }
        }
        else
        {
            cout << cli::ASSETS_SWAP_OFFER_ID << " is not provided";
            return -1;
        }

        return DoWalletFunc(vm, [](auto&& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
        {
            return 0;
        },
        [offerId, offerIdStr](auto&& vm, auto&& wallet, auto&& walletDB, DexBoard::Ptr dex)
        {
            auto order = dex->getDexOrder(offerId);
            if (!order)
            {
                cout << "offer with " << cli::ASSETS_SWAP_OFFER_ID << ":" << offerIdStr << "is not found";
                return -1;
            }

            if (!order->isMine())
            {
                cout << "offer with " << cli::ASSETS_SWAP_OFFER_ID << ":" << offerIdStr << "is not your offer";
                return -1;
            }

            order->cancel();
            dex->publishOrder(*order);

            cout << "offer with " << cli::ASSETS_SWAP_OFFER_ID << ":" << offerIdStr << " cancelled" << std::endl;
            
            return 0;
        });

        return 1;
    }

    int AssetSwapAccept(const po::variables_map& vm)
    {
        CheckAssetsAllowed(vm);

        std::string offerIdStr;
        DexOrderID offerId;
        if (auto it = vm.find(cli::ASSETS_SWAP_OFFER_ID); it != vm.end())
        {
            offerIdStr = vm[cli::ASSETS_SWAP_OFFER_ID].as<string>();
            if (offerIdStr.empty())
            {
                cout << cli::ASSETS_SWAP_OFFER_ID << " is empty.";
                return -1;
            }

            if (!offerId.FromHex(offerIdStr))
            {
                cout << cli::ASSETS_SWAP_OFFER_ID << " is malformed";
                return -1;
            }
        }
        else
        {
            cout << cli::ASSETS_SWAP_OFFER_ID << " is not provided";
            return -1;
        }

        return DoWalletFunc(vm, [](auto&& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
        {
            return 0;
        },
        [offerId, offerIdStr](auto&& vm, auto&& wallet, auto&& walletDB, DexBoard::Ptr dex)
        {
            auto order = dex->getDexOrder(offerId);
            if (!order)
            {
                cout << "offer with " << cli::ASSETS_SWAP_OFFER_ID << ":" << offerIdStr << "is not found";
                return -1;
            }

            if (order->isMine())
            {
                cout << "offer with " << cli::ASSETS_SWAP_OFFER_ID << ":" << offerIdStr << "is your offer";
                return -1;
            }

            Asset::ID sendAsset = order->getSendAssetId();
            if (sendAsset)
            {
                auto sendAssetInfo = walletDB->findAsset(sendAsset);
                if (!sendAssetInfo)
                {
                    cout << "Not enough funds to send for " << cli::ASSETS_SWAP_OFFER_ID << ":" << offerIdStr;
                    return -1;
                }
            }

            Asset::ID receiveAsset = order->getReceiveAssetId();
            if (receiveAsset)
            {
                auto receiveAssetInfo = walletDB->findAsset(receiveAsset);
                if (!receiveAssetInfo)
                {
                    cout << "Unknown receive asset id in order " << cli::ASSETS_SWAP_OFFER_ID << ":" << offerIdStr;
                    return -1;
                }
            }

            Amount fee = 100000;
            CoinsSelectionInfo csi;
            csi.m_requestedSum = order->getSendAmount();
            csi.m_assetID = sendAsset;
            csi.m_explicitFee = fee;
            csi.Calculate(walletDB->getCurrentHeight(), walletDB, false);

            if (!csi.m_isEnought)
            {
                cout << "Not enough funds to send for " << cli::ASSETS_SWAP_OFFER_ID << ":" << offerIdStr;
                return -1;
            }

            auto params = beam::wallet::CreateDexTransactionParams(
                            offerId,
                            order->getSBBSID(),
                            sendAsset,
                            order->getSendAmount(),
                            receiveAsset,
                            order->getReceiveAmount(),
                            fee
                            );

            
            auto txId = wallet->StartTransaction(params);
            
            cout << "Asset swap for offer id: " << offerIdStr << " started" << std::endl
                 << "TxId: " << txId << std::endl;
            return 0;
        });

        return 1;
    }
}  // namespace wallet_cli
