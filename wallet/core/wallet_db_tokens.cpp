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

#include "wallet_db_common.h"

namespace beam::wallet
{
    namespace
    {
        TxParameters GenerateCommonAddressPart(Amount amount, Asset::ID assetId, const std::string& clientVersion)
        {
            TxParameters params;

            if (amount > 0)
            {
                params.SetParameter(TxParameterID::Amount, amount);
            }

            if (assetId != Asset::s_InvalidID)
            {
                params.SetParameter(TxParameterID::AssetID, assetId);
            }

            // Commented it because, these versions bloat the address size (anatolse)
            // TODO: Replace it with something shorter if we 
            //if (!clientVersion.empty())
            //{
            //    params.SetParameter(TxParameterID::ClientVersion, clientVersion);
            //}

            AppendLibraryVersion(params);
            return params;
        }
    }

    std::string GenerateOfflineToken(const WalletAddress& address, const IWalletDB& walletDB, Amount amount, Asset::ID assetId, const std::string& clientVersion, uint32_t offlineCount /* = 0 */)
    {
        if (!offlineCount)
            offlineCount = 10; // default
        auto vouchers = GenerateVoucherList(walletDB.get_KeyKeeper(), address.m_OwnID, offlineCount);

        TxParameters params = GenerateCommonAddressPart(amount, assetId, clientVersion);

        params.SetParameter(TxParameterID::TransactionType, beam::wallet::TxType::PushTransaction);
        params.SetParameter(TxParameterID::ShieldedVoucherList, vouchers);
        params.SetParameter(TxParameterID::PeerAddr,            address.m_BbsAddr);
        params.SetParameter(TxParameterID::PeerEndpoint,        address.m_Endpoint);
        params.SetParameter(TxParameterID::IsPermanentPeerID,   address.isPermanent());

        return beam::to_string(params);
    }

    std::string GenerateMaxPrivacyToken(const WalletAddress& address, const IWalletDB& walletDB, Amount amount, Asset::ID assetId, const std::string& clientVersion)
    {
        auto vouchers = GenerateVoucherList(walletDB.get_KeyKeeper(), address.m_OwnID, 1);
        if (vouchers.empty())
            return "";

        TxParameters params = GenerateCommonAddressPart(amount, assetId, clientVersion);

        params.SetParameter(TxParameterID::TransactionType, beam::wallet::TxType::PushTransaction);
        params.SetParameter(TxParameterID::PeerEndpoint, address.m_Endpoint);
        params.SetParameter(TxParameterID::Voucher, vouchers.front());

        return beam::to_string(params);
    }

    std::string GeneratePublicToken(const WalletAddress& address, const IWalletDB& walletDB, const std::string& clientVersion)
    {
        assert(address.m_OwnID);
        IPrivateKeyKeeper2::Method::CreateOfflineAddr mAddr;
        mAddr.m_iEndpoint = address.m_OwnID;

        if (walletDB.get_KeyKeeper()->InvokeSync(mAddr) != IPrivateKeyKeeper2::Status::Success)
            return "";

        TxParameters params = GenerateCommonAddressPart(0, Asset::s_InvalidID, clientVersion);

        params.SetParameter(TxParameterID::TransactionType, beam::wallet::TxType::PushTransaction);
        params.SetParameter(TxParameterID::PublicAddreessGen, mAddr.m_Addr);
        params.SetParameter(TxParameterID::PublicAddressGenSig, mAddr.m_Signature);
        params.SetParameter(TxParameterID::PeerEndpoint, address.m_Endpoint);
        AppendLibraryVersion(params);

        return beam::to_string(params);
    }

    std::string  GenerateRegularOldToken(const WalletAddress& address)
    {
        return beam::to_string(address.m_BbsAddr);
    }

    std::string  GenerateRegularNewToken(const WalletAddress& address, Amount amount, Asset::ID assetId, const std::string& clientVersion)
    {
        TxParameters params = GenerateCommonAddressPart(amount, assetId, clientVersion);

        params.SetParameter(TxParameterID::PeerAddr, address.m_BbsAddr);
        params.SetParameter(TxParameterID::PeerEndpoint, address.m_Endpoint);
        params.SetParameter(TxParameterID::IsPermanentPeerID, address.isPermanent());
        params.SetParameter(TxParameterID::TransactionType, TxType::Simple);

        return beam::to_string(params);
    }

    std::string  GenerateTokenDefaultAddr(TokenType type, IWalletDB::Ptr walletDB, uint32_t offlineCount /* = 0 */)
    {
        WalletAddress wa;
        walletDB->getDefaultAddressAlways(wa);
        return GenerateToken(type, wa, walletDB, offlineCount);
    }

    std::string  GenerateToken(TokenType type, const WalletAddress& address, IWalletDB::Ptr walletDB, uint32_t offlineCount /* = 0 */)
    {
        switch (type)
        {
        case TokenType::Public:
            {
                auto token = GeneratePublicToken(address, *walletDB, "");
                BEAM_LOG_INFO() << "Generated public offline address: " << token;
                return token;
            }

        case TokenType::RegularOldStyle:
            {
                auto token = GenerateRegularOldToken(address);
                BEAM_LOG_INFO() << "Generated regular old style address: " << token;
                return token;
            }

        case TokenType::RegularNewStyle:
             {
                auto token = GenerateRegularNewToken(address, 0, 0, "");
                BEAM_LOG_INFO() << "Generated regular new style address:" << token;
                return token;
            }

        case TokenType::Offline:
            {
                auto token = GenerateOfflineToken(address, *walletDB, 0, 0, "", offlineCount);
                BEAM_LOG_INFO() << "Generated offline address: " << token;
                return token;
            }

        case TokenType::MaxPrivacy:
            {
                auto token = GenerateMaxPrivacyToken(address, *walletDB, 0, 0, "");
                BEAM_LOG_INFO() << "Generated max privacy address: " << token;
                return token;
            }

        default:
            throw std::runtime_error("Unexpected address type");
        }
    }

    TokenType GetTokenType(const std::string& token)
    {
        const auto addrType = GetAddressType(token);
        switch (addrType)
        {
            case TxAddressType::Offline: return TokenType::Offline;
            case TxAddressType::MaxPrivacy: return TokenType::MaxPrivacy;
            case TxAddressType::PublicOffline: return TokenType::Public;
            case TxAddressType::Regular:
            {
                if(auto params = ParseParameters(token))
                {
                    WalletID wid;
                    if(!params->GetParameter(TxParameterID::PeerAddr, wid))
                    {
                        assert(false);
                        return TokenType::RegularOldStyle;
                    }

                    if (beam::to_string(wid) == token)
                    {
                        return TokenType::RegularOldStyle;
                    }

                    return TokenType::RegularNewStyle;
                }
                else
                {
                    assert(false);
                    return TokenType::Unknown;
                }
            }
            default: return TokenType::Unknown;
        }
    }

    uint32_t GetCoinStatus(const Coin& c)
    {
        return c.m_status;
    }

    uint32_t GetCoinStatus(const ShieldedCoin& c)
    {
        return c.m_Status;
    }
}
