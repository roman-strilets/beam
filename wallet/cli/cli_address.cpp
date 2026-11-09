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

// Address commands: creation, listing, expiration and verification.

#include "cli_internal.h"

namespace wallet_cli
{
namespace
{
    bool CreateNewAddress(const po::variables_map& vm,
                         const IWalletDB::Ptr& walletDB,
                         const std::string& defaultComment = "")
    {
        auto comment = defaultComment.empty()
            ? vm[cli::NEW_ADDRESS_COMMENT].as<string>()
            : defaultComment;
        auto expiration = vm[cli::EXPIRATION_TIME].as<string>();

        WalletAddress::ExpirationStatus expirationStatus;
        if (expiration == cli::EXPIRATION_TIME_24H)
        {
            expirationStatus = WalletAddress::ExpirationStatus::OneDay;
        }
        else if (expiration == cli::EXPIRATION_TIME_AUTO)
        {
            expirationStatus = WalletAddress::ExpirationStatus::Auto;
        }
        else if (expiration == cli::EXPIRATION_TIME_NEVER)
        {
            expirationStatus = WalletAddress::ExpirationStatus::Never;
        }
        else
        {
            std::cerr << boost::format(kErrorAddrExprTimeInvalid) % cli::EXPIRATION_TIME % expiration;
            return false;
        }

        WalletAddress address(comment);
        walletDB->createAddress(address);
        address.setExpirationStatus(expirationStatus);
        walletDB->saveAddress(address);

        return true;
    }

    const char* GetAddressTypeString(wallet::TokenType tokenType)
    {
        switch (tokenType)
        {
            case TokenType::Public: return "public offline";
            case TokenType::MaxPrivacy: return "max privacy";
            case TokenType::RegularNewStyle: return "regular new style";
            case TokenType::RegularOldStyle: return "regular old style";
            case TokenType::Offline: return "offline";
            default: return "unknown";
        }
    }
}  // namespace

    int ChangeAddressExpiration(const po::variables_map& vm)
    {
        string token = vm[cli::WALLET_ADDR].as<string>();
        string expiration = vm[cli::EXPIRATION_TIME].as<string>();

        WalletAddress::ExpirationStatus expirationStatus;
        if (expiration == cli::EXPIRATION_TIME_24H)
        {
            expirationStatus = WalletAddress::ExpirationStatus::OneDay;
        }
        else if (expiration == cli::EXPIRATION_TIME_AUTO)
        {
            expirationStatus = WalletAddress::ExpirationStatus::Auto;
        }
        else if (expiration == cli::EXPIRATION_TIME_NEVER)
        {
            expirationStatus = WalletAddress::ExpirationStatus::Never;
        }
        else if (expiration == cli::EXPIRATION_TIME_NOW)
        {
            expirationStatus = WalletAddress::ExpirationStatus::Expired;
        }
        else
        {
            BEAM_LOG_ERROR() << boost::format(kErrorAddrExprTimeInvalid)
                        % cli::EXPIRATION_TIME
                        % expiration;
            return -1;
        }

        auto walletDB = OpenDataBase(vm);
        if (token == "*")
        {
            WalletID zeroID(Zero);
            if (storage::changeAddressExpiration(*walletDB, zeroID, expirationStatus))
            {
                std::cout << boost::format(kAllAddrExprChanged) % expiration << endl;
            }
        }
        else
        {
            if (auto address = walletDB->getAddressByToken(token, false))
            {
                address->setExpirationStatus(expirationStatus);
                walletDB->saveAddress(*address);

                std::cout << boost::format(kAddrExprChanged) % token % expiration << endl;
            }
            else
            {
                throw std::runtime_error("Cannot find specified existing address.");
            }
        }
        return -1;
    }

   int GetAddress(const po::variables_map& vm)
   {
        auto walletDB = OpenDataBase(vm);

        WalletAddress address;
        if (auto it = vm.find(cli::RECEIVER_ADDR); it != vm.end())
        {
            auto receiver = it->second.as<string>();

            WalletID walletID;
            if (!walletID.FromHex(receiver))
            {
                throw std::runtime_error("Invalid existing address format");
            }

            auto existing = walletDB->getAddress(walletID);

            if (!existing)
            {
                throw std::runtime_error("Cannot find specified existing address.");
            }

            if (existing->isExpired())
            {
                throw std::runtime_error("Specified existing address is already expired");
            }

            address = *existing;
        }
        else
            walletDB->getDefaultAddressAlways(address);

        auto type = TokenType::RegularNewStyle;
        uint32_t offlineCount = 10;

        if (auto it2 = vm.find(cli::PUBLIC_OFFLINE); it2 != vm.end() && it2->second.as<bool>())
        {
            type = TokenType::Public;
        }
        else if (it2 = vm.find(cli::MAX_PRIVACY_ADDRESS); it2 != vm.end() && it2->second.as<bool>())
        {
            type = TokenType::MaxPrivacy;
        }
        else if (it2 = vm.find(cli::OFFLINE_COUNT); it2 != vm.end())
        {
            type = TokenType::Offline;
            offlineCount = it2->second.as<Positive<uint32_t>>().value;
        }

        GenerateToken(type, address, walletDB, offlineCount); // result is emitted via log

        return 0;
    }

    int ShowAddressList(const po::variables_map& vm)
    {
        auto walletDB = OpenDataBase(vm);
        auto addresses = walletDB->getAddresses(true);

        if (addresses.empty())
        {
            std::cout << "You do not have any addresses.";
            return 0;
        }

        std::cout << endl;
        for (const auto& address : addresses)
        {
            auto addrType = GetTokenType(address.m_Token);


            std::cout
                << kAddrListType    << GetAddressTypeString(addrType) << std::endl
                << kAddrListComment << address.m_label << std::endl
                << kAddrListAddress << address.m_Token << std::endl;

            if (address.m_BbsAddr.IsValid())
            {
                std::cout << kAddrListBbsAddr << beam::to_string(address.m_BbsAddr) << std::endl;
            }

            const auto expirationDateText = (address.m_duration == 0)
                ? cli::EXPIRATION_TIME_NEVER
                : format_timestamp(kTimeStampFormat3x3, address.getExpirationTime() * 1000, false);

            const auto creationDateText = format_timestamp(kTimeStampFormat3x3, address.getCreateTime() * 1000, false);

            const auto& ep = (wallet::TokenType::RegularOldStyle == addrType) ? address.m_BbsAddr.m_Pk : address.m_Endpoint;

            std::cout
                << kAddrListEndpoint << beam::to_base58(ep) << std::endl
                << kAddrListActive   << (address.isExpired() ? "false" : "true") << std::endl
                << kAddrListExprDate << expirationDateText << std::endl
                << kAddrListCreated  << creationDateText << std::endl
                << std::endl;
        }

        return 0;
    }

    int VerifyAddress(const po::variables_map& vm)
    {
        auto walletDB = OpenDataBase(vm);

        WalletAddress wa;

        string sToken = vm[cli::WALLET_ADDR].as<string>();
        if (sToken.empty() || (sToken == "*"))
            walletDB->getDefaultAddressAlways(wa);
        else
        {
            auto pAddr = walletDB->getAddressByToken(sToken);
            if (!pAddr)
            {
                std::cout << "No such address" << std::endl;
                return 1;
            }

            wa = std::move(*pAddr);
        }

        std::cout
            << kAddrListAddress << wa.m_Token << std::endl
            << kAddrListEndpoint << beam::to_base58(wa.m_Endpoint) << std::endl;

        if (!wa.isOwn())
        {
            std::cout << "Address not owned" << std::endl;
            return 1;
        }

        auto pKk = walletDB->get_KeyKeeper();
        if (!pKk)
        {
            std::cout << "Read-only mode." << std::endl;
            return 1;
        }

        IPrivateKeyKeeper2::Method::DisplayEndpoint m;
        m.m_iEndpoint = wa.m_OwnID;
        auto st = pKk->InvokeSync(m);
        if (IPrivateKeyKeeper2::Status::Success != st)
        {
            std::cout << "HW wallet status: " << st << std::endl;
            return 1;
        }

        return 0;
    }

    int CreateNewAddress(const po::variables_map& vm)
    {
        auto walletDB = OpenDataBase(vm);
        if (!CreateNewAddress(vm, walletDB))
        {
            return -1;
        }

        if (!vm.count(cli::LISTEN))
        {
            return 0;
        }

        return Listen(vm);
    }
}  // namespace wallet_cli
