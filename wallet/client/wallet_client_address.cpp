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

// Addresses and tokens.

#include "wallet_client_internal.h"

namespace beam::wallet
{
    void WalletClient::getAddress(const WalletID& addr)
    {
        try
        {
            const auto address = m_walletDB->getAddress(addr);
            onGetAddress(addr, address, m_walletDB->getVoucherCount(addr));
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
        catch (...) {
            BEAM_LOG_UNHANDLED_EXCEPTION();
        }
    }

    void WalletClient::getAddresses(bool own)
    {
        onAddresses(own, m_walletDB->getAddresses(own));
    }

    void WalletClient::saveAddress(const WalletAddress& address)
    {
        m_walletDB->saveAddress(address);
    }

    void WalletClient::generateNewAddress()
    {
        try
        {
            WalletAddress address;
            m_walletDB->createAddress(address);
            onGeneratedNewAddress(address);
        }
        catch (const CannotGenerateSecretException&)
        {
            onNewAddressFailed();
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
        catch (...) {
            BEAM_LOG_UNHANDLED_EXCEPTION();
        }
    }

    void WalletClient::generateNewAddress(AsyncCallback<const WalletAddress&>&& callback)
    {
        try
        {
            WalletAddress address;
            m_walletDB->createAddress(address);

            postFunctionToClientContext([address, cb = std::move(callback)]()
            {
                cb(address);
            });
        }
        catch (const CannotGenerateSecretException&)
        {
            onNewAddressFailed();
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
        catch (...) {
            BEAM_LOG_UNHANDLED_EXCEPTION();
        }
    }

    void WalletClient::generateToken(TokenType type, Amount amount, Asset::ID aid, std::string sVer, bool newAddress, AsyncCallback<std::string&&>&& callback)
    {
        std::string sToken;
        try
        {
            WalletAddress wa;
            if (newAddress)
            {
                m_walletDB->createAddress(wa);
                wa.setExpirationStatus(WalletAddress::ExpirationStatus::Auto);
            }
            else
            {
                m_walletDB->getDefaultAddressAlways(wa);
            }

            switch (type)
            {
            case TokenType::Offline:
                sToken = GenerateOfflineToken(wa, *m_walletDB, amount, aid, sVer, 1);
                break;

            case TokenType::MaxPrivacy:
                sToken = GenerateMaxPrivacyToken(wa, *m_walletDB, amount, aid, sVer);
                break;

            default:
                sToken = GenerateToken(type, wa, m_walletDB);
            }

            if (newAddress)
            {
                wa.m_Token = sToken;
                m_walletDB->saveAddress(wa);
            }
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
        catch (...) {
            BEAM_LOG_UNHANDLED_EXCEPTION();
        }

        postFunctionToClientContext([sToken = std::move(sToken), cb = std::move(callback)]() mutable
        {
            cb(std::move(sToken));
        });
    }

    void WalletClient::deleteAddress(const WalletID& addr)
    {
        try
        {
            m_walletDB->deleteAddress(addr);
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
        catch (...) {
            BEAM_LOG_UNHANDLED_EXCEPTION();
        }
    }

    void WalletClient::deleteAddressByToken(const std::string& token)
    {
        try
        {
            m_walletDB->deleteAddressByToken(token);
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
        catch (...) {
            BEAM_LOG_UNHANDLED_EXCEPTION();
        }
    }

    void WalletClient::verifyOnHw(const std::string& token)
    {
        try
        {
            auto pKk = m_walletDB->get_KeyKeeper();
            if (pKk)
            {
                auto waddr = m_walletDB->getAddressByToken(token);
                if (waddr && waddr->isOwn())
                {
                    struct MyHandler
                        :public IPrivateKeyKeeper2::Handler
                    {
                        void OnDone(IPrivateKeyKeeper2::Status::Type) override {}
                    };

                    auto pHandler = std::make_shared<MyHandler>();

                    IPrivateKeyKeeper2::Method::DisplayEndpoint m;
                    m.m_iEndpoint = waddr->m_OwnID;
                    pKk->InvokeAsync(m, pHandler);
                }
            }

        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
        catch (...) {
            BEAM_LOG_UNHANDLED_EXCEPTION();
        }
    }

    void WalletClient::updateAddress(const WalletID& wid, const std::string& name, WalletAddress::ExpirationStatus expirationStatus)
    {
        try
        {
            auto addr = m_walletDB->getAddress(wid);

            if (addr)
            {
                if (addr->isOwn())
                {
                    addr->setExpirationStatus(expirationStatus);
                }
                addr->setLabel(name);
                m_walletDB->saveAddress(*addr);
            }
            else
            {
                BEAM_LOG_ERROR() << "Address " << to_string(wid) << " is absent.";
            }
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
    }

    void WalletClient::updateAddress(const WalletID& wid, const std::string& name, beam::Timestamp expirationTime)
    {
        try
        {
            auto addr = m_walletDB->getAddress(wid);

            if (addr)
            {
                if (addr->isOwn())
                {
                    addr->setExpirationTime(expirationTime);
                }
                addr->setLabel(name);
                m_walletDB->saveAddress(*addr);
            }
            else
            {
                BEAM_LOG_ERROR() << "Address " << to_string(wid) << " is absent.";
            }
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
    }

    void WalletClient::activateAddress(const WalletID& wid)
    {
        try
        {
            auto addr = m_walletDB->getAddress(wid);
            if (addr)
            {
                if (addr->isOwn())
                {
                    addr->setExpirationStatus(WalletAddress::ExpirationStatus::Auto);
                }
                m_walletDB->saveAddress(*addr);
            }
            else
            {
                BEAM_LOG_ERROR() << "Address " << to_string(wid) << " is absent.";
            }
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
        catch (...)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION();
        }
    }

    void WalletClient::getAddressByToken(const std::string& token, AsyncCallback<const boost::optional<WalletAddress>&, size_t>&& callback)
    {
        auto addr = m_walletDB->getAddressByToken(token);

        size_t vouchersCount = 0;
        if (addr && addr->m_BbsAddr != Zero)
        {
            vouchersCount = m_walletDB->getVoucherCount(addr->m_BbsAddr);
        }

        postFunctionToClientContext([addr, vouchersCount, cb = std::move(callback)]() {
            cb(addr, vouchersCount);
        });
    }

    void WalletClient::getAddress(const WalletID& wid, AsyncCallback<const boost::optional<WalletAddress>&, size_t>&& callback)
    {
        try
        {
            auto addr = m_walletDB->getAddress(wid);
            size_t vouchersCount = m_walletDB->getVoucherCount(wid);

            postFunctionToClientContext([addr, vouchersCount, cb = std::move(callback)]()
            {
                cb(addr, vouchersCount);
            });
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
        catch (...)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION();
        }
    }

    void WalletClient::saveVouchers(const ShieldedVoucherList& vouchers, const WalletID& walletID)
    {
        try
        {
            storage::SaveVouchers(*m_walletDB, vouchers, walletID);
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
        catch (...) 
        {
            BEAM_LOG_UNHANDLED_EXCEPTION();
        }
    }

    void WalletClient::getPublicAddress()
    {
        WalletAddress wa;
        m_walletDB->getDefaultAddressAlways(wa);

        onPublicAddress(GeneratePublicToken(wa, *m_walletDB, std::string()));
    }
}
