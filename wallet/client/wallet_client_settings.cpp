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

// Node connection, wallet settings and seed phrase.

#include "wallet_client_internal.h"

namespace beam::wallet
{
    void WalletClient::syncWithNode()
    {
        assert(!m_nodeNetwork.expired());
        if (auto s = m_nodeNetwork.lock())
        {
            s->Connect();
    }
    }

    void WalletClient::setNodeAddress(const std::string& addr)
    {
        if (auto s = m_nodeNetwork.lock())
        {
            s->setNodeAddress(addr);
        }
        else
        {
            io::Address address;
            if (address.resolve(addr.c_str()))
            {
                m_initialNodeAddrStr = addr;
            }
            else
            {
                BEAM_LOG_ERROR() << "Unable to resolve node address: " << addr;
                onWalletError(ErrorType::HostResolvedError);
            }
        }
    }

    void WalletClient::changeWalletPassword(const SecString& pass)
    {
        m_walletDB->changePassword(pass);
    }

    void WalletClient::getNetworkStatus()
    {
        if (m_walletError.is_initialized() && !isConnected())
        {
            onWalletError(*m_walletError);
            return;
        }

        onNodeConnectionChanged(isConnected());
    }

    void WalletClient::rescan()
    {
        try
        {
            assert(!m_wallet.expired());
            auto s = m_wallet.lock();
            if (s)
            {
                s->Rescan();
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

    void WalletClient::checkNetworkAddress(const std::string& addr)
    {
        io::Address nodeAddr;
        onAddressChecked(addr, nodeAddr.resolve(addr.c_str()));
    }

    void WalletClient::setMaxPrivacyLockTimeLimitHours(uint8_t limit)
    {
        m_walletDB->set_MaxPrivacyLockTimeLimitHours(limit);
        updateStatus();
    }

    void WalletClient::getMaxPrivacyLockTimeLimitHours(AsyncCallback<uint8_t>&& callback)
    {
        auto limit = m_walletDB->get_MaxPrivacyLockTimeLimitHours();
        postFunctionToClientContext([res = std::move(limit), cb = std::move(callback)]() 
        {
            cb(res);
        });
    }

    void WalletClient::setCoinConfirmationsOffset(uint32_t val)
    {
        m_walletDB->setCoinConfirmationsOffset(val);
    }

    void WalletClient::removeRawSeedPhrase()
    {
        m_walletDB->removeVarRaw(SEED_PARAM_NAME);
    }

    void WalletClient::readRawSeedPhrase(AsyncCallback<const std::string&>&& callback)
    {
        ByteBuffer b;
        if (m_walletDB->getBlob(SEED_PARAM_NAME, b))
        {
            std::string phrase((char*)b.data());
            postFunctionToClientContext([res = std::move(phrase), cb = std::move(callback)]() 
            {
                cb(res);
            });
        }
    }

    void WalletClient::getCoinConfirmationsOffset(AsyncCallback<uint32_t>&& callback)
    {
        auto confirmationOffset = m_walletDB->getCoinConfirmationsOffset();
        postFunctionToClientContext([res = std::move(confirmationOffset), cb = std::move(callback)]() 
        {
            cb(res);
        });
    }

    void WalletClient::enableBodyRequests(bool value)
    {
        auto s = m_wallet.lock();
        if (s)
        {
            s->EnableBodyRequests(value);
        }
    }
}
