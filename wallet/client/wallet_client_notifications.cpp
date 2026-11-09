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

// Notifications, exchange rates and instant messages.

#include "wallet_client_internal.h"

namespace beam::wallet
{
    namespace
    {
        const size_t kMaxImMessageLength = 1024;
    }

    void WalletClient::switchOnOffExchangeRates(bool isActive)
    {
        if (auto s = m_exchangeRateProvider.lock()) // none if SBBS is disabled
        {
            s->setOnOff(isActive);
        }
    }

    void WalletClient::switchOnOffNotifications(Notification::Type type, bool isActive)
    {
        m_notificationCenter->switchOnOffNotifications(type, isActive);
    }

    void WalletClient::getNotifications()
    {
        onNotificationsChanged(ChangeAction::Reset, m_notificationCenter->getNotifications());
    }

    void WalletClient::markNotificationAsRead(const ECC::uintBig& id)
    {
        m_notificationCenter->markNotificationAsRead(id);
    }

    void WalletClient::deleteNotification(const ECC::uintBig& id)
    {
        m_notificationCenter->deleteNotification(id);
    }

    void WalletClient::getExchangeRates()
    {
        if (auto s = m_exchangeRateProvider.lock()) // none if SBBS is disabled
        {
            onExchangeRates(s->getRates());
        }
        else
        {
            onExchangeRates({});
        }
    }

    void WalletClient::getVerificationInfo()
    {
        if (auto s = m_verificationProvider.lock()) // none if SBBS is disabled
        {
            const auto info = s->getInfo();
            onVerificationInfo(info);
        }
        else
        {
            onVerificationInfo({});
        }
    }

    void WalletClient::markAppNotificationAsRead(const TxID& id)
    {
        auto w = m_wallet.lock();
        if (w)
        {
            w->markAppNotificationAsRead(id);
        }
    }

    void WalletClient::sendInstantMessage(const WalletID& peerID, const WalletID& myID, ByteBuffer&& message)
    {
        auto timestamp = getTimestamp();

        ByteBuffer trimmed_message(message);
        if (trimmed_message.size() >= kMaxImMessageLength)
        {
            trimmed_message.erase(trimmed_message.begin() + kMaxImMessageLength, trimmed_message.end());
        }

        m_walletDB->storeIM(timestamp, peerID, myID, std::string(trimmed_message.begin(), trimmed_message.end()), false, true);

        auto s = m_wallet.lock();
        if (s)
        {
            s->sendInstantSbbsMessage(timestamp, peerID, myID, std::move(trimmed_message));
        }
    }

    void WalletClient::getChats()
    {
        auto chats = m_walletDB->getChats();
        postFunctionToClientContext([this, chats]()
            {
                onGetChatList(chats);
            });
    }

    void WalletClient::markIMsasRead(const std::vector<std::pair<Timestamp, WalletID>>&& ims)
    {
        std::vector<std::pair<Timestamp, WalletID>> messages(ims);
        m_walletDB->markIMsasRead(messages);
    }

    void WalletClient::getInstantMessages(const WalletID& peerID)
    {
        auto messages = m_walletDB->readIMs(peerID);
        postFunctionToClientContext([this, messages]()
            {
                onGetChatMessages(messages);
            });
    }

    void WalletClient::removeChat(const WalletID& peerID)
    {
        m_walletDB->removeChat(peerID);
        postFunctionToClientContext([this, peerID]()
            {
                onChatRemoved(peerID);
            });
    }
}
