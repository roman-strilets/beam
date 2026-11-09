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
    vector<Notification> WalletDB::getNotifications() const
    {
        vector<Notification> res;
        const char* req = "SELECT * FROM " NOTIFICATIONS_NAME " ORDER BY createTime DESC;";
        sqlite::Statement stm(this, req);

        while (stm.step())
        {
            auto& notification = res.emplace_back();
            int colIdx = 0;
            ENUM_NOTIFICATION_FIELDS(STM_GET_LIST, NOSEP, notification);
        }
        return res;
    }

    void WalletDB::saveNotification(const Notification& notification)
    {
        const char* selectReq = "SELECT * FROM " NOTIFICATIONS_NAME " WHERE ID=?1;";
        sqlite::Statement selectStm(this, selectReq);
        selectStm.bind(1, notification.m_ID);

        if (selectStm.step())
        {
            const char* updateReq = "UPDATE " NOTIFICATIONS_NAME " SET type=?2, state=?3, createTime=?4, content=?5 WHERE ID=?1;";
            sqlite::Statement updateStm(this, updateReq);

            updateStm.bind(1, notification.m_ID);
            updateStm.bind(2, notification.m_type);
            updateStm.bind(3, notification.m_state);
            updateStm.bind(4, notification.m_createTime);
            updateStm.bind(5, notification.m_content);
            updateStm.step();
        }
        else
        {
            const char* insertReq = "INSERT INTO " NOTIFICATIONS_NAME " (" ENUM_NOTIFICATION_FIELDS(LIST, COMMA, ) ") VALUES(" ENUM_NOTIFICATION_FIELDS(BIND_LIST, COMMA, ) ");";
            sqlite::Statement stm(this, insertReq);
            int colIdx = 0;
            ENUM_NOTIFICATION_FIELDS(STM_BIND_LIST, NOSEP, notification);
            stm.step();
        }
    }

    void WalletDB::storeIM(Timestamp time, const WalletID& counterpart, const WalletID& mySbbs, const std::string& message, bool isIncome, bool isRead)
    {
        sqlite::Statement stm(this, "INSERT OR IGNORE INTO " IM_NAME " (" ENUM_IM_FIELDS(LIST, COMMA, ) ") VALUES(null,?,?,?,?,?,?);");
        stm.bind(1, time);
        stm.bind(2, counterpart);
        stm.bind(3, mySbbs);
        stm.bind(4, message);
        stm.bind(5, isIncome);
        stm.bind(6, isRead);
        stm.step();

        if (sqlite3_changes(_db) > 0)
        {
            for (const auto sub : m_subscribers)
            {
                sub->onIMSaved(time, counterpart, message, isIncome);
            }
        }
    }

    std::vector<InstantMessage> WalletDB::readIMs(bool all)
    {
        std::vector<InstantMessage> messages;
        const char* req = all
            ? "SELECT * FROM " IM_NAME " ORDER BY timestamp DESC;"
            : "SELECT * FROM " IM_NAME " WHERE ID > ?1 ORDER BY timestamp DESC;";

        sqlite::Statement stm(this, req);
        if (!all) stm.bind(1, m_lastReadIMId);

        while (stm.step())
        {
            auto& message = messages.emplace_back();
            int colIdx = 0;
            ENUM_IM_FIELDS(STM_GET_LIST, NOSEP, message);
        }

        if (!messages.empty())
        {
            auto message_with_largest_id = std::max_element(
                messages.begin(), messages.end(),
                [] (const InstantMessage& largest, const InstantMessage& first)
                {
                    return largest.m_id < first.m_id;
                });
            m_lastReadIMId = message_with_largest_id->m_id;
            storage::setVar(*this, LAST_READ_IM_ID, m_lastReadIMId);
        }

        return messages;
    }

    std::vector<InstantMessage> WalletDB::readIMs(const WalletID& counterpart)
    {
        std::vector<InstantMessage> messages;

        const char* req = "SELECT * FROM " IM_NAME " WHERE counterpart=?1 ORDER BY timestamp DESC;";

        sqlite::Statement stm(this, req);
        stm.bind(1, counterpart);

        while (stm.step())
        {
            auto& message = messages.emplace_back();
            int colIdx = 0;
            ENUM_IM_FIELDS(STM_GET_LIST, NOSEP, message);
        }

        return messages;
    }

    std::vector<std::pair<WalletID, bool>> WalletDB::getChats()
    {
        const char* req = "SELECT DISTINCT counterpart FROM " IM_NAME ";";

        sqlite::Statement stm(this, req);

        std::vector<std::pair<WalletID, bool>> chats;
        while (stm.step())
        {
            ByteBuffer counterpartID;
            stm.get(0, counterpartID);
            WalletID peerID;
            if (peerID.FromBuf(counterpartID)) {
                const char* req2 = "SELECT count(*) FROM " IM_NAME " WHERE is_read = 0;";
                sqlite::Statement stm2(this, req2);
                bool hasUnread = false;
                if (stm2.step()) {
                    int unreadCnt = 0;
                    stm2.get(0, unreadCnt);
                    hasUnread = unreadCnt > 0;
                }
                chats.push_back(std::make_pair(peerID, hasUnread));
            }

        }
        return chats;
    }

    void WalletDB::markIMsasRead(const std::vector<std::pair<Timestamp, WalletID>>& ims)
    {
        const char* req = "UPDATE " IM_NAME " SET is_read=1 WHERE timestamp=?1 AND counterpart=?2;";
        for (const auto& im : ims) {
            sqlite::Statement stm(this, req);

            stm.bind(1, im.first);
            stm.bind(2, im.second);
            stm.step();
        }
    }

    void WalletDB::removeChat(const WalletID& counterpart)
    {
        const char* req = "DELETE FROM " IM_NAME " WHERE counterpart=?1;";

        sqlite::Statement stm(this, req);
        stm.bind(1, counterpart);
        stm.step();
    }

    std::vector<OutgoingWalletMessage> WalletDB::getWalletMessages() const
    {
        std::vector<OutgoingWalletMessage> messages;
        sqlite::Statement stm(this, "SELECT * FROM " WALLET_MESSAGE_NAME " ;");
        while (stm.step())
        {
            auto& message = messages.emplace_back();
            int colIdx = 0;
            ENUM_WALLET_MESSAGE_FIELDS(STM_GET_LIST, NOSEP, message);
        }
        return messages;
    }

    uint64_t WalletDB::saveWalletMessage(const WalletID& peerID, const Blob& msg)
    {
        const char* req = "INSERT INTO " WALLET_MESSAGE_NAME " (PeerID, Message) VALUES(?,?)";
        sqlite::Statement stm(this, req);
        stm.bind(1, peerID);
        stm.bind(2, msg);

        stm.step();

        return sqlite3_last_insert_rowid(_db);
    }

    void WalletDB::deleteWalletMessage(uint64_t id)
    {
        sqlite::Statement stm(this, "DELETE FROM " WALLET_MESSAGE_NAME " WHERE ID == ?1;");
        stm.bind(1, id);
        stm.step();
    }

    std::vector<IncomingWalletMessage> WalletDB::getIncomingWalletMessages() const
    {
        std::vector<IncomingWalletMessage> messages;
        sqlite::Statement stm(this, "SELECT * FROM " INCOMING_WALLET_MESSAGE_NAME " ;");
        while (stm.step())
        {
            auto& message = messages.emplace_back();
            int colIdx = 0;
            ENUM_INCOMING_WALLET_MESSAGE_FIELDS(STM_GET_LIST, NOSEP, message);
        }
        return messages;
    }

    uint64_t WalletDB::saveIncomingWalletMessage(BbsChannel channel, const Blob& message)
    {
        const char* req = "INSERT INTO " INCOMING_WALLET_MESSAGE_NAME " (Channel, Message) VALUES(?,?);";
        sqlite::Statement stm(this, req);
        stm.bind(1, channel);
        stm.bind(2, message);

        stm.step();

        return sqlite3_last_insert_rowid(_db);
    }

    void WalletDB::deleteIncomingWalletMessage(uint64_t id)
    {
        sqlite::Statement stm(this, "DELETE FROM " INCOMING_WALLET_MESSAGE_NAME " WHERE ID == ?1;");
        stm.bind(1, id);
        stm.step();
    }
}
