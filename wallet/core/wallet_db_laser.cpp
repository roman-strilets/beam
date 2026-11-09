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
    void WalletDB::saveLaserChannel(const ILaserChannelEntity& ch)
    {
        const auto& channelID = ch.get_chID();
        BEAM_LOG_DEBUG() << "Save channel: "
                    << to_hex(channelID->m_pData, channelID->nBytes);
        const char* selectReq = "SELECT * FROM " LASER_CHANNELS_NAME " WHERE chID=?1;";
        sqlite::Statement stm2(this, selectReq);
        stm2.bind(1, channelID->m_pData, channelID->nBytes);

        if (stm2.step())
        {
            const char* updateReq = "UPDATE " LASER_CHANNELS_NAME " SET bbsID=?2, trgWID=?3, state=?4, fee=?5, locktime=?6, amountMy=?7, amountTrg=?8, amountCurrentMy=?9, amountCurrentTrg=?10, lockHeight=?11, bbsTimestamp=?12, data=?13 WHERE chID=?1;";
            sqlite::Statement stm(this, updateReq);

            stm.bind(1, channelID->m_pData, channelID->nBytes);
            stm.bind(2, ch.get_ownID());
            stm.bind(3, ch.get_trgWID());
            stm.bind(4, ch.get_State());
            stm.bind(5, ch.get_fee());
            stm.bind(6, ch.getLocktime());
            stm.bind(7, ch.get_amountMy());
            stm.bind(8, ch.get_amountTrg());
            stm.bind(9, ch.get_amountCurrentMy());
            stm.bind(10, ch.get_amountCurrentTrg());
            stm.bind(11, ch.get_LockHeight());
            stm.bind(12, ch.get_BbsTimestamp());
            stm.bind(13, ch.get_Data().data(), ch.get_Data().size());
            stm.step();
        }
        else
        {
            const char* insertReq = "INSERT INTO " LASER_CHANNELS_NAME " (" ENUM_LASER_CHANNEL_FIELDS(LIST, COMMA, ) ") VALUES(" ENUM_LASER_CHANNEL_FIELDS(BIND_LIST, COMMA, ) ");";
            sqlite::Statement stm(this, insertReq);
            stm.bind(1, channelID->m_pData, channelID->nBytes);
            stm.bind(2, ch.get_ownID());
            stm.bind(3, ch.get_trgWID());
            stm.bind(4, ch.get_State());
            stm.bind(5, ch.get_fee());
            stm.bind(6, ch.getLocktime());
            stm.bind(7, ch.get_amountMy());
            stm.bind(8, ch.get_amountTrg());
            stm.bind(9, ch.get_amountCurrentMy());
            stm.bind(10, ch.get_amountCurrentTrg());
            stm.bind(11, ch.get_LockHeight());
            stm.bind(12, ch.get_BbsTimestamp());
            stm.bind(13, ch.get_Data().data(), ch.get_Data().size());
            stm.step();
        }

        flushDB();
    }

    bool WalletDB::getLaserChannel(const std::shared_ptr<uintBig_t<16>>& chId,
                                   TLaserChannelEntity* entity)
    {
        const char* selectReq = "SELECT " LASER_CHANNEL_FIELDS " FROM " LASER_CHANNELS_NAME " WHERE chID=?1;";
        sqlite::Statement stm(this, selectReq);
        stm.bind(1, chId->m_pData, chId->nBytes);

        if (stm.step())
        {
            stm.get(LaserFields::LASER_CH_ID, std::get<LaserFields::LASER_CH_ID>(*entity));
            stm.get(LaserFields::LASER_MY_BBS_ID, std::get<LaserFields::LASER_MY_BBS_ID>(*entity));
            stm.get(LaserFields::LASER_TRG_WID, std::get<LaserFields::LASER_TRG_WID>(*entity));
            stm.get(LaserFields::LASER_STATE, std::get<LaserFields::LASER_STATE>(*entity));
            stm.get(LaserFields::LASER_FEE, std::get<LaserFields::LASER_FEE>(*entity));
            stm.get(LaserFields::LASER_LOCKTIME, std::get<LaserFields::LASER_LOCKTIME>(*entity));
            stm.get(LaserFields::LASER_AMOUNT_MY, std::get<LaserFields::LASER_AMOUNT_MY>(*entity));
            stm.get(LaserFields::LASER_AMOUNT_TRG, std::get<LaserFields::LASER_AMOUNT_TRG>(*entity));
            stm.get(LaserFields::LASER_AMOUNT_CURRENT_MY, std::get<LaserFields::LASER_AMOUNT_CURRENT_MY>(*entity));
            stm.get(LaserFields::LASER_AMOUNT_CURRENT_TRG, std::get<LaserFields::LASER_AMOUNT_CURRENT_TRG>(*entity));
            stm.get(LaserFields::LASER_LOCK_HEIGHT, std::get<LaserFields::LASER_LOCK_HEIGHT>(*entity));
            stm.get(LaserFields::LASER_BBS_TIMESTAMP, std::get<LaserFields::LASER_BBS_TIMESTAMP>(*entity));
            stm.get(LaserFields::LASER_DATA, std::get<LaserFields::LASER_DATA>(*entity));
            return true;
        }
        return false;
    }

    bool WalletDB::removeLaserChannel(
            const std::shared_ptr<uintBig_t<16>>& chId)
    {
        BEAM_LOG_INFO() << "Removing channel: "
                   << to_hex(chId->m_pData, chId->nBytes);

        const char* selectReq = "SELECT chID FROM " LASER_CHANNELS_NAME " WHERE chID=?1;";
        sqlite::Statement selectStm(this, selectReq);
        selectStm.bind(1, chId->m_pData, chId->nBytes);

        try
        {
            if (selectStm.step())
            {
                uintBig_t<16> checkChId;
                selectStm.get(0, checkChId);
                if (*chId == checkChId)
                {
                    const char* deleteReq = "DELETE FROM " LASER_CHANNELS_NAME " WHERE chID=?1;";
                    sqlite::Statement stm(this, deleteReq);
                    stm.bind(1, chId->m_pData, chId->nBytes);

                    stm.step();
                    return true;
                }
            }
        }
        catch (const runtime_error&)
        {
        }

        return false;
    }

    std::vector<TLaserChannelEntity> WalletDB::loadLaserChannels(uint8_t state)
    {
        std::vector<TLaserChannelEntity> channels;

        sqlite::Statement countStm(this, "SELECT COUNT(*) FROM " LASER_CHANNELS_NAME ";");
        countStm.step();
        
        uint64_t count;
        countStm.get(0, count);
        channels.reserve(count);

        sqlite::Statement stm(
            this, 
            state
                ? "SELECT * FROM " LASER_CHANNELS_NAME " WHERE state=?1;"
                : "SELECT * FROM " LASER_CHANNELS_NAME ";");
        if (state)
            stm.bind(1, state);

        while (stm.step())
        {
            auto& entity = channels.emplace_back();

            stm.get(LaserFields::LASER_CH_ID, std::get<LaserFields::LASER_CH_ID>(entity));
            stm.get(LaserFields::LASER_MY_BBS_ID, std::get<LaserFields::LASER_MY_BBS_ID>(entity));
            stm.get(LaserFields::LASER_TRG_WID, std::get<LaserFields::LASER_TRG_WID>(entity));
            stm.get(LaserFields::LASER_STATE, std::get<LaserFields::LASER_STATE>(entity));
            stm.get(LaserFields::LASER_FEE, std::get<LaserFields::LASER_FEE>(entity));
            stm.get(LaserFields::LASER_LOCKTIME, std::get<LaserFields::LASER_LOCKTIME>(entity));
            stm.get(LaserFields::LASER_AMOUNT_MY, std::get<LaserFields::LASER_AMOUNT_MY>(entity));
            stm.get(LaserFields::LASER_AMOUNT_TRG, std::get<LaserFields::LASER_AMOUNT_TRG>(entity));
            stm.get(LaserFields::LASER_AMOUNT_CURRENT_MY, std::get<LaserFields::LASER_AMOUNT_CURRENT_MY>(entity));
            stm.get(LaserFields::LASER_AMOUNT_CURRENT_TRG, std::get<LaserFields::LASER_AMOUNT_CURRENT_TRG>(entity));
            stm.get(LaserFields::LASER_LOCK_HEIGHT, std::get<LaserFields::LASER_LOCK_HEIGHT>(entity));
            stm.get(LaserFields::LASER_BBS_TIMESTAMP, std::get<LaserFields::LASER_BBS_TIMESTAMP>(entity));
            stm.get(LaserFields::LASER_DATA, std::get<LaserFields::LASER_DATA>(entity));
        }

        return channels;
    }
}
