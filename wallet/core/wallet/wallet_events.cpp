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

#include "wallet_common.h"

namespace beam::wallet
{
    void Wallet::RequestEvents()
    {
        if (!m_OwnedNodesOnline)
        {
            return;
        }

        Block::SystemState::Full sTip;
        m_WalletDB->get_History().get_Tip(sTip);

        Height h = GetEventsHeightNext();
        assert(h <= sTip.get_Height() + 1);
        if (h > sTip.get_Height())
            return;

        if (!m_PendingEvents.empty())
        {
            if (m_PendingEvents.begin()->m_Msg.m_HeightMin == h)
                return; // already pending
            DeleteReq(*m_PendingEvents.begin());
        }

        MyRequestEvents::Ptr pReq(new MyRequestEvents);
        pReq->m_Msg.m_HeightMin = h;
        PostReqUnique(*pReq);
    }

    void Wallet::AbortEvents()
    {
        if (!m_PendingEvents.empty())
            DeleteReq(*m_PendingEvents.begin());
    }

    void Wallet::OnRequestComplete(MyRequestEvents& r)
    {
        struct MyParser
            :public proto::Event::IGroupParser
        {
            Wallet& m_This;
            MyParser(Wallet& x) :m_This(x) {}

            void OnEventType(proto::Event::Shielded& evt) override
            {
                m_This.ProcessEventShieldedUtxo(evt, m_Height);
            }

            void OnEventType(proto::Event::AssetCtl& evt) override
            {
                m_This.ProcessEventAsset(evt, m_Height);
            }

            void OnEventType(proto::Event::Utxo& evt) override
            {
                m_This.ProcessEventUtxo(evt, m_Height);
            }

        } p(*this);

        uint32_t nCount = p.Proceed(r.m_Res.m_Events);

        if (nCount < proto::Event::s_Max)
        {
            Block::SystemState::Full sTip;
            m_WalletDB->get_History().get_Tip(sTip);

            SetEventsHeight(sTip.get_Height());
            if (!m_IsTreasuryHandled)
                SetTreasuryHandled(true); // to be able to switch to unsafe node
        }
        else
        {
            SetEventsHeight(p.m_Height);
            RequestEvents(); // maybe more events pending
        }
    }

    void Wallet::SetEventsHeight(Height h)
    {
        storage::setNextEventHeight(*m_WalletDB, h + 1); // we're actually saving the next
    }

    Height Wallet::GetEventsHeightNext() const
    {
        return storage::getNextEventHeight(*m_WalletDB);
    }

    void Wallet::ProcessEventUtxo(const proto::Event::Utxo& evt, Height h)
    {
        CoinID cid = evt.m_Cid;
        // filter-out false positives
        if (!m_WalletDB->IsRecoveredMatch(cid, evt.m_Commitment))
            return;

        bool bAdd = 0 != (proto::Event::Flags::Add & evt.m_Flags);
        CacheCommitment(evt.m_Commitment, evt.m_Maturity, bAdd);
        ProcessEventUtxo(cid, h, evt.m_Maturity, bAdd, evt.m_User);
    }

    void Wallet::ProcessEventUtxo(const CoinID& cid, Height h, Height hMaturity, bool bAdd, const Output::User& user)
    {
        Coin c;
        c.m_ID = cid;
        bool bExists = m_WalletDB->findCoin(c);
        c.m_maturity = hMaturity;

        const auto* data = Output::User::ToPacked(user);
        if (!memis0(data->m_TxID.m_pData, sizeof(TxID)))
        {
            c.m_createTxId.emplace();
            std::copy_n(data->m_TxID.m_pData, sizeof(TxID), c.m_createTxId->begin());
        }

        BEAM_LOG_INFO() << "CoinID: " << c.m_ID << " Maturity=" << hMaturity << (bAdd ? " Confirmed" : " Spent") << ", Height=" << h;

        if (bAdd)
        {
            beam::setmin(c.m_confirmHeight, h); // in case of std utxo proofs - the event height may be bigger than actual utxo height

            // Check if this Coin participates in any active transaction
            // if it does and mark it as outgoing (bug: ux_504)
            for (const auto& [txid, txptr] : m_ActiveTransactions)
            {
                std::vector<Coin::ID> icoins;
                txptr->GetParameter(TxParameterID::InputCoins, icoins);
                if (std::find(icoins.begin(), icoins.end(), c.m_ID) != icoins.end())
                {
                    c.m_status = Coin::Status::Outgoing;
                    c.m_spentTxId = txid;
                    BEAM_LOG_INFO() << "CoinID: " << c.m_ID << " marked as Outgoing";
                }
            }
        }
        else
        {
            if (!bExists)
                return; // should alert!

            beam::setmin(c.m_spentHeight, h); // reported spend height may be bigger than it actuall was (in case of macroblocks)
        }

        m_WalletDB->saveCoin(c);
    }

    void Wallet::ProcessEventAsset(const proto::Event::AssetCtl& assetCtl, Height h)
    {
        // TODO
    }

    void Wallet::ProcessEventShieldedUtxo(const proto::Event::Shielded& shieldedEvt, Height h)
    {
        auto shieldedCoin = m_WalletDB->getShieldedCoin(shieldedEvt.m_CoinID.m_Key);
        if (!shieldedCoin)
        {
            shieldedCoin = ShieldedCoin{};
        }

        shieldedCoin->m_CoinID = shieldedEvt.m_CoinID;
        shieldedCoin->m_TxoID = shieldedEvt.m_TxoID;

        bool isAdd = 0 != (proto::Event::Flags::Add & shieldedEvt.m_Flags);
        if (isAdd)
        {
            shieldedCoin->m_confirmHeight = std::min(shieldedCoin->m_confirmHeight, h);
        }
        else
        {
            shieldedCoin->m_spentHeight = std::min(shieldedCoin->m_spentHeight, h);
        }

        // m_createTxId is kept if assigned locally, it's never taken from the TxID the sender packed into the coin.
        // See restoreTransactionFromShieldedCoin

        // Check if this Coin participates in any active transaction
        for (const auto& [txid, txptr] : m_ActiveTransactions)
        {

            std::vector<IPrivateKeyKeeper2::ShieldedInput> inputShielded;
            txptr->GetParameter(TxParameterID::InputCoinsShielded, inputShielded);
            if (std::find(inputShielded.begin(), inputShielded.end(), shieldedEvt.m_CoinID) != inputShielded.end())
            {
                shieldedCoin->m_Status = ShieldedCoin::Status::Outgoing;;
                shieldedCoin->m_spentTxId = txid;
                BEAM_LOG_INFO() << "Shielded output, ID: " << shieldedEvt.m_TxoID << " marked as Outgoing";
            }
        }

        m_WalletDB->saveShieldedCoin(*shieldedCoin);

        BEAM_LOG_INFO() << "Shielded output, ID: " << shieldedEvt.m_TxoID << (isAdd ? " Confirmed" : " Spent") << ", Height=" << h;
        RestoreTransactionFromShieldedCoin(*shieldedCoin);
    }

    void Wallet::OnEventsSerif(const Hash::Value& hv, Height h)
    {
        static const char szEvtSerif[] = "EventsSerif";

        HeightHash hh;
        if (!storage::getVar(*m_WalletDB, szEvtSerif, hh))
        {
            hh.m_Hash = Zero;
            // If this is the 1st time we received the serif - we do NOT assume the wallet was synced ok. It can potentially be already out-of-sync.
            // Hence once node and wallet are both upgraded from older version - there will always be initial rescan.
            hh.m_Height = MaxHeight;
        }

        bool bHashChanged = (hh.m_Hash != hv);
        bool bMustRescan = bHashChanged;
        if (bHashChanged)
        {
            // Node Serif has changed (either connected to different node, or it rescanned the blockchain). The events stream may not be consistent with ours.
            Height h0 = GetEventsHeightNext();
            if (!h0)
                bMustRescan = false; // nothing to rescan atm
            else
            {
                if (h0 > hh.m_Height)
                {
                    // Our events are consistent and full up to height h0-1.
                    bMustRescan = (h >= h0);
                }
            }

            hh.m_Hash = hv;
        }

        if (bHashChanged || (h != hh.m_Height))
        {
            BEAM_LOG_INFO() << "Events Serif changed: " << (bHashChanged ? "new Hash, " : "") << "Height=" << h << (bMustRescan ? ", Resyncing" : "");

            hh.m_Height = h;
            storage::setVar(*m_WalletDB, szEvtSerif, hh);
        }

        if (bMustRescan)
            Rescan();

    }

    void Wallet::RestoreTransactionFromShieldedCoin(ShieldedCoin& coin)
    {
        // add virtual transaction for receiver
        storage::restoreTransactionFromShieldedCoin(*m_WalletDB, coin, *this);
    }
}
