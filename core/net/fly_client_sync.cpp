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

#include "core/net/fly_client.h"

namespace beam {
namespace proto {

void FlyClient::NetworkStd::Connection::OnMsg(NewTip&& msg)
{
	if (!msg.m_Description.m_Number.v)
		return; // ignore

    if (m_Tip == msg.m_Description)
        return; // redundant msg

    if (msg.m_Description.m_ChainWork <= m_Tip.m_ChainWork)
        ThrowUnexpected();

    if (!(msg.m_Description.IsValid()))
        ThrowUnexpected();

    m_Dependent.m_vec.clear();

    if (m_pSync && m_pSync->m_vConfirming.empty() && !m_pSync->m_TipBeforeGap.m_Number.v && !m_Tip.IsNext(msg.m_Description))
        m_pSync->m_TipBeforeGap = m_Tip;

    m_Tip = msg.m_Description;

    if (!Rules::get().IsPbftWhitelistMode())
        OnNewTip();
}

void FlyClient::NetworkStd::Connection::OnNewTip()
{
    if (!m_pSync)
    {
        if (ShouldSync())
        {
            StartSync();
        }
        else
        {
            m_This.m_Client.OnTipUnchanged();
            AssignRequests();
        }
    }
}

void FlyClient::NetworkStd::Connection::OnMsg(PbftStamp&& msg)
{
    Connection::Test(msg, m_Tip);
    // all good
    OnNewTip();
}

void FlyClient::NetworkStd::Connection::StartSync()
{
    assert(ShouldSync());
    KillTimer();

    Block::SystemState::Full sTip;
    m_This.m_Client.get_History().get_Tip(sTip);
    if (sTip.IsNext(m_Tip))
    {
        // simple case
        m_This.m_Client.get_History().AddStates(&m_Tip, 1);
        PrioritizeSelf();
        AssignRequests();
        m_This.m_Client.OnNewTip();
    }
    else
    {
        // starting search
        m_pSync.reset(new SyncCtx);
        m_pSync->m_LowHeight = m_Tip.get_Height();
        SearchBelow(m_Tip.get_Height(), 1);
    }
}

void FlyClient::NetworkStd::Connection::SearchBelow(Height h, uint32_t nCount)
{
    assert(ShouldSync() && m_pSync && m_pSync->m_vConfirming.empty());
    assert(nCount);

    struct Walker :public Block::SystemState::IHistory::IWalker
    {
        std::vector<Block::SystemState::Full> m_vStates;
        uint32_t m_Count;
        Block::Number m_TipNum;

        bool OnState(const Block::SystemState::Full& s) override
        {
            if (s.m_Number.v >= m_TipNum.v)
                return true;

            m_vStates.push_back(s);
            return m_vStates.size() < m_Count;
        }
    } w;

    w.m_Count = nCount;
    w.m_vStates.reserve(nCount);
    w.m_TipNum = m_Tip.m_Number;
    m_This.m_Client.get_History().Enum(w, &h);

    if (w.m_vStates.empty())
    {
        ZeroObject(m_pSync->m_Confirmed);
        RequestChainworkProof();
    }
    else
    {
        if (w.m_vStates.front().IsNext(m_Tip))
        {
            // only the last block has been rolled-back. Typical in pbft mode (cannibalization), but can happen in PoW mode too
            m_pSync->m_Confirmed = w.m_vStates.front();
            PostChainworkProof();
        }
        else
        {
            GetCommonState msg;
            msg.m_IDs.resize(w.m_vStates.size());

            for (size_t i = 0; i < msg.m_IDs.size(); i++)
                w.m_vStates[i].get_ID(msg.m_IDs[i]);

            Send(msg);

            m_pSync->m_vConfirming.swap(w.m_vStates);
        }
    }
}

void FlyClient::NetworkStd::Connection::OnMsg(ProofCommonState&& msg)
{
    if (!m_pSync)
        ThrowUnexpected();

    std::vector<Block::SystemState::Full> vStates = std::move(m_pSync->m_vConfirming);
    if (vStates.empty())
        ThrowUnexpected();

    if (!ShouldSync())
    {
        m_pSync.reset();
        return; // other connection was faster
    }

    size_t iState;
    for (iState = 0; ; iState++)
    {
        if (vStates.size() == iState)
        {
            // not found. Theoretically it's possible that the current tip is lower than the requested range (but highly unlikely)
            if (m_Tip.m_Number.v > vStates.back().m_Number.v)
                ThrowUnexpected();

            SearchBelow(m_Tip.get_Height(), 1); // restart
            return;

        }
        if (vStates[iState].m_Number.v == msg.m_ID.m_Number.v)
            break;
    }

    if (!m_Tip.IsValidProofState(msg.m_ID, msg.m_Proof))
        ThrowUnexpected();

    if ((m_pSync->m_LowHeight < vStates.front().get_Height()) && iState)
        SearchBelow(m_pSync->m_LowHeight + 1, 1); // restart the search from this height
    else
    {
        const Block::SystemState::Full& s = vStates[iState];
        Merkle::Hash hv;
        s.get_Hash(hv);
        if (hv != msg.m_ID.m_Hash)
        {
            if (iState != vStates.size() - 1)
                ThrowUnexpected(); // the disproof should have been for the last requested state

            SearchBelow(vStates.back().get_Height(), static_cast<uint32_t>(vStates.size() * 2)); // all the range disproven. Search below
        }
        else
        {
            m_pSync->m_Confirmed = s;
            RequestChainworkProof();
        }
    }
}

struct FlyClient::NetworkStd::Connection::StateArray
{
    std::vector<Block::SystemState::Full> m_vec;

    bool Find(const Block::SystemState::Full&) const;
};

bool FlyClient::NetworkStd::Connection::StateArray::Find(const Block::SystemState::Full& s) const
{
    struct Cmp {
        bool operator () (const Block::SystemState::Full& s, Block::Number num) const { return s.m_Number.v < num.v; }
    };

    // the array should be sorted (this is verified by chainworkproof verification)
    std::vector<Block::SystemState::Full>::const_iterator it = std::lower_bound(m_vec.begin(), m_vec.end(), s.m_Number, Cmp());
    return (m_vec.end() != it) && (*it == s);
}

void FlyClient::NetworkStd::Connection::PostChainworkProof()
{
    assert(m_pSync);

    if (!Rules::get().IsConstantSpan())
    {
        // make sure we have at least header preceding the current tip. Otherwise we may constantly experience deep reorgs
        struct Walker :public Block::SystemState::IHistory::IWalker
        {
            Block::SystemState::Full m_State;
            
            bool OnState(const Block::SystemState::Full& s) override
            {
                m_State = s;
                return false;
            }
        } w;


        Height hTip = m_Tip.get_Height();
        bool bHavePrev =
            (m_Tip.m_Number.v <= 1) &&
            !m_This.m_Client.get_History().Enum(w, &hTip) &&
            w.m_State.IsNext(m_Tip);

        if (!bHavePrev)
        {
            proto::GetHdr msg;
            msg.m_ID.m_Number.v = m_Tip.m_Number.v - 1;
            msg.m_ID.m_Hash = m_Tip.m_Prev;
            Send(msg);
        }
    }

    SyncCtx::Ptr pSync = std::move(m_pSync);
    StateArray arr;
    PostChainworkProof(arr, pSync->m_Confirmed.get_Height());
}


void FlyClient::NetworkStd::Connection::RequestChainworkProof()
{
    assert(ShouldSync() && m_pSync && m_pSync->m_vConfirming.empty());

    if ((Flags::Owned & m_Flags) || (Rules::Consensus::Pbft == Rules::get().m_Consensus))
    {
        // for trusted nodes this is not required. Go straight to finish
        // In Pbft chainwork proof is irrelevant (though supported anyway)
        PostChainworkProof();
    }
    else
    {
        GetProofChainWork msg;
        msg.m_LowerBound = m_pSync->m_Confirmed.m_ChainWork;
        Send(msg);

        m_pSync->m_TipBeforeGap.m_Number.v = 0;
        m_pSync->m_LowHeight = m_pSync->m_Confirmed.get_Height();
    }
}

void FlyClient::NetworkStd::Connection::OnMsg(ProofChainWork&& msg)
{
    if (!m_pSync || !m_pSync->m_vConfirming.empty())
        ThrowUnexpected();

    if (msg.m_Proof.m_LowerBound != m_pSync->m_Confirmed.m_ChainWork)
        ThrowUnexpected();

    Block::SystemState::Full sTip;
    if (!msg.m_Proof.IsValid(&sTip))
        ThrowUnexpected();

    if (sTip != m_Tip)
        ThrowUnexpected();

    SyncCtx::Ptr pSync = std::move(m_pSync);

    if (!ShouldSync())
        return;

    // Unpack the proof, convert it to one sorted array. For convenience
    StateArray arr;
	msg.m_Proof.UnpackStates(arr.m_vec);

    if (pSync->m_TipBeforeGap.m_Number.v && pSync->m_Confirmed.m_Number.v)
    {
        // Since there was a gap in the tips reported by the node (which is typical in case of reorgs) - there is a possibility that our m_Confirmed is no longer valid.
        // If either the m_Confirmed or the m_TipBeforeGap are mentioned in the chainworkproof - then there's no problem with reorg.
        // And since chainworkproof usually contains a "tail" of consecutive headers - there should be no problem, unless the reorg is huge
        // Otherwise sync should be repeated
        if (!arr.Find(pSync->m_TipBeforeGap) &&
            !arr.Find(pSync->m_Confirmed))
        {
            StartSync(); // again
            return;
        }
    }

    PostChainworkProof(arr, pSync->m_LowHeight);
}

void FlyClient::NetworkStd::Connection::PostChainworkProof(const StateArray& arr, Height hLowHeight)
{
    struct Walker :public Block::SystemState::IHistory::IWalker
    {
        Height m_LowHeight;
        Height m_LowErase;
        const StateArray* m_pArr;

        bool OnState(const Block::SystemState::Full& s) override
        {
            if (s.get_Height() <= m_LowHeight)
                return false;

            if (m_pArr->Find(s))
                return false;

            m_LowErase = s.get_Height();
            return true;
        }
    } w;

    w.m_LowErase = MaxHeight;
    w.m_LowHeight = hLowHeight;
    w.m_pArr = &arr;

    m_This.m_Client.get_History().Enum(w, NULL);

    if (w.m_LowErase != MaxHeight)
    {
        bool bRolledBack = true;

        if (!Rules::get().IsConstantSpan())
        {
            // check if we only removed 1 empty block, and have the previous one.
            // It's a typical scenario for PBFT, and In such a case we won't report the rollback
            
            struct Walker2 :public Block::SystemState::IHistory::IWalker
            {
                Block::SystemState::Full m_pS[2];
                uint32_t m_Count = 0;

                bool OnState(const Block::SystemState::Full& s) override
                {
                    assert(m_Count < _countof(m_pS));
                    m_pS[m_Count] = s; // copy

                    return ++m_Count < _countof(m_pS);
                }
            } w2;

            m_This.m_Client.get_History().Enum(w2, nullptr);
            assert(w2.m_Count); // otherwise nothing to erase

            if ((w2.m_pS[1].m_Number.v + 1 == w2.m_pS[0].m_Number.v) &&
                (w2.m_pS[1].get_Height() < w.m_LowErase))
            {
                const auto& d = Cast::Reinterpret<Block::Pbft::HdrData>(w2.m_pS[0].m_PoW);
                if (Block::Pbft::HdrData::Flags::Empty & d.m_Flags1)
                    bRolledBack = false;
            }
        }

        m_This.m_Client.get_History().DeleteFrom(w.m_LowErase);

        // if more connections are opened simultaneously - notify them
        for (ConnectionList::iterator it = m_This.m_Connections.begin(); m_This.m_Connections.end() != it; ++it)
        {
            const Connection& c = *it;
            if (c.m_pSync)
                beam::setmin(c.m_pSync->m_LowHeight, w.m_LowErase - 1);
        }

        if (bRolledBack)
            m_This.m_Client.OnRolledBack();
    }

    if (arr.m_vec.empty())
        m_This.m_Client.get_History().AddStates(&m_Tip, 1);
    else
        m_This.m_Client.get_History().AddStates(&arr.m_vec.front(), arr.m_vec.size());
    PrioritizeSelf();

    m_This.m_Client.OnNewTip(); // finished!
    AssignRequests();
}


void FlyClient::NetworkStd::Connection::PrioritizeSelf()
{
    m_This.m_Connections.erase(ConnectionList::s_iterator_to(*this));
    m_This.m_Connections.push_front(*this);
}

} // namespace proto
} // namespace beam
