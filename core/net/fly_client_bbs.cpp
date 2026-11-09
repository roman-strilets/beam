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

void FlyClient::NetworkStd::BbsSubscribe(BbsChannel ch, Timestamp ts, IBbsReceiver* p)
{
    BbsSubscriptions::iterator it = m_BbsSubscriptions.find(ch);
    if (m_BbsSubscriptions.end() == it)
    {
        if (!p)
            return;

        m_BbsSubscriptions.emplace(std::make_pair(ch, std::make_pair(p, ts)));
    }
    else
    {
        if (p)
        {
            it->second.first = p;
            it->second.second = ts;
            return;
        }

        m_BbsSubscriptions.erase(it);
    }

    proto::BbsSubscribe msg;
    msg.m_TimeFrom = ts;
    msg.m_Channel = ch;
    msg.m_On = (nullptr != p);

    for (ConnectionList::iterator it2 = m_Connections.begin(); m_Connections.end() != it2; ++it2)
        if (it2->IsLive() && it2->IsSecureOut())
            it2->Send(msg);
}

void FlyClient::NetworkStd::Connection::OnMsg(BbsMsg&& msg)
{
    BbsSubscriptions::iterator it = m_This.m_BbsSubscriptions.find(msg.m_Channel);
    if (m_This.m_BbsSubscriptions.end() != it)
    {
        it->second.second = msg.m_TimePosted;

        assert(it->second.first);
        it->second.first->OnMsg(std::move(msg));
    }
}

//////////////
// BbsMiner
void FlyClient::NetworkStd::BbsMiner::Post(RequestBbsMsg& r)
{
    if (!m_pEvtMined)
    {
        m_pEvtMined = io::AsyncEvent::create(io::Reactor::get_Current(), [this]() { OnMined(); });
        m_iCurrent = 1;

#if defined(EMSCRIPTEN)
        uint32_t nThreads = 3;
#else
        uint32_t nThreads = MyThread::hardware_concurrency();
#endif
        nThreads = (nThreads > 1) ? (nThreads - 1) : 1; // leave at least 1 vacant core for other things
        m_vThreads.resize(nThreads);

        for (uint32_t i = 0; i < nThreads; i++)
            m_vThreads[i] = MyThread(&BbsMiner::Thread, this, i);
    }

    auto pNode = std::make_unique<RequestNode>();
    pNode->m_pRequest = &r;

    {
        std::scoped_lock<std::mutex> scope(m_Mutex);
        m_lstToMine.push_back(*pNode.release());
        m_NewTask.notify_all();
    }
}

void FlyClient::NetworkStd::BbsMiner::Stop()
{
    if (!m_vThreads.empty())
    {
        {
            std::unique_lock<std::mutex> scope(m_Mutex);
            m_iCurrent = 0;
            m_NewTask.notify_all();
        }

        for (size_t i = 0; i < m_vThreads.size(); i++)
            if (m_vThreads[i].joinable())
                m_vThreads[i].join();

        m_vThreads.clear();
    }

    m_pEvtMined.reset();
    m_lstToMine.Clear();
    m_lstMined.Clear();
}

void FlyClient::NetworkStd::BbsMiner::Thread(uint32_t iThread)
{
    proto::Bbs::NonceType nStep = static_cast<uint32_t>(m_vThreads.size());

    while (true)
    {
        ECC::Hash::Processor hpPartial(Uninitialized);
        uint64_t iCurrent;

        for (std::unique_lock<std::mutex> scope(m_Mutex); ; m_NewTask.wait(scope))
        {
            if (!m_iCurrent)
                return; // shutdown

            if (!m_lstToMine.empty())
            {
                if (!m_hpPartial.IsInitialized())
                {
                    m_hpPartial.Reset();
                    proto::Bbs::get_HashPartial(m_hpPartial, Cast::Up<RequestBbsMsg>(*m_lstToMine.front().m_pRequest).m_Msg);
                    assert(m_hpPartial.IsInitialized());
                }

                hpPartial = m_hpPartial;
                iCurrent = m_iCurrent;
                break;
            }
        }

        Timestamp ts = 0;
        proto::Bbs::NonceType nonce = iThread;
        bool bSuccess = false;

        for (uint32_t i = 0; ; i++)
        {
            if (m_iCurrent != iCurrent)
                break;

            if (!(i & 0xff))
                ts = getTimestamp();

            // attempt to mine it
            ECC::Hash::Value hv;
            ECC::Hash::Processor hp = hpPartial;
            hp
                << ts
                << nonce
                >> hv;

            if (proto::Bbs::IsHashValid(hv))
            {
                bSuccess = true;
                break;
            }

            nonce += nStep;
        }

        if (bSuccess)
        {
            bool bNotify = false;

            {
                std::unique_lock<std::mutex> scope(m_Mutex);

                if (m_iCurrent == iCurrent)
                {
                    assert(!m_lstToMine.empty());
                    auto& n = m_lstToMine.front();
                    auto& msg = Cast::Up<RequestBbsMsg>(*n.m_pRequest).m_Msg;

                    msg.m_TimePosted = ts;
                    msg.m_Nonce = nonce;

                    if (m_lstMined.empty())
                        bNotify = true;

                    m_lstToMine.pop_front();
                    m_lstMined.push_back(n);

                    m_iCurrent = iCurrent + 1; // advance
                    m_hpPartial.Reset(Uninitialized);
                }
            }

            if (bNotify)
                m_pEvtMined->post();
        }
    }
}

void FlyClient::NetworkStd::BbsMiner::OnMined()
{
    if (!m_lstMined.empty())
    {
        do
        {
            auto& n = m_lstMined.front();
            m_lstMined.pop_front();
            get_ParentObj().m_lst.push_back(n);

        } while (!m_lstMined.empty());

        get_ParentObj().OnNewRequests();
    }
}

} // namespace proto
} // namespace beam
