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

FlyClient::NetworkStd::~NetworkStd()
{
    Disconnect();
}

void FlyClient::NetworkStd::Connect()
{
    if (m_Connections.size() == m_Cfg.m_vNodes.size())
    {
        // force (re) connect
        for (ConnectionList::iterator it = m_Connections.begin(); m_Connections.end() != it; ++it)
        {
            Connection& c = *it;
            if (c.IsLive() && c.IsSecureOut())
                continue;

            c.ResetAll();
            if (m_Cfg.m_UseProxy)
                c.Connect(c.m_Addr, m_Cfg.m_ProxyAddr);
            else
                c.Connect(c.m_Addr);
        }
    }
    else
    {
        Disconnect();

        for (size_t i = 0; i < m_Cfg.m_vNodes.size(); i++)
        {
            Connection* pConn = new Connection(*this);
            pConn->m_Addr = m_Cfg.m_vNodes[i];
            if (m_Cfg.m_UseProxy)
                pConn->Connect(pConn->m_Addr, m_Cfg.m_ProxyAddr);
            else
                pConn->Connect(pConn->m_Addr);
        }
    }
}

void FlyClient::NetworkStd::Disconnect()
{
    while (!m_Connections.empty())
        delete &m_Connections.front();
}

FlyClient::NetworkStd::Connection* FlyClient::NetworkStd::get_ActiveConnection()
{
    Connection* pRet = nullptr;

    for (ConnectionList::iterator it = m_Connections.begin(); m_Connections.end() != it; ++it)
    {
        Connection& c = *it;
        if (!c.IsLive() || !c.IsAtTip())
            continue;

        if (pRet)
        {
            if (pRet->m_Dependent.m_vec.size() >= c.m_Dependent.m_vec.size())
                continue;
        }

        pRet = &c;
    }

    return pRet;
}

void FlyClient::NetworkStd::DependentSubscribe(bool bSubscribe)
{
    bool b0 = HasDependentSubscriptions();

    if (bSubscribe)
        m_DependentSubscriptions++;
    else
    {
        if (m_DependentSubscriptions)
            m_DependentSubscriptions--;
    }

    if (HasDependentSubscriptions() != b0)
        OnDependentSubscriptionChanged();
}

void FlyClient::NetworkStd::OnDependentSubscriptionChanged()
{
    for (ConnectionList::iterator it = m_Connections.begin(); m_Connections.end() != it; ++it)
    {
        Connection& c = *it;
        if (c.IsLoginSent())
            c.SendLoginPlus(); // i.e. resend
    }
}

const Merkle::Hash* FlyClient::NetworkStd::get_DependentState(uint32_t& nCount)
{
    nCount = 0;

    auto* pC = get_ActiveConnection();
    if (!pC)
        return nullptr;

    const auto& v = pC->m_Dependent.m_vec;
    nCount = (uint32_t) v.size();
    return nCount ? &v.front() : nullptr;
}

FlyClient::NetworkStd::Connection::Connection(NetworkStd& x)
    : m_This(x)
{
    m_This.m_Connections.push_back(*this);
    ResetVars();
}

FlyClient::NetworkStd::Connection::~Connection()
{
    ResetInternal();
    m_This.m_Connections.erase(ConnectionList::s_iterator_to(*this));

}

bool FlyClient::NetworkStd::Connection::ShouldSync() const
{
    Block::SystemState::Full sTip;
    return !m_This.m_Client.get_History().get_Tip(sTip) || (sTip.m_ChainWork < m_Tip.m_ChainWork);
}

void FlyClient::NetworkStd::Connection::ResetVars()
{
    ZeroObject(m_Tip);
    m_Flags = 0;
    m_NodeID = Zero;

    m_Dependent.m_pQuery.reset();
    m_Dependent.m_vec.clear();
}

void FlyClient::NetworkStd::Connection::ResetInternal()
{
    m_pSync.reset();
	KillTimer();

    if (Flags::Owned & m_Flags)
        m_This.m_Client.OnOwnedNode(m_NodeID, false);

    if (Flags::ReportedConnected & m_Flags)
        m_This.OnNodeConnected(false);

    while (!m_lst.empty())
    {
        RequestNode& n = m_lst.front();
        m_lst.pop_front();
        m_This.m_lst.push_back(n);
    }
}

void FlyClient::NetworkStd::Connection::SendLoginPlus()
{
    SendLogin();

    if (!m_This.HasDependentSubscriptions())
        return;
    if (Flags::DependentPending & m_Flags)
        return;

    Send(proto::Ping());
    m_Flags |= Flags::DependentPending;
}

void FlyClient::NetworkStd::Connection::OnConnectedSecure()
{
	SendLoginPlus();

    if (!(Flags::ReportedConnected & m_Flags))
    {
        m_Flags |= Flags::ReportedConnected;
        m_This.OnNodeConnected(true);
    }
}

void FlyClient::NetworkStd::Connection::SetupLogin(Login& msg)
{
    msg.m_Flags |= LoginFlags::SendPeers;

    if (m_This.m_Cfg.m_PreferOnlineMining)
        msg.m_Flags |= LoginFlags::MiningFinalization;
    if (m_This.HasDependentSubscriptions())
        msg.m_Flags |= LoginFlags::WantDependentState;

    m_This.OnLoginSetup(msg);
}

void FlyClient::NetworkStd::Connection::OnDisconnect(const DisconnectReason& dr)
{
    m_This.OnConnectionFailed(dr);
	ResetAll();
    SetTimer(m_This.m_Cfg.m_ReconnectTimeout_ms);
}

void FlyClient::NetworkStd::Connection::ResetAll()
{
	NodeConnection::Reset();
	ResetInternal();
	ResetVars();
}

void FlyClient::NetworkStd::Connection::SetTimer(uint32_t timeout_ms)
{
    if (!m_pTimer)
        m_pTimer = io::Timer::create(io::Reactor::get_Current());

    m_pTimer->start(timeout_ms, false, [this]() { OnTimer(); });
}

void FlyClient::NetworkStd::Connection::KillTimer()
{
    if (m_pTimer)
        m_pTimer->cancel();
}

void FlyClient::NetworkStd::Connection::OnTimer()
{
    if (IsLive())
    {
        if (m_This.m_Cfg.m_PollPeriod_ms)
        {
            ResetAll();
            uint32_t timeout_ms = std::max(Rules::get().DA.Target_ms, m_This.m_Cfg.m_PollPeriod_ms);
            SetTimer(timeout_ms);
        }
    }
    else
    {
        ResetAll();
        if (m_This.m_Cfg.m_UseProxy) Connect(m_Addr, m_This.m_Cfg.m_ProxyAddr);
        else Connect(m_Addr);
    }
}

void FlyClient::NetworkStd::Connection::OnMsg(Authentication&& msg)
{
    NodeConnection::OnMsg(std::move(msg));

    switch (msg.m_IDType)
    {
    case IDType::Node:
        {
            if (Flags::Node & m_Flags)
                ThrowUnexpected();

            m_Flags |= Flags::Node;
            m_NodeID = msg.m_ID;

            Key::IKdf::Ptr pKdf;
            m_This.m_Client.get_Kdf(pKdf);
            if (pKdf)
            {
                ProveKdfObscured(*pKdf, IDType::Owner);
            }
            else
            {
                Key::IPKdf::Ptr ownerKdf;
                m_This.m_Client.get_OwnerKdf(ownerKdf);
                if (ownerKdf)
                {
                    ProvePKdfObscured(*ownerKdf, IDType::Viewer);
                }
            }
        }
        break;

    case IDType::Viewer:
        {
            if ((Flags::Owned & m_Flags) || !(Flags::Node & m_Flags))
                ThrowUnexpected();

            Key::IPKdf::Ptr pubKdf;
            m_This.m_Client.get_OwnerKdf(pubKdf);
            if (!(pubKdf && IsPKdfObscured(*pubKdf, msg.m_ID)))
                ThrowUnexpected();

            //  viewer confirmed!
            m_Flags |= Flags::Owned;
            m_This.m_Client.OnOwnedNode(m_NodeID, true);
        }
        break;

    default: // suppress warning
        break;
    }
}

void FlyClient::NetworkStd::Connection::OnMsg(GetBlockFinalization&& msg)
{
    if (!(Flags::Owned & m_Flags))
        ThrowUnexpected();

    Key::IKdf::Ptr pKdf;
    m_This.m_Client.get_Kdf(pKdf);
    if (!pKdf)
        ThrowUnexpected(); // ?!

    Key::IPKdf::Ptr pOwner = pKdf;

    Key::Index iIdx = 0;
    if (CoinID(Zero).get_ChildKdfIndex(iIdx))
        pKdf = MasterKey::get_Child(*pKdf, iIdx);

    Block::Builder bb(iIdx, *pKdf, *pOwner, msg.m_Height);
    // TODO: aidMax
    bb.AddCoinbaseAndKrn(msg.m_Fees);

    proto::BlockFinalization msgOut;
    msgOut.m_Value.reset(new Transaction);
    bb.m_Txv.MoveInto(*msgOut.m_Value);
    msgOut.m_Value->m_Offset = -bb.m_Offset;
    msgOut.m_Value->Normalize();

    Send(msgOut);
}

void FlyClient::NetworkStd::Connection::OnLogin(Login&& msg, uint32_t /* nFlagsPrev */)
{
    AssignRequests();

    if (LoginFlags::Bbs & m_LoginFlags)
        for (BbsSubscriptions::const_iterator it = m_This.m_BbsSubscriptions.begin(); m_This.m_BbsSubscriptions.end() != it; ++it)
        {
            proto::BbsSubscribe msgOut;
            msgOut.m_TimeFrom = it->second.second;
            msgOut.m_Channel = it->first;
            msgOut.m_On = true;
            Send(msgOut);
        }
}

void FlyClient::NetworkStd::Connection::OnMsg(DependentContextChanged&& msg)
{
    if (msg.m_vCtxs.empty())
        ThrowUnexpected();

    if (msg.m_PrefixDepth)
    {
        if (m_Dependent.m_vec.size() < msg.m_PrefixDepth)
            ThrowUnexpected();

        m_Dependent.m_vec.resize(msg.m_PrefixDepth + msg.m_vCtxs.size());

        for (size_t i = 0; i < msg.m_vCtxs.size(); i++)
            m_Dependent.m_vec[i + msg.m_PrefixDepth] = msg.m_vCtxs[i];
    }
    else
        m_Dependent.m_vec.swap(msg.m_vCtxs);

    if (this == m_This.get_ActiveConnection())
        m_This.m_Client.OnDependentStateChanged();
}

void FlyClient::NetworkStd::Connection::OnMsg(EventsSerif&& msg)
{
    if (!(Flags::Owned & m_Flags))
        ThrowUnexpected();

    // TODO: handle complex situation, where multiple owned nodes are connected
    m_This.m_Client.OnEventsSerif(msg.m_Value, msg.m_Height);
}

void FlyClient::NetworkStd::Connection::OnMsg(PeerInfo&& msg)
{
    m_This.m_Client.OnNewPeer(msg.m_ID, msg.m_LastAddr);
}

} // namespace proto
} // namespace beam
