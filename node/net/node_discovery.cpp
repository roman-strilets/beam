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

#include "node_common.h"

namespace beam {

struct Node::Beacon::OutCtx
{
	int m_Refs;
	OutCtx() :m_Refs(0) {}

	struct UvRequest
		:public uv_udp_send_t
	{
		IMPLEMENT_GET_PARENT_OBJ(OutCtx, m_Request)
	} m_Request;

	uv_buf_t m_BufDescr;

#pragma pack (push, 1)
	struct Message
	{
		Merkle::Hash m_CfgChecksum;
		PeerID m_NodeID;
		uint16_t m_Port; // in network byte order
	};
#pragma pack (pop)

	Message m_Message; // the message broadcasted

	void Release()
	{
		assert(m_Refs > 0);
		if (!--m_Refs)
			delete this;
	}

	static void OnDone(uv_udp_send_t* req, int status);
};

Node::Beacon::Beacon()
	:m_pUdp(NULL)
	,m_pOut(NULL)
{
}

Node::Beacon::~Beacon()
{
	if (m_pUdp)
		uv_close((uv_handle_t*) m_pUdp, OnClosed);

	if (m_pOut)
		m_pOut->Release();
}

uint16_t Node::Beacon::get_Port()
{
	uint16_t nPort = get_ParentObj().m_Cfg.m_BeaconPort;
	return nPort ? nPort : get_ParentObj().m_Cfg.m_Listen.port();
}

void Node::Beacon::Start()
{
	assert(!m_pUdp);
	m_pUdp = new uv_udp_t;

	uv_udp_init(&io::Reactor::get_Current().get_UvLoop(), m_pUdp);
	m_pUdp->data = this;

	m_BufRcv.resize(sizeof(OutCtx::Message));

	io::Address addr;
	addr.port(get_Port());

	sockaddr_in sa;
	addr.fill_sockaddr_in(sa);

	if (uv_udp_bind(m_pUdp, (sockaddr*)&sa, UV_UDP_REUSEADDR)) // should allow multiple nodes on the same machine (for testing)
		beam::ThrowLastError();

	if (uv_udp_recv_start(m_pUdp, AllocBuf, OnRcv))
		beam::ThrowLastError();

	if (uv_udp_set_broadcast(m_pUdp, 1))
		beam::ThrowLastError();

	m_pTimer = io::Timer::create(io::Reactor::get_Current());
	m_pTimer->start(get_ParentObj().m_Cfg.m_BeaconPeriod_ms, true, [this]() { OnTimer(); }); // periodic timer
	OnTimer();
}

void Node::Beacon::OnTimer()
{
	if (!m_pOut)
	{
		m_pOut = new OutCtx;
		m_pOut->m_Refs = 1;

		m_pOut->m_Message.m_CfgChecksum = Rules::get().get_LastFork().m_Hash;
		m_pOut->m_Message.m_NodeID = get_ParentObj().m_MyPublicID;
		m_pOut->m_Message.m_Port = htons(get_ParentObj().m_Cfg.m_Listen.port());

		m_pOut->m_BufDescr.base = (char*) &m_pOut->m_Message;
		m_pOut->m_BufDescr.len = sizeof(m_pOut->m_Message);
	}
	else
		if (m_pOut->m_Refs > 1)
			return; // send still pending

	io::Address addr;
	addr.port(get_Port());
	addr.ip(INADDR_BROADCAST);

	sockaddr_in sa;
	addr.fill_sockaddr_in(sa);

	m_pOut->m_Refs++;

	int nErr = uv_udp_send(&m_pOut->m_Request, m_pUdp, &m_pOut->m_BufDescr, 1, (sockaddr*) &sa, OutCtx::OnDone);
	if (nErr)
		m_pOut->Release();
}

void Node::Beacon::OutCtx::OnDone(uv_udp_send_t* req, int /* status */)
{
	UvRequest* pVal = (UvRequest*)req;
	assert(pVal);

	pVal->get_ParentObj().Release();
}

void Node::Beacon::OnRcv(uv_udp_t* handle, ssize_t nread, const uv_buf_t* buf, const struct sockaddr* pSa, unsigned flags)
{
	OutCtx::Message msg;
	if (sizeof(msg) != nread)
		return;

	memcpy(&msg, buf->base, sizeof(msg)); // copy it to prevent (potential) datatype misallignment and etc.

	if (msg.m_CfgChecksum != Rules::get().get_LastFork().m_Hash)
		return;

	Beacon* pThis = (Beacon*)handle->data;

	if (pThis->get_ParentObj().m_MyPublicID == msg.m_NodeID)
		return;

	io::Address addr(*(sockaddr_in*)pSa);
	addr.port(ntohs(msg.m_Port));

	pThis->get_ParentObj().m_PeerMan.OnPeer(msg.m_NodeID, addr, true);
}

void Node::Beacon::AllocBuf(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf)
{
	Beacon* pThis = (Beacon*)handle->data;
	assert(pThis);

	buf->base = (char*) &pThis->m_BufRcv.at(0);
	buf->len = sizeof(OutCtx::Message);
}

void Node::Beacon::OnClosed(uv_handle_t* p)
{
	assert(p);
	delete (uv_udp_t*) p;
}

void Node::PeerMan::Initialize()
{
	const Config& cfg = get_ParentObj().m_Cfg;

	for (uint32_t i = 0; i < cfg.m_Connect.size(); i++)
	{
		PeerID id0(Zero);
		OnPeer(id0, cfg.m_Connect[i], true);
	}

	// peers
	m_pTimerUpd = io::Timer::create(io::Reactor::get_Current());
	m_pTimerUpd->start(cfg.m_Timeout.m_PeersUpdate_ms, true, [this]() {
		Update();
		get_ParentObj().HedgeLowBody(); // when nothing is received, RefreshCongestions() isn't called
	});

	m_pTimerFlush = io::Timer::create(io::Reactor::get_Current());
	m_pTimerFlush->start(cfg.m_Timeout.m_PeersDbFlush_ms, true, [this]() { OnFlush(); });

	{
		NodeDB::WalkerPeer wlk;
		for (get_ParentObj().m_Processor.get_DB().EnumPeers(wlk); wlk.MoveNext(); )
		{
			if (wlk.m_Data.m_ID == get_ParentObj().m_MyPublicID)
				continue; // could be left from previous run?

			PeerMan::PeerInfo* pPi = OnPeer(wlk.m_Data.m_ID, io::Address::from_u64(wlk.m_Data.m_Address), false);
			if (!pPi)
				continue;

			// set rating
			uint32_t r = wlk.m_Data.m_Rating;
			if (!r)
				Ban(*pPi);
			else
				SetRating(*pPi, r);

			pPi->m_LastSeen = wlk.m_Data.m_LastSeen;
			pPi->m_LastConnectAttempt = pPi->m_LastSeen;
		}
	}
}

void Node::PeerMan::OnFlush()
{
	NodeDB& db = get_ParentObj().m_Processor.get_DB();

	db.PeersDel();

	const PeerMan::RawRatingSet& rs = get_Ratings();

	for (PeerMan::RawRatingSet::const_iterator it = rs.begin(); rs.end() != it; ++it)
	{
		const PeerMan::PeerInfo& pi = it->get_ParentObj();

		NodeDB::WalkerPeer::Data d;
		d.m_ID = pi.m_ID.m_Key;
		d.m_Rating = pi.m_RawRating.m_Value;
		d.m_Address = pi.m_Addr.m_Value.u64();
		d.m_LastSeen = pi.m_LastSeen;

		db.PeerIns(d);
	}
}

void Node::PeerMan::ActivateMorePeers(uint32_t nTime_ms)
{
	const Config& cfg = get_ParentObj().m_Cfg; // alias
	if (!cfg.m_PeersPersistent)
		return;

	for (uint32_t i = 0; i < cfg.m_Connect.size(); i++)
	{
		PeerID id0(Zero);
		PeerInfo* pPi = OnPeer(id0, cfg.m_Connect[i], true);
		if (pPi)
			ActivatePeerSafe(*pPi, nTime_ms);
	}
}

void Node::PeerMan::ActivatePeer(PeerInfo& pi)
{
	PeerInfoPlus& pip = Cast::Up<PeerInfoPlus>(pi);
	if (pip.m_Live.m_p)
		return; //?

	Peer* p = get_ParentObj().AllocPeer(pip.m_Addr.m_Value);
	pip.Attach(*p);

	p->Connect(pip.m_Addr.m_Value);
	p->m_Port = pip.m_Addr.m_Value.port();
}

void Node::PeerMan::DeactivatePeer(PeerInfo& pi)
{
	PeerInfoPlus& pip = (PeerInfoPlus&)pi;
	if (!pip.m_Live.m_p)
		return; //?

	pip.m_Live.m_p->DeleteSelf(false, proto::NodeConnection::ByeReason::Other);
}

PeerManager::PeerInfo* Node::PeerMan::AllocPeer()
{
	PeerInfoPlus* p = new PeerInfoPlus;
	p->m_Live.m_p = nullptr;
	return p;
}

void Node::PeerMan::DeletePeer(PeerInfo& pi)
{
	delete &Cast::Up<PeerInfoPlus>(pi);
}

void Node::PeerMan::PeerInfoPlus::Attach(Peer& p)
{
	assert(!m_Live.m_p && !p.m_pInfo);
	m_Live.m_p = &p;
	p.m_pInfo = this;

	PeerManager::TimePoint tp;
	p.m_This.m_PeerMan.m_LiveSet.insert(m_Live);
}

void Node::PeerMan::PeerInfoPlus::DetachStrict()
{
	assert(m_Live.m_p && (this == m_Live.m_p->m_pInfo));

	m_Live.m_p->m_This.m_PeerMan.m_LiveSet.erase(PeerMan::LiveSet::s_iterator_to(m_Live));

	m_Live.m_p->m_pInfo = nullptr;
	m_Live.m_p = nullptr;
}

} // namespace beam
