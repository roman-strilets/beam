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

void Node::Bbs::CalcMsgKey(NodeDB::WalkerBbs::Data& d)
{
	ECC::Hash::Processor()
		<< d.m_Message
		<< d.m_Channel
		>> d.m_Key;
}

uint32_t Node::Bbs::WantedMsg::get_Timeout_ms()
{
	return get_ParentObj().get_ParentObj().m_Cfg.m_Timeout.m_GetBbsMsg_ms;
}

void Node::Bbs::WantedMsg::OnExpired(const KeyType& key)
{
	proto::BbsGetMsg msg;
	msg.m_Key = key;

	for (PeerList::iterator it = get_ParentObj().get_ParentObj().m_lstPeers.begin(); get_ParentObj().get_ParentObj().m_lstPeers.end() != it; ++it)
	{
		Peer& peer = *it;
		if (peer.m_LoginFlags & proto::LoginFlags::Bbs)
			peer.Send(msg);
	}

	get_ParentObj().MaybeCleanup();
}

bool Node::Bbs::IsInLimits() const
{
	const NodeDB::BbsTotals& lims = get_ParentObj().m_Cfg.m_Bbs.m_Limit;

	return
		(m_Totals.m_Count <= lims.m_Count) &&
		(m_Totals.m_Size <= lims.m_Size);
}

void Node::Bbs::Cleanup()
{
	NodeDB& db = get_ParentObj().m_Processor.get_DB();
	NodeDB::WalkerBbsTimeLen wlk;

	Timestamp ts = getTimestamp() - get_ParentObj().m_Cfg.m_Bbs.m_MessageTimeout_s;

	for (db.EnumAllBbs(wlk); wlk.MoveNext(); )
	{
		if (IsInLimits() && (wlk.m_Time >= ts))
			break;

		db.BbsDel(wlk.m_ID);
		m_Totals.m_Count--;
		m_Totals.m_Size -= wlk.m_Size;
	}

	m_LastCleanup_ms = GetTime_ms();
}

void Node::Bbs::MaybeCleanup()
{
	if (IsInLimits())
	{
		uint32_t dt_ms = GetTime_ms() - m_LastCleanup_ms;
		if (dt_ms < get_ParentObj().m_Cfg.m_Bbs.m_CleanupPeriod_ms)
			return;
	}
	Cleanup();
}

void Node::Peer::BroadcastBbs()
{
	m_This.m_Bbs.MaybeCleanup();

	if (!(proto::LoginFlags::Bbs & m_LoginFlags))
		return;

	if (IsChocking())
		return;

	size_t nExtra = 0;

	NodeDB& db = m_This.m_Processor.get_DB();
	NodeDB::WalkerBbsLite wlk;

	wlk.m_ID = m_CursorBbs;
	for (db.EnumAllBbsSeq(wlk); wlk.MoveNext(); )
	{
		proto::BbsHaveMsg msgOut;
		msgOut.m_Key = wlk.m_Key;
		Send(msgOut);

		nExtra += wlk.m_Size;
		if (IsChocking(nExtra))
			break;
	}

	m_CursorBbs = wlk.m_ID;
}

void Node::Peer::OnMsg(proto::BbsMsg&& msg)
{
	if (Rules::get().IsPastFork_<1>(m_This.m_Processor.m_Cursor.m_hh.m_Height) && (Rules::Consensus::FakePoW != Rules::get().m_Consensus))
	{
		// test the hash
		ECC::Hash::Value hv;
		proto::Bbs::get_Hash(hv, msg);

		if (!proto::Bbs::IsHashValid(hv))
			return; // drop
	}

	if (!m_This.m_Cfg.m_Bbs.IsEnabled())
		ThrowUnexpected();

	if (msg.m_Message.size() > proto::Bbs::s_MaxMsgSize)
		ThrowUnexpected("Bbs msg too large"); // will also ban this peer

	Timestamp t = getTimestamp();

	if (msg.m_TimePosted > t + Rules::get().DA.MaxAhead_s)
		return; // too much ahead of time

	if (msg.m_TimePosted + m_This.m_Cfg.m_Bbs.m_MessageTimeout_s < t)
		return; // too old

	if (msg.m_TimePosted + Rules::get().DA.MaxAhead_s < m_This.m_Bbs.m_HighestPosted_s)
		return; // don't allow too much out-of-order messages

	NodeDB& db = m_This.m_Processor.get_DB();
	NodeDB::WalkerBbs wlk;

	wlk.m_Data.m_Channel = msg.m_Channel;
	wlk.m_Data.m_TimePosted = msg.m_TimePosted;
	wlk.m_Data.m_Message = Blob(msg.m_Message);
	msg.m_Nonce.Export(wlk.m_Data.m_Nonce);

	Bbs::CalcMsgKey(wlk.m_Data);

	if (db.BbsFind(wlk.m_Data.m_Key))
		return; // already have it

	m_This.m_Bbs.MaybeCleanup();

	uint64_t id = db.BbsIns(wlk.m_Data);
	m_This.m_Bbs.m_W.Delete(wlk.m_Data.m_Key);

	beam::setmax(m_This.m_Bbs.m_HighestPosted_s, msg.m_TimePosted);
	m_This.m_Bbs.m_Totals.m_Count++;
	m_This.m_Bbs.m_Totals.m_Size += wlk.m_Data.m_Message.n;

	// 1. Send to other BBS-es

	proto::BbsHaveMsg msgOut;
	msgOut.m_Key = wlk.m_Data.m_Key;

	for (PeerList::iterator it = m_This.m_lstPeers.begin(); m_This.m_lstPeers.end() != it; ++it)
	{
		Peer& peer = *it;
		if (this == &peer)
			continue;

		if (!(peer.m_LoginFlags & proto::LoginFlags::Bbs) || peer.IsChocking())
			continue;

		peer.Send(msgOut);
	}

	// 2. Send to subscribed
	typedef Bbs::Subscription::BbsSet::iterator It;

	Bbs::Subscription::InBbs key;
	key.m_Channel = msg.m_Channel;

	for (std::pair<It, It> range = m_This.m_Bbs.m_Subscribed.equal_range(key); range.first != range.second; range.first++)
	{
		Bbs::Subscription& s = range.first->get_ParentObj();
		assert(s.m_Cursor < id);

		if (s.m_pPeer->IsChocking())
			continue;

		s.m_pPeer->SendBbsMsg(wlk.m_Data);
		s.m_Cursor = id;

		s.m_pPeer->IsChocking(); // in case it's chocking - for faster recovery recheck it ASAP
	}
}

void Node::Peer::OnMsg(proto::BbsHaveMsg&& msg)
{
	if (!m_This.m_Cfg.m_Bbs.IsEnabled())
		ThrowUnexpected();

	NodeDB& db = m_This.m_Processor.get_DB();
	if (db.BbsFind(msg.m_Key)) {
		// stupid compiler insists on parentheses here!
		return; // already have it
	}

	if (!m_This.m_Bbs.m_W.Add(msg.m_Key)) {
		// stupid compiler insists on parentheses here!
		return; // already waiting for it
	}

	proto::BbsGetMsg msgOut;
	msgOut.m_Key = msg.m_Key;
	Send(msgOut);
}

void Node::Peer::OnMsg(proto::BbsGetMsg&& msg)
{
	if (!m_This.m_Cfg.m_Bbs.IsEnabled())
		ThrowUnexpected();

	NodeDB& db = m_This.m_Processor.get_DB();
	NodeDB::WalkerBbs wlk;

	wlk.m_Data.m_Key = msg.m_Key;
	if (!db.BbsFind(wlk))
		return; // don't have it

	SendBbsMsg(wlk.m_Data);
}

void Node::Peer::SendBbsMsg(const NodeDB::WalkerBbs::Data& d)
{
	proto::BbsMsg msgOut;
	msgOut.m_Channel = d.m_Channel;
	msgOut.m_TimePosted = d.m_TimePosted;
	d.m_Message.Export(msgOut.m_Message);
	msgOut.m_Nonce = d.m_Nonce;
	Send(msgOut);
}

void Node::Peer::OnMsg(proto::BbsSubscribe&& msg)
{
	if (!m_This.m_Cfg.m_Bbs.IsEnabled())
		ThrowUnexpected();

	Bbs::Subscription::InPeer key;
	key.m_Channel = msg.m_Channel;

	Bbs::Subscription::PeerSet::iterator it = m_Subscriptions.find(key);
	if ((m_Subscriptions.end() == it) != msg.m_On)
		return;

	if (msg.m_On)
	{
		Bbs::Subscription* pS = new Bbs::Subscription;
		pS->m_pPeer = this;

		pS->m_Bbs.m_Channel = msg.m_Channel;
		pS->m_Peer.m_Channel = msg.m_Channel;
		m_This.m_Bbs.m_Subscribed.insert(pS->m_Bbs);
		m_Subscriptions.insert(pS->m_Peer);

		pS->m_Cursor = m_This.m_Processor.get_DB().BbsFindCursor(msg.m_TimeFrom) - 1;

		BroadcastBbs(*pS);
	}
	else
		Unsubscribe(it->get_ParentObj());
}

void Node::Peer::BroadcastBbs(Bbs::Subscription& s)
{
	if (IsChocking())
		return;

	NodeDB& db = m_This.m_Processor.get_DB();
	NodeDB::WalkerBbs wlk;

	wlk.m_Data.m_Channel = s.m_Peer.m_Channel;
	wlk.m_ID = s.m_Cursor;

	for (db.EnumBbsCSeq(wlk); wlk.MoveNext(); )
	{
		SendBbsMsg(wlk.m_Data);
		if (IsChocking())
			break;
	}

	s.m_Cursor = wlk.m_ID;
}

void Node::Peer::OnMsg(proto::BbsResetSync&& msg)
{
	if (!m_This.m_Cfg.m_Bbs.IsEnabled())
		ThrowUnexpected();

	m_CursorBbs = m_This.m_Processor.get_DB().BbsFindCursor(msg.m_TimeFrom) - 1;
	BroadcastBbs();
}

} // namespace beam
