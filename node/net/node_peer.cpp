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

void Node::Peer::OnRequestTimeout()
{
	assert(Flags::Connected & m_Flags);
	assert(!m_lstTasks.empty());

	BEAM_LOG_WARNING() << "Peer " << m_RemoteAddr << " request timeout";

	if (m_pInfo)
		ModifyRatingWrtData(0); // task (request) wasn't handled in time.

	DeleteSelf(false, ByeReason::Timeout);
}

void Node::Peer::OnResendPeers()
{
	PeerMan& pm = m_This.m_PeerMan;
	const PeerMan::RawRatingSet& rs = pm.get_Ratings();
	uint32_t nRemaining = pm.m_Cfg.m_DesiredHighest;

	for (PeerMan::RawRatingSet::const_iterator it = rs.begin(); nRemaining && (rs.end() != it); ++it)
	{
		const PeerMan::PeerInfo& pi = it->get_ParentObj();
		if ((Flags::PiRcvd & m_Flags) && (&pi == m_pInfo))
			continue; // skip

		if (!pi.m_RawRating.m_Value)
			continue; // banned

		if (getTimestamp() - pi.m_LastSeen > pm.m_Cfg.m_TimeoutRecommend_s)
			continue; // not seen for a while

		if (pi.m_Addr.m_Value.empty())
			continue; // address unknown, can't recommend

		proto::PeerInfo msg;
		msg.m_ID = pi.m_ID.m_Key;
		msg.m_LastAddr = pi.m_Addr.m_Value;
		Send(msg);

		nRemaining--;
	}
}

void Node::Peer::GenerateSChannelNonce(ECC::Scalar::Native& nonce)
{
	m_This.NextNonce(nonce);
}

void Node::Peer::OnTrafic(uint8_t msgCode, uint32_t msgSize, bool bOut)
{
	if (m_This.m_Cfg.m_LogTraficUsage)
		std::cout << "** " << (bOut ? "<-" : "->") << " " << m_RemoteAddr << " Size=" << msgSize << ", Msg=" << static_cast<uint32_t>(msgCode) << '\n';
}

void Node::Peer::OnConnectedSecure()
{
	BEAM_LOG_DEBUG() << "Peer " << m_RemoteAddr << " Connected";

	m_Flags |= Flags::Connected;

	if (!(Flags::Accepted & m_Flags) && m_This.m_Cfg.m_Listen.port())
	{
		// we've connected to the peer, let it now know our port
		proto::PeerInfoSelf msgPi;
		msgPi.m_Port = m_This.m_Cfg.m_Listen.port();
		Send(msgPi);
	}

	if (Flags::Probe & m_Flags)
		return; // just wait for peer to send its ID

	ProveID(m_This.m_MyPrivateID, proto::IDType::Node);

	SendLogin();

	if (m_This.m_Processor.IsTreasuryHandled() && !m_This.m_Processor.IsFastSync())
	{
		proto::NewTip msg;
		msg.m_Description = m_This.m_Processor.m_Cursor.m_Full;
		Send(msg);

		if (m_This.m_Validator.ShouldSendStamp())
			PbftSendStamp();
	}
}

void Node::Peer::SetupLogin(proto::Login& msg)
{
	msg.m_Flags |= proto::LoginFlags::SendPeers; // request a another node to periodically send a list of recommended peers

	if (m_This.m_PostStartSynced)
		msg.m_Flags |= proto::LoginFlags::SpreadingTransactions; // indicate ability to receive and broadcast transactions

	if (m_This.m_Cfg.m_Bbs.IsEnabled())
		msg.m_Flags |= proto::LoginFlags::Bbs; // indicate ability to receive and broadcast BBS messages
}

void Node::Peer::PbftSendStamp()
{
	if (m_Tip.m_hh == m_This.m_Processor.m_Cursor.m_hh)
		return;

	Send(m_This.m_Validator.m_Stamp.m_Msg);
}

Height Node::Peer::get_MinPeerFork()
{
	return m_This.m_Processor.m_Cursor.m_hh.m_Height + 1;
}

void Node::Peer::OnMsg(proto::Authentication&& msg)
{
	proto::NodeConnection::OnMsg(std::move(msg));
	BEAM_LOG_DEBUG() << "Peer " << m_RemoteAddr << " Auth. Type=" << msg.m_IDType << ", ID=" << msg.m_ID;

	if (proto::IDType::Owner == msg.m_IDType)
	{
		bool b = ShouldFinalizeMining();

		for (const auto& acc : m_This.m_Processor.m_vAccounts)
		{
			if (IsKdfObscured(*acc.m_pOwner, msg.m_ID))
			{
				m_Flags |= Flags::Owner | Flags::Viewer;
				m_pAccount = &acc;
				ProvePKdfObscured(*acc.m_pOwner, proto::IDType::Viewer);
				break;
			}
		}

		if (!b && ShouldFinalizeMining())
			m_This.m_Miner.OnFinalizerChanged(this);
	}

	if (proto::IDType::Viewer == msg.m_IDType)
	{
		for (const auto& acc : m_This.m_Processor.m_vAccounts)
		{
			if (IsPKdfObscured(*acc.m_pOwner, msg.m_ID))
			{
				m_Flags |= Flags::Viewer;
				m_pAccount = &acc;
				ProvePKdfObscured(*acc.m_pOwner, proto::IDType::Viewer);
			}
		}
	}

	MaybeSendSerif();

	if (proto::IDType::Node != msg.m_IDType)
		return;

	if ((Flags::PiRcvd & m_Flags) || (msg.m_ID == Zero))
		ThrowUnexpected();

	m_Flags |= Flags::PiRcvd;
	BEAM_LOG_DEBUG() << m_RemoteAddr << " received PI";

	PeerMan& pm = m_This.m_PeerMan; // alias
	PeerManager::TimePoint tp;

	Timestamp ts = getTimestamp();

	if (m_pInfo)
	{
		// probably we connected by the address
		if (m_pInfo->m_ID.m_Key == msg.m_ID)
		{
			assert(!(Flags::Accepted & m_Flags));
			m_pInfo->m_LastSeen = ts;

			TakeTasks();
			return; // all settled (already)
		}

		// detach from it
		PeerMan::PeerInfoPlus& pip = *m_pInfo;
		m_pInfo->DetachStrict();

		if (pip.m_ID.m_Key == Zero)
		{
			BEAM_LOG_INFO() << "deleted anonymous PI";
			pm.Delete(pip); // it's anonymous.
		}
		else
		{
			BEAM_LOG_INFO() << "PeerID is different";
			pm.OnActive(pip, false);
			pm.RemoveAddr(pip); // turned-out to be wrong
		}
	}

	if (msg.m_ID == m_This.m_MyPublicID)
	{
		BEAM_LOG_WARNING() << "Loopback connection";
		DeleteSelf(false, ByeReason::Loopback);
		return;
	}

	io::Address addr;

	bool bAddrValid = (m_Port > 0);
	if (bAddrValid)
	{
		addr = m_RemoteAddr;
		addr.port(m_Port);
	} else
		BEAM_LOG_INFO() << "No PI port"; // doesn't accept incoming connections?


	PeerMan::PeerInfoPlus* pPi = Cast::Up<PeerMan::PeerInfoPlus>(pm.OnPeer(msg.m_ID, addr, bAddrValid));
	assert(pPi);

	if (!(Flags::Accepted & m_Flags))
		pPi->m_LastSeen = ts;

	if (Flags::Probe & m_Flags)
	{
		BEAM_LOG_INFO() << *pPi << " probed ok";
		DeleteSelf(false, ByeReason::Probed);
		return;
	}

	if (pPi->m_Live.m_p)
	{
		BEAM_LOG_INFO() << "Duplicate connection with the same PI.";
		// Duplicate connection. In this case we have to choose wether to terminate this connection, or the previous. The best is to do it asymmetrically.
		// We decide this based on our Node IDs.
		// In addition, if the older connection isn't completed yet (i.e. it's our connect attempt) - it's prefered for deletion, because such a connection may be impossible (firewalls and friends).
		Peer* pDup = pPi->m_Live.m_p;

		if (!pDup->IsSecureOut() || (m_This.m_MyPublicID > msg.m_ID))
		{
			// detach from that peer
			pPi->DetachStrict();
			pDup->DeleteSelf(false, ByeReason::Duplicate);
		}
		else
		{
			DeleteSelf(false, ByeReason::Duplicate);
			return;
		}
	}

	if (!pPi->m_RawRating.m_Value)
	{
		BEAM_LOG_INFO() << "Banned PI. Ignoring";
		DeleteSelf(false, ByeReason::Ban);
		return;
	}

	// attach to it
	pPi->Attach(*this);
	pm.OnActive(*pPi, true);

	BEAM_LOG_DEBUG() << *m_pInfo << " connected, info updated";

	if ((Flags::Accepted & m_Flags) && !pPi->m_Addr.m_Value.empty())
	{
		uint32_t nPingThreshold_s = pm.m_Cfg.m_TimeoutRecommend_s / 3;
		if ((ts - pPi->m_LastSeen > nPingThreshold_s) &&
			(ts - pPi->m_LastConnectAttempt > nPingThreshold_s))
		{
			pPi->m_LastConnectAttempt = ts;

			BEAM_LOG_DEBUG() << *m_pInfo << " probing connection in opposite direction";


			Peer* p = m_This.AllocPeer(pPi->m_Addr.m_Value);
			p->m_Flags |= Flags::Probe;
			p->Connect(pPi->m_Addr.m_Value);
			p->m_Port = pPi->m_Addr.m_Value.port();
		}
	}

	TakeTasks();
}

bool Node::Peer::ShouldAssignTasks()
{
	// Current design: don't ask anything from non-authenticated peers
	if (!((Peer::Flags::PiRcvd & m_Flags) && m_pInfo))
		return false;

	return true;
}

bool Node::Peer::ShouldFinalizeMining()
{
	return
		(Flags::Owner & m_Flags) &&
		(proto::LoginFlags::MiningFinalization & m_LoginFlags) &&
		(m_pAccount->m_pOwner == m_This.m_Keys.m_pOwner) && // in multi-account node this is important
		m_This.m_Cfg.m_PreferOnlineMining;
}

void Node::Peer::OnMsg(proto::Bye&& msg)
{
	BEAM_LOG_DEBUG() << "Peer " << m_RemoteAddr << " Received Bye." << msg.m_Reason;
	NodeConnection::OnMsg(std::move(msg));
}

void Node::Peer::OnDisconnect(const DisconnectReason& dr)
{
	int nLogLevel = BEAM_LOG_LEVEL_DEBUG;

	bool bIsErr = true;
	uint8_t nByeReason = 0;

	switch (dr.m_Type)
	{
	default: assert(false);

	case DisconnectReason::Io:
	case DisconnectReason::Drown:
		break;

	case DisconnectReason::Bye:
		bIsErr = false;
		break;

	case DisconnectReason::ProcessingExc:
		switch (dr.m_ExceptionDetails.m_ExceptionType)
		{
		case proto::NodeProcessingException::Type::TimeOutOfSync:
			if (dr.m_ExceptionDetails.m_ExceptionType == proto::NodeProcessingException::Type::TimeOutOfSync && m_This.m_Cfg.m_Observer)
				m_This.m_Cfg.m_Observer->OnSyncError(IObserver::Error::TimeDiffToLarge);
			break;

		case proto::NodeProcessingException::Type::Incompatible:
			break;

		default:
			nLogLevel = BEAM_LOG_LEVEL_WARNING;
		}

		nByeReason = ByeReason::Ban;
		break;

	case DisconnectReason::Protocol:
		nLogLevel = BEAM_LOG_LEVEL_WARNING;
		nByeReason = ByeReason::Ban;
		break;
	}

	BEAM_LOG_MESSAGE(nLogLevel) << m_RemoteAddr << ": " << dr;

	DeleteSelf(bIsErr, nByeReason);
}

void Node::Peer::ReleaseTasks()
{
	while (!m_lstTasks.empty())
		ReleaseTask(m_lstTasks.front());
}

void Node::Peer::ReleaseTask(Task& t)
{
	assert(this == t.m_pOwner);
	t.m_pOwner = NULL;

	if (t.m_nCount)
	{
		uint32_t& nCounter = t.m_Key.second ? m_This.m_nTasksPackBody : m_This.m_nTasksPackHdr;
		assert(nCounter >= t.m_nCount);

		nCounter -= t.m_nCount;
		t.m_nCount = 0;
	}

	m_lstTasks.erase(TaskList::s_iterator_to(t));

	if (t.m_bSkeleton)
	{
		m_This.OnHdrSkeletonReleased(t);
		return;
	}

	m_This.m_lstTasksUnassigned.push_back(t);

	if (t.m_bNeeded)
		m_This.TryAssignTask(t);
	else
		m_This.DeleteUnassignedTask(t);
}

void Node::Peer::DeleteSelf(bool bIsError, uint8_t nByeReason)
{
	BEAM_LOG_DEBUG() << "-Peer " << m_RemoteAddr;

	if (nByeReason && (Flags::Connected & m_Flags))
	{
		proto::Bye msg;
		msg.m_Reason = nByeReason;
		Send(msg);
	}

	if (this == m_This.m_Miner.m_pFinalizer)
	{
		m_Flags &= ~Flags::Owner;
		m_LoginFlags &= proto::LoginFlags::MiningFinalization;

		assert(!ShouldFinalizeMining());
		m_This.m_Miner.OnFinalizerChanged(NULL);
	}

	m_Tip.m_Full.m_Number.v = 0; // prevent reassigning the tasks
	m_Flags &= ~Flags::HasTreasury;

	ReleaseTasks();
	Unsubscribe();

	if (m_pInfo)
	{
		PeerMan::PeerInfoPlus& pip = *m_pInfo;
		m_pInfo->DetachStrict();

		PeerManager& pm = m_This.m_PeerMan; // alias

		pm.OnActive(pip, false);

		if (bIsError)
		{
			if (ByeReason::Ban == nByeReason)
				pm.Ban(pip);
			else
			{
				PeerManager::TimePoint tp;
				uint32_t dt_ms = tp.get() - pip.m_LastActivity_ms;
				if (dt_ms < pm.m_Cfg.m_TimeoutDisconnect_ms)
				{
					uint32_t val =
						(pip.m_RawRating.m_Value > PeerManager::Rating::PenaltyNetworkErr) ?
						(pip.m_RawRating.m_Value - PeerManager::Rating::PenaltyNetworkErr) :
						1;
					pm.SetRating(pip, val);
				}
			}
		}

		if (pm.get_Ratings().size() > pm.m_Cfg.m_DesiredTotal)
		{
			bool bDelete =
				!pip.m_LastSeen || // never seen
				((1 == pip.m_RawRating.m_Value) && pm.IsOutdated(pip)); // lowest rating, not seen for a while

			if (bDelete)
			{
				BEAM_LOG_DEBUG() << pip << " Deleted";
				pm.Delete(pip);
			}
		}
	}

	SetTxCursor(nullptr);

	m_This.m_lstPeers.erase(PeerList::s_iterator_to(*this));
	delete this;
}

void Node::Peer::Unsubscribe(Bbs::Subscription& s)
{
	m_This.m_Bbs.m_Subscribed.erase(Bbs::Subscription::BbsSet::s_iterator_to(s.m_Bbs));
	m_Subscriptions.erase(Bbs::Subscription::PeerSet::s_iterator_to(s.m_Peer));
	delete &s;
}

void Node::Peer::Unsubscribe()
{
	while (!m_Subscriptions.empty())
		Unsubscribe(m_Subscriptions.begin()->get_ParentObj());
}

void Node::Peer::TakeTasks()
{
	if (!ShouldAssignTasks())
		return;

	for (TaskList::iterator it = m_This.m_lstTasksUnassigned.begin(); m_This.m_lstTasksUnassigned.end() != it; )
		m_This.TryAssignTask(*it++, *this);
}

void Node::Peer::OnMsg(proto::Pong&&)
{
	if (!(Flags::Chocking & m_Flags))
		ThrowUnexpected();

	m_Flags &= ~Flags::Chocking;

	// not chocking - continue broadcast
	BroadcastTxs();
	BroadcastBbs();

	for (Bbs::Subscription::PeerSet::iterator it = m_Subscriptions.begin(); m_Subscriptions.end() != it; ++it)
		BroadcastBbs(it->get_ParentObj());
}

void Node::Peer::OnMsg(proto::NewTip&& msg)
{
	if (msg.m_Description.m_ChainWork < m_Tip.m_Full.m_ChainWork)
		ThrowUnexpected();

	m_Tip.m_Full = msg.m_Description;
	m_Tip.m_Full.get_ID(m_Tip.m_hh);

	m_setRejected.clear();
	m_Flags |= Flags::HasTreasury;

	Processor& p = m_This.m_Processor;
	bool bTipNeeded = p.m_Cursor.IsRemoteNeeded(m_Tip);

	BEAM_LOG_MESSAGE(bTipNeeded ? BEAM_LOG_LEVEL_INFO : BEAM_LOG_LEVEL_DEBUG) << "Peer " << m_RemoteAddr << " Tip: " << LogTip(m_Tip);

	if (bTipNeeded && Rules::get().IsPbftWhitelistMode())
		bTipNeeded = false; // delay it, till we receive the stamp. Make sure we store only confirmed headers, OR those that we explicitly requested

	if (bTipNeeded)
		OnNewTip2();

	TakeTasks();

	if (!m_This.m_UpdatedFromPeers)
	{
		m_This.m_UpdatedFromPeers = true; // at least 1 peer reported actual tip

		ZeroObject(m_This.m_SyncStatus);
		m_This.UpdateSyncStatus();
	}

	m_This.m_Validator.SendState(*this);
}

void Node::Peer::OnNewTip2()
{
	if (!m_pInfo)
		return;

	Processor& p = m_This.m_Processor;
	switch (p.OnState(m_Tip.m_Full, m_pInfo->m_ID.m_Key))
	{
	case NodeProcessor::DataStatus::Invalid:
		m_Tip.m_Full.m_TimeStamp > getTimestamp() && m_This.m_Cfg.m_Observer ?
			m_This.m_Cfg.m_Observer->OnSyncError(IObserver::Error::TimeDiffToLarge):
			ThrowUnexpected();
		// no break;

	case NodeProcessor::DataStatus::Accepted:
		// don't give explicit reward for this header. Instead - most likely we'll request this block from that peer, and it'll have a chance to boost its rating
		m_This.RefreshCongestions();
		break; // since we made OnPeerInsane handling asynchronous - no need to return rapidly

	default:
		break; // suppress warning
	}
}

Node::Task& Node::Peer::get_FirstTask()
{
	if (m_lstTasks.empty())
		ThrowUnexpected();
	return m_lstTasks.front();
}

void Node::Peer::OnFirstTaskDone()
{
	ReleaseTask(get_FirstTask());
	SetTimerWrtFirstTask();

	// Refrain from using TakeTasks(), it will only try to assign tasks to this peer
	m_This.RefreshCongestions();
	m_This.m_Processor.TryGoUpAsync();
}

void Node::Peer::ModifyRatingWrtData(size_t nSize)
{
	PeerManager::TimePoint tp;
	uint32_t dt_ms = tp.get() - get_FirstTask().m_TimeAssigned_ms;

	// Calculate the weighted average of the effective bandwidth.
	// We assume the "previous" bandwidth bw0 was calculated within "previous" window t0, and the total download amount was v0 = t0 * bw0.
	// Hence, after accounting for newly-downloaded data, the average bandwidth becomes:
	// <bw> = (v0 + v1) / (t0 + t1) = (bw0 * t0 + v1) / (t0 + t1)
	//
	const uint32_t t0_s = 3;
	const uint32_t t0_ms = t0_s * 1000;
	uint64_t tTotal_ms = static_cast<uint64_t>(t0_ms) + dt_ms;
	assert(tTotal_ms); // can't overflow

	uint32_t bw0 = PeerManager::Rating::ToBps(m_pInfo->m_RawRating.m_Value);
	uint64_t v = static_cast<uint64_t>(bw0) * t0_s + nSize;

	uint32_t bwAvg = static_cast<uint32_t>(v * 1000 / tTotal_ms);

	uint32_t nRatingAvg = PeerManager::Rating::FromBps(bwAvg);

	m_This.m_PeerMan.m_LiveSet.erase(PeerMan::LiveSet::s_iterator_to(Cast::Up<PeerMan::PeerInfoPlus>(m_pInfo)->m_Live));
	m_This.m_PeerMan.SetRating(*m_pInfo, nRatingAvg);
	m_This.m_PeerMan.m_LiveSet.insert(Cast::Up<PeerMan::PeerInfoPlus>(m_pInfo)->m_Live);
}

void Node::Peer::OnMsg(proto::DataMissing&&)
{
	Task& t = get_FirstTask();

	std::pair<Task::Key, uint64_t> kRej;
	kRej.first = t.m_Key;
	if (t.m_Key.second)
		kRej.second = t.m_sidTrgRequested.m_Row;
	else
		kRej.second = 0;

	m_setRejected.insert(kRej);

	OnFirstTaskDone();
}

void Node::Peer::OnLogin(proto::Login&& msg, uint32_t nFlagsPrev)
{
	if (Flags::Probe & m_Flags)
	{
		DeleteSelf(false, ByeReason::Probed);
		return;
	}

	if ((m_LoginFlags ^ nFlagsPrev) & proto::LoginFlags::SendPeers)
	{
		if (m_LoginFlags & proto::LoginFlags::SendPeers)
		{
			if (!m_pTimerPeers)
				m_pTimerPeers = io::Timer::create(io::Reactor::get_Current());

			m_pTimerPeers->start(m_This.m_Cfg.m_Timeout.m_TopPeersUpd_ms, true, [this]() { OnResendPeers(); });

			OnResendPeers();
		}
		else
		{
			if (m_pTimerPeers)
				m_pTimerPeers->cancel();
		}
	}

	bool b;
	{
		TemporarySwap ts(m_LoginFlags, nFlagsPrev);
		b = ShouldFinalizeMining();
	}

	if (m_This.m_Cfg.m_Bbs.IsEnabled() &&
		!(proto::LoginFlags::Bbs & nFlagsPrev) &&
		(proto::LoginFlags::Bbs & m_LoginFlags))
	{
		proto::BbsResetSync msgOut;
		msgOut.m_TimeFrom = std::min(m_This.m_Bbs.m_HighestPosted_s, getTimestamp() - Rules::get().DA.MaxAhead_s);
		Send(msgOut);
	}

	MaybeSendSerif();
	MaybeSendDependent();

	if (b != ShouldFinalizeMining()) {
		// stupid compiler insists on parentheses!
		m_This.m_Miner.OnFinalizerChanged(b ? NULL : this);
	}

	m_This.m_Validator.SendState(*this);

	BroadcastTxs();
	BroadcastBbs();
}

bool Node::Peer::IsChocking(size_t nExtra /* = 0 */)
{
	if (Flags::Chocking & m_Flags)
		return true;

	if (get_Unsent() + nExtra  <= m_This.m_Cfg.m_BandwidthCtl.m_Chocking)
		return false;

	OnChocking();
	return true;
}

void Node::Peer::OnChocking()
{
	if (!(Flags::Chocking & m_Flags))
	{
		m_Flags |= Flags::Chocking;
		Send(proto::Ping());
	}
}

void Node::Peer::SetTxCursor(TxPool::Fluff::Element::Send* p)
{
	if (m_pCursorTx)
	{
		assert(m_pCursorTx != p);
		m_This.m_TxPool.Release(*m_pCursorTx);
	}

	m_pCursorTx = p;
	if (m_pCursorTx)
		m_pCursorTx->m_Refs++;
}

void Node::Peer::BroadcastTxs()
{
	if (!(proto::LoginFlags::SpreadingTransactions & m_LoginFlags))
		return;

	// TODO: send dependent txs

	if (IsChocking())
		return;

	for (size_t nExtra = 0; ; )
	{
		TxPool::Fluff::SendQueue::iterator itNext;
		if (m_pCursorTx)
		{
			itNext = TxPool::Fluff::SendQueue::s_iterator_to(*m_pCursorTx);
			++itNext;
		}
		else
			itNext = m_This.m_TxPool.m_SendQueue.begin();

		if (m_This.m_TxPool.m_SendQueue.end() == itNext)
			break; // all sent

		SetTxCursor(&(*itNext));

		if (!m_pCursorTx->m_pThis)
			continue; // already deleted
		auto& x = *m_pCursorTx->m_pThis;

		proto::HaveTransaction msgOut;
		msgOut.m_ID = x.m_Tx.m_Key;
		Send(msgOut);

		nExtra += x.m_Profit.m_Stats.m_Size;
		if (IsChocking(nExtra))
			break;
	}
}

void Node::Peer::MaybeSendSerif()
{
	if (!(Flags::Viewer & m_Flags) || (Flags::SerifSent & m_Flags))
		return;
	assert(m_pAccount);

	proto::EventsSerif msg;
	msg.m_Height = m_pAccount->m_hTxoHi;
	msg.m_Value = m_pAccount->m_Serif;

	Send(msg);
	m_Flags |= Flags::SerifSent;
}

void Node::Peer::MaybeSendDependent()
{
	auto& vec = m_Dependent.m_vSent; // alias
	if (m_Dependent.m_nSent.v != m_This.m_Processor.m_Cursor.m_Full.m_Number.v)
	{
		m_Dependent.m_nSent.v = m_This.m_Processor.m_Cursor.m_Full.m_Number.v;
		vec.clear();
	}

	if (!(proto::LoginFlags::WantDependentState & m_LoginFlags))
		return;

	auto* pTop = m_This.m_TxDependent.m_pBest;
	if (!pTop)
		return;

	proto::DependentContextChanged msg;
	for (msg.m_PrefixDepth = std::min(pTop->m_Depth, (uint32_t) vec.size()); msg.m_PrefixDepth; msg.m_PrefixDepth--, pTop = pTop->m_pParent)
		if (vec[msg.m_PrefixDepth - 1] == pTop->m_Context.m_Key)
			break;

	vec.resize(msg.m_PrefixDepth);

	pTop = m_This.m_TxDependent.m_pBest;
	if (pTop->m_Depth == msg.m_PrefixDepth)
		return; // no change

	assert(vec.size() < pTop->m_Depth);
	vec.resize(pTop->m_Depth);
	msg.m_vCtxs.resize(vec.size() - msg.m_PrefixDepth);

	for (uint32_t i = pTop->m_Depth; ; pTop = pTop->m_pParent)
	{
		const auto& hv = pTop->m_Context.m_Key;
		vec[--i] = hv;

		uint32_t i2 = i - msg.m_PrefixDepth;
		msg.m_vCtxs[i2] = hv;

		if (!i2)
			break;
	}

	Send(msg);
}

} // namespace beam
