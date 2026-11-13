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

bool Node::SyncStatus::operator == (const SyncStatus& x) const
{
	return
		(m_Done == x.m_Done) &&
		(m_Total == x.m_Total);
}

void Node::SyncStatus::ToRelative(Height hDone0)
{
	beam::setmin(hDone0, m_Done); // prevent overflow (though should not happen)

	assert(m_Total); // never 0, accounts at least for treasury
	beam::setmin(hDone0, m_Total - 1); // prevent "indefinite" situation where sync status is 0/0

	m_Done -= hDone0;
	m_Total -= hDone0;
}

void Node::RefreshCongestions()
{
	for (TaskSet::iterator it = m_setTasks.begin(); m_setTasks.end() != it; ++it)
		it->m_bNeeded = false;

	m_Processor.EnumCongestions();

	for (TaskList::iterator it = m_lstTasksUnassigned.begin(); m_lstTasksUnassigned.end() != it; )
	{
		Task& t = *(it++);
		if (!t.m_bNeeded)
			DeleteUnassignedTask(t);
	}

	// Header packs are assigned here, top-down. The branches are enumerated bottom-up, so the top one would get a free slot last,
	// whereas the hdr skeleton window moves only once it links to the branches below.
	std::vector<Task*> vHdrs;
	for (TaskList::iterator it = m_lstTasksUnassigned.begin(); m_lstTasksUnassigned.end() != it; ++it)
		if (!it->m_Key.second)
			vHdrs.push_back(&*it);

	std::sort(vHdrs.begin(), vHdrs.end(), [](const Task* a, const Task* b) { return a->m_Key.first.m_Number.v > b->m_Key.first.m_Number.v; });

	for (Task* pTask : vHdrs)
		TryAssignTask(*pTask);

	UpdateHdrSkeleton();
	HedgeLowBody();
}

void Node::HedgeLowBody()
{
	if (!m_Cfg.m_BodyHedge_ms || !m_Processor.IsFastSync() || !m_Processor.m_BodyWindows.m_Size)
		return;

	const uint64_t numNext = m_Processor.m_Cursor.m_Full.m_Number.v + 1;

	// the pack right above the cursor. The tasks are ordered by the key, the number goes first
	Task* pOrig = nullptr;
	for (TaskSet::iterator it = m_setTasks.begin(); m_setTasks.end() != it; ++it)
	{
		Task& t = *it;
		if (t.m_Key.first.m_Number.v > numNext)
			break;

		if (!t.m_Key.second || (t.m_Key.first.m_Number.v != numNext))
			continue;

		if (t.m_bHedge || t.m_bHedged)
			return; // already
		if (t.m_pOwner && t.m_bNeeded)
			pOrig = &t;
	}

	if (!pOrig)
		return;

	PeerManager::TimePoint tp;
	if (tp.get() - pOrig->m_TimeRequested_ms < m_Cfg.m_BodyHedge_ms)
		return;

	Task* pTask = new Task;
	pTask->m_Key = pOrig->m_Key;
	pTask->m_sidTrg = pOrig->m_sidTrg;
	pTask->m_bNeeded = true;
	pTask->m_nCount = 0;
	pTask->m_pOwner = nullptr;
	pTask->m_bHedge = true;

	m_setTasks.insert(*pTask);
	m_lstTasksUnassigned.push_back(*pTask);

	// the best rated peer, other than the one that is slow
	for (PeerMan::LiveSet::iterator it = m_PeerMan.m_LiveSet.begin(); m_PeerMan.m_LiveSet.end() != it; ++it)
	{
		Peer& p = *it->m_p;
		if ((&p != pOrig->m_pOwner) && TryAssignTask(*pTask, p))
		{
			pOrig->m_bHedged = true;
			pTask->m_bHedged = true;
			BEAM_LOG_INFO() << "Body pack " << pTask->m_Key.first << " is requested from " << pOrig->m_pOwner->m_RemoteAddr << " for "
				<< (tp.get() - pOrig->m_TimeRequested_ms) << " ms, requesting it from " << p.m_RemoteAddr << " as well";
			return;
		}
	}

	DeleteUnassignedTask(*pTask);
}

void Node::UpdateHdrSkeleton()
{
	const uint64_t nStep = HdrSkeleton::s_Step;
	const uint64_t numCursor = m_Processor.m_Cursor.m_Full.m_Number.v;

	auto& setPoints = m_HdrSkeleton.m_setPoints; // alias
	while (!setPoints.empty() && (setPoints.begin()->first <= numCursor))
		setPoints.erase(setPoints.begin());

	NodeDB::StateID sidLow;
	bool bActive =
		m_Cfg.m_HdrSkeletonWindow &&
		Rules::get().IsConstantSpan() && // EnumHdrs addresses headers by height, which is the number only for constant span
		m_Processor.get_HdrGapLow(sidLow, setPoints) &&
		(sidLow.m_Number.v > numCursor + nStep * 2);

	Block::Number& numNext = m_HdrSkeleton.m_numNext; // alias

	if (!bActive)
	{
		if (numNext.v)
		{
			BEAM_LOG_INFO() << "Hdr skeleton stopped";
			StopHdrSkeleton();
		}
		return;
	}

	m_HdrSkeleton.m_numLow = sidLow.m_Number;

	const uint64_t nWindow = nStep * m_Cfg.m_HdrSkeletonWindow;

	// Restart if not started yet, or if the lowest known header is out of the skeleton window: either below it
	// (the skeleton lagged behind), or far above (switched to another branch).
	if (!numNext.v || (numNext.v >= sidLow.m_Number.v) || (sidLow.m_Number.v - numNext.v > nWindow + nStep))
	{
		numNext.v = sidLow.m_Number.v - nStep;
		BEAM_LOG_INFO() << "Hdr skeleton from " << numNext.v << ", lowest known header " << sidLow.m_Number.v;
	}

	while (IsHdrSkeletonPointExpected(numNext) && (sidLow.m_Number.v - numNext.v <= nWindow))
	{
		if (IsHdrSkeletonPointKnown(numNext))
		{
			// Requested already, or there's a header at this height: received before the skeleton restart (or the node restart),
			// or downloaded by the regular download. Requesting it again would only bring the header we have.
			numNext.v -= nStep;
			continue;
		}

		if (!RequestHdrSkeletonPoint(numNext, sidLow.m_Number))
		{
			// No suitable peer at the moment. If the regular download waits for this point (see TryAssignTask) - stop the skeleton, to let it go on.
			// Once restarted, it skips the points requested already.
			bool bAwaited = false;
			for (TaskList::iterator it = m_lstTasksUnassigned.begin(); m_lstTasksUnassigned.end() != it; ++it)
			{
				const Task& t = *it;
				if (!t.m_Key.second && (t.m_Key.first.m_Number.v == numNext.v))
				{
					bAwaited = true;
					break;
				}
			}

			if (bAwaited)
			{
				BEAM_LOG_INFO() << "Hdr skeleton point " << numNext.v << " can't be requested, skeleton stopped";
				StopHdrSkeleton();
			}
			break;
		}

		numNext.v -= nStep;
	}
}

bool Node::IsHdrSkeletonPointExpected(Block::Number num) const
{
	return
		m_HdrSkeleton.m_numNext.v &&
		(num.v == m_HdrSkeleton.m_numNext.v) &&
		(num.v > m_Processor.m_Cursor.m_Full.m_Number.v + HdrSkeleton::s_Step); // the last pack above the cursor is left to the regular download
}

void Node::StopHdrSkeleton()
{
	m_HdrSkeleton.m_numNext.v = 0;

	// The packs that waited for the skeleton points, otherwise they'd be assigned only at the next RefreshCongestions()
	for (TaskList::iterator it = m_lstTasksUnassigned.begin(); m_lstTasksUnassigned.end() != it; )
		TryAssignTask(*it++);
}

Node::Task::Key Node::get_HdrSkeletonKey(Block::Number num)
{
	// Header request with zero hash: can't collide with the requests driven by congestions
	Task::Key key;
	key.first.m_Number = num;
	key.first.m_Hash = Zero;
	key.second = false;
	return key;
}

bool Node::IsHdrSkeletonPointPending(Block::Number num) const
{
	Task tKey;
	tKey.m_Key = get_HdrSkeletonKey(num);

	return m_setTasks.end() != m_setTasks.find(tKey);
}

bool Node::IsHdrSkeletonPointKnown(Block::Number num)
{
	if (IsHdrSkeletonPointPending(num))
		return true;

	NodeDB::WalkerState ws;
	m_Processor.get_DB().EnumStatesAt(ws, num);
	return ws.MoveNext();
}

bool Node::RequestHdrSkeletonPoint(Block::Number num, Block::Number numMinTip)
{
	const Task::Key key = get_HdrSkeletonKey(num);

	// The point is tiny, but the reply is queued after the replies to the peer's previous tasks.
	// Choose the peer with the shortest queue, prioritize w.r.t. rating among equal ones.
	Peer* pPeer = nullptr;
	size_t nQueue = 0;

	for (PeerMan::LiveSet::iterator it = m_PeerMan.m_LiveSet.begin(); m_PeerMan.m_LiveSet.end() != it; ++it)
	{
		Peer& p = *it->m_p;
		if (!p.ShouldAssignTasks())
			continue;

		if (p.m_Tip.m_Full.m_Number.v < numMinTip.v)
			continue; // the peer may be on another branch, or behind (also the case for a peer being deleted)

		if (p.m_setRejected.end() != p.m_setRejected.find(std::make_pair(key, uint64_t(0))))
			continue; // the peer doesn't have it

		size_t n = p.m_lstTasks.size();
		if (!pPeer || (n < nQueue))
		{
			pPeer = &p;
			nQueue = n;
		}
	}

	if (pPeer)
	{
		Peer& p = *pPeer;

		proto::EnumHdrs msg;
		msg.m_Height.m_Min = num.v;
		msg.m_Height.m_Max = num.v;
		p.Send(msg);

		Task* pTask = new Task;
		pTask->m_Key = key;
		pTask->m_bSkeleton = true;
		pTask->m_numSkeletonMinTip = numMinTip;
		pTask->m_bNeeded = false; // irrelevant, not driven by congestions (see OnHdrSkeletonReleased)
		pTask->m_nCount = 0;
		pTask->m_sidTrg.SetNull();
		pTask->m_pOwner = nullptr;

		m_setTasks.insert(*pTask);
		AssignTask(*pTask, p);

		return true;
	}

	return false;
}

void Node::OnHdrSkeletonReleased(Task& t)
{
	assert(t.m_bSkeleton && !t.m_pOwner && !t.m_nCount);

	Block::Number num = t.m_Key.first.m_Number;
	Block::Number numMinTip = t.m_numSkeletonMinTip;
	bool bRetry = !t.m_bSkeletonDone && m_HdrSkeleton.m_numNext.v;

	m_setTasks.erase(TaskSet::s_iterator_to(t));
	delete &t;

	// The peer disconnected or doesn't have it. Ask another one, otherwise the gap would be filled by the regular (sequential) download.
	if (bRetry && !RequestHdrSkeletonPoint(num, numMinTip))
		BEAM_LOG_INFO() << "Hdr skeleton point " << num.v << " lost";
}

bool Node::IsHdrSkeletonBranch(const Task& t) const
{
	// The header request for a branch is its lowest known header's parent
	return
		m_HdrSkeleton.m_numNext.v &&
		!t.m_Key.second &&
		(t.m_Key.first.m_Number.v + 1 == m_HdrSkeleton.m_numLow.v);
}

void Node::UpdateSyncStatus()
{
	if (!m_UpdatedFromPeers)
		return;

	SyncStatus stat = m_SyncStatus;
	UpdateSyncStatusRaw();

	if (!m_PostStartSynced && (m_SyncStatus.m_Done == m_SyncStatus.m_Total) && !m_Processor.IsFastSync())
	{
		m_PostStartSynced = true;

		BEAM_LOG_INFO() << "Tx replication is ON";

		for (PeerList::iterator it = m_lstPeers.begin(); m_lstPeers.end() != it; ++it)
		{
			Peer& peer = *it;
			if ((Peer::Flags::Connected & peer.m_Flags) && !(Peer::Flags::Probe & peer.m_Flags))
				peer.SendLogin();
		}

		m_Validator.OnNewState();
	}

	if (m_Cfg.m_Observer && !(m_SyncStatus == stat))
		m_Cfg.m_Observer->OnSyncProgress();
}

void Node::UpdateSyncStatusRaw()
{
	uint64_t hTotal = m_Processor.m_Cursor.m_Full.m_Number.v;
	uint64_t hDoneBlocks = hTotal;
	uint64_t hDoneHdrs = hTotal;

	if (m_Processor.IsFastSync())
		hTotal = m_Processor.m_SyncData.m_Target.m_Number.v;

	auto AccountHdrs = [&](const Task& t)
	{
		beam::setmax(hTotal, t.m_sidTrg.m_Number.v);
		if (t.m_sidTrg.m_Number.v > t.m_Key.first.m_Number.v)
			beam::setmax(hDoneHdrs, m_Processor.m_Cursor.m_Full.m_Number.v + t.m_sidTrg.m_Number.v - t.m_Key.first.m_Number.v);
	};

	// The branch the hdr skeleton is built for. Its pack request may stay unassigned for a while (awaits a skeleton point,
	// or the limit of hdr packs in flight is taken by the packs below), whereas the headers are downloaded below it.
	const Task* pSkeletonBranch = nullptr;
	bool bHdrsInProgress = false;

	// The lowest requested block. In fast sync the blocks are requested in windows (see NodeProcessor::BodyWindows), all the blocks
	// below the lowest one are received, the higher windows can still have gaps below them.
	uint64_t numBlocksLow = 0;

	for (TaskSet::iterator it = m_setTasks.begin(); m_setTasks.end() != it; ++it)
	{
		const Task& t = *it;
		if (t.m_bSkeleton)
		{
			// not driven by congestions (m_bNeeded is irrelevant), always owned
			bHdrsInProgress = true;
			continue;
		}

		if (!t.m_bNeeded)
			continue;
		if (m_Processor.m_Cursor.m_Full.m_Number.v >= t.m_sidTrg.m_Number.v)
			continue;

		bool bBlock = t.m_Key.second;
		if (bBlock)
		{
			assert(t.m_Key.first.m_Number.v);
			beam::setmax(hTotal, t.m_sidTrg.m_Number.v);
			beam::setmax(hDoneHdrs, t.m_sidTrg.m_Number.v);

			if (m_Processor.IsFastSync() && (t.m_Key.first.m_Number.v <= m_Processor.m_SyncData.m_Target.m_Number.v))
				beam::setmax(hDoneHdrs, m_Processor.m_SyncData.m_Target.m_Number.v); // a window may end below the target, but the headers up to it are known

			if (!numBlocksLow || (numBlocksLow > t.m_Key.first.m_Number.v))
				numBlocksLow = t.m_Key.first.m_Number.v;
		}
		else
		{
			if (!t.m_pOwner)
			{
				if (IsHdrSkeletonBranch(t))
					pSkeletonBranch = &t;
				continue; // don't account for unowned
			}

			bHdrsInProgress = true;
			AccountHdrs(t);
		}
	}

	// Account for the skeleton branch only while some peer actually works on the headers.
	// Otherwise it may be a fake tip that nobody can serve.
	if (pSkeletonBranch && bHdrsInProgress)
		AccountHdrs(*pSkeletonBranch);

	if (numBlocksLow)
		beam::setmax(hDoneBlocks, numBlocksLow - 1); // all the blocks below had been dloaded

	// account for treasury
	hTotal++;

	if (m_Processor.IsTreasuryHandled())
	{
		hDoneBlocks++;
		hDoneHdrs++;
	}

	// corrections
	beam::setmax(hDoneHdrs, hDoneBlocks);
	beam::setmax(hTotal, hDoneHdrs);

	// consider the timestamp of the tip, upon successful sync it should not be too far in the past
	if (!m_Processor.m_Cursor.m_Full.m_Number.v)
	{
		// hTotal++; - what for?
	}
	else
	{
		Timestamp ts0_s = m_Processor.m_Cursor.m_Full.m_TimeStamp;
		Timestamp ts1_s = getTimestamp();

		const Timestamp tolerance_s = 60 * 60 * 24 * 2; // 2 days tolerance. In case blocks not created for some reason (mining turned off on testnet or etc.) we still don't want to get stuck in sync mode
		ts0_s += tolerance_s;

		if (ts1_s > ts0_s)
		{
			ts1_s -= ts0_s;

			hTotal++;

			const uint32_t& trg_ms = Rules::get().DA.Target_ms;
			if (trg_ms)
				beam::setmax(hTotal, m_Processor.m_Cursor.m_Full.m_Number.v + ts1_s * 1000 / trg_ms);
		}

	}

	m_SyncStatus.m_Total = hTotal * (SyncStatus::s_WeightHdr + SyncStatus::s_WeightBlock);
	m_SyncStatus.m_Done = hDoneHdrs * SyncStatus::s_WeightHdr + hDoneBlocks * SyncStatus::s_WeightBlock;
}

void Node::DeleteUnassignedTask(Task& t)
{
	assert(!t.m_pOwner && !t.m_nCount);
	m_lstTasksUnassigned.erase(TaskList::s_iterator_to(t));
	m_setTasks.erase(TaskSet::s_iterator_to(t));
	delete &t;
}

uint32_t Node::WantedTx::get_Timeout_ms()
{
	return get_ParentObj().m_Cfg.m_Timeout.m_GetTx_ms;
}

void Node::WantedTx::OnExpired(const KeyType& key)
{
	proto::GetTransaction msg;
	msg.m_ID = key;

	for (PeerList::iterator it = get_ParentObj().m_lstPeers.begin(); get_ParentObj().m_lstPeers.end() != it; ++it)
	{
		Peer& peer = *it;
		if (peer.m_LoginFlags & proto::LoginFlags::SpreadingTransactions)
			peer.Send(msg);
	}
}

void Node::Wanted::Clear()
{
	while (!m_lst.empty())
		DeleteInternal(m_lst.back());
}

void Node::Wanted::DeleteInternal(Item& n)
{
	m_lst.erase(List::s_iterator_to(n));
	m_set.erase(Set::s_iterator_to(n));
	delete &n;
}

void Node::Wanted::Delete(Item& n)
{
	bool bFront = (&m_lst.front() == &n);
	DeleteInternal(n);

	if (bFront)
		SetTimer();
}

bool Node::Wanted::Delete(const KeyType& key)
{
	Item n;
	n.m_Key = key;

	Set::iterator it = m_set.find(n);
	if (m_set.end() == it)
		return false;

	Delete(*it);
	return true;
}

bool Node::Wanted::Add(const KeyType& key)
{
	Item n;
	n.m_Key = key;
	Set::iterator it = m_set.find(n);
	if (m_set.end() != it)
		return false; // already waiting for it

	bool bEmpty = m_lst.empty();

	Item* p = new Item;
	p->m_Key = key;
	p->m_Advertised_ms = GetTime_ms();

	m_set.insert(*p);
	m_lst.push_back(*p);

	if (bEmpty)
		SetTimer();

	return true;
}

void Node::Wanted::SetTimer()
{
	if (m_lst.empty())
	{
		if (m_pTimer)
			m_pTimer->cancel();
	}
	else
	{
		if (!m_pTimer)
			m_pTimer = io::Timer::create(io::Reactor::get_Current());

		uint32_t dt = GetTime_ms() - m_lst.front().m_Advertised_ms;
		const uint32_t timeout_ms = get_Timeout_ms();

		m_pTimer->start((timeout_ms > dt) ? (timeout_ms - dt) : 0, false, [this]() { OnTimer(); });
	}
}

void Node::Wanted::OnTimer()
{
	uint32_t t_ms = GetTime_ms();
	const uint32_t timeout_ms = get_Timeout_ms();

	while (!m_lst.empty())
	{
		Item& n = m_lst.front();
		if (t_ms - n.m_Advertised_ms < timeout_ms)
		{
			SetTimer();
			break;
		}

		OnExpired(n.m_Key); // should not invalidate our structure
		Delete(n); // will also reschedule the timer
	}
}

void Node::TryAssignTask(Task& t)
{
	// Prioritize w.r.t. rating!
	for (PeerMan::LiveSet::iterator it = m_PeerMan.m_LiveSet.begin(); m_PeerMan.m_LiveSet.end() != it; ++it)
	{
		Peer& p = *it->m_p;
		if (TryAssignTask(t, p))
			return;
	}
}

bool Node::IsSyncTargetReachedBy(Peer& p)
{
	Block::SystemState::ID id;
	id.m_Number = p.m_Tip.m_Full.m_Number;
	id.m_Hash = p.m_Tip.m_hh.m_Hash;

	Peer::SyncTargetReached& x = p.m_SyncTargetReached; // alias
	const NodeDB::StateID& sidTrg = m_Processor.m_SyncData.m_Target; // alias

	if ((x.m_idTip == id) && (x.m_sidTrg.m_Row == sidTrg.m_Row) && (x.m_sidTrg.m_Number.v == sidTrg.m_Number.v))
		return true; // checked already

	if (!m_Processor.IsSyncTargetReachedBy(id))
		return false; // not cached: the missing headers may arrive

	x.m_idTip = id;
	x.m_sidTrg = sidTrg;
	return true;
}

bool Node::TryAssignTask(Task& t, Peer& p)
{
	BEAM_LOG_VERBOSE() << "Peer " << p.m_RemoteAddr << " trying to assign task " << t.m_Key.first;

	if (!p.ShouldAssignTasks())
	{
		BEAM_LOG_VERBOSE() << "not auth";
		return false;
	}

	if (p.m_Tip.m_Full.m_Number.v < t.m_Key.first.m_Number.v)
	{
		BEAM_LOG_VERBOSE() << "behind";
		return false;
	}

	if (p.m_Tip.m_Full.m_Number.v == t.m_Key.first.m_Number.v)
	{
		if (t.m_Key.first.m_Number.v)
		{
			if (p.m_Tip.m_hh.m_Hash != t.m_Key.first.m_Hash)
			{
				BEAM_LOG_VERBOSE() << "diverged";
				return false;
			}
		}
		else
		{
			// treasury
			if (!(Peer::Flags::HasTreasury & p.m_Flags))
				return false;
		}
	}

	std::pair<Task::Key, uint64_t> kRej;
	kRej.first = t.m_Key;
	if (t.m_Key.second)
		kRej.second = t.m_sidTrg.m_Row;
	else
		kRej.second = 0;

	if (p.m_setRejected.end() != p.m_setRejected.find(kRej))
	{
		BEAM_LOG_VERBOSE() << "task already rejected";
		return false;
	}

	// check if the peer currently transfers a block
	uint32_t nBlocks = 0, nHdrPacks = 0;
	for (TaskList::iterator it = p.m_lstTasks.begin(); p.m_lstTasks.end() != it; ++it)
	{
		if (it->m_Key.second)
			nBlocks++;
		else
			if (it->m_nCount)
				nHdrPacks++;
	}

	// assign
	if (t.m_Key.second)
	{
		// The hedge (see HedgeLowBody) may exceed the limits by one: they're usually taken by the packs above it
		if (!t.m_bHedge && (m_nTasksPackBody >= m_Cfg.m_MaxConcurrentBodyPacks))
		{
			BEAM_LOG_VERBOSE() << "too many body packs requested";
			return false;
		}

		if (m_Cfg.m_MaxBodyPacksPerPeer && (nBlocks >= m_Cfg.m_MaxBodyPacksPerPeer + (t.m_bHedge ? 1 : 0)))
		{
			BEAM_LOG_VERBOSE() << "too many body packs requested from peer";
			return false;
		}

		const NodeProcessor::SyncData& sd = m_Processor.m_SyncData; // alias
		bool bDiluted = (t.m_Key.first.m_Number.v <= sd.m_Target.m_Number.v);

		if (bDiluted && m_Processor.IsFastSync() && !IsSyncTargetReachedBy(p))
		{
			BEAM_LOG_VERBOSE() << "sync target is not in the peer's chain";
			return false;
		}

		// The top of the requested range. The peer anchors it in its active chain, and sends the blocks from the bottom up, but not above it.
		// Diluted blocks are requested up to the target, or up to the end of the window (see NodeProcessor::BodyWindows)
		NodeDB::StateID sidTop = t.m_sidTrg;
		if (bDiluted && (sidTop.m_Number.v > sd.m_Target.m_Number.v))
			sidTop = sd.m_Target;

		if (p.m_Tip.m_Full.m_Number.v < sidTop.m_Number.v)
		{
			BEAM_LOG_VERBOSE() << "behind the requested range";
			return false; // it's not in the peer's active chain
		}

		proto::GetBodyPack msg;
		msg.m_Top.m_Number = sidTop.m_Number;
		msg.m_CountExtra.v = sidTop.m_Number.v - t.m_Key.first.m_Number.v;

		if (!bDiluted || m_Processor.IsFastSync())
			m_Processor.get_DB().get_StateHash(sidTop.m_Row, msg.m_Top.m_Hash);
		else
			msg.m_Top.m_Hash = Zero; // treasury

		if (bDiluted)
		{
			// fast-sync mode, diluted blocks request.
			msg.m_Block0 = sd.m_n0;
			msg.m_HorizonLo1 = sd.m_TxoLo;
			msg.m_HorizonHi1 = sd.m_Target.m_Number;
		}

		p.Send(msg);

		t.m_nCount = 1; // body packs are counted, not blocks
		m_nTasksPackBody++;

		t.m_n0 = m_Processor.m_SyncData.m_n0;
		t.m_nTxoLo = m_Processor.m_SyncData.m_TxoLo;
	}
	else
	{
		const uint32_t nMaxHdrRequests = proto::g_HdrPackMaxSize * std::max(m_Cfg.m_MaxConcurrentHdrPacks, 1u);
		if (m_nTasksPackHdr >= nMaxHdrRequests)
		{
			BEAM_LOG_VERBOSE() << "too many hdrs requested";
			return false; // too many hdrs requested
		}

		if (nBlocks)
		{
			BEAM_LOG_VERBOSE() << "peer busy";
			return false; // don't requests headers from the peer that transfers a block
		}

		if (m_Cfg.m_MaxHdrPacksPerPeer && (nHdrPacks >= m_Cfg.m_MaxHdrPacksPerPeer))
		{
			BEAM_LOG_VERBOSE() << "too many hdrs requested from peer";
			return false;
		}

		if (m_HdrSkeleton.m_numNext.v)
		{
			// If the top of this pack is a skeleton point (awaited, or to be requested next) - wait for it. Once it arrives, this request
			// is replaced by the pack below it. Otherwise both packs would be downloaded, overlapping almost completely, and the following
			// regular download would be shifted w.r.t. the skeleton, duplicating its packs.
			bool bWait =
				IsHdrSkeletonPointExpected(t.m_Key.first.m_Number) ||
				IsHdrSkeletonPointPending(t.m_Key.first.m_Number);

			if (bWait)
			{
				BEAM_LOG_VERBOSE() << "awaiting hdr skeleton point";
				return false;
			}
		}

		uint32_t nPackSize = std::min(proto::g_HdrPackMaxSize, nMaxHdrRequests - m_nTasksPackHdr);

		// try to avoid big overlaps when asking for headers, but also try to ask for as many headers as possible
		// The best guess seems to be based on the diff between current height and target header (to either side)
		// For big sync tasks (including deep reorgs) big packs are ok. 
		Block::Number n0 = m_Processor.m_Cursor.m_Full.m_Number;
		uint64_t dn = (t.m_Key.first.m_Number.v > n0.v) ? (t.m_Key.first.m_Number.v - n0.v) : (n0.v - t.m_Key.first.m_Number.v);

		if (nPackSize > dn)
			nPackSize = dn ? (uint32_t) dn : 1;

		proto::GetHdrPack msg;
		msg.m_Top = t.m_Key.first;
		msg.m_Count = nPackSize;
		p.Send(msg);

		t.m_nCount = nPackSize;
		m_nTasksPackHdr += nPackSize;
	}

	m_lstTasksUnassigned.erase(TaskList::s_iterator_to(t));
	AssignTask(t, p);

	return true;
}

void Node::AssignTask(Task& t, Peer& p)
{
	bool bEmpty = p.m_lstTasks.empty();

	assert(!t.m_pOwner);
	t.m_pOwner = &p;
	t.m_sidTrgRequested = t.m_sidTrg;

	p.m_lstTasks.push_back(t);

	PeerManager::TimePoint tp;
	m_PeerMan.m_LiveSet.erase(PeerMan::LiveSet::s_iterator_to(Cast::Up<PeerMan::PeerInfoPlus>(p.m_pInfo)->m_Live));
	m_PeerMan.ResetRatingBoost(*p.m_pInfo);
	m_PeerMan.m_LiveSet.insert(Cast::Up<PeerMan::PeerInfoPlus>(p.m_pInfo)->m_Live);

	t.m_TimeAssigned_ms = tp.get();
	t.m_TimeRequested_ms = t.m_TimeAssigned_ms;

	if (bEmpty)
		p.SetTimerWrtFirstTask();

	if (t.m_bSkeleton)
	{
		BEAM_LOG_DEBUG() << "Peer " << p.m_RemoteAddr << " hdr skeleton point " << t.m_Key.first.m_Number.v;
	}
	else
	{
		BEAM_LOG_DEBUG() << "Peer " << p.m_RemoteAddr << " assigned task " << t.m_Key.first;
	}
}

void Node::Peer::SetTimerWrtFirstTask()
{
	if (m_lstTasks.empty())
	{
		assert(m_pTimerRequest);
		m_pTimerRequest->cancel();
	}
	else
	{
		// TODO - timer w.r.t. rating, i.e. should not exceed much the best avail peer rating

		Task& t = m_lstTasks.front();

		// The peer replies in order, so the reply to this task starts only now. Measure the bandwidth from this moment
		// (see ModifyRatingWrtData), otherwise the tasks queued behind others would make the peer look slow.
		PeerManager::TimePoint tp;
		t.m_TimeAssigned_ms = tp.get();

		uint32_t timeout_ms = t.m_Key.second ?
			m_This.m_Cfg.m_Timeout.m_GetBlock_ms :
			m_This.m_Cfg.m_Timeout.m_GetState_ms;

		if (!m_pTimerRequest)
			m_pTimerRequest = io::Timer::create(io::Reactor::get_Current());

		m_pTimerRequest->start(timeout_ms, false, [this]() { OnRequestTimeout(); });
	}
}

Node::Peer* Node::AllocPeer(const beam::io::Address& addr)
{
	Peer* pPeer = new Peer(*this);
	m_lstPeers.push_back(*pPeer);

	pPeer->m_UnsentHiMark = m_Cfg.m_BandwidthCtl.m_Drown;
	pPeer->m_pInfo = NULL;
	pPeer->m_Flags = 0;
	pPeer->m_Port = 0;
	ZeroObject(pPeer->m_Tip);
	pPeer->m_RemoteAddr = addr;
	pPeer->m_CursorBbs = std::numeric_limits<int64_t>::max();
	pPeer->m_pCursorTx = nullptr;

	pPeer->m_Tip.m_hh.m_Hash = Rules::get().Prehistoric;

	BEAM_LOG_DEBUG() << "+Peer " << addr;

	return pPeer;
}

void Node::Keys::InitSingleKey(const ECC::uintBig& seed)
{
	Key::IKdf::Ptr pKdf;
	ECC::HKdf::Create(pKdf, seed);
	SetSingleKey(pKdf);
}

void Node::Keys::SetSingleKey(const Key::IKdf::Ptr& pKdf)
{
	m_nMinerSubIndex = 0;
	m_pMiner = pKdf;
	m_pGeneric = pKdf;
	m_pOwner = pKdf;

	if (CoinID(Zero).get_ChildKdfIndex(m_nMinerSubIndex))
		m_pMiner = MasterKey::get_Child(*pKdf, m_nMinerSubIndex);
}

void Node::Initialize()
{
	if (m_Cfg.m_VerificationThreads < 0)
		// use all the cores, don't subtract 'mining threads'. Verification has higher priority
		m_Cfg.m_VerificationThreads = m_Processor.m_ExecutorMT.get_Threads();

	m_Processor.m_ExecutorMT.set_Threads(std::max<uint32_t>(m_Cfg.m_VerificationThreads, 1U));

	m_Processor.m_Horizon = m_Cfg.m_Horizon;
	m_Processor.m_BodyWindows.m_Size = m_Cfg.m_BodyWindow;
	m_Processor.m_BodyWindows.m_Ahead = m_Cfg.m_BodyWindowsAhead;
	m_Processor.m_BodyWindows.m_MaxCache = m_Cfg.m_BodyCacheMax;
	m_Processor.m_GoUpBudget_ms = m_Cfg.m_GoUpBudget_ms;
	m_Processor.Initialize(m_Cfg.m_sPathLocal.c_str(), m_Cfg.m_ProcessorParams, m_Cfg.m_Observer ? m_Cfg.m_Observer->GetLongActionHandler() : nullptr);

	if (m_Cfg.m_ProcessorParams.m_EraseSelfID)
	{
		m_Processor.get_DB().ParamSet(NodeDB::ParamID::MyID, nullptr, nullptr);
		BEAM_LOG_INFO() << "Node ID erased";

		io::Reactor::get_Current().stop();
		return;
	}

	const auto& r = Rules::get();
	if (Rules::Consensus::Pbft == r.m_Consensus)
		m_Validator.Initialize();

	InitKeys();
	InitIDs();

	BEAM_LOG_INFO() << "Node ID=" << m_MyPublicID;
	BEAM_LOG_INFO() << "Initial Tip: " << LogTip(m_Processor.m_Cursor);
	BEAM_LOG_INFO() << "Tx replication is OFF";

	if (!m_Cfg.m_Treasury.empty() && !m_Processor.IsTreasuryHandled()) {
		// stupid compiler insists on parentheses here!
		m_Processor.OnTreasury(Blob(m_Cfg.m_Treasury));
	}

	RefreshAccounts();

	ZeroObject(m_SyncStatus);
	RefreshCongestions();

	if (m_Cfg.m_Listen.port())
	{
		m_Server.Listen(m_Cfg.m_Listen);
		if (m_Cfg.m_BeaconPeriod_ms)
			m_Beacon.Start();
	}

	m_PeerMan.Initialize();
	m_Miner.Initialize();
	m_Validator.OnNewState();
	m_Processor.get_DB().get_BbsTotals(m_Bbs.m_Totals);
	m_Bbs.Cleanup();
	m_Bbs.m_HighestPosted_s = m_Processor.get_DB().get_BbsMaxTime();

	if (m_Cfg.m_TestMode.m_FakePowSolveTime_ms && (Rules::Consensus::FakePoW == r.m_Consensus))
		m_PostStartSynced = true;
}

uint32_t Node::get_AcessiblePeerCount() const
{
	return static_cast<uint32_t>(m_PeerMan.get_Addrs().size());
}

const PeerManager::AddrSet& Node::get_AcessiblePeerAddrs() const
{
	return m_PeerMan.get_Addrs();
}

void Node::InitKeys()
{
	if (m_Keys.m_pOwner)
	{
		// Ensure the miner key makes sense.
		if (m_Keys.m_pMiner && m_Keys.m_nMinerSubIndex && m_Keys.m_pOwner->IsSame(*m_Keys.m_pMiner))
		{
			// BB2.1
			CorruptionException exc;
			exc.m_sErr = "Incompatible miner key. Please regenerate with the latest version";
			throw exc;

		}
	}
	else
		m_Keys.m_pMiner = nullptr; // can't mine without owner view key, because it's used for Tagging

	if (!m_Keys.m_pGeneric)
	{
		if (m_Keys.m_pMiner)
			m_Keys.m_pGeneric = m_Keys.m_pMiner;
		else
		{
			// use arbitrary, inited from system random. Needed for misc things, such as secure channel, decoys and etc.
			ECC::NoLeak<ECC::uintBig> seed;
			ECC::GenRandom(seed.V);
			ECC::HKdf::Create(m_Keys.m_pGeneric, seed.V);
		}
	}
}

void Node::InitIDs()
{
	ECC::GenRandom(m_NonceLast.V);

	ECC::NoLeak<ECC::Scalar> s;
	Blob blob(s.V.m_Value);
	bool bNewID =
		m_Cfg.m_ProcessorParams.m_ResetSelfID ||
		!m_Processor.get_DB().ParamGet(NodeDB::ParamID::MyID, NULL, &blob);

	if (bNewID)
	{
		NextNonce(m_MyPrivateID);
		s.V = m_MyPrivateID;
		m_Processor.get_DB().ParamSet(NodeDB::ParamID::MyID, NULL, &blob);
	}
	else
		m_MyPrivateID = s.V;

	m_MyPublicID.FromSk(m_MyPrivateID);
}

struct Node::AccountRefreshCtx
{
	AccountRefreshCtx(Node& n) :m_This(n) {}

	Node& m_This;
	std::map<Merkle::Hash, size_t> m_Map;

	BlobEncoder m_Coder;

	void AddAccount(const Key::IPKdf::Ptr&, Key::IPKdf*);
	void InsertAccount(const NodeProcessor::Account&);

	void get_AccountOwnerID(Merkle::Hash&, const Key::IPKdf::Ptr& pOwner, Key::IPKdf* pMiner);
};

void Node::AccountRefreshCtx::get_AccountOwnerID(Merkle::Hash& hv, const Key::IPKdf::Ptr& pOwner, Key::IPKdf* pMiner)
{
	ECC::Hash::Processor hp;
	hp << uint32_t(4); // change this whenever we change the format of the saved events

	ECC::Hash::Value hv1(Zero);

	ECC::Scalar::Native sk;
	pOwner->DerivePKey(sk, hv1);
	hp << sk;

	if (pMiner)
	{
		// rescan also when miner subkey changes, to recover possible decoys that were rejected earlier
		pMiner->DerivePKey(sk, hv1);
		hp
			<< m_This.m_Keys.m_nMinerSubIndex
			<< sk;
	}

	hp >> hv;
}


void Node::AccountRefreshCtx::AddAccount(const Key::IPKdf::Ptr& pOwner, Key::IPKdf* pMiner)
{
	auto& accs = m_This.m_Processor.m_vAccounts; // alias

	// check if this account already exists
	if (!m_Map.empty())
	{
		Merkle::Hash hv;
		get_AccountOwnerID(hv, pOwner, pMiner);

		auto it = m_Map.find(hv);
		if (m_Map.end() != it)
		{
			// found!
			auto& acc = accs[it->second];
			m_Map.erase(it);

			acc.m_pOwner = pOwner;

			// save the owner key in DB
			m_This.m_Processor.get_DB().DeleteAccountOnly(acc.m_iAccount);
			InsertAccount(acc);

			return;
		}
	}

	NodeDB::AccountIndex iAccount = 1;

	for (auto& d : accs)
	{
		if (d.m_pOwner && d.m_pOwner->IsSame(*pOwner))
		{
			d.m_pOwner = pOwner; // prefer this instance
			return;
		}

		if (iAccount == d.m_iAccount)
			iAccount = d.m_iAccount + 1;
	}

	// new account
	auto& acc = accs.emplace_back();

	acc.m_iAccount = iAccount;
	acc.m_hTxoHi = m_This.m_Processor.Num2Height(m_This.m_Processor.m_Extra.m_TxoHi);
	acc.m_Serif = m_This.NextNonce();

	acc.m_pOwner = pOwner;
	InsertAccount(acc);
}

void Node::AccountRefreshCtx::InsertAccount(const NodeProcessor::Account& acc)
{
	assert(acc.m_pOwner);
	ByteBuffer buf;
	buf.resize(acc.m_pOwner->ExportP(nullptr));

	if (!buf.empty())
	{
		acc.m_pOwner->ExportP(&buf.front());
		m_Coder.Encrypt(buf);
	}


	NodeDB::WalkerAccount::DataPlus d;
	Cast::Down<NodeDB::WalkerAccount::Data>(d) = acc;
	d.m_Owner = buf;

	m_This.m_Processor.get_DB().InsertAccount(d);
}

void Node::RefreshAccounts()
{
	auto& accs = m_Processor.m_vAccounts;
	assert(accs.empty());

	AccountRefreshCtx arc(*this);
	Cast::Reinterpret<ECC::Scalar>(arc.m_Coder.m_hvSecret.V) = m_MyPrivateID;

	// Current accounts
	{
		NodeDB::WalkerAccount wlk;
		for (m_Processor.get_DB().EnumAccounts(wlk); wlk.MoveNext(); )
		{
			auto& acc = accs.emplace_back();
			Cast::Down<NodeDB::WalkerAccount::Data>(acc) = wlk.m_Data;

			static_assert(sizeof(Merkle::Hash) < sizeof(ECC::HKdfPub::Packed), "");

			auto& key = wlk.m_Data.m_Owner; // alias

			if (sizeof(Merkle::Hash) == key.n)
			{
				// older-format account. Would be upgraded on 1st run
				const auto& hv = *reinterpret_cast<const Merkle::Hash*>(key.p);
				arc.m_Map[hv] = accs.size() - 1;
			}
			else
			{
				if (arc.m_Coder.Decrypt(key) && (sizeof(ECC::HKdfPub::Packed) == key.n))
				{
					const auto& p = *reinterpret_cast<const ECC::HKdfPub::Packed*>(key.p);

					auto pPKdf = std::make_shared<ECC::HKdfPub>();
					if (pPKdf->Import(p))
						acc.m_pOwner = std::move(pPKdf); // loaded ok

					ECC::SecureErase(Cast::NotConst(key.p), key.n);
				}
			}
		}
	}

	uint32_t nAdd = static_cast<uint32_t>(accs.size());

	if (m_Keys.m_pOwner)
		arc.AddAccount(m_Keys.m_pOwner, m_Keys.m_pMiner.get());

	for (const auto& pExtra : m_Keys.m_Accounts.m_vAdd)
		arc.AddAccount(pExtra, nullptr);

	nAdd = static_cast<uint32_t>(accs.size()) - nAdd; // how many new added

	uint32_t nDel = 0;
	for (auto& acc : accs)
	{
		// filter-out those to be deleted.
		// Never delete the current owner account
		if (acc.m_pOwner && acc.m_pOwner.get() != m_Keys.m_pOwner.get())
		{
			bool bDelete = m_Keys.m_Accounts.m_Del.m_All;
			if (!bDelete && !m_Keys.m_Accounts.m_Del.m_Eps.empty())
			{
				auto it = m_Keys.m_Accounts.m_Del.m_Eps.find(acc.get_Endpoint());
				if (m_Keys.m_Accounts.m_Del.m_Eps.end() != it)
				{
					m_Keys.m_Accounts.m_Del.m_Eps.erase(it);
					bDelete = true;
				}
			}

			if (bDelete)
				acc.m_pOwner.reset();
		}

		if (acc.m_pOwner)
			acc.InitFromOwner();
		else
			nDel++;
	}
	// delete unused accounts
	if (nDel)
	{
		assert(nDel <= accs.size());
		BEAM_LOG_INFO() << "Owned accounts removed: " << nDel;

		size_t iDst = 0;
		for (size_t iSrc = 0; iSrc < accs.size(); iSrc++)
		{
			auto& acc = accs[iSrc];
			if (acc.m_pOwner)
			{
				if (iDst != iSrc)
					accs[iDst] = std::move(acc);
				iDst++;
			}
			else
				m_Processor.get_DB().DeleteAccountWithEvents(acc.m_iAccount);
		}

		assert(iDst + nDel == accs.size());
		accs.resize(iDst);
	}

	if (nAdd)
	{
		assert(nAdd <= accs.size());
		BEAM_LOG_INFO() << "Owned accounts added: " << nAdd;

		try
		{
			m_Processor.RescanAccounts(nAdd);
		}
		catch (const std::runtime_error&)
		{
			m_Processor.RollbackDB();
			throw;
		}
	}

	std::ostringstream os;
	os << "Owned accounts :" << std::endl;

	for (const auto& acc : accs)
		os << '\t' << acc.get_Endpoint() << std::endl;

	BEAM_LOG_INFO() << os.str();
}

Node::~Node()
{
	BEAM_LOG_INFO() << "Node stopping...";

	m_Miner.HardAbortSafe();
	if (m_Cfg.m_pExternalPOW)
		m_Cfg.m_pExternalPOW->stop();

	for (size_t i = 0; i < m_Miner.m_vThreads.size(); i++)
	{
		PerThread& pt = m_Miner.m_vThreads[i];
		if (pt.m_pReactor)
			pt.m_pReactor->stop();

		if (pt.m_Thread.joinable())
			pt.m_Thread.join();
	}
	m_Miner.m_vThreads.clear();

	for (PeerList::iterator it = m_lstPeers.begin(); m_lstPeers.end() != it; ++it)
		it->m_LoginFlags = 0; // prevent re-assigning of tasks in the next loop

	m_HdrSkeleton.m_numNext.v = 0; // don't re-request skeleton points from the remaining peers

	while (!m_lstPeers.empty())
		m_lstPeers.front().DeleteSelf(false, proto::NodeConnection::ByeReason::Stopping);

	while (!m_lstTasksUnassigned.empty())
		DeleteUnassignedTask(m_lstTasksUnassigned.front());

	assert(m_setTasks.empty());

	m_Processor.Stop();

	if (!std::uncaught_exceptions() && m_Processor.get_DB().IsOpen())
		m_PeerMan.OnFlush();

	BEAM_LOG_INFO() << "Node stopped";
}

void Node::Server::OnAccepted(io::TcpStream::Ptr&& newStream, int errorCode)
{
	if (newStream)
	{
		BEAM_LOG_DEBUG() << "New peer connected: " << newStream->address();
		Peer* p = get_ParentObj().AllocPeer(newStream->peer_address());
		p->Accept(std::move(newStream));
		p->m_Flags |= Peer::Flags::Accepted;
		p->SecureConnect();
	}
}

} // namespace beam
