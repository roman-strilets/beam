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

#include "processor_bic.h"
#include "processor_multiblock.h"

namespace beam {

NodeProcessor::CongestionCache::TipCongestion* NodeProcessor::CongestionCache::Find(const NodeDB::StateID& sid)
{
	TipCongestion* pRet = nullptr;

	for (TipList::iterator it = m_lstTips.begin(); m_lstTips.end() != it; ++it)
	{
		TipCongestion& x = *it;
		if (!x.IsContained(sid))
			continue;

		// in case of several matches prefer the one with lower height
		if (pRet && (pRet->m_Number.v <= x.m_Number.v))
			continue;

		pRet = &x;
	}

	return pRet;
}

bool NodeProcessor::CongestionCache::TipCongestion::IsContained(const NodeDB::StateID& sid)
{
	if (sid.m_Number.v > m_Number.v)
		return false;

	uint64_t dn = m_Number.v - sid.m_Number.v;
	if (dn >= m_Rows.size())
		return false;

	return (m_Rows.at(dn) == sid.m_Row);
}

NodeProcessor::CongestionCache::TipCongestion* NodeProcessor::EnumCongestionsInternal()
{
	assert(IsTreasuryHandled());

	CongestionCache cc;
	cc.m_lstTips.swap(m_CongestionCache.m_lstTips);

	CongestionCache::TipCongestion* pMaxTarget = nullptr;
	bool bMaxTargetNeedsHeaders = false;
	Difficulty::Raw cwMaxTarget;

	// Find all potentially missing data
	NodeDB::WalkerState ws;
	for (m_DB.EnumTips(ws); ws.MoveNext(); )
	{
		NodeDB::StateID& sid = ws.m_Sid; // alias
		if (NodeDB::StateFlags::Reachable & m_DB.GetStateFlags(sid.m_Row))
			continue;

		Difficulty::Raw wrk;
		m_DB.get_ChainWork(sid.m_Row, wrk);

		if (wrk < m_Cursor.m_Full.m_ChainWork)
			continue; // not interested in tips behind the current cursor

		CongestionCache::TipCongestion* pEntry = nullptr;
		bool bCheckCache = true;
		bool bNeedHdrs = false;

		while (true)
		{
			if (bCheckCache)
			{
				CongestionCache::TipCongestion* p = cc.Find(sid);
				if (p)
				{
					assert(p->m_Number.v >= sid.m_Number.v);
					while (p->m_Number.v > sid.m_Number.v)
					{
						p->m_Number.v--;
						p->m_Rows.pop_front();
					}

					if (pEntry)
					{
						for (size_t i = pEntry->m_Rows.size(); i--; p->m_Number.v++)
							p->m_Rows.push_front(pEntry->m_Rows.at(i));

						m_CongestionCache.m_lstTips.Delete(*pEntry);
					}

					cc.m_lstTips.erase(CongestionCache::TipList::s_iterator_to(*p));
					m_CongestionCache.m_lstTips.push_back(*p);

					while (NodeDB::StateFlags::Reachable & m_DB.GetStateFlags(p->m_Rows.at(p->m_Rows.size() - 1)))
						p->m_Rows.pop_back(); // already retrieved

					assert(p->m_Rows.size());

					sid.m_Row = p->m_Rows.at(p->m_Rows.size() - 1);
					sid.m_Number.v = p->m_Number.v - (p->m_Rows.size() - 1);

					pEntry = p;
					bCheckCache = false;
				}
			}

			if (!pEntry)
			{
				pEntry = m_CongestionCache.m_lstTips.Create_back();
				pEntry->m_Number.v = sid.m_Number.v;
			}

			if (bCheckCache)
			{
				CongestionCache::TipCongestion* p = m_CongestionCache.Find(sid);
				if (p)
				{
					assert(p != pEntry);

					// copy the rest
					for (size_t i = p->m_Number.v - sid.m_Number.v; i < p->m_Rows.size(); i++)
						pEntry->m_Rows.push_back(p->m_Rows.at(i));

					sid.m_Row = p->m_Rows.at(p->m_Rows.size() - 1);
					sid.m_Number.v = p->m_Number.v - (p->m_Rows.size() - 1);

					bCheckCache = false;
				}
			}

			if (pEntry->m_Number.v >= sid.m_Number.v + pEntry->m_Rows.size())
				pEntry->m_Rows.push_back(sid.m_Row);

			if (1u == sid.m_Number.v)
				break;

			if (!m_DB.get_Prev(sid))
			{
				bNeedHdrs = true;
				break;
			}

			if (NodeDB::StateFlags::Reachable & m_DB.GetStateFlags(sid.m_Row))
				break;
		}

		assert(pEntry && pEntry->m_Rows.size());
		pEntry->m_bNeedHdrs = bNeedHdrs;

		Difficulty::Raw cw;
		m_DB.get_ChainWork(pEntry->m_Rows.at(0), cw);

		if (bNeedHdrs)
		{
			Difficulty::Raw cw2;
			m_DB.get_ChainWork(pEntry->m_Rows.at(pEntry->m_Rows.size() - 1), cw2);
			cw2.Negate();
			cw2 += cw; // difficulty of the very 1st block is missing, nevermind

			// ensure cw is no bigger than twice cw2 (proven Chainwork)
			cw2 += cw2;
			if (cw > cw2)
				cw = cw2;
		}

		// check if this candidate is better. Select the one with bigger ChainWork
		// If the candidate has all the headers down to the genesis - use the proven ChainWork
		// If headers are missing - use the *estimated* ChainWork, which is
		//		claimed ChainWork (in the last header)
		//		no bigger than twice proven ChainWork 

		pEntry->m_ChainWorkEst = cw;

		if (!pMaxTarget || (cwMaxTarget < cw))
		{
			pMaxTarget = pEntry;
			cwMaxTarget = cw;
			bMaxTargetNeedsHeaders = bNeedHdrs;
		}
	}

	return bMaxTargetNeedsHeaders ? nullptr : pMaxTarget;
}

void NodeProcessor::EnumCongestions()
{
	if (!IsTreasuryHandled())
	{
		Block::SystemState::ID id;
		ZeroObject(id);
		NodeDB::StateID sidTrg;
		sidTrg.SetNull();

		RequestData(id, true, sidTrg);
		return;
	}

	CongestionCache::TipCongestion* pMaxTarget = EnumCongestionsInternal();

	// Check the fast-sync status
	if (pMaxTarget)
	{
		bool bFirstTime =
			!IsFastSync() &&
			IsBigger3(pMaxTarget->m_Number.v, m_Cursor.m_Full.m_Number.v, m_Horizon.m_Sync.Hi, m_Horizon.m_Sync.Hi / 2);

		if (bFirstTime)
		{
			// first time target acquisition
			m_SyncData.m_n0.v = pMaxTarget->m_Number.v - pMaxTarget->m_Rows.size();

			if (pMaxTarget->m_Number.v > m_Horizon.m_Sync.Lo)
				m_SyncData.m_TxoLo.v = pMaxTarget->m_Number.v - m_Horizon.m_Sync.Lo;

			beam::setmax(m_SyncData.m_TxoLo.v, m_Extra.m_TxoLo.v);
		}

		// check if the target should be moved fwd
		bool bTrgChange =
			(IsFastSync() || bFirstTime) &&
			IsBigger2(pMaxTarget->m_Number.v, m_SyncData.m_Target.m_Number.v, m_Horizon.m_Sync.Hi);

		if (bTrgChange)
		{
			Block::Number numTargetPrev(bFirstTime ? (pMaxTarget->m_Number.v - pMaxTarget->m_Rows.size()) : m_SyncData.m_Target.m_Number.v);

			m_SyncData.m_Target.m_Number.v = pMaxTarget->m_Number.v - m_Horizon.m_Sync.Hi;
			m_SyncData.m_Target.m_Row = pMaxTarget->m_Rows.at(pMaxTarget->m_Number.v - m_SyncData.m_Target.m_Number.v);

			if (m_SyncData.m_TxoLo.v)
			{
				// ensure no old blocks, which could be generated with incorrect TxLo
				//
				// Deleting all the blocks in the range is a time-consuming operation, whereas it's VERY unlikely there's any block in there
				// So we'll limit the height range by the maximum "sane" value (which is also very unlikely to contain any block).
				//
				// In a worst-case scenario (extremely unlikely) the sync will fail, then all the blocks will be deleted, and sync restarts
				Block::Number numMaxSane(m_Cursor.m_Full.m_Number.v + Rules::get().MaxRollback);
				if (numTargetPrev.v < numMaxSane.v)
				{
					if (m_SyncData.m_Target.m_Number.v <= numMaxSane.v)
						DeleteBlocksInRange(m_SyncData.m_Target, numTargetPrev);
					else
					{
						NodeDB::StateID sid;
						sid.m_Number = numMaxSane;
						sid.m_Row = pMaxTarget->m_Rows.at(pMaxTarget->m_Number.v - numMaxSane.v);

						DeleteBlocksInRange(sid, numTargetPrev);
					}
				}
			}

			SaveSyncData();
		}

		if (bFirstTime)
		{
			LogSyncData();

			// Drop the kernel index only if most of the kernels are yet to come, otherwise the rebuild costs more
			if (m_SyncData.m_n0.v * 2 < m_SyncData.m_Target.m_Number.v)
				SuspendKernelIndex();
		}
	}

	// request missing data
	for (CongestionCache::TipList::iterator it = m_CongestionCache.m_lstTips.begin(); m_CongestionCache.m_lstTips.end() != it; ++it)
	{
		CongestionCache::TipCongestion& x = *it;

		if (!(x.m_bNeedHdrs || (&x == pMaxTarget)))
			continue; // current policy - ask only for blocks with the largest proven (wrt headers) chainwork

		Block::SystemState::ID id;

		NodeDB::StateID sidTrg;
		sidTrg.m_Number = x.m_Number;
		sidTrg.m_Row = x.m_Rows.at(0);

		if (!x.m_bNeedHdrs)
		{
			if (IsFastSync() && !x.IsContained(m_SyncData.m_Target))
				continue; // ignore irrelevant branches

			NodeDB::StateID sid;
			sid.m_Number.v = x.m_Number.v - (x.m_Rows.size() - 1);
			sid.m_Row = x.m_Rows.at(x.m_Rows.size() - 1);

			if (IsFastSync() && m_BodyWindows.m_Size && (sid.m_Number.v <= m_SyncData.m_Target.m_Number.v))
			{
				RequestBodyWindows(x, sid.m_Number);
				continue;
			}

			m_DB.get_StateHash(sid.m_Row, id.m_Hash);
			id.m_Number = sid.m_Number;

			RequestDataInternal(id, sid.m_Row, true, sidTrg);
		}
		else
		{
			uint64_t rowid = x.m_Rows.at(x.m_Rows.size() - 1);

			Block::SystemState::Full s;
			m_DB.get_State(rowid, s);

			id.m_Number.v = s.m_Number.v - 1;
			id.m_Hash = s.m_Prev;

			RequestDataInternal(id, rowid, false, sidTrg);
		}
	}
}

void NodeProcessor::RequestBodyWindows(CongestionCache::TipCongestion& x, Block::Number numLow)
{
	// numLow is the first missing block of the branch, its parent is reachable. The branch contains the target.
	const uint64_t nSize = m_BodyWindows.m_Size;
	const uint64_t numTrg = m_SyncData.m_Target.m_Number.v;
	assert(nSize && (numLow.v <= numTrg) && (x.m_Number.v >= numTrg));
	assert(x.m_Number.v - numLow.v + 1 == x.m_Rows.size());

	auto RowAt = [&x](uint64_t num) {
		return x.m_Rows.at(x.m_Number.v - num);
	};

	auto IsReceived = [this, &RowAt](uint64_t num) {
		NodeDB::StateID sid;
		sid.m_Number.v = num;
		sid.m_Row = RowAt(num);
		return IsBlockReceived(sid);
	};

	uint64_t num0 = numLow.v;
	for (uint32_t i = 0; ; i++)
	{
		uint64_t num1 = ((num0 - 1) / nSize + 1) * nSize; // the window end
		beam::setmin(num1, numTrg);

		// Each window is requested from its first missing block, so the received blocks of a window are its prefix.
		// This is true for the lowest window too: its blocks below numLow are applied, and the cached ones aren't reachable.
		bool bRequest = true;
		if (IsReceived(num1))
			bRequest = false; // received, but not reachable yet
		else
		{
			if (IsReceived(num0))
			{
				// the first missing one is in (num0, num1]
				uint64_t a = num0, b = num1;
				while (b - a > 1)
				{
					uint64_t c = a + (b - a) / 2;
					if (IsReceived(c))
						a = c;
					else
						b = c;
				}
				num0 = b;
			}
		}

		if (bRequest)
		{
			Block::SystemState::ID id;
			id.m_Number.v = num0;
			m_DB.get_StateHash(RowAt(num0), id.m_Hash);

			NodeDB::StateID sidTrg;
			sidTrg.m_Number.v = num1;
			sidTrg.m_Row = RowAt(num1);

			RequestDataInternal(id, RowAt(num0), true, sidTrg);
		}

		if ((num1 >= numTrg) || (i >= m_BodyWindows.m_Ahead))
			break;

		num0 = num1 + 1;
	}
}

bool NodeProcessor::get_CachedRows(const NodeDB::StateID& sid, Height nCountExtra, std::deque<uint64_t>::const_iterator& out)
{
	EnumCongestionsInternal();

	CongestionCache::TipCongestion* pVal = m_CongestionCache.Find(sid);
	if (pVal)
	{
		assert(pVal->m_Number.v >= sid.m_Number.v);
		uint64_t dn = (pVal->m_Number.v - sid.m_Number.v);

		if (pVal->m_Rows.size() > nCountExtra + dn)
		{
			out = pVal->m_Rows.cbegin() + dn;
			return true;
		}
	}
	return false;
}

bool NodeProcessor::IsSyncTargetReachedBy(const Block::SystemState::ID& id)
{
	assert(IsFastSync());
	const NodeDB::StateID& sidTrg = m_SyncData.m_Target; // alias

	if (id.m_Number.v < sidTrg.m_Number.v)
		return false;

	NodeDB::StateID sid;
	sid.m_Row = m_DB.StateFindSafe(id);
	if (!sid.m_Row)
		return false; // unknown header, can't tell
	sid.m_Number = id.m_Number;

	if (sid.m_Row == sidTrg.m_Row)
		return true;

	// The target isn't reachable yet (it's above the cursor), so its branch is in the congestion cache, down to the cursor.
	// Each entry contains the rows of its branch down to the 1st non-reachable state.
	for (auto& x : m_CongestionCache.m_lstTips)
		if (x.IsContained(sid) && x.IsContained(sidTrg))
			return true;

	return false;
}

bool NodeProcessor::get_HdrGapLow(NodeDB::StateID& sid, const StateSet& setSkip)
{
	// Select by the estimated ChainWork, same as the sync target (see EnumCongestionsInternal). Neither the claimed ChainWork
	// nor the number can be trusted for a branch with missing headers: a fake header with an arbitrary number is cheap.
	const CongestionCache::TipCongestion* pBest = nullptr;

	for (const auto& x : m_CongestionCache.m_lstTips)
	{
		if (x.m_ChainWorkEst == Zero)
			continue; // no proven work (single header)

		if (setSkip.end() != setSkip.find(std::make_pair(x.m_Number.v, x.m_Rows.at(0))))
			continue;

		if (!pBest || (pBest->m_ChainWorkEst < x.m_ChainWorkEst))
			pBest = &x;
	}

	if (!pBest || !pBest->m_bNeedHdrs)
		return false;

	assert(!pBest->m_Rows.empty());
	sid.m_Row = pBest->m_Rows.back();
	sid.m_Number.v = pBest->m_Number.v - (pBest->m_Rows.size() - 1);
	return true;
}

uint32_t NodeProcessor::get_MaxAutoRollback()
{
	return Rules::get().MaxRollback;
}

Block::Number NodeProcessor::get_LowestManualReturnNumber()
{
	return Block::Number(std::max(m_Extra.m_TxoHi.v, m_Extra.m_Fossil.v));
}

Block::Number NodeProcessor::get_LowestReturnNumber()
{
	Block::Number numRet = get_LowestManualReturnNumber();

	Block::Number n0 = IsFastSync() ? m_SyncData.m_n0 : m_Cursor.m_Full.m_Number;
	uint32_t nMaxRollback = get_MaxAutoRollback();

	if (n0.v > nMaxRollback)
	{
		n0.v -= nMaxRollback;
		beam::setmax(numRet.v, n0.v);
	}

	return numRet;
}

void NodeProcessor::RequestDataInternal(const Block::SystemState::ID& id, uint64_t row, bool bBlock, const NodeDB::StateID& sidTrg)
{
	if (id.m_Number.v < get_LowestReturnNumber().v) {
		m_UnreachableLog.Log(id);
		return;
	}

	if (!m_ManualSelection.IsAllowed(id.m_Number, id.m_Hash)) {
		return;
	}

	RequestData(id, bBlock, sidTrg);
}

void NodeProcessor::UnreachableLog::Log(const Block::SystemState::ID& id)
{
	uint32_t nTime_ms = GetTimeNnz_ms();
	if (m_hvLast == id.m_Hash) {
		// suppress spam logging for 10 sec.
		if (m_Time_ms && (nTime_ms - m_Time_ms < 10000))
			return;
	}
	else {
		m_hvLast = id.m_Hash;
	}

	m_Time_ms = nTime_ms;
	BEAM_LOG_WARNING() << id << " State unreachable"; // probably will pollute the log, but it's a critical situation anyway
}

void NodeProcessor::TryGoUp()
{
	if (!IsTreasuryHandled())
		return;

	bool bDirty = false;
	uint64_t rowid = m_Cursor.m_Row;

	m_GoUpStart_ms = GetTime_ms();
	m_bGoUpYield = false;

	bool bAtEmptyBlock =
		rowid &&
		(Rules::Consensus::Pbft == Rules::get().m_Consensus) &&
		(Block::Pbft::HdrData::Flags::Empty & Cast::Reinterpret<Block::Pbft::HdrData>(m_Cursor.m_Full.m_PoW).m_Flags1);

	while (true)
	{
		NodeDB::StateID sidTrg;
		bool bFound = false;

		{
			NodeDB::WalkerState ws;
			m_DB.EnumFunctionalTips(ws);

			if (ws.MoveNext())
			{
				sidTrg = ws.m_Sid;

				Difficulty::Raw wrkTrg;
				m_DB.get_ChainWork(sidTrg.m_Row, wrkTrg);

				assert(wrkTrg >= m_Cursor.m_Full.m_ChainWork);
				bFound = (wrkTrg != m_Cursor.m_Full.m_ChainWork); // otherwise already at maximum (though maybe at different tip)
			}
			else
				assert(!m_Cursor.m_Row); // nowhere to go
		}

		if (bFound)
			TryGoTo(sidTrg);
		else
		{
			if (!TryGoUpCached())
				break;
		}

		bDirty = true;

		if (m_bGoUpYield)
			break;
	}

	if (bDirty)
	{
		PruneOld();
		if (m_Cursor.m_Row != rowid)
		{
			if (bAtEmptyBlock)
				m_DB.DeleteState(rowid, rowid); // cannibalized. No reason to keep empty forked blocks

			OnNewState();
		}
	}

	if (m_bGoUpYield)
		OnGoUpYielded();
}

void NodeProcessor::TryGoTo(NodeDB::StateID& sidTrg)
{
	// Calculate the path
	std::vector<uint64_t> vPath;
	while (true)
	{
		vPath.push_back(sidTrg.m_Row);

		if (!m_DB.get_Prev(sidTrg))
		{
			sidTrg.SetNull();
			break;
		}

		if (NodeDB::StateFlags::Active & m_DB.GetStateFlags(sidTrg.m_Row))
			break;
	}

	RollbackTo(sidTrg.m_Number);

	ApplyPath(vPath, false);
}

bool NodeProcessor::TryGoUpCached()
{
	auto& m = m_BodyCache.m_Map; // alias
	if (m.empty())
		return false;

	if (!IsFastSync())
	{
		m.clear(); // should not happen
		return false;
	}

	m.erase(m.begin(), m.upper_bound(m_Cursor.m_Full.m_Number.v)); // stale

	std::vector<uint64_t> vPath;
	for (auto it = m.begin(); (m.end() != it) && (it->first == m_Cursor.m_Full.m_Number.v + vPath.size() + 1); ++it)
		vPath.push_back(it->second.m_Row);

	if (vPath.empty())
		return false;

	std::reverse(vPath.begin(), vPath.end());

	NodeDB::StateID sid0 = m_Cursor.get_Sid();
	size_t nCached0 = m.size();

	ApplyPath(vPath, true);

	NodeDB::StateID sid1 = m_Cursor.get_Sid();
	return (sid0.m_Row != sid1.m_Row) || (sid0.m_Number.v != sid1.m_Number.v) || (nCached0 != m.size());
}

void NodeProcessor::ApplyPath(const std::vector<uint64_t>& vPath, bool bCached)
{
	MultiblockContext mbc(*this);
	bool bContextFail = false, bKeepBlocks = false;

	NodeDB::StateID sidFwd = m_Cursor.get_Sid();
	PeerID pidFwd = Zero; // for cached blocks, the entry is removed from the cache before the block is handled

	// cached blocks: functional followers of the cursor, its parent is reachable
	uint32_t nCountNextFPrev = (bCached && sidFwd.m_Row) ? m_DB.get_StateCountNextF(sidFwd.m_Row) : 0;

	size_t iPos = vPath.size();
	while (iPos)
	{
		if (bCached && m_GoUpBudget_ms && (iPos < vPath.size()) && (GetTime_ms() - m_GoUpStart_ms >= m_GoUpBudget_ms))
		{
			// the rest is applied by the next TryGoUp(). The cursor is at the last applied block, as if the path ended there
			m_bGoUpYield = true;
			break;
		}

		sidFwd.m_Number.v = m_Cursor.m_Full.m_Number.v + 1;
		sidFwd.m_Row = vPath[--iPos];

		Block::SystemState::Full s;
		NodeDB::StateForApply sfa;
		BodyCache::Entry ce;

		if (bCached)
		{
			auto it = m_BodyCache.m_Map.find(sidFwd.m_Number.v);
			bool bValid = (m_BodyCache.m_Map.end() != it) && (it->second.m_Row == sidFwd.m_Row);
			if (bValid)
			{
				// take it out of the cache: the cache can be cleared while the block is handled (fast-sync failure)
				ce = std::move(it->second);
				m_BodyCache.m_Map.erase(it);
				pidFwd = ce.m_Peer;

				m_DB.get_StateForApply(sidFwd.m_Row, s, sfa);

				// Not a follower of the cursor, or received in a different way: the cached body is stale, not an error
				bValid =
					(sfa.m_RowPrev == m_Cursor.m_Row) &&
					!(NodeDB::StateFlags::Functional & sfa.m_Flags);
			}

			if (!bValid)
			{
				// as if the path ended at the cursor
				sidFwd = m_Cursor.get_Sid();
				iPos++;
				break;
			}
		}
		else
			m_DB.get_State(sidFwd.m_Row, s); // need it for logging anyway

		HeightHash hh;
		s.get_ID(hh);

		if (!HandleBlock(hh, sidFwd.m_Row, s, mbc, bCached ? &ce : nullptr))
		{
			bContextFail = mbc.m_bFail = true;

			if (m_Cursor.m_Full.m_Number.v + 1 == m_SyncData.m_TxoLo.v)
				mbc.OnFastSyncFailedOnLo();

			break;
		}

		// Update mmr and cursor
		if (m_Cursor.m_Full.m_Number.v)
			m_Mmr.m_States.Append(m_Cursor.m_hh.m_Hash);

		if (bCached)
		{
			// the state is already marked by set_StateApplied()
			m_DB.MoveFwdApplied(sidFwd, m_Cursor.m_Row, nCountNextFPrev, sfa, s.m_ChainWork);
			nCountNextFPrev = sfa.m_CountNextF;
		}
		else
			m_DB.MoveFwd(sidFwd);

		m_Cursor.m_Full = s;
		m_Cursor.m_hh = hh;
		InitCursor(true, sidFwd);

		if (IsFastSync() && !bCached)
			m_DB.DelStateBlockPP(sidFwd.m_Row); // save space

		if (mbc.m_InProgress.m_Max.v == m_SyncData.m_Target.m_Number.v)
		{
			if (!mbc.Flush())
				break;

			OnFastSyncOver(mbc, bContextFail);

			if (mbc.m_bFail)
				bKeepBlocks = true;
		}

		if (mbc.m_bFail)
			break;
	}

	if (mbc.Flush())
		return; // at position

	if (!bContextFail)
		BEAM_LOG_WARNING() << "Context-free verification failed: " << mbc.m_sErr;

	RollbackTo(Block::Number(mbc.m_InProgress.m_Min.v - 1));

	if (bKeepBlocks)
		return;

	if (mbc.m_bMultiPeer)
	{
		// The failed range has blocks of several peers. It's deleted as a whole: the blocks of other peers in it may be unverified.
		// The context-dependent failure is in the last block, its peer is the culprit. Otherwise the culprit is unknown, it will be found
		// once the blocks are received again: from now on the verification is completed on each peer change.
		BEAM_LOG_WARNING() << "Blocks of several peers failed, verifying by peer from now on";
		m_bPeerBarrier = true;

		bool bPid = bContextFail;
		if (bPid)
		{
			if (bCached)
				mbc.m_pidLast = pidFwd;
			else
				bPid = m_DB.get_Peer(sidFwd.m_Row, mbc.m_pidLast);
		}

		if (!bPid)
			mbc.m_pidLast = Zero;
	}

	if (!(mbc.m_pidLast == Zero))
	{
		OnPeerInsane(mbc.m_pidLast);

		// delete all the consequent blocks from this peer
		for (; iPos; iPos--)
		{
			NodeDB::StateID sidNext;
			sidNext.m_Row = vPath[iPos - 1];
			sidNext.m_Number.v = sidFwd.m_Number.v + 1;

			PeerID pid;
			if (!get_BlockPeer(sidNext, pid))
				break;

			if (pid != mbc.m_pidLast)
				break;

			sidFwd = sidNext;
		}
	}

	BEAM_LOG_INFO() << "Deleting blocks range: " << (m_Cursor.m_Full.m_Number.v + 1) << "-" <<  sidFwd.m_Number.v;

	DeleteBlocksInRange(sidFwd, m_Cursor.m_Full.m_Number);
}

void NodeProcessor::OnFastSyncOver(MultiblockContext& mbc, bool& bContextFail)
{
	assert(mbc.m_InProgress.m_Max.v == m_SyncData.m_Target.m_Number.v);

	mbc.m_pidLast = Zero; // don't blame the last peer if something goes wrong
	NodeDB::StateID sidFail;
	sidFail.SetNull(); // suppress warning

	{
		// ensure no reduced UTXOs are left
		NodeDB::WalkerTxo wlk;
		for (m_DB.EnumTxos(wlk, mbc.m_id0); wlk.MoveNext(); )
		{
			if (wlk.m_SpendHeight != MaxHeight)
				continue;

			if (TxoIsNaked(wlk.m_Value))
			{
				bContextFail = mbc.m_bFail = true;
				mbc.m_sErr = "Utxo unsigned";
				m_DB.FindStateByTxoID(sidFail, wlk.m_ID);
				break;
			}
		}
	}

	if (mbc.m_bFail)
	{
		BEAM_LOG_WARNING() << "Fast-sync failed: " << mbc.m_sErr;

		if (!m_DB.get_Peer(sidFail.m_Row, mbc.m_pidLast))
			mbc.m_pidLast = Zero;

		if (m_SyncData.m_TxoLo.v > m_SyncData.m_n0.v)
		{
			mbc.OnFastSyncFailed(true);
		}
		else
		{
			// try to preserve blocks, recover them from the TXOs.

			ByteBuffer bbP, bbE;
			while (m_Cursor.m_Full.m_Number.v > m_SyncData.m_n0.v)
			{
				NodeDB::StateID sid = m_Cursor.get_Sid();

				bbP.clear();
				if (!GetBlock(sid, &bbE, &bbP, m_SyncData.m_n0, m_SyncData.m_TxoLo, m_SyncData.m_Target.m_Number, true))
					OnCorrupted();

				if (sidFail.m_Number.v == sid.m_Number.v)
				{
					bbP.clear();
					m_DB.SetStateNotFunctional(sid.m_Row);
				}

				RollbackTo(Block::Number(sid.m_Number.v - 1));

				PeerID peer;
				if (!m_DB.get_Peer(sid.m_Row, peer))
					peer = Zero;

				m_DB.SetStateBlock(sid.m_Row, bbP, bbE, peer);
				m_DB.set_StateTxosAndExtra(sid.m_Row, nullptr, nullptr, nullptr);
			}

			mbc.OnFastSyncFailed(false);
		}
	}
	else
	{
		BEAM_LOG_INFO() << "Fast-sync succeeded";

		// raise fossil height, hTxoLo, hTxoHi
		RaiseHorizons(m_Cursor.m_Full.m_Number, m_Cursor.m_Full.m_Number, m_SyncData.m_TxoLo);

		ZeroObject(m_SyncData);
		SaveSyncData();
		m_BodyCache.m_Map.clear();

		OnFastSyncSucceeded();
	}
}

void NodeProcessor::RollbackTo(Block::Number num)
{
	assert(num.v <= m_Cursor.m_Full.m_Number.v);
	if (num.v == m_Cursor.m_Full.m_Number.v)
		return;

	assert(num.v >= m_Extra.m_Fossil.v);

	IPbftHandler* pPbft = nullptr;
	bool bFictive = false;

	if (Rules::Consensus::Pbft == Rules::get().m_Consensus)
	{
		pPbft = get_PbftHandler();

		bFictive =
			(num.v + 1 == m_Cursor.m_Full.m_Number.v) &&
			(Block::Pbft::HdrData::Flags::Empty & Cast::Reinterpret<Block::Pbft::HdrData>(m_Cursor.m_Full.m_PoW).m_Flags1);
	}

	NodeDB::StateID sid = m_Cursor.get_Sid();

	if (!bFictive)
	{
		TxoID id0 = get_TxosBefore(Block::Number(num.v + 1));

		// undo outputs
		struct MyWalker
			:public ITxoWalker_UnspentNaked
		{
			NodeProcessor* m_pThis;

			bool OnTxo(const NodeDB::WalkerTxo& wlk, Height hCreate, Output& outp) override
			{
				BlockInterpretCtx bic(*m_pThis, hCreate, false);
				if (!bic.HandleBlockElement(outp))
					OnCorrupted();
				return true;
			}
		};

		MyWalker wlk2;
		wlk2.m_pThis = this;

		Block::NumberRange nr;
		nr.m_Min = Block::Number(num.v + 1);
		nr.m_Max = m_Cursor.m_Full.m_Number;
		EnumTxos(wlk2, nr);

		m_DB.TxoDelFrom(id0);

		// undo inputs, Kernels, shielded elements, and cursor
		ByteBuffer bbE, bbR;
		TxVectors::Eternal txve;

		BlockInterpretCtx::ChangesFlushGlobal cf(*this);

		while (sid.m_Number.v > num.v)
		{
			// inputs
			std::vector<NodeDB::StateInput> v;
			m_DB.get_StateInputs(sid.m_Row, v);

			for (size_t i = 0; i < v.size(); i++)
			{
				const auto& src = v[i];
				TxoID id = src.get_ID();
				if (id >= id0)
					continue; // created and spent within this range - skip it (already deleted actually)

				Input inp;
				src.Get(inp.m_Commitment);

				InputAux inpAux;
				inpAux.m_ID = id;
				inpAux.m_Maturity = GetInputMaturity(id);

				UndoInput(inp, inpAux);

				m_DB.TxoSetSpent(id, MaxHeight);
			}

			m_DB.set_StateInputs(sid.m_Row, nullptr, 0);

			// kernels
			txve.m_vKernels.clear();
			bbE.clear();
			bbR.clear();
			m_DB.GetStateBlock(sid.m_Row, nullptr, &bbE, &bbR);

			Deserializer der;
			der.reset(bbE);
			der & Cast::Down<TxVectors::Eternal>(txve);

			Height h = Num2Height(sid);

			BlockInterpretCtx bic(*this, h, false);
			bic.m_Rollback.swap(bbR);
			bic.m_ShieldedIns = static_cast<uint32_t>(-1); // suppress assertion
			bic.m_ShieldedOuts = static_cast<uint32_t>(-1);
			bic.m_nKrnIdx = static_cast<uint32_t>(-1);

			bic.HandlePbftReward(txve.m_vKernels, pPbft);
			bic.HandleElementVecBwd(txve.m_vKernels, txve.m_vKernels.size());

			bic.m_Rollback.swap(bbR);
			assert(bbR.empty());

			m_DB.MoveBack(sid);
		}

		cf.Do(*this);

		m_Extra.m_Txos = id0;
		m_ValCache.OnShLo(m_Extra.m_ShieldedOutputs);
	}
	else
		m_DB.MoveBack(sid);

	// other stuff


	m_RecentStates.RollbackTo(num);
	m_Mmr.m_States.ShrinkTo(m_Mmr.m_States.N2I(sid.m_Number));

	InitCursor(false, sid);

	if (!bFictive)
	{
		// height-dependent events
		Height h = m_Cursor.m_hh.m_Height;

		for (const auto& acc : m_vAccounts)
			m_DB.DeleteEventsFrom(acc.m_iAccount, h + 1);

		m_DB.AssetEvtsDeleteFrom(h + 1);
		m_DB.ShieldedOutpDelFrom(h + 1);
		m_DB.KrnInfoDelFrom(h + 1);

		if (!TestDefinition())
			OnCorrupted();

		OnRolledBack();
	}
}

void NodeProcessor::AdjustManualRollbackNumber(Block::Number& num)
{
	Block::Number numMin = get_LowestManualReturnNumber();
	if (num.v < numMin.v)
	{
		BEAM_LOG_INFO() << "Can't go below Number " << numMin.v;
		num = numMin;
	}
}

void NodeProcessor::ManualRollbackInternal(Block::Number num)
{
	bool bChanged = false;

	if (IsFastSync() && (m_SyncData.m_Target.m_Number.v > num.v))
	{
		BEAM_LOG_INFO() << "Fast-sync abort...";

		RollbackTo(m_SyncData.m_n0);
		DeleteBlocksInRange(m_SyncData.m_Target, m_SyncData.m_n0);

		ZeroObject(m_SyncData);
		SaveSyncData();
		m_BodyCache.m_Map.clear();

		bChanged = true;
	}

	if (m_Cursor.m_Full.m_Number.v > num.v)
	{
		RollbackTo(num);
		bChanged = true;
	}

	if (bChanged)
		OnNewState();
}

void NodeProcessor::ManualRollbackTo(Block::Number num)
{
	BEAM_LOG_INFO() << "Manual rollback to " << num.v << "...";

	AdjustManualRollbackNumber(num);

	if (m_Cursor.m_Full.m_Number.v > num.v)
	{
		m_ManualSelection.m_Sid.m_Number.v = num.v + 1;
		m_DB.get_StateHash(FindActiveAtStrict(m_ManualSelection.m_Sid.m_Number), m_ManualSelection.m_Sid.m_Hash);

		m_ManualSelection.m_Forbidden = true;

		m_ManualSelection.Save();
		m_ManualSelection.Log();
	}

	ManualRollbackInternal(num);
}

void NodeProcessor::ManualSelect(const Block::SystemState::ID& sid)
{
	if ((MaxHeight == sid.m_Number.v) || !sid.m_Number.v)
		return; // ignore

	m_ManualSelection.m_Sid = sid;
	m_ManualSelection.m_Forbidden = false;
	m_ManualSelection.Save();
	m_ManualSelection.Log();

	if (m_Cursor.m_Full.m_Number.v >= sid.m_Number.v)
	{
		Merkle::Hash hv;
		m_DB.get_StateHash(FindActiveAtStrict(sid.m_Number), hv);

		if (hv == sid.m_Hash) {
			BEAM_LOG_INFO() << "Already at correct branch";
		}
		else
		{
			Block::Number num(sid.m_Number.v - 1);
			AdjustManualRollbackNumber(num);

			if (num.v == sid.m_Number.v - 1) {
				BEAM_LOG_INFO() << "Rolling back to " << num.v;
				ManualRollbackInternal(num);
			}
			else {
				BEAM_LOG_INFO() << "Unable to rollback below incorrect branch. Please resync from the beginning";
			}
		}
	}
}

NodeProcessor::DataStatus::Enum NodeProcessor::OnStateInternal(const Block::SystemState::Full& s, Block::SystemState::ID& id, bool bAlreadyChecked)
{
	s.get_ID(id);

	if (!(bAlreadyChecked || s.IsValid()))
	{
		BEAM_LOG_WARNING() << id << " header invalid!";
		return DataStatus::Invalid;
	}

	return TestStateAcceptable(s, id);
}

NodeProcessor::DataStatus::Enum NodeProcessor::TestStateAcceptable(const Block::SystemState::Full& s, const Block::SystemState::ID& id)
{
	const Rules& r = Rules::get();
	if (Rules::Consensus::Pbft != r.m_Consensus)
	{
		Timestamp ts = getTimestamp();
		if (s.m_TimeStamp > ts)
		{
			ts = s.m_TimeStamp - ts; // dt
			if (ts > r.DA.MaxAhead_s)
			{
				BEAM_LOG_WARNING() << id << " Timestamp ahead by " << ts;
				return DataStatus::Invalid;
			}
		}
	}

	if (s.m_Number.v < get_LowestReturnNumber().v)
	{
		m_UnreachableLog.Log(id);
		return DataStatus::Unreachable;
	}

	if (m_DB.StateFindSafe(id))
		return DataStatus::Rejected;

	return DataStatus::Accepted;
}

NodeProcessor::DataStatus::Enum NodeProcessor::OnState(const Block::SystemState::Full& s, const PeerID& peer)
{
	Block::SystemState::ID id;

	DataStatus::Enum ret = OnStateSilent(s, peer, id, false);
	if (DataStatus::Accepted == ret)
	{
		BEAM_LOG_INFO() << id << " Header accepted";
	}
	
	return ret;
}

NodeProcessor::DataStatus::Enum NodeProcessor::OnStateSilent(const Block::SystemState::Full& s, const PeerID& peer, Block::SystemState::ID& id, bool bAlreadyChecked)
{
	DataStatus::Enum ret = OnStateInternal(s, id, bAlreadyChecked);
	if (DataStatus::Accepted == ret)
		m_DB.InsertState(s, peer);

	return ret;
}

bool NodeProcessor::OnStatePack(const Block::SystemState::Full* pV, size_t nCount, const PeerID& peer)
{
	if (!nCount)
		return true;

	// The hash of each header is the prev of the next one, only the last one is calculated
	Merkle::Hash hvLast;
	pV[nCount - 1].get_Hash(hvLast);

	auto fnHash = [pV, nCount, &hvLast](size_t i) -> const Merkle::Hash& {
		return (i + 1 < nCount) ? pV[i + 1].m_Prev : hvLast;
	};

	// Test all of them first. Then it's known in advance if the next header will be inserted right after the current one.
	std::vector<DataStatus::Enum> vStatus(nCount);

	for (size_t i = 0; i < nCount; i++)
	{
		Block::SystemState::ID id;
		id.m_Number = pV[i].m_Number;
		id.m_Hash = fnHash(i);

		vStatus[i] = TestStateAcceptable(pV[i], id);
		if (DataStatus::Invalid == vStatus[i])
			return false;
	}

	uint64_t rowPrev = 0; // the previous header, if it was just inserted
	for (size_t i = 0; i < nCount; i++)
	{
		if (DataStatus::Accepted != vStatus[i])
		{
			rowPrev = 0;
			continue;
		}

		bool bNextFollows = (i + 1 < nCount) && (DataStatus::Accepted == vStatus[i + 1]);
		rowPrev = m_DB.InsertState(pV[i], fnHash(i), peer, rowPrev, bNextFollows);
	}

	return true;
}

NodeProcessor::DataStatus::Enum NodeProcessor::OnBlock(const Block::SystemState::ID& id, const Blob& bbP, const Blob& bbE, const PeerID& peer)
{
	NodeDB::StateID sid;
	sid.m_Row = m_DB.StateFindSafe(id);
	if (!sid.m_Row)
	{
		BEAM_LOG_WARNING() << id << " Block unexpected";
		return DataStatus::Rejected;
	}

	sid.m_Number = id.m_Number;
	return OnBlock(sid, bbP, bbE, peer);
}

NodeProcessor::DataStatus::Enum NodeProcessor::OnBlock(const NodeDB::StateID& sid, const Blob& bbP, const Blob& bbE, const PeerID& peer)
{
	size_t nSize = size_t(bbP.n) + size_t(bbE.n);
	if (nSize > Rules::get().MaxBodySize)
	{
		// TODO: don't account for QC and metadata
		BEAM_LOG_WARNING() << LogSid(m_DB, sid) << " Block too large: " << nSize;
		return DataStatus::Invalid;
	}

	if (IsBodyCached(sid.m_Number))
	{
		// Above the cursor, so it's not applied, and can't be functional unless received before the cache was used (restart).
		// If it is - the cached body is ignored when applied.
		auto it = m_BodyCache.m_Map.try_emplace(sid.m_Number.v).first;
		BodyCache::Entry& e = it->second;

		if (e.m_Row == sid.m_Row)
		{
			BEAM_LOG_WARNING() << LogSid(m_DB, sid) << " Block already received";
			return DataStatus::Rejected;
		}

		// a new one, or replaces a block of another branch
		e.m_Row = sid.m_Row;
		bbP.Export(e.m_Body.m_Perishable);
		bbE.Export(e.m_Body.m_Eternal);
		e.m_Peer = peer;

		return DataStatus::Accepted;
	}

	if (NodeDB::StateFlags::Functional & m_DB.GetStateFlags(sid.m_Row))
	{
		BEAM_LOG_WARNING() << LogSid(m_DB, sid) << " Block already received";
		return DataStatus::Rejected;
	}

	if (sid.m_Number.v < get_LowestReturnNumber().v)
		return DataStatus::Unreachable;

	m_DB.SetStateBlock(sid.m_Row, bbP, bbE, peer);
	m_DB.SetStateFunctional(sid.m_Row);

	return DataStatus::Accepted;
}

bool NodeProcessor::IsBodyCached(Block::Number num) const
{
	// only with the body windows: they request the missing blocks wrt the cache, and limit the blocks received ahead of the cursor
	return
		IsFastSync() &&
		m_BodyWindows.m_Size &&
		(num.v > m_Cursor.m_Full.m_Number.v) &&
		(num.v <= m_SyncData.m_Target.m_Number.v);
}

const NodeProcessor::BodyCache::Entry* NodeProcessor::BodyCache::Find(const NodeDB::StateID& sid) const
{
	auto it = m_Map.find(sid.m_Number.v);
	if ((m_Map.end() == it) || (it->second.m_Row != sid.m_Row))
		return nullptr;
	return &it->second;
}

void NodeProcessor::BodyCache::DeleteRange(Block::Number numMin, Block::Number numMax)
{
	if (numMin.v <= numMax.v)
		m_Map.erase(m_Map.lower_bound(numMin.v), m_Map.upper_bound(numMax.v));
}

bool NodeProcessor::IsBlockReceived(const NodeDB::StateID& sid)
{
	return m_BodyCache.Find(sid) || (NodeDB::StateFlags::Functional & m_DB.GetStateFlags(sid.m_Row));
}

bool NodeProcessor::get_BlockPeer(const NodeDB::StateID& sid, PeerID& pid)
{
	const BodyCache::Entry* pE = m_BodyCache.Find(sid);
	if (pE)
	{
		pid = pE->m_Peer;
		return true;
	}

	return m_DB.get_Peer(sid.m_Row, pid);
}

NodeProcessor::DataStatus::Enum NodeProcessor::OnTreasury(const Blob& blob)
{
	if (Rules::get().TreasuryChecksum == Zero)
		return DataStatus::Invalid; // should be no treasury

	ECC::Hash::Value hv;
	ECC::Hash::Processor()
		<< blob
		>> hv;

	if (Rules::get().TreasuryChecksum != hv)
		return DataStatus::Invalid;

	if (IsTreasuryHandled())
		return DataStatus::Rejected;

	if (!HandleTreasury(blob))
		return DataStatus::Invalid;

	m_Extra.m_Txos++;
	m_Extra.m_TxosTreasury = m_Extra.m_Txos;
	m_DB.ParamSet(NodeDB::ParamID::Treasury, &m_Extra.m_TxosTreasury, &blob);

	BEAM_LOG_INFO() << "Treasury verified";

	RescanAccounts(static_cast<uint32_t>(m_vAccounts.size()));

	OnNewState();
	TryGoUp();

	return DataStatus::Accepted;
}

bool NodeProcessor::Tip::IsRemoteNeeded(const Tip& tRemote) const
{
	int n = m_Full.m_ChainWork.cmp(tRemote.m_Full.m_ChainWork);
	if (n > 0)
		return false;
	if (n < 0)
		return true;

	return m_hh != tRemote.m_hh;
}

} // namespace beam
