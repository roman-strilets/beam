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

#include "processor_common.h"

namespace beam {

void NodeProcessor::OnCorrupted()
{
	CorruptionException exc;
	exc.m_sErr = "node data";
	throw exc;
}

NodeProcessor::Horizon::Horizon()
{
	SetInfinite();
}

void NodeProcessor::Horizon::SetInfinite()
{
	m_Branching = MaxHeight;
	m_Sync.Lo = MaxHeight;
	m_Sync.Hi = MaxHeight;
	m_Local.Lo = MaxHeight;
	m_Local.Hi = MaxHeight;
}

void NodeProcessor::Horizon::SetStdFastSync()
{
	uint32_t r = Rules::get().MaxRollback;
	m_Branching = r / 4; // inferior branches would be pruned when height difference is this.

	m_Sync.Hi = r;
	m_Sync.Lo = r * 3; // 3-day period

	m_Local.Hi = r * 2; // slightly higher than m_Sync.Loc, to feed other fast synchers
	m_Local.Lo = r * 180; // 180-day period
}

void NodeProcessor::Horizon::Normalize()
{
	beam::setmax(m_Branching, Height(1));

	Height r = Rules::get().MaxRollback;

	beam::setmax(m_Sync.Hi, std::max(r, m_Branching));
	beam::setmax(m_Sync.Lo, m_Sync.Hi);

	// Some nodes in production have a bug: if (Sync.Lo == Sync.Hi) - the last generated block that they send may be incorrect
	// Workaround: make sure (Sync.Lo > Sync.Hi), at least by 1
	//
	// After HF2 the workaround can be removed
	if ((m_Sync.Lo == m_Sync.Hi) && (m_Sync.Hi < MaxHeight))
		m_Sync.Lo++;

	// though not required, we prefer m_Local to be no less than m_Sync
	beam::setmax(m_Local.Hi, m_Sync.Hi);
	beam::setmax(m_Local.Lo, std::max(m_Local.Hi, m_Sync.Lo));
}

void NodeProcessor::Initialize(const char* szPath)
{
	StartParams sp; // defaults
	Initialize(szPath, sp);
}

void NodeProcessor::Initialize(const char* szPath, const StartParams& sp, ILongAction* pExternalHandler)
{
	m_DB.Open(szPath);
	m_DbTx.Start(m_DB);
	m_pExternalHandler = pExternalHandler;
	if (sp.m_CheckIntegrity)
	{
		BEAM_LOG_INFO() << "DB integrity check...";
		m_DB.CheckIntegrity();
	}

	const auto& r = Rules::get();

	Merkle::Hash hv;
	Blob blob(hv);

	NodeDB::StateID sid;
	m_DB.get_Cursor(sid);
	InitCursor(false, sid);

	bool bUpdateChecksum = !m_DB.ParamGet(NodeDB::ParamID::CfgChecksum, NULL, &blob);
	if (!bUpdateChecksum)
	{
		const HeightHash* pFork = r.FindFork(hv);
		if (&r.get_LastFork() != pFork)
		{
			if (!pFork)
			{
				std::ostringstream os;
				os << "Data configuration is incompatible: " << hv;
				throw std::runtime_error(os.str());
			}

			if (m_Cursor.m_hh.m_Height >= pFork[1].m_Height)
			{
				std::ostringstream os;
				os << "Data configuration: " << hv << ", Fork didn't happen at " << pFork[1].m_Height;
				throw std::runtime_error(os.str());
			}

			bUpdateChecksum = true;
		}
	}

	if (bUpdateChecksum)
	{
		BEAM_LOG_INFO() << "Settings configuration";

		blob = Blob(r.get_LastFork().m_Hash);
		m_DB.ParamSet(NodeDB::ParamID::CfgChecksum, NULL, &blob);
	}

	ZeroObject(m_Extra);
	m_Extra.m_Fossil.v = m_DB.ParamIntGetDef(NodeDB::ParamID::NumberFossil);
	m_Extra.m_TxoLo.v = m_DB.ParamIntGetDef(NodeDB::ParamID::NumberTxoLo);
	m_Extra.m_TxoHi.v = m_DB.ParamIntGetDef(NodeDB::ParamID::NumberTxoHi);

	ZeroObject(m_SyncData);

	blob.p = &m_SyncData;
	blob.n = sizeof(m_SyncData);
	m_DB.ParamGet(NodeDB::ParamID::SyncData, nullptr, &blob);

	LogSyncData();

	if (!m_DB.IsKernelIndexed())
	{
		if (IsFastSync())
			SuspendKernelIndex(); // restart in the middle of the fast-sync
		else
			m_DB.KernelIndexRestore(); // shouldn't happen, unless the fast-sync state was lost
	}

	if (r.TreasuryChecksum == Zero)
		m_Extra.m_TxosTreasury = 1; // artificial gap
	else
		m_DB.ParamGet(NodeDB::ParamID::Treasury, &m_Extra.m_TxosTreasury, nullptr, nullptr);

	auto aidMax = m_DB.ParamIntGetDef(NodeDB::ParamID::AidMax);
	if (!aidMax && r.CA.ForeignEnd)
	{
		aidMax = r.CA.ForeignEnd;
		m_DB.ParamIntSet(NodeDB::ParamID::AidMax, aidMax);
	}

	m_Mmr.m_Assets.m_Count = aidMax - r.CA.ForeignEnd;
	m_Extra.m_ShieldedOutputs = m_DB.ShieldedOutpGet(std::numeric_limits<int64_t>::max());
	m_Mmr.m_Shielded.m_Count = m_DB.ParamIntGetDef(NodeDB::ParamID::ShieldedInputs);
	m_Mmr.m_Shielded.m_Count += m_Extra.m_ShieldedOutputs;

	InitializeMapped(szPath);
	m_Extra.m_Txos = get_TxosBefore(Block::Number(m_Cursor.m_Full.m_Number.v + 1));

	bool bRebuildNonStd = false;
	if ((StartParams::RichInfo::Off | StartParams::RichInfo::On) & sp.m_RichInfoFlags)
	{
		uint32_t bOn = !!(StartParams::RichInfo::On & sp.m_RichInfoFlags);
		if (m_DB.ParamIntGetDef(NodeDB::ParamID::RichContractInfo) != bOn)
		{
			m_DB.ParamIntSet(NodeDB::ParamID::RichContractInfo, bOn);
			if (bOn)
				bRebuildNonStd = true;
		}
	}

	if (StartParams::RichInfo::UpdShader & sp.m_RichInfoFlags)
	{
		m_DB.ParamSet(NodeDB::ParamID::RichContractParser, nullptr, &sp.m_RichParser);
		
		if (!bRebuildNonStd && m_DB.ParamIntGetDef(NodeDB::ParamID::RichContractInfo))
			bRebuildNonStd = true;
	}

	uint64_t nFlags1 = m_DB.ParamIntGetDef(NodeDB::ParamID::Flags1);
	if (bRebuildNonStd || (NodeDB::Flags1::PendingRebuildNonStd & nFlags1))
	{
		RebuildNonStd();
		m_DB.ParamIntSet(NodeDB::ParamID::Flags1, nFlags1 & ~NodeDB::Flags1::PendingRebuildNonStd);
	}

	TestDefinitionStrict();

	if (IsFastSync() && (m_Cursor.m_Full.m_Number.v >= m_SyncData.m_Target.m_Number.v))
	{
		// the blocks up to the target were applied and verified, but finalizing was interrupted (see FinalizeFastSync)
		BEAM_LOG_INFO() << "Fast-sync target is reached, finalizing";
		FinalizeFastSync();
		m_bFastSyncFinalizedOnStart = true; // the owner should call OnFastSyncSucceeded() once it's ready (e.g. the accounts are loaded)
	}

	CommitDB();

	m_Horizon.Normalize();

	if (PruneOld() && !sp.m_Vacuum)
	{
		BEAM_LOG_INFO() << "Old data was just removed from the DB. Some space can be freed by vacuum";
	}

	if (sp.m_Vacuum)
		Vacuum();

	if (m_ManualSelection.Load())
		m_ManualSelection.Log();
	else
		m_ManualSelection.Reset();

	TryGoUp();
}

void NodeProcessor::ManualSelection::Reset()
{
	m_Sid.m_Number.v = MaxHeight; // don't set it to 0, it may interfer with treasury in RequestData()
	m_Sid.m_Hash = Zero;
	m_Forbidden = false;
}

void NodeProcessor::ManualSelection::ResetAndSave()
{
	Reset();
	Save();
	Log();
}

bool NodeProcessor::ManualSelection::Load()
{
	Blob blob(m_Sid.m_Hash);
	if (!get_ParentObj().m_DB.ParamGet(NodeDB::ParamID::ForbiddenState, &m_Sid.m_Number.v, &blob))
		return false;

	const uint64_t flag = (MaxHeight >> 1) + 1;
	m_Forbidden = !(flag & m_Sid.m_Number.v);

	if (!m_Forbidden)
		m_Sid.m_Number.v &= ~flag;

	return true;
}

void NodeProcessor::ManualSelection::Save() const
{
	auto n = m_Sid.m_Number.v;
	if (MaxHeight == n)
		get_ParentObj().m_DB.ParamDelSafe(NodeDB::ParamID::ForbiddenState);
	else
	{
		if (!m_Forbidden)
		{
			const uint64_t flag = (MaxHeight >> 1) + 1;
			n |= flag;
		}

		Blob blob(m_Sid.m_Hash);
		get_ParentObj().m_DB.ParamSet(NodeDB::ParamID::ForbiddenState, &n, &blob);
	}
}

void NodeProcessor::ManualSelection::Log() const
{
	if (MaxHeight == m_Sid.m_Number.v) {
		BEAM_LOG_INFO() << "Manual selection state reset";
	} else {
		BEAM_LOG_INFO() << (m_Forbidden ? "Forbidden" : "Selected") << " state: " << m_Sid;
	}
}

bool NodeProcessor::ManualSelection::IsAllowed(Block::Number num, const Merkle::Hash& hv) const
{
	return (m_Sid.m_Number.v == num.v) ? IsAllowed(hv) : true;
}

bool NodeProcessor::ManualSelection::IsAllowed(const Merkle::Hash& hv) const
{
	bool bMatch = (hv == m_Sid.m_Hash);
	if (bMatch != m_Forbidden)
		return true;

	Block::SystemState::ID sid;
	sid.m_Number = m_Sid.m_Number;
	sid.m_Hash = hv;
	BEAM_LOG_WARNING() << sid << " State forbidden";
	return false;
}

void NodeProcessor::InitializeMapped(const char* sz)
{
	if (InitMapping(sz, false))
	{
		BEAM_LOG_INFO() << "Mapping image found";
		if (TestDefinition())
			return; // ok

		BEAM_LOG_WARNING() << "Definition mismatch, discarding mapped image";
		m_Mapped.Close();
		InitMapping(sz, true);
	}

	InitializeUtxos();

	NodeDB::WalkerContractData wlk;
	for (m_DB.ContractDataEnum(wlk); wlk.MoveNext(); )
		m_Mapped.m_Contract.Toggle(wlk.m_Key, wlk.m_Val, true);
}

void NodeProcessor::TestDefinitionStrict()
{
	if (!TestDefinition())
	{
		BEAM_LOG_ERROR() << "Definition mismatch";
		OnCorrupted();
	}
}

bool NodeProcessor::TestDefinition()
{
	if (!m_Cursor.m_Full.m_Number.v || (m_Cursor.m_Full.m_Number.v < m_SyncData.m_TxoLo.v))
		return true; // irrelevant

	Merkle::Hash hv;
	Evaluator ev(*this);
	ev.get_Definition(hv);

	return m_Cursor.m_Full.m_Definition == hv;
}

// Ridiculous! Had to write this because strmpi isn't standard!
int My_strcmpi(const char* sz1, const char* sz2)
{
	while (true)
	{
		int c1 = std::tolower(*sz1++);
		int c2 = std::tolower(*sz2++);
		if (c1 < c2)
			return -1;
		if (c1 > c2)
			return 1;

		if (!c1)
			break;
	}
	return 0;
}

void NodeProcessor::get_MappingPath(std::string& sPath, const char* sz)
{
	// derive mapping path from db path
	sPath = sz;

	static const char szSufix[] = ".db";
	const size_t nSufix = _countof(szSufix) - 1;

	if ((sPath.size() >= nSufix) && !My_strcmpi(sPath.c_str() + sPath.size() - nSufix, szSufix))
		sPath.resize(sPath.size() - nSufix);

	sPath += "-utxo-image.bin";
}

bool NodeProcessor::InitMapping(const char* sz, bool bForceReset)
{
	// derive mapping path from db path
	std::string sPath;
	get_MappingPath(sPath, sz);

	Mapped::Stamp us;
	Blob blob(us);

	// don't use the saved image if no height: we may contain treasury UTXOs, but no way to verify the contents
	if (bForceReset || !m_Cursor.m_Full.m_Number.v || !m_DB.ParamGet(NodeDB::ParamID::MappingStamp, nullptr, &blob))
	{
		us = 1U;
		us.Negate();
	}

	return m_Mapped.Open(sPath.c_str(), us);
}

void NodeProcessor::LogSyncData()
{
	if (!IsFastSync())
		return;

	BEAM_LOG_INFO() << "Fast-sync mode up to block number " << m_SyncData.m_Target.m_Number.v << ", TxoLo=" << m_SyncData.m_TxoLo.v;
}

void NodeProcessor::SaveSyncData()
{
	if (IsFastSync())
	{
		Blob blob(&m_SyncData, sizeof(m_SyncData));
		m_DB.ParamSet(NodeDB::ParamID::SyncData, nullptr, &blob);
	}
	else
	{
		m_DB.ParamSet(NodeDB::ParamID::SyncData, nullptr, nullptr);
		m_DB.KernelIndexRestore(); // fast-sync is over or aborted. Same tx, so it's atomic with the sync data
	}
}

void NodeProcessor::SuspendKernelIndex()
{
	// The kernel index gets a random insert per kernel, and most of the pages written during the initial sync
	// belong to it. Instead it's rebuilt at the end of the fast-sync.
	// Meanwhile the kernel lookups need only the visibility window (past Fork2).
	Height dh = Rules::get().MaxKernelValidityDH;
	Height h = m_Cursor.m_hh.m_Height;

	m_DB.KernelIndexSuspend((h > dh) ? (h - dh) : 0, dh);
}

Asset::ID NodeProcessor::get_AidMax() const
{
	return (Asset::ID) (Rules::get().CA.ForeignEnd + m_Mmr.m_Assets.m_Count);
}

NodeProcessor::Mmr::Mmr(NodeDB& db)
	:m_States(db)
	,m_Shielded(db, NodeDB::StreamType::ShieldedMmr, true)
	,m_Assets(db, NodeDB::StreamType::AssetsMmr, true)
{
}

NodeProcessor::NodeProcessor()
	:m_Mmr(m_DB)
{
}

NodeProcessor::~NodeProcessor()
{
	if (m_DbTx.IsInProgress())
	{
		try {
			CommitMappingAndDB();
		} catch (const CorruptionException& e) {
			BEAM_LOG_ERROR() << "DB Commit failed: %s" << e.m_sErr;
		}
	}
}

void NodeProcessor::CommitMappingAndDB()
{
	Mapped::Stamp us;

	bool bFlushMapping = (m_Mapped.IsOpen() && m_Mapped.get_Hdr().m_Dirty);

	if (bFlushMapping)
	{
		Blob blob(us);

		if (m_DB.ParamGet(NodeDB::ParamID::MappingStamp, nullptr, &blob)) {
			ECC::Hash::Processor() << us >> us;
		} else {
			ECC::GenRandom(us);
		}

		m_DB.ParamSet(NodeDB::ParamID::MappingStamp, nullptr, &blob);
	}

	m_DbTx.Commit();

	if (bFlushMapping)
		m_Mapped.FlushStrict(us);
}

void NodeProcessor::Vacuum()
{
	if (m_DbTx.IsInProgress())
		m_DbTx.Commit();

	BEAM_LOG_INFO() << "DB compacting...";
	m_DB.Vacuum();
	BEAM_LOG_INFO() << "DB compacting completed";

	m_DbTx.Start(m_DB);
}

void NodeProcessor::CommitDB()
{
	if (m_DbTx.IsInProgress())
	{
		CommitMappingAndDB();
		m_DbTx.Start(m_DB);
	}
}

void NodeProcessor::RollbackDB()
{
	if (m_DbTx.IsInProgress())
	{
		m_DbTx.Rollback();
	}
}

void NodeProcessor::InitCursor(bool bMovingUp, const NodeDB::StateID& sid)
{
	if (sid.m_Number.v)
	{
		m_Cursor.m_Row = sid.m_Row;
		assert(m_Cursor.m_Row);

		m_Mmr.m_States.m_Count = sid.m_Number.v - 1;

		if (bMovingUp)
		{
			assert(m_Cursor.m_Full.m_Number.v == sid.m_Number.v); // must already initialized
			m_Cursor.m_History = m_Cursor.m_HistoryNext;

			assert(m_Cursor.m_Full.get_Height() == m_Cursor.m_hh.m_Height);
		}
		else
		{
			m_DB.get_State(sid.m_Row, m_Cursor.m_Full);
			m_Mmr.m_States.get_Hash(m_Cursor.m_History);

			m_DB.get_StateExtra(sid.m_Row, &m_Cursor.m_StateExtra, sizeof(m_Cursor.m_StateExtra));

			m_Cursor.m_bKernels = false;

			m_Cursor.m_Full.get_Hash(m_Cursor.m_hh.m_Hash);
			m_Cursor.m_hh.m_Height = m_Cursor.m_Full.get_Height();
		}

		m_Mmr.m_States.get_PredictedHash(m_Cursor.m_HistoryNext, m_Cursor.m_hh.m_Hash);
	}
	else
	{
		m_Mmr.m_States.m_Count = 0;
		ZeroObject(m_Cursor);
		m_Cursor.m_hh.m_Hash = Rules::get().Prehistoric;
	}

	m_Cursor.m_DifficultyNext = get_NextDifficulty();
}

Block::SystemState::ID NodeProcessor::Cursor::get_ID() const
{
	Block::SystemState::ID id;
	id.m_Number = m_Full.m_Number;
	id.m_Hash = m_hh.m_Hash;
	return id;
}

NodeDB::StateID NodeProcessor::Cursor::get_Sid() const
{
	NodeDB::StateID sid;
	sid.m_Number = m_Full.m_Number;
	sid.m_Row = m_Row;
	return sid;
}

NodeProcessor::IPbftHandler* NodeProcessor::get_PbftHandler()
{
	return nullptr;
}

std::ostream& operator << (std::ostream& s, const LogSid& sid)
{
	Block::SystemState::ID id;
	id.m_Number = sid.m_Sid.m_Number;
	sid.m_DB.get_StateHash(sid.m_Sid.m_Row, id.m_Hash);

	s << id;
	return s;
}

bool NodeProcessor::get_HdrAt(Block::SystemState::Full& s, Height h)
{
	assert(h <= m_Cursor.m_hh.m_Height); // must be checked earlier

	if (h == m_Cursor.m_hh.m_Height)
		s = m_Cursor.m_Full;
	else
	{
		if (!h)
			return false;

		NodeDB::StateID sid;
		FindAtivePastHeight(sid, h);
		m_DB.get_State(sid.m_Row, s); // can be higher than requested
	}

	return true;
}

uint64_t NodeProcessor::FindActiveAtStrict(Block::Number num)
{
	const RecentStates::Entry* pE = m_RecentStates.Get(num);
	if (pE)
		return pE->m_RowID;

	return m_DB.FindActiveStateStrict(num);
}

Height NodeProcessor::Num2Height(const NodeDB::StateID& sid)
{
	if (Rules::get().IsConstantSpan())
		return sid.m_Number.v;

	if (!sid.m_Number.v)
		return 0;

	Block::SystemState::Full s;
	m_DB.get_State(sid.m_Row, s);
	return s.get_Height();
}

Height NodeProcessor::Num2Height(Block::Number num)
{
	if (Rules::get().IsConstantSpan())
		return num.v;

	if (!num.v)
		return 0;

	auto row = FindActiveAtStrict(num);

	Block::SystemState::Full s;
	m_DB.get_State(row, s);
	return s.get_Height();
}

Block::Number NodeProcessor::FindAtivePastHeight(Height h)
{
	assert(h <= m_Cursor.m_hh.m_Height);

	if (Rules::get().IsConstantSpan())
		return Block::Number(h);

	NodeDB::StateID sid;
	FindAtivePastHeight(sid, h);
	return sid.m_Number;
}

void NodeProcessor::FindAtivePastHeight(NodeDB::StateID& sid, Height h)
{
	assert(h <= m_Cursor.m_hh.m_Height);

	if (h)
	{
		if (h == m_Cursor.m_hh.m_Height)
			sid = m_Cursor.get_Sid();
		else
		{
			const Rules& r = Rules::get();
			if (r.IsConstantSpan())
			{
				sid.m_Number.v = h;
				sid.m_Row = FindActiveAtStrict(sid.m_Number);
			}
			else
			{
				// Search w.r.t. chainwork
				Difficulty::Raw d;
				r.Height2Difficulty(d, h);
				m_DB.FindActiveStateStrictLowBound(sid, d);
			}
		}
	}
	else
		sid.SetNull();

}

Difficulty NodeProcessor::get_NextDifficulty()
{
	const Rules& r = Rules::get(); // alias

	if (!m_Cursor.m_Full.m_Number.v || (Rules::Consensus::PoW != r.m_Consensus))
		return r.DA.Difficulty0; // 1st block

	THW thw0, thw1;

	get_MovingMedianEx(m_Cursor.m_Full.m_Number, r.DA.WindowMedian1, thw1);

	if (m_Cursor.m_Full.m_Number.v - 1 >= r.DA.WindowWork)
	{
		get_MovingMedianEx(Block::Number(m_Cursor.m_Full.m_Number.v - r.DA.WindowWork), r.DA.WindowMedian1, thw0);
	}
	else
	{
		get_MovingMedianEx(Block::Number(1), r.DA.WindowMedian1, thw0); // awkward to look for median, since they're immaginary. But makes sure we stick to the same median search and rounding (in case window is even).

		// how many immaginary prehistoric blocks should be offset
		uint32_t nDelta = r.DA.WindowWork - static_cast<uint32_t>(m_Cursor.m_Full.m_Number.v - 1);

		thw0.first -= static_cast<int64_t>(r.DA.get_Target_s()) * nDelta;
		thw0.second.first -= nDelta;

		Difficulty::Number wrk, wrk2;
		r.DA.Difficulty0.Unpack(wrk);
		wrk2.get_Slice().SetMul(wrk.get_ConstSlice(), MultiWord::ConstSlice{ &nDelta, 1 });

		thw0.second.second.ToNumber(wrk);
		wrk -= wrk2;
		thw0.second.second.FromNumber(wrk);
	}

	assert(r.DA.WindowWork > r.DA.WindowMedian1); // when getting median - the target height can be shifted by some value, ensure it's smaller than the window
	// means, the height diff should always be positive
	assert(thw1.second.first > thw0.second.first);

	uint32_t dh = static_cast<uint32_t>(thw1.second.first - thw0.second.first);

	uint32_t dtTrg_s = r.DA.get_Target_s() * dh;

	// actual dt, only making sure it's non-negative
	uint32_t dtSrc_s = (thw1.first > thw0.first) ? static_cast<uint32_t>(thw1.first - thw0.first) : 0;

	if (r.IsPastFork_<1>(m_Cursor.m_hh.m_Height))
	{
		// Apply dampening. Recalculate dtSrc_s := dtSrc_s * M/N + dtTrg_s * (N-M)/N
		// Use 64-bit arithmetic to avoid overflow

		uint64_t nVal =
			static_cast<uint64_t>(dtSrc_s) * r.DA.Damp.M +
			static_cast<uint64_t>(dtTrg_s) * (r.DA.Damp.N - r.DA.Damp.M);

		uint32_t dt_s = static_cast<uint32_t>(nVal / r.DA.Damp.N);

		if ((dt_s > dtSrc_s) != (dt_s > dtTrg_s)) // another overflow verification. The result normally must sit between src and trg (assuming valid damp parameters, i.e. M < N).
			dtSrc_s = dt_s;
	}

	// apply "emergency" threshold
	beam::setmin(dtSrc_s, dtTrg_s * 2);
	beam::setmax(dtSrc_s, dtTrg_s / 2);


	Difficulty::Raw& dWrk = thw0.second.second;
	dWrk.Negate();
	dWrk += thw1.second.second;

	Difficulty res;
	res.Calculate(dWrk, dh, dtTrg_s, dtSrc_s);

	return res;
}

void NodeProcessor::get_MovingMedianEx(Block::Number numLast, uint32_t nWindow, THW& res)
{
	std::vector<THW> v;
	v.reserve(nWindow);

	assert(numLast.v >= 1);
	uint64_t rowLast = 0;

	while (v.size() < nWindow)
	{
		v.emplace_back();
		THW& thw = v.back();

		if (numLast.v)
		{
			const RecentStates::Entry* pE = m_RecentStates.Get(numLast);

			Block::SystemState::Full sDb;
			if (!pE)
			{
				if (rowLast)
				{
					if (!m_DB.get_Prev(rowLast))
						OnCorrupted();
				}
				else
					rowLast = FindActiveAtStrict(numLast);

				m_DB.get_State(rowLast, sDb);
			}

			const Block::SystemState::Full& s = pE ? pE->m_State : sDb;

			thw.first = s.m_TimeStamp;
			thw.second.first = s.m_Number.v;
			thw.second.second = s.m_ChainWork;

			numLast.v--;
		}
		else
		{
			// append "prehistoric" blocks of starting difficulty and perfect timing
			const THW& thwSrc = v[v.size() - 2];

			thw.first = thwSrc.first - Rules::get().DA.get_Target_s();
			thw.second.first = thwSrc.second.first - 1;
			thw.second.second = thwSrc.second.second - Rules::get().DA.Difficulty0; // don't care about overflow
		}
	}

	std::sort(v.begin(), v.end()); // there's a better algorithm to find a median (or whatever order), however our array isn't too big, so it's ok.
	// In case there are multiple blocks with exactly the same Timestamp - the ambiguity is resolved w.r.t. Height.

	res = v[nWindow >> 1];
}

Timestamp NodeProcessor::get_MovingMedian()
{
	if (!m_Cursor.m_Full.m_Number.v)
		return 0;

	THW thw;
	get_MovingMedianEx(m_Cursor.m_Full.m_Number, Rules::get().DA.WindowMedian0, thw);
	return thw.first;
}

Executor& NodeProcessor::get_Executor()
{
	if (!m_pExecSync)
	{
		m_pExecSync = std::make_unique<MyExecutor>();
		m_pExecSync->m_Ctx.m_pThis = m_pExecSync.get();
		m_pExecSync->m_Ctx.m_iThread = 0;
	}

	return *m_pExecSync;
}

uint32_t NodeProcessor::MyExecutor::get_Threads()
{
	return 1;
}

void NodeProcessor::MyExecutor::Push(TaskAsync::Ptr&& pTask)
{
	ExecAll(*pTask);
}

uint32_t NodeProcessor::MyExecutor::Flush(uint32_t)
{
	return 0;
}

void NodeProcessor::MyExecutor::ExecAll(TaskSync& t)
{
	ECC::InnerProduct::BatchContext::Scope scope(m_Ctx.m_BatchCtx);
	t.Exec(m_Ctx);
}

} // namespace beam
