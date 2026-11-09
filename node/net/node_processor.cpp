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

void Node::Processor::RequestData(const Block::SystemState::ID& id, bool bBlock, const NodeDB::StateID& sidTrg)
{
	Node::Task tKey;
	tKey.m_Key.first = id;
	tKey.m_Key.second = bBlock;

	TaskSet::iterator it = get_ParentObj().m_setTasks.find(tKey);
	if (get_ParentObj().m_setTasks.end() == it)
	{
		BEAM_LOG_INFO() << "Requesting " << (bBlock ? "block" : "header") << " " << id;

		Node::Task* pTask = new Node::Task;
		pTask->m_Key = tKey.m_Key;
		pTask->m_sidTrg = sidTrg;
		pTask->m_bNeeded = true;
		pTask->m_nCount = 0;
		pTask->m_pOwner = NULL;

		get_ParentObj().m_setTasks.insert(*pTask);
		get_ParentObj().m_lstTasksUnassigned.push_back(*pTask);

		get_ParentObj().TryAssignTask(*pTask);

	}
	else
	{
		Node::Task& t = *it;
		t.m_bNeeded = true;

		if (t.m_sidTrg.m_Number.v <= sidTrg.m_Number.v)
			t.m_sidTrg = sidTrg;

		if (!t.m_pOwner)
			get_ParentObj().TryAssignTask(t);
	}
}

void Node::Processor::OnPeerInsane(const PeerID& peerID)
{
	// Deleting the insane peer in-place is dangerous, because we may be invoked in its context.
	// Use "async-delete mechanism
	if (!m_pAsyncPeerInsane)
	{
		io::AsyncEvent::Callback cb = [this]() { FlushInsanePeers(); };
		m_pAsyncPeerInsane = io::AsyncEvent::create(io::Reactor::get_Current(), std::move(cb));
	}

	m_lstInsanePeers.push_back(peerID);
	m_pAsyncPeerInsane->get_trigger()();
}

void Node::Processor::FlushInsanePeers()
{
	for (; !m_lstInsanePeers.empty(); m_lstInsanePeers.pop_front())
	{
		bool bCreate = false;
		PeerMan::PeerInfoPlus* pInfo = Cast::Up<PeerMan::PeerInfoPlus>(get_ParentObj().m_PeerMan.Find(m_lstInsanePeers.front(), bCreate));

		if (pInfo)
		{
			Peer* pPeer = pInfo->m_Live.m_p;
			if (pPeer)
				pPeer->DeleteSelf(true, proto::NodeConnection::ByeReason::Ban);
			else
				get_ParentObj().m_PeerMan.Ban(*pInfo);

		}
	}
}

void Node::Processor::OnNewState()
{
	m_Cwp.Reset();

	if (!IsTreasuryHandled())
		return;

	BEAM_LOG_INFO() << "My Tip: " << LogTip(m_Cursor);

	if (IsFastSync())
		return;

	get_ParentObj().m_TxReject.clear();
	get_ParentObj().DeleteOutdated(); // Better to delete all irrelevant txs explicitly, even if the node is supposed to mine
	// because in practice mining could be OFF (for instance, if miner key isn't defined, and owner wallet is offline).
	get_ParentObj().m_TxDependent.Clear();

	if (get_ParentObj().m_Miner.IsEnabled())
	{
		get_ParentObj().m_Miner.HardAbortSafe();
		get_ParentObj().m_Miner.SetTimer(0, true); // async start mining
	}

	bool bDonSendTip = m_Cursor.m_Full.m_Number.v && IsFastSync();
	if (!bDonSendTip)
	{
		const Rules& r = Rules::get();

		proto::NewTip msg;
		msg.m_Description = m_Cursor.m_Full;

		bool bSendStamp = get_ParentObj().m_Validator.ShouldSendStamp();

		for (PeerList::iterator it = get_ParentObj().m_lstPeers.begin(); get_ParentObj().m_lstPeers.end() != it; ++it)
		{
			Peer& peer = *it;
			if (!(Peer::Flags::Connected & peer.m_Flags))
				continue;

			if (Rules::Consensus::Pbft == r.m_Consensus)
			{
				// In pbft send our tip even if it's the same as peer's
				if (m_Cursor.m_hh.m_Height < peer.m_Tip.m_hh.m_Height)
					continue;
			}
			else
			{
				if (msg.m_Description.m_Number.v)
				{
					// send our tip if it's either ahead OR same chainwork but different from peer's
					if (!peer.m_Tip.IsRemoteNeeded(m_Cursor))
						continue;
				}
				else
				{
					if (Peer::Flags::HasTreasury & peer.m_Flags)
						continue;
				}
			}

			peer.Send(msg);

			if (bSendStamp)
			{
				// TODO: don't send stamp iff this peer participated in the most recent round
				peer.PbftSendStamp();
			}
		}
	}

	get_ParentObj().RefreshCongestions();

	IObserver* pObserver = get_ParentObj().m_Cfg.m_Observer;
	if (pObserver)
		pObserver->OnStateChanged();

	get_ParentObj().MaybeGenerateRecovery();

	get_ParentObj().m_Validator.OnNewState();
}

void Node::Processor::OnFastSyncSucceeded()
{
	if (m_vAccounts.empty())
		return;

	auto hTxoHi = Num2Height(m_Extra.m_TxoHi);

	for (auto& acc : m_vAccounts)
	{
		acc.m_hTxoHi = hTxoHi;
		get_DB().SetAccountTxoHi(acc);
	}

	for (PeerList::iterator it = get_ParentObj().m_lstPeers.begin(); get_ParentObj().m_lstPeers.end() != it; ++it)
	{
		Peer& peer = *it;
		peer.m_Flags &= ~Peer::Flags::SerifSent;
		peer.MaybeSendSerif();
	}
}

void Node::MaybeGenerateRecovery()
{
	if (!m_PostStartSynced || m_Cfg.m_Recovery.m_sPathOutput.empty() || !m_Cfg.m_Recovery.m_Granularity)
		return;

	Height h0 = m_Processor.get_DB().ParamIntGetDef(NodeDB::ParamID::LastRecoveryHeight);
	Height h1 = m_Processor.m_Cursor.m_hh.m_Height;
	if (h1 < h0 + m_Cfg.m_Recovery.m_Granularity)
		return;

	BEAM_LOG_INFO() << "Generating recovery...";

	std::ostringstream os;
	os
		<< m_Cfg.m_Recovery.m_sPathOutput
		<< m_Processor.m_Cursor.m_hh;

	std::string sPath = os.str();

	std::string sTmp = sPath;
	sTmp += ".tmp";

	bool bOk = GenerateRecoveryInfo(sTmp.c_str());
	if (bOk)
	{
#ifdef WIN32
		bOk =
			MoveFileExW(Utf8toUtf16(sTmp.c_str()).c_str(), Utf8toUtf16(sPath.c_str()).c_str(), MOVEFILE_REPLACE_EXISTING) ||
			(GetLastError() == ERROR_FILE_NOT_FOUND);
#else // WIN32
		bOk =
			!rename(sTmp.c_str(), sPath.c_str()) ||
			(ENOENT == errno);
#endif // WIN32
	}

	if (bOk) {
		BEAM_LOG_INFO() << "Recovery generation done";
		m_Processor.get_DB().ParamIntSet(NodeDB::ParamID::LastRecoveryHeight, h1);
	} else
	{
		BEAM_LOG_INFO() << "Recovery generation failed";
		beam::DeleteFile(sTmp.c_str());
	}
}

void Node::Processor::OnRolledBack()
{
	BEAM_LOG_INFO() << "Rolled back to: " << LogTip(m_Cursor);

	TxPool::Fluff& txp = get_ParentObj().m_TxPool;
	while (!txp.m_lstOutdated.empty())
	{
		TxPool::Fluff::Element& x = txp.m_lstOutdated.back().get_ParentObj();
		if (x.m_Hist.m_Height <= m_Cursor.m_hh.m_Height)
			break;

		txp.SetState(x, TxPool::Fluff::State::Fluffed); // may be deferred by the next loop
	}

	// Shielded txs that referenced shielded outputs which were reverted - must be reprocessed
	for (TxPool::Fluff::ProfitSet::iterator it = txp.m_setProfit.begin(); txp.m_setProfit.end() != it; )
	{
		TxPool::Fluff::Element& x = (it++)->get_ParentObj();
		if (!IsShieldedInPool(*x.m_pValue))
		{
			get_ParentObj().OnTransactionDeferred(std::move(x.m_pValue), nullptr, nullptr, true);
			get_ParentObj().m_TxPool.Delete(x);
		}
	}

	TxPool::Stem& txps = get_ParentObj().m_Dandelion;
	for (TxPool::Stem::TimeSet::iterator it = txps.m_setTime.begin(); txps.m_setTime.end() != it; )
	{
		TxPool::Stem::Element& x = (it++)->get_ParentObj();
		if (!IsShieldedInPool(*x.m_pValue))
			txps.Delete(x);
	}

	while (!txp.m_lstWaitFluff.empty())
	{
		TxPool::Fluff::Element& x = txp.m_lstWaitFluff.back().get_ParentObj();
		if (x.m_Hist.m_Height <= m_Cursor.m_hh.m_Height)
			break;

		txp.Delete(x); // never mind
	}

	get_ParentObj().m_TxDependent.Clear();

	IObserver* pObserver = get_ParentObj().m_Cfg.m_Observer;
	if (pObserver)
		pObserver->OnRolledBack();

	get_ParentObj().m_Validator.OnRolledBack();
}

void Node::Processor::MyExecutorMT::RunThread(uint32_t iThread)
{
	MyExecutor::MyContext ctx;
	ctx.m_iThread = iThread;
	ECC::InnerProduct::BatchContext::Scope scope(ctx.m_BatchCtx);

	RunThreadCtx(ctx);
}

void Node::Processor::OnModified()
{
	if (!m_bFlushPending)
	{
		if (!m_pFlushTimer)
			m_pFlushTimer = io::Timer::create(io::Reactor::get_Current());

		m_pFlushTimer->start(50, false, [this]() { OnFlushTimer(); });

		m_bFlushPending = true;
	}
}

void Node::Processor::TryGoUpAsync()
{
	if (!m_bGoUpPending)
	{
		if (!m_pGoUpTimer)
			m_pGoUpTimer = io::Timer::create(io::Reactor::get_Current());

		m_pGoUpTimer->start(0, false, [this]() { OnGoUpTimer(); });

		m_bGoUpPending = true;
	}
}

void Node::Processor::OnGoUpTimer()
{
	m_bGoUpPending = false;
	TryGoUp();
	get_ParentObj().RefreshCongestions();
	get_ParentObj().UpdateSyncStatus();
}

void Node::Processor::Stop()
{
	m_ExecutorMT.Stop();
	m_bGoUpPending = false;
	m_bFlushPending = false;

	if (m_pGoUpTimer)
	{
		m_pGoUpTimer->cancel();
	}

	if (m_pFlushTimer)
	{
		m_pFlushTimer->cancel();
	}
}

uint32_t Node::Processor::get_MaxAutoRollback()
{
	uint32_t h = NodeProcessor::get_MaxAutoRollback();
	if (m_Cursor.m_Full.m_Number.v)
	{
		Timestamp ts_s = getTimestamp();
		if (ts_s < m_Cursor.m_Full.m_TimeStamp + get_ParentObj().m_Cfg.m_RollbackLimit.m_TimeoutSinceTip_s)
			beam::setmin(h, get_ParentObj().m_Cfg.m_RollbackLimit.m_Max);
	}
	return h;
}

void Node::Processor::OnEvent(Height h, const proto::Event::Base& evt)
{
	if (get_ParentObj().m_Cfg.m_LogEvents)
	{
		std::ostringstream os;
		os << "Event Height=" << h << ", ";
		evt.Dump(os);

		BEAM_LOG_INFO() << os.str();
	}
}

void Node::Processor::OnDummy(const CoinID& cid, Height)
{
	NodeDB& db = get_DB();
	if (db.GetDummyHeight(cid) != MaxHeight)
		return;

	// recovered
	Height h = get_ParentObj().SampleDummySpentHeight();
	h += get_ParentObj().m_Cfg.m_Dandelion.m_DummyLifetimeHi * 2; // add some factor, to make sure the original creator node will spent it before us (if it's still running)

	db.InsertDummy(h, cid);
}

void Node::Processor::InitializeUtxosProgress(uint64_t done, uint64_t total)
{
	auto& node = get_ParentObj();

	if (node.m_Cfg.m_Observer)
		node.m_Cfg.m_Observer->InitializeUtxosProgress(done, total);   
}

void Node::Processor::OnFlushTimer()
{
	m_bFlushPending = false;
	CommitDB();
}

void Node::Processor::FlushDB()
{
	if (m_bFlushPending)
	{
		assert(m_pFlushTimer);
		m_pFlushTimer->cancel();

		OnFlushTimer();
	}
}

void Node::Processor::OnInvalidBlock(const Block::SystemState::Full& s, const Block::Body& block)
{
	if (!get_ParentObj().m_Miner.IsEnabled())
		return;

	// format path
	std::string sPath = get_ParentObj().m_Cfg.m_sPathLocal;
	{
		for (uint32_t i = (uint32_t) sPath.size(); ; )
		{
			if (!i--)
			{
				sPath.clear();
				break;
			}

			if (('/' == sPath[i]) || ('\\' == sPath[i]))
			{
				sPath.resize(i + 1);
				break;
			}
		}

		Merkle::Hash hv;
		s.get_Hash(hv);

		std::ostringstream os;
		os << s.m_Number.v << '-' << hv << ".inv_block";
		sPath += os.str();
	}

	beam::FStream fs;
	if (fs.Open(sPath.c_str(), false))
	{
		Serializer ser;
		ser & s;
		ser & block;

		fs.write(ser.buffer().first, ser.buffer().second);
	}
}

void Node::Processor::GenerateProofStateStrict(Merkle::HardProof& proof, Block::Number num)
{
	assert(num.v < m_Cursor.m_Full.m_Number.v);

	Merkle::ProofBuilderHard bld;

	m_Mmr.m_States.get_Proof(bld, m_Mmr.m_States.N2I(num));

	proof.swap(bld.m_Proof);

	struct MyProofBuilder
		:public ProofBuilderHard
	{
		using ProofBuilderHard::ProofBuilderHard;
		bool get_History(Merkle::Hash&) override { return false; }
	};

	MyProofBuilder pb(*this, proof);
	pb.GenerateProof();
}

void Node::Processor::GenerateProofShielded(Merkle::Proof& p, const uintBigFor<TxoID>::Type& mmrIdx)
{
	TxoID nIdx;
	mmrIdx.Export(nIdx);

	m_Mmr.m_Shielded.get_Proof(p, nIdx);

	struct MyProofBuilder
		:public NodeProcessor::ProofBuilder
	{
		using ProofBuilder::ProofBuilder;
		bool get_Shielded(Merkle::Hash&) override { return false; }
	};

	MyProofBuilder pb(*this, p);
	pb.GenerateProof();
}

bool Node::Processor::BuildCwp()
{
	if (!m_Cwp.IsEmpty())
		return true; // already built

	if (!m_Cursor.m_Full.m_Number.v)
		return false;

	struct Source
		:public Block::ChainWorkProof::ISource
	{
		Processor& m_Proc;

		Source(Processor& proc)
			:m_Proc(proc)
		{}

		void get_StateAt(Block::SystemState::Full& s, const Difficulty::Raw& d) override
		{
			uint64_t rowid = m_Proc.get_DB().FindStateWorkGreater(d);
			m_Proc.get_DB().get_State(rowid, s);
		}

		void get_Proof(Merkle::IProofBuilder& bld, Block::Number num) override
		{
			m_Proc.m_Mmr.m_States.get_Proof(bld, num.v ? (num.v - 1) : 0);
		}
	};

	Source src(*this);

	m_Cwp.Create(src, m_Cursor.m_Full);

	Evaluator ev(*this);
	ev.get_Live(m_Cwp.m_hvRootLive);

	return true;
}

NodeProcessor::IPbftHandler* Node::Processor::get_PbftHandler()
{
	assert(Rules::Consensus::Pbft == Rules::get().m_Consensus);
	return &get_ParentObj().m_Validator;
}

} // namespace beam
