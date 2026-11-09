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

bool Node::Miner::IsEnabled() const 
{
	if (!get_ParentObj().m_Keys.m_pOwner)
		return false;

	if (!get_ParentObj().m_Cfg.m_pExternalPOW && m_vThreads.empty())
		return false;

	if (Rules::Consensus::PoW == Rules::get().m_Consensus)
		return true;

	if (get_ParentObj().m_Cfg.m_TestMode.m_FakePowSolveTime_ms > 0)
		return true;

	return m_FakeBlocksToGenerate > 0;
}

void Node::Miner::Initialize()
{
	const Config& cfg = get_ParentObj().m_Cfg;
	if (!cfg.m_MiningThreads && !cfg.m_pExternalPOW)
		return;

	if (Rules::Consensus::Pbft == Rules::get().m_Consensus)
		return;

	m_LastRestart_ms = 0;
	m_pEvtMined = io::AsyncEvent::create(io::Reactor::get_Current(), [this]() { OnMined(); });

	if (cfg.m_MiningThreads) {
		m_vThreads.resize(cfg.m_MiningThreads);
		for (uint32_t i = 0; i < cfg.m_MiningThreads; i++) {
			PerThread &pt = m_vThreads[i];
			pt.m_pReactor = io::Reactor::create();
			pt.m_pEvt = io::AsyncEvent::create(*pt.m_pReactor, [this, i]() { OnRefresh(i); });
			pt.m_Thread = std::thread(&Miner::RunMinerThread, this, pt.m_pReactor, Rules::get());
		}
	}

	SetTimer(0, true); // async start mining
}

void Node::Miner::RunMinerThread(const io::Reactor::Ptr& pReactor, const Rules& r)
{
	Rules::Scope scopeRules(r);
	pReactor->run();
}

void Node::Miner::OnFinalizerChanged(Peer* p)
{
	// always prefer newer (in case there are several ones)
	m_pFinalizer = p;
	if (!m_pFinalizer)
	{
		// try to find another one
		for (PeerList::iterator it = get_ParentObj().m_lstPeers.begin(); get_ParentObj().m_lstPeers.end() != it; ++it)
			if (it->ShouldFinalizeMining())
			{
				m_pFinalizer = &(*it);
				break;
			}
	}

	Restart();
}

void Node::Miner::OnRefresh(uint32_t iIdx)
{
	while (true)
	{
		Task::Ptr pTask;
		Block::SystemState::Full s;

		{
			std::scoped_lock<std::mutex> scope(m_Mutex);
			if (!m_pTask || *m_pTask->m_pStop)
				break;

			pTask = m_pTask;
			s = pTask->m_Hdr; // local copy
		}

		ECC::Hash::Value hv; // pick pseudo-random initial nonce for mining.
		ECC::Hash::Processor()
			<< pTask->m_hvNonceSeed
			<< iIdx
			>> hv;

		static_assert(s.m_PoW.m_Nonce.nBytes <= hv.nBytes);
		s.m_PoW.m_Nonce = hv;

		Block::PoW::Cancel fnCancel = [this, pTask](bool bRetrying)
		{
			if (*pTask->m_pStop)
				return true;

			if (bRetrying)
			{
				std::scoped_lock<std::mutex> scope(m_Mutex);
				if (pTask != m_pTask)
					return true; // soft restart triggered
			}

			return false;
		};

		if (Rules::Consensus::FakePoW == Rules::get().m_Consensus)
		{
			uint32_t timeout_ms = get_ParentObj().m_Cfg.m_TestMode.m_FakePowSolveTime_ms;

			bool bSolved = timeout_ms == 0;

			for (uint32_t t0_ms = GetTime_ms(); !bSolved; )
			{
				if (fnCancel(true))
					break;
				std::this_thread::sleep_for(std::chrono::milliseconds(50));

				uint32_t dt_ms = GetTime_ms() - t0_ms;

				if (dt_ms >= timeout_ms)
				{
					bSolved = true;
					break;
				}
			}

			if (!bSolved)
				continue;

			ZeroObject(s.m_PoW.m_Indices); // keep the difficulty intact
		}
		else
		{
			try
			{
				if (!s.GeneratePoW(fnCancel))
					continue;
			}
			catch (const std::exception& ex)
			{
				BEAM_LOG_DEBUG() << ex.what();
				break;
			}
		}

		std::scoped_lock<std::mutex> scope(m_Mutex);

		if (*pTask->m_pStop)
			continue; // either aborted, or other thread was faster

		pTask->m_Hdr = s; // save the result
		*pTask->m_pStop = true;
		m_pTask = pTask; // In case there was a soft restart we restore the one that we mined.

		m_pEvtMined->post();
		break;
	}
}

void Node::Miner::HardAbortSafe()
{
	m_pTaskToFinalize.reset();
	m_FeesTrg = 0;

	std::scoped_lock<std::mutex> scope(m_Mutex);

	if (m_pTask)
	{
		*m_pTask->m_pStop = true;
		m_pTask.reset();
	}

	if (get_ParentObj().m_Cfg.m_pExternalPOW)
	{
		bool bHadTasks = false;

		for (size_t i = 0; i < _countof(m_External.m_ppTask); i++)
		{
			Task::Ptr& pTask = m_External.m_ppTask[i];
			if (!pTask)
				continue;

			assert(*pTask->m_pStop); // should be the same stop inficator
			pTask.reset();
			bHadTasks = true;
		}

		if (bHadTasks)
			get_ParentObj().m_Cfg.m_pExternalPOW->stop_current();
	}
}

void Node::Miner::SoftRestart()
{
	if (!IsEnabled() || m_pTaskToFinalize)
		return;

	uint32_t nTimeout_ms = 0;
	if (m_LastRestart_ms)
	{
		uint32_t nMin_ms = get_ParentObj().m_Cfg.m_Timeout.m_MiningSoftRestart_ms;
		uint32_t nElapsed_ms = GetTimeNnz_ms() - m_LastRestart_ms;
		if (nElapsed_ms < nMin_ms)
			nTimeout_ms = nMin_ms - nElapsed_ms;
	}

	SetTimer(nTimeout_ms, false);
}

void Node::Miner::SetTimer(uint32_t timeout_ms, bool bHard)
{
	if (!IsEnabled())
		return;

	if (!m_pTimer)
		m_pTimer = io::Timer::create(io::Reactor::get_Current());
	else
		if (m_bTimerPending && !bHard)
			return;

	m_pTimer->start(timeout_ms, false, [this]() { OnTimer(); });
	m_bTimerPending = true;
}

void Node::Miner::OnTimer()
{
	m_bTimerPending = false;
	Restart();
}

bool Node::Miner::Restart()
{
	m_LastRestart_ms = GetTimeNnz_ms();

	if (!IsEnabled())
		return false; //  n/a

	if (!get_ParentObj().m_Processor.IsTreasuryHandled() || get_ParentObj().m_Processor.IsFastSync())
		return false;

	m_pTaskToFinalize.reset();

	const Keys& keys = get_ParentObj().m_Keys;

	if (m_pFinalizer)
	{
		if (Peer::Flags::Finalizing & m_pFinalizer->m_Flags)
			return false; // wait until we receive that outdated finalization
	}
	else
		if (!keys.m_pMiner)
			return false; // offline mining is disabled

	NodeProcessor::BlockContext bc(
		get_ParentObj().m_TxPool,
		keys.m_nMinerSubIndex,
		keys.m_pMiner ? *keys.m_pMiner : *keys.m_pGeneric,
		keys.m_pOwner ? *keys.m_pOwner : *keys.m_pGeneric);

	bc.m_pParent = get_ParentObj().m_TxDependent.m_pBest;

	if (m_pFinalizer)
		bc.m_Mode = NodeProcessor::BlockContext::Mode::Assemble;

	bool bRes = get_ParentObj().m_Processor.GenerateNewBlock(bc);

	if (!bRes)
	{
		BEAM_LOG_WARNING() << "Block generation failed, can't mine!";
		return false;
	}

	if (!IsShouldMine(bc))
		return false;

	Task::Ptr pTask(std::make_shared<Task>());
	Cast::Down<NodeProcessor::GeneratedBlock>(*pTask) = std::move(bc);

	if (m_pFinalizer)
	{
		const NodeProcessor::GeneratedBlock& x = *pTask;
		BEAM_LOG_INFO() << "Block generated: Number=" << x.m_Hdr.m_Number.v << ", Fee=" << x.m_Fees << ", Waiting for owner response...";

		proto::GetBlockFinalization msg;
		msg.m_Height = pTask->m_Hdr.get_Height();
		msg.m_Fees = pTask->m_Fees;
		m_pFinalizer->Send(msg);

		assert(!(Peer::Flags::Finalizing & m_pFinalizer->m_Flags));
		m_pFinalizer->m_Flags |= Peer::Flags::Finalizing;

		m_pTaskToFinalize = std::move(pTask);
	}
	else
		StartMining(std::move(pTask));

	return true;
}

bool Node::Miner::IsShouldMine(const NodeProcessor::GeneratedBlock& bc)
{
	if (bc.m_Fees >= m_FeesTrg)
		return true;

	BEAM_LOG_INFO() << "Block generation no change";
	return false;
}

void Node::Miner::StartMining(Task::Ptr&& pTask)
{
	assert(pTask && !m_pTaskToFinalize);

	const NodeProcessor::GeneratedBlock& x = *pTask;
	if (!IsShouldMine(x))
		return;

	BEAM_LOG_INFO() << "Block generated: Number=" << x.m_Hdr.m_Number.v << ", Fee=" << x.m_Fees << ", Difficulty=" << x.m_Hdr.m_PoW.m_Difficulty << ", Size=" << (x.m_Body.m_Perishable.size() + x.m_Body.m_Eternal.size());

	m_FeesTrg = x.m_Fees + 1;

	pTask->m_hvNonceSeed = get_ParentObj().NextNonce();

	// let's mine it.
	{
		std::scoped_lock<std::mutex> scope(m_Mutex);

		if (m_pTask)
		{
			if (*m_pTask->m_pStop)
				return; // block already mined, probably notification to this thread on its way. Ignore the newly-constructed block
			pTask->m_pStop = m_pTask->m_pStop; // use the same soft-restart indicator
		}
		else
		{
			pTask->m_pStop = std::make_shared<std::atomic<bool>>(false);
		}

		m_pTask = std::move(pTask);

		for (size_t i = 0; i < m_vThreads.size(); i++)
			m_vThreads[i].m_pEvt->post();
	}

	OnRefreshExternal();
}

Node::Miner::Task::Ptr& Node::Miner::External::get_At(uint64_t jobID)
{
	return m_ppTask[static_cast<size_t>(jobID % _countof(m_ppTask))];
}

void Node::Miner::OnRefreshExternal()
{
	if (!get_ParentObj().m_Cfg.m_pExternalPOW)
		return;

	// NOTE the mutex is locked here
	BEAM_LOG_INFO() << "New job for external miner";

	uint64_t jobID = ++m_External.m_jobID;
	m_External.get_At(jobID) = m_pTask;

	auto fnCancel = []() { return false; };

	Merkle::Hash hv;
	m_pTask->m_Hdr.get_HashForPoW(hv);

	get_ParentObj().m_Cfg.m_pExternalPOW->new_job(std::to_string(jobID), hv, m_pTask->m_Hdr.m_PoW, m_pTask->m_Hdr.get_Height(), BIND_THIS_MEMFN(OnMinedExternal), fnCancel);
}

IExternalPOW::BlockFoundResult Node::Miner::OnMinedExternal()
{
	std::string jobID_;
	Block::PoW POW;
	Height h;

	assert(get_ParentObj().m_Cfg.m_pExternalPOW);
	get_ParentObj().m_Cfg.m_pExternalPOW->get_last_found_block(jobID_, h, POW);

	uint64_t jobID = 0;
	std::from_chars(jobID_.data(), jobID_.data() + jobID_.size(), jobID); // on failure jobID remains 0

	std::scoped_lock<std::mutex> scope(m_Mutex);

	bool bReject = (m_External.m_jobID - jobID >= _countof(m_External.m_ppTask));

	BEAM_LOG_INFO() << "Solution from external miner. jobID=" << jobID << ", Current.jobID=" << m_External.m_jobID << ", Accept=" << static_cast<uint32_t>(!bReject);

	if (bReject)
	{
		BEAM_LOG_INFO() << "Solution is rejected due it is outdated.";
		return IExternalPOW::solution_expired; // outdated
	}

	Task::Ptr& pTask = m_External.get_At(jobID);

	if (!pTask || *pTask->m_pStop)
	{
		BEAM_LOG_INFO() << "Solution is rejected due block mining has been canceled.";
		return IExternalPOW::solution_rejected; // already cancelled
	}

	pTask->m_Hdr.m_PoW.m_Nonce = POW.m_Nonce;
	pTask->m_Hdr.m_PoW.m_Indices = POW.m_Indices;

	if (!pTask->m_Hdr.IsValidPoW())
	{
		BEAM_LOG_INFO() << "invalid solution from external miner";
		return IExternalPOW::solution_rejected;
	}

	IExternalPOW::BlockFoundResult result(IExternalPOW::solution_accepted);
	Merkle::Hash hv;
	pTask->m_Hdr.get_Hash(hv);
	result._blockhash = to_hex(hv.m_pData, hv.nBytes);

	m_pTask = pTask;
	*m_pTask->m_pStop = true;
	m_pEvtMined->post();

	return result;
}

void Node::Miner::OnMined()
{
	Task::Ptr pTask;
	{
		std::scoped_lock<std::mutex> scope(m_Mutex);
		if (!(m_pTask && *m_pTask->m_pStop))
			return; //?!
		pTask.swap(m_pTask);
	}

	Block::SystemState::ID id;
	pTask->m_Hdr.get_ID(id);

	BEAM_LOG_INFO() << "New block mined: " << id;

	Processor& p = get_ParentObj().m_Processor; // alias
	NodeProcessor::DataStatus::Enum eStatus = p.OnState(pTask->m_Hdr, get_ParentObj().m_MyPublicID);
	switch (eStatus)
	{
	default:
	case NodeProcessor::DataStatus::Invalid:
		// Some bug?
		BEAM_LOG_WARNING() << "Mined block rejected as invalid!";
		return;

	case NodeProcessor::DataStatus::Rejected:
		// Someone else mined exactly the same block!
		BEAM_LOG_WARNING() << "Mined block duplicated";
		return;

	case NodeProcessor::DataStatus::Accepted:
		break; // ok
	}
	assert(NodeProcessor::DataStatus::Accepted == eStatus);

	eStatus = p.OnBlock(id, pTask->m_Body.m_Perishable, pTask->m_Body.m_Eternal, get_ParentObj().m_MyPublicID);
	assert(NodeProcessor::DataStatus::Accepted == eStatus);
	if (m_FakeBlocksToGenerate)
	{
		assert(Rules::Consensus::FakePoW == Rules::get().m_Consensus);
		--m_FakeBlocksToGenerate;
	}
	p.FlushDB();
	p.TryGoUpAsync(); // will likely trigger OnNewState(), and spread this block to the network
}

} // namespace beam
