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

#pragma once

#include "processor_common.h"
#include "processor_bic.h"

namespace beam {

struct NodeProcessor::MultiSigmaContext
{
	static const uint32_t s_Chunk = 0x400;

	struct Node
	{
		struct ID
			:public boost::intrusive::set_base_hook<>
		{
			TxoID m_Value;
			bool operator < (const ID& x) const { return (m_Value < x.m_Value); }

			IMPLEMENT_GET_PARENT_OBJ(Node, m_ID)
		} m_ID;

		ECC::Scalar::Native m_pS[s_Chunk];
		uint32_t m_Min, m_Max;

		typedef boost::intrusive::multiset<ID> IDSet;
	};


	std::mutex m_Mutex;
	Node::IDSet m_Set;

	void Add(TxoID id0, uint32_t nCount, const ECC::Scalar::Native*);
	void ClearLocked();

	~MultiSigmaContext()
	{
		ClearLocked();
	}

	void Calculate(ECC::Point::Native&, NodeProcessor&);

private:

	struct MyTask;

	void DeleteRaw(Node&);
	std::vector<ECC::Point::Native> m_vRes;

	virtual Sigma::CmList& get_List() = 0;
	virtual void PrepareList(NodeProcessor&, const Node&) = 0;
};

struct NodeProcessor::MultiShieldedContext
	:public NodeProcessor::MultiSigmaContext
{
	ValidatedCache m_Vc;

	void MoveToGlobalCache(ValidatedCache& vc)
	{
		m_Vc.MoveInto(vc);
		vc.ShrinkTo(10 * 1024);
	}

	bool IsValid(const TxVectors::Eternal&, Height, ECC::InnerProduct::BatchContext&, uint32_t iVerifier, uint32_t nTotal, ValidatedCache&);
	void Prepare(const TxVectors::Eternal&, NodeProcessor&, Height);

private:

	Sigma::CmListVec m_Lst;

	bool IsValid(const TxKernelShieldedInput&, Height hScheme, std::vector<ECC::Scalar::Native>& vBuf, ECC::InnerProduct::BatchContext&);

	Sigma::CmList& get_List() override
	{
		return m_Lst;
	}

	void PrepareList(NodeProcessor& np, const Node& n) override
	{
		m_Lst.m_vec.resize(s_Chunk); // will allocate if empty
		np.get_DB().ShieldedRead(n.m_ID.m_Value + n.m_Min, &m_Lst.m_vec.front() + n.m_Min, n.m_Max - n.m_Min);
	}

	struct Walker
		:public TxKernel::IWalker
	{
		virtual bool OnKrn(const TxKernelShieldedInput&) = 0;

		bool OnKrn(const TxKernel& krn) override
		{
			if (TxKernel::Subtype::ShieldedInput != krn.get_Subtype())
				return true;
			return OnKrn(Cast::Up<TxKernelShieldedInput>(krn));
		}

	};
};

struct NodeProcessor::MultiAssetContext
	:public NodeProcessor::MultiSigmaContext
{
	struct BatchCtx
		:public Asset::Proof::BatchContext
	{
		MultiAssetContext& m_Ctx;
		BatchCtx(MultiAssetContext& ctx) :m_Ctx(ctx) {}

		std::vector<ECC::Scalar::Native> m_vKs;

		bool IsValid(Height, ECC::Point::Native& hGen, const Asset::Proof&) override;
	};

private:

	struct List
		:public Sigma::CmList
	{
		Asset::ID m_Begin;
		bool get_At(ECC::Point::Storage& pt_s, uint32_t iIdx) override
		{
			Asset::Base(m_Begin + iIdx).get_GeneratorSafe(pt_s);
			return true;
		}

	} m_Lst;

	Sigma::CmList& get_List() override
	{
		return m_Lst;
	}

	void PrepareList(NodeProcessor& np, const Node& n) override
	{
		static_assert(sizeof(n.m_ID.m_Value) >= sizeof(m_Lst.m_Begin));

		// TODO: maybe cache it in DB
		m_Lst.m_Begin = static_cast<Asset::ID>(n.m_ID.m_Value);
	}
};

struct NodeProcessor::MultiblockContext
{
	NodeProcessor& m_This;

	std::mutex m_Mutex;

	TxoID m_id0;
	Block::NumberRange m_InProgress;
	PeerID  m_pidLast;
	bool m_bMultiPeer = false; // m_InProgress has blocks of several peers. If it fails - m_pidLast is not necessarily the culprit

	MultiblockContext(NodeProcessor& np)
		:m_This(np)
	{
		m_InProgress.m_Max = m_This.m_Cursor.m_Full.m_Number;
		m_InProgress.m_Min.v = m_InProgress.m_Max.v + 1;
		assert(m_InProgress.IsEmpty());

		m_id0 = m_This.get_TxosBefore(Block::Number(m_This.m_SyncData.m_n0.v + 1)); // inputs of blocks below TxLo must be before this

		if (m_This.IsFastSync())
			m_Sigma.Import(m_This.m_SyncData.m_Sigma);

		m_pidLast = Zero;
	}

	~MultiblockContext()
	{
		m_pGroup.reset(); // not pushed yet, drop
		m_This.get_Executor().Flush();

		if (m_bBatchDirty)
		{
			// make sure we don't leave batch context in an invalid state
			struct Task0 :public Executor::TaskSync {
				void Exec(Executor::Context&) override
				{
					ECC::InnerProduct::BatchContext* pBc = ECC::InnerProduct::BatchContext::s_pInstance;
					if (pBc)
						pBc->Reset();
				}
			};

			Task0 t;
			m_This.get_Executor().ExecAll(t);
		}
	}

	ECC::Scalar::Native m_Offset;
	ECC::Point::Native m_Sigma;

	MultiShieldedContext m_Msc;
	MultiAssetContext m_Mac;

	size_t m_SizePending = 0;
	std::atomic<bool> m_bFail = false;
	bool m_bBatchDirty = false;
	std::string m_sErr;

	struct MyTask
		:public Executor::TaskAsync
	{
		void Exec(Executor::Context&) override;
		virtual ~MyTask() {}

		struct Shared
		{
			typedef std::shared_ptr<Shared> Ptr;

			MultiblockContext& m_Mbc;
			uint32_t m_Done;

			Shared(MultiblockContext& mbc)
				:m_Mbc(mbc)
				,m_Done(0)
			{
			}

			virtual ~Shared() {} // auto

			virtual void Exec(uint32_t iVerifier) = 0;
		};

		struct SharedBlock
			:public Shared
		{
			typedef std::shared_ptr<SharedBlock> Ptr;

			Block::Body m_Body;
			size_t m_Size;
			TxBase::Context m_Ctx;
			Block::Number m_Number;

			SharedBlock(MultiblockContext& mbc, Block::Number num)
				:Shared(mbc)
				,m_Number(num)
			{
				m_Ctx.m_Params.m_Kind = TxBase::Kind::Block;
			}

			virtual ~SharedBlock() {} // auto

			void Exec(uint32_t iVerifier) override;
		};

		struct SharedContractSigs
			:public Shared
		{
			std::vector<BlockInterpretCtx::ContractSig> m_vSigs;
			uint32_t m_nVerifiers;

			using Shared::Shared;
			virtual ~SharedContractSigs() {} // auto

			void Exec(uint32_t iVerifier) override;
		};

		// Items pushed together: each verifier runs its part of every item in one task
		typedef std::vector<Shared::Ptr> Group;

		std::shared_ptr<Group> m_pGroup;
		uint32_t m_iVerifier;
	};

	// Items not pushed to the executor yet
	std::shared_ptr<MyTask::Group> m_pGroup;
	size_t m_SizeGroup = 0;

	bool Flush()
	{
		FlushInternal();
		return !m_bFail;
	}

	void FlushInternal()
	{
		if (m_bFail || m_InProgress.IsEmpty())
			return;

		PushGroup();

		Executor& ex = m_This.get_Executor();
		ex.Flush();

		if (m_bFail)
			return;

		if (m_bBatchDirty)
		{
			struct Task1 :public Executor::TaskSync
			{
				MultiblockContext* m_pMbc;
				ECC::Point::Native* m_pBatchSigma;
				void Exec(Executor::Context&) override
				{
					ECC::InnerProduct::BatchContext* pBc = ECC::InnerProduct::BatchContext::s_pInstance;
					if (pBc && !pBc->Flush())
					{
						{
							std::unique_lock<std::mutex> scope(m_pMbc->m_Mutex);
							(*m_pBatchSigma) += pBc->m_Sum;

						}
						pBc->m_Sum = Zero;
					}
				}
			};

			ECC::Point::Native ptBatchSigma;

			Task1 t;
			t.m_pMbc = this;
			t.m_pBatchSigma = &ptBatchSigma;
			ex.ExecAll(t);
			assert(!m_bFail);
			m_bBatchDirty = false;

			m_Msc.Calculate(ptBatchSigma, m_This);
			m_Mac.Calculate(ptBatchSigma, m_This);

			if (!(ptBatchSigma == Zero))
			{
				m_sErr = "Sigma nnz";
				m_bFail = true;
				return;
			}
		}

		if (m_This.IsFastSync())
		{
			if (!(m_Offset == Zero))
			{
				ECC::Mode::Scope scopeFast(ECC::Mode::Fast);

				m_Sigma += ECC::Context::get().G * m_Offset;
				m_Offset = Zero;
			}

			if (m_InProgress.m_Max.v == m_This.m_SyncData.m_TxoLo.v)
			{
				BEAM_LOG_INFO() << "TxoLo reached. Finalyzing multi-block balance";

				if (m_Sigma != Zero)
				{
					m_bFail = true;
					m_sErr = "multi-block Sigma nnz";
					OnFastSyncFailedOnLo();

					return;
				}
			}

			m_Sigma.Export(m_This.m_SyncData.m_Sigma);
			m_This.SaveSyncData();
		}
		else
		{
			assert(m_Offset == Zero);
			assert(m_Sigma == Zero);
		}

		m_InProgress.m_Min.v = m_InProgress.m_Max.v + 1;
		m_bMultiPeer = false;

		m_Msc.MoveToGlobalCache(m_This.m_ValCache);
	}

	void OnNextBlockPid(const PeerID& pid)
	{
		if (m_bFail)
			return;

		if (!m_InProgress.IsEmpty())
		{
			bool bPidChanged = (m_pidLast != pid);

			bool bMustFlush =
				(m_InProgress.m_Max.v == m_This.m_SyncData.m_TxoLo.v) || // range complete up to TxLo
				(bPidChanged && m_This.m_bPeerBarrier); // a range of several peers failed before, find the culprit

			if (bMustFlush)
			{
				if (!Flush())
					return;
			}
			else
			{
				if (bPidChanged)
					m_bMultiPeer = true;
			}
		}

		m_pidLast = pid;
	}

	void OnBlock(const MyTask::SharedBlock::Ptr& pShared)
	{
		assert(pShared->m_Ctx.m_Height.m_Min == pShared->m_Ctx.m_Height.m_Max);
		assert(pShared->m_Ctx.m_Height.m_Min > m_This.m_Cursor.m_hh.m_Height);

		if (m_bFail)
			return;

		const size_t nSizeMax = 1024 * 1024 * 10; // fair enough

		Executor& ex = m_This.get_Executor();
		for (uint32_t nTasks = static_cast<uint32_t>(-1); ; )
		{
			{
				std::unique_lock<std::mutex> scope(m_Mutex);
				if (m_SizePending <= nSizeMax)
				{
					m_SizePending += pShared->m_Size;
					break;
				}
			}

			PushGroup(); // the pending size includes the items not pushed yet

			assert(nTasks);
			nTasks = ex.Flush(nTasks - 1);
		}

		m_InProgress.m_Max = pShared->m_Number;

		bool bFull = (pShared->m_Number.v > m_This.m_SyncData.m_Target.m_Number.v);

		pShared->m_Ctx.m_Params.m_Kind = bFull ? TxBase::Kind::Block : TxBase::Kind::SparseBlock;
		pShared->m_Ctx.m_Params.m_pAbort = &m_bFail;
		pShared->m_Ctx.m_Params.m_nVerifiers = ex.get_Threads();

		// pre-Realize all the kernels, since they'll be tested asynchronously (in worker threads)
		for (const auto& pKrn : pShared->m_Body.m_vKernels)
			pKrn->EnsureID();

		m_Msc.Prepare(pShared->m_Body, m_This, pShared->m_Ctx.m_Height.m_Min);

		AddToGroup(pShared, pShared->m_Size);
	}

	void OnContractSigs(std::vector<BlockInterpretCtx::ContractSig>&& vSigs)
	{
		if (m_bFail)
			return;

		auto pShared = std::make_shared<MyTask::SharedContractSigs>(*this);
		pShared->m_vSigs = std::move(vSigs);
		pShared->m_nVerifiers = m_This.get_Executor().get_Threads();

		AddToGroup(pShared, 0);
	}

	void PushTasks(const MyTask::Shared::Ptr& pShared, TxBase::Context::Params& pars)
	{
		Executor& ex = m_This.get_Executor();

		pars.m_pAbort = &m_bFail;
		pars.m_nVerifiers = ex.get_Threads();

		AddToGroup(pShared, 0);
		PushGroup();
	}

	void AddToGroup(const MyTask::Shared::Ptr& pShared, size_t nSize)
	{
		// Pushing a task costs a mutex lock and a thread wake-up per verifier. Small blocks are pushed in groups.
		const size_t nGroupItemsMax = 32;
		const size_t nGroupSizeMax = 1024 * 256;

		m_bBatchDirty = true;

		if (!m_pGroup)
			m_pGroup = std::make_shared<MyTask::Group>();

		m_pGroup->push_back(pShared);
		m_SizeGroup += nSize;

		if ((m_pGroup->size() >= nGroupItemsMax) || (m_SizeGroup >= nGroupSizeMax))
			PushGroup();
	}

	void PushGroup()
	{
		if (!m_pGroup)
			return;

		Executor& ex = m_This.get_Executor();
		uint32_t nVerifiers = ex.get_Threads();

		for (uint32_t i = 0; i < nVerifiers; i++)
		{
			auto pTask = std::make_unique<MyTask>();
			pTask->m_pGroup = m_pGroup;
			pTask->m_iVerifier = i;
			ex.Push(std::move(pTask));
		}

		m_pGroup.reset();
		m_SizeGroup = 0;
	}

	void OnFastSyncFailed(bool bDeleteBlocks)
	{
		// rapid rollback
		m_This.RollbackTo(m_This.m_SyncData.m_n0);
		m_InProgress.m_Max = m_This.m_Cursor.m_Full.m_Number;
		m_InProgress.m_Min.v = m_InProgress.m_Max.v + 1;

		if (bDeleteBlocks)
			m_This.DeleteBlocksInRange(m_This.m_SyncData.m_Target, m_This.m_SyncData.m_n0);

		m_This.m_SyncData.m_Sigma = Zero;

		if (m_This.m_SyncData.m_TxoLo.v > m_This.m_SyncData.m_n0.v)
		{
			BEAM_LOG_INFO() << "Retrying with lower TxLo";
			m_This.m_SyncData.m_TxoLo = m_This.m_SyncData.m_n0;
		}
		else {
			BEAM_LOG_WARNING() << "TxLo already low";
		}

		m_This.SaveSyncData();

		m_pidLast = Zero; // don't blame the last peer for the failure!
		m_bMultiPeer = false; // the range is already rolled back and deleted
	}

	void OnFastSyncFailedOnLo()
	{
		// probably problem in lower blocks
		BEAM_LOG_WARNING() << "Fast-sync failed on first above-TxLo block.";
		m_pidLast = Zero; // don't blame the last peer
		OnFastSyncFailed(true);
	}
};

} // namespace beam
