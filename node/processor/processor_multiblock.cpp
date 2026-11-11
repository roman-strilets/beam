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

#include "processor_multiblock.h"

namespace beam {

void NodeProcessor::MultiSigmaContext::ClearLocked()
{
	while (!m_Set.empty())
		DeleteRaw(m_Set.begin()->get_ParentObj());
}

void NodeProcessor::MultiSigmaContext::DeleteRaw(Node& n)
{
	m_Set.erase(Node::IDSet::s_iterator_to(n.m_ID));
	delete &n;
}

void NodeProcessor::MultiSigmaContext::Add(TxoID id0, uint32_t nCount, const ECC::Scalar::Native* pS)
{
	uint32_t nOffset = static_cast<uint32_t>(id0 % s_Chunk);

	Node::ID key;
	key.m_Value = id0 - nOffset;

	std::unique_lock<std::mutex> scope(m_Mutex);

	while (nCount)
	{
		uint32_t nPortion = std::min(nCount, s_Chunk - nOffset);
		
		Node::IDSet::iterator it = m_Set.find(key);
		bool bNew = (m_Set.end() == it);
		if (bNew)
		{
			Node* pN = new Node;
			pN->m_ID = key;
			m_Set.insert(pN->m_ID);
			it = Node::IDSet::s_iterator_to(pN->m_ID);
		}

		Node& n = it->get_ParentObj();
		if (bNew)
		{
			n.m_Min = nOffset;
			n.m_Max = nOffset + nPortion;
		}
		else
		{
			beam::setmin(n.m_Min, nOffset);
			beam::setmax(n.m_Max, nOffset + nPortion);
		}

		ECC::Scalar::Native* pT = n.m_pS + nOffset;
		for (uint32_t i = 0; i < nPortion; i++)
			pT[i] += pS[i];

		pS += nPortion;
		nCount -= nPortion;
		key.m_Value += s_Chunk;
		nOffset = 0;
	}
}

struct NodeProcessor::MultiSigmaContext::MyTask
	:public Executor::TaskSync
{
	MultiSigmaContext* m_pThis;
	const Node* m_pNode;

	void Exec(Executor::Context& ctx) override
	{
		ECC::Point::Native& val = m_pThis->m_vRes[ctx.m_iThread];
		val = Zero;

		uint32_t i0, nCount;
		ctx.get_Portion(i0, nCount, m_pNode->m_Max - m_pNode->m_Min);
		i0 += m_pNode->m_Min;

		m_pThis->get_List().Calculate(val, i0, nCount, m_pNode->m_pS);
	}
};

void NodeProcessor::MultiSigmaContext::Calculate(ECC::Point::Native& res, NodeProcessor& np)
{
	Executor& ex = np.get_Executor();
	uint32_t nThreads = ex.get_Threads();

	while (!m_Set.empty())
	{
		Node& n = m_Set.begin()->get_ParentObj();
		assert(n.m_Min < n.m_Max);
		assert(n.m_Max <= s_Chunk);

		m_vRes.resize(nThreads);
		PrepareList(np, n);

		MyTask t;
		t.m_pThis = this;
		t.m_pNode = &n;

		ex.ExecAll(t);

		for (uint32_t i = 0; i < nThreads; i++)
			res += m_vRes[i];

		DeleteRaw(n);
	}
}

bool NodeProcessor::MultiShieldedContext::IsValid(const TxKernelShieldedInput& krn, Height hScheme, std::vector<ECC::Scalar::Native>& vKs, ECC::InnerProduct::BatchContext& bc)
{
	const Lelantus::Proof& x = krn.m_SpendProof;
	uint32_t N = x.m_Cfg.get_N();
	if (!N)
		return false;

	vKs.resize(N);
	memset0(&vKs.front(), sizeof(ECC::Scalar::Native) * N);

	ECC::Point::Native hGen;
	if (krn.m_pAsset)
		BEAM_VERIFY(hGen.Import(krn.m_pAsset->m_hGen)); // must already be tested in krn.IsValid();

	ECC::Oracle oracle;
	oracle << krn.get_Msg();

	if (Rules::get().IsPastFork_<3>(hScheme))
	{
		oracle << krn.m_NotSerialized.m_hvShieldedState;
		Asset::Proof::Expose(oracle, hScheme, krn.m_pAsset);
	}

	if (!x.IsValid(bc, oracle, &vKs.front(), &hGen))
		return false;

	TxoID id1 = krn.m_WindowEnd;
	if (id1 >= N)
		Add(id1 - N, N, &vKs.front());
	else
		Add(0, static_cast<uint32_t>(id1), &vKs.front() + N - static_cast<uint32_t>(id1));

	return true;
}

bool NodeProcessor::MultiShieldedContext::IsValid(const TxVectors::Eternal& txve, Height h, ECC::InnerProduct::BatchContext& bc, uint32_t iVerifier, uint32_t nTotal, ValidatedCache& vc)
{
	struct MyWalker
		:public Walker
	{
		std::vector<ECC::Scalar::Native> m_vKs;
		MultiShieldedContext* m_pThis;
		ValidatedCache* m_pVc;
		ECC::InnerProduct::BatchContext* m_pBc;
		uint32_t m_iVerifier;
		uint32_t m_Total;
		Height m_Height;

		bool OnKrn(const TxKernelShieldedInput& v) override
		{
			if (!m_iVerifier)
			{
				ECC::Hash::Value hv;
				{
					ECC::Hash::Processor hp;
					hp.Serialize(v);
					hp << v.m_NotSerialized.m_hvShieldedState;
					hp >> hv;
				}

				bool bFound;
				{
					std::unique_lock<std::mutex> scope(m_pThis->m_Mutex);

					bFound =
						m_pThis->m_Vc.Find(hv) ||
						m_pVc->Find(hv);

					if (!bFound)
						m_pThis->m_Vc.Insert(hv, v.m_WindowEnd);
				}

				if (!bFound && !m_pThis->IsValid(v, m_Height, m_vKs, *m_pBc))
					return false;
			}

			if (++m_iVerifier == m_Total)
				m_iVerifier = 0;

			return true;
		}

	} wlk;
	wlk.m_pThis = this;
	wlk.m_pVc = &vc;
	wlk.m_pBc = &bc;
	wlk.m_iVerifier = iVerifier;
	wlk.m_Total = nTotal;
	wlk.m_Height = h;

	return wlk.Process(txve.m_vKernels);
}

void NodeProcessor::MultiShieldedContext::Prepare(const TxVectors::Eternal& txve, NodeProcessor& np, Height h)
{
	if (!Rules::get().IsPastFork_<3>(h))
		return;

	struct MyWalker
		:public Walker
	{
		NodeProcessor* m_pProc;

		bool OnKrn(const TxKernelShieldedInput& v) override
		{
			auto& hv = Cast::NotConst(v.m_NotSerialized.m_hvShieldedState);
			// set it anyway, even if below HF3. This way the caching is more robust.

			auto nStatePos = v.m_WindowEnd - 1;
			if (nStatePos < m_pProc->m_Extra.m_ShieldedOutputs)
				m_pProc->get_DB().ShieldedStateRead(nStatePos, &hv, 1);
			else
				hv = Zero;

			return true;
		}

	} wlk;

	wlk.m_pProc = &np;

	wlk.Process(txve.m_vKernels);
}

bool NodeProcessor::MultiAssetContext::BatchCtx::IsValid(Height hScheme, ECC::Point::Native& hGen, const Asset::Proof& p)
{
	assert(ECC::InnerProduct::BatchContext::s_pInstance);
	ECC::InnerProduct::BatchContext& bc = *ECC::InnerProduct::BatchContext::s_pInstance;

	const Rules& r = Rules::get();
	const Sigma::Cfg& cfg = r.CA.m_ProofCfg;
	uint32_t N = cfg.get_N();
	if (!N)
		return false; // ?!

	m_vKs.resize(N); // will allocate if empty
	memset0(&m_vKs.front(), sizeof(ECC::Scalar::Native) * N);

	if (!p.IsValidPrepare(hGen, bc, &m_vKs.front()))
		return false;

	if (r.IsPastFork_<6>(hScheme))
	{
		m_Ctx.Add(0, 1, &m_vKs.front());
		m_Ctx.Add(p.m_Begin + 1u, N - 1, &m_vKs.front() + 1);
	}
	else
		m_Ctx.Add(p.m_Begin, N, &m_vKs.front());

	return true;
}

void NodeProcessor::MultiblockContext::MyTask::Exec(Executor::Context&)
{
	assert(m_pGroup && !m_pGroup->empty());

	MultiAssetContext::BatchCtx bcAssets(m_pGroup->front()->m_Mbc.m_Mac);
	Asset::Proof::BatchContext::Scope scopeAssets(bcAssets);

	for (const auto& pShared : *m_pGroup)
		pShared->Exec(m_iVerifier);
}

void NodeProcessor::MultiblockContext::MyTask::SharedContractSigs::Exec(uint32_t iVerifier)
{
	std::string sErr;

	try {
		for (size_t i = iVerifier; i < m_vSigs.size(); i += m_nVerifiers)
		{
			if (m_Mbc.m_bFail)
				return;

			m_vSigs[i].Check();
		}

		return;

	} catch (const std::exception& e) {
		sErr = std::string("Contract signature: ") + e.what();
	}

	std::unique_lock<std::mutex> scope(m_Mbc.m_Mutex);

	if (!m_Mbc.m_bFail)
	{
		m_Mbc.m_bFail = true;
		m_Mbc.m_sErr = std::move(sErr);
	}
}

void NodeProcessor::MultiblockContext::MyTask::SharedBlock::Exec(uint32_t iVerifier)
{
	TxBase::Context ctx;
	ctx.m_Params = m_Ctx.m_Params;
	ctx.m_Height = m_Ctx.m_Height;
	ctx.m_iVerifier = iVerifier;

	bool bSparse = (m_Number.v <= m_Mbc.m_This.m_SyncData.m_TxoLo.v);

	beam::TxBase txbDummy;
	if (bSparse)
		txbDummy.m_Offset = Zero;

	bool bValid = true;
	std::string sErr;

	try {
		ctx.ValidateAndSummarizeStrict(bSparse ? txbDummy : m_Body, m_Body.get_Reader());

		if (!m_Mbc.m_Msc.IsValid(m_Body, m_Ctx.m_Height.m_Min, *ECC::InnerProduct::BatchContext::s_pInstance, iVerifier, m_Ctx.m_Params.m_nVerifiers, m_Mbc.m_This.m_ValCache))
		{
			Exc::CheckpointTxt cp("Shielded proof");
			TxBase::Fail_Signature();
		}

	} catch (const std::exception& e) {
		sErr = e.what();
		bValid = false;
	}

	std::unique_lock<std::mutex> scope(m_Mbc.m_Mutex);

	if (!m_Mbc.m_bFail)
	{
		assert(m_Done < ctx.m_Params.m_nVerifiers);

		bool bLast = (++m_Done == ctx.m_Params.m_nVerifiers);
		if (bLast)
		{
			assert(m_Mbc.m_SizePending >= m_Size);
			m_Mbc.m_SizePending -= m_Size;
		}

		if (bValid)
		{
			try {

				m_Ctx.MergeStrict(ctx);

				if (bLast)
				{
					if (!bSparse)
						m_Ctx.TestSigma();
					else
					{
						m_Mbc.m_Offset += m_Body.m_Offset;
						m_Mbc.m_Sigma += m_Ctx.m_Sigma;
					}
				}

			} catch (const std::exception& e) {
				bValid = false;
				sErr = e.what();
			}
		}
	}

	if (!bValid)
	{
		m_Mbc.m_bFail = true;
		m_Mbc.m_sErr = std::move(sErr);
	}
}

bool NodeProcessor::ValidateAndSummarize(TxBase::Context& ctx, const TxBase& txb, TxBase::IReader&& r, std::string& sErr)
{
	struct MyShared
		:public MultiblockContext::MyTask::Shared
	{
		TxBase::Context* m_pCtx;
		const TxBase* m_pTx;
		TxBase::IReader* m_pR;

		MyShared(MultiblockContext& mbc)
			:MultiblockContext::MyTask::Shared(mbc)
		{
		}

		virtual ~MyShared() {} // auto

		void Exec(uint32_t iThread) override
		{
			TxBase::Context ctx;
			ctx.m_Params = m_pCtx->m_Params;
			ctx.m_Height = m_pCtx->m_Height;
			ctx.m_iVerifier = iThread;

			TxBase::IReader::Ptr pR;
			m_pR->Clone(pR);

			bool bValid = true;
			std::string sErr;

			try {
				ctx.ValidateAndSummarizeStrict(*m_pTx, std::move(*pR));
			} catch (const std::exception& e) {
				bValid = false;
				sErr = e.what();
			}

			std::unique_lock<std::mutex> scope(m_Mbc.m_Mutex);

			if (!m_Mbc.m_bFail)
			{
				if (bValid)
				{
					try {
						m_pCtx->MergeStrict(ctx);
					} catch (const std::exception& e) {
						bValid = false;
						sErr = e.what();
					}
				}

				if (!bValid)
				{
					m_Mbc.m_bFail = true;
					m_Mbc.m_sErr = std::move(sErr);
				}

			}
		}
	};

	MultiblockContext mbc(*this);

	std::shared_ptr<MyShared> pShared = std::make_shared<MyShared>(mbc);

	pShared->m_pCtx = &ctx;
	pShared->m_pTx = &txb;
	pShared->m_pR = &r;

	// No need to realize all the kernels, since this is invoked synchronously (we wait till everything finishes before exiting this func).
	// hence no risk of race w.r.t. kernel realization

	mbc.m_InProgress.m_Max.v++; // dummy, just to emulate ongoing progress
	mbc.PushTasks(pShared, ctx.m_Params);

	if (mbc.Flush())
		return true;

	sErr = std::move(mbc.m_sErr);
	return false;
}

} // namespace beam
