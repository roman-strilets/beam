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

struct NodeProcessor::KrnFlyMmr
	:public Merkle::FlyMmr
{
	const TxVectors::Eternal& m_Txve;

	KrnFlyMmr(const TxVectors::Eternal& txve)
		:m_Txve(txve)
	{
		m_Count = txve.m_vKernels.size();
	}

	void LoadElement(Merkle::Hash& hv, uint64_t n) const override {
		assert(n < m_Count);
		hv = m_Txve.m_vKernels[n]->get_ID();
	}
};

void NodeProcessor::EnsureCursorKernels()
{
	if (!m_Cursor.m_bKernels && m_Cursor.m_Row)
	{
		TxVectors::Eternal txve;
		ReadKrns(m_Cursor.m_Row, txve);

		KrnFlyMmr fmmr(txve);
		fmmr.get_Hash(m_Cursor.m_hvKernels);
		m_Cursor.m_bKernels = true;

	}
}

NodeProcessor::Evaluator::Evaluator(NodeProcessor& p)
	:m_Proc(p)
{
	m_Height = m_Proc.m_Cursor.m_hh.m_Height;
	m_Number = m_Proc.m_Cursor.m_Full.m_Number;
}

bool NodeProcessor::Evaluator::get_History(Merkle::Hash& hv)
{
	const Cursor& c = m_Proc.m_Cursor;
	if (m_Number.v == c.m_Full.m_Number.v)
		hv = c.m_History;
	else
	{
		assert(m_Number.v == c.m_Full.m_Number.v + 1);
		hv = c.m_HistoryNext;
	}
	return true;
}

bool NodeProcessor::Evaluator::get_Utxos(Merkle::Hash& hv)
{
	m_Proc.m_Mapped.m_Utxo.get_Hash(hv);
	return true;
}

bool NodeProcessor::Evaluator::get_Kernels(Merkle::Hash& hv)
{
	m_Proc.EnsureCursorKernels();
	hv = m_Proc.m_Cursor.m_hvKernels;
	return true;
}

bool NodeProcessor::Evaluator::get_Logs(Merkle::Hash& hv)
{
	hv = m_Proc.m_Cursor.m_StateExtra.m_hvLogs;
	return true;
}

void NodeProcessor::ReadKrns(uint64_t rowid, TxVectors::Eternal& txve)
{
	ByteBuffer bbE;
	m_DB.GetStateBlock(rowid, nullptr, &bbE, nullptr);

	Deserializer der;
	der.reset(bbE);
	der & txve;
}

bool NodeProcessor::Evaluator::get_Shielded(Merkle::Hash& hv)
{
	m_Proc.m_Mmr.m_Shielded.get_Hash(hv);
	return true;
}

bool NodeProcessor::Evaluator::get_Assets(Merkle::Hash& hv)
{
	m_Proc.m_Mmr.m_Assets.get_Hash(hv);
	return true;
}

bool NodeProcessor::Evaluator::get_Contracts(Merkle::Hash& hv)
{
	m_Proc.m_Mapped.m_Contract.get_Hash(hv);
	return true;
}

bool NodeProcessor::EvaluatorEx::get_Kernels(Merkle::Hash& hv)
{
	hv = m_hvKernels;
	return true;
}

bool NodeProcessor::EvaluatorEx::get_Logs(Merkle::Hash& hv)
{
	hv = m_Comms.m_hvLogs;
	return true;
}

bool NodeProcessor::EvaluatorEx::get_CSA(Merkle::Hash& hv)
{
	if (!Evaluator::get_CSA(hv))
		return false;

	m_Comms.m_hvCSA = hv;
	return true;
}

void NodeProcessor::ProofBuilder::OnProof(Merkle::Hash& hv, bool bNewOnRight)
{
	m_Proof.emplace_back();
	m_Proof.back().first = bNewOnRight;
	m_Proof.back().second = hv;
}

void NodeProcessor::ProofBuilderHard::OnProof(Merkle::Hash& hv, bool bNewOnRight)
{
	m_Proof.emplace_back();
	m_Proof.back() = hv;
}

struct NodeProcessor::ProofBuilder_PrevState
	:public ProofBuilder
{
	Merkle::Hash m_hvHistory;
	StateExtra::Full m_StateExtra;

	ProofBuilder_PrevState(NodeProcessor& p, Merkle::Proof& proof, const NodeDB::StateID& sid)
		:ProofBuilder(p, proof)
	{
		if (p.m_Cursor.m_Full.m_Number.v == sid.m_Number.v)
		{
			m_hvHistory = p.m_Cursor.m_History;
			Cast::Down<StateExtra::Comms>(m_StateExtra) = Cast::Down<StateExtra::Comms>(p.m_Cursor.m_StateExtra);
		}
		else
		{
			uint64_t nCount = sid.m_Number.v - 1;
			TemporarySwap<uint64_t> ts(nCount, p.m_Mmr.m_States.m_Count);
			p.m_Mmr.m_States.get_Hash(m_hvHistory);

			p.m_DB.get_StateExtra(sid.m_Row, &m_StateExtra, sizeof(m_StateExtra));
		}
	}

	bool get_History(Merkle::Hash& hv) override
	{
		hv = m_hvHistory;
		return true;
	}

	bool get_CSA(Merkle::Hash& hv) override
	{
		hv = m_StateExtra.m_hvCSA;
		return true;
	}
};

Height NodeProcessor::get_ProofKernel(Merkle::Proof* pProof, TxKernel::Ptr* ppRes, NodeDB::StateID& sid, const Merkle::Hash& idKrn, const HeightPos* pPos)
{
	Height h;

	if (pPos)
	{
		if (pPos->m_Height > m_Cursor.m_hh.m_Height)
			return 0;
		h = pPos->m_Height;
	}
	else
		h = m_DB.FindKernel(idKrn);

	if (!h)
		return 0;

	FindAtivePastHeight(sid, h);

	TxVectors::Eternal txve;
	ReadKrns(sid.m_Row, txve);

	// find the target kernel
	auto iTrg = static_cast<uint32_t>(txve.m_vKernels.size());
	if (pPos)
	{
		if (pPos->m_Pos >= iTrg)
			return 0; // oob
		iTrg = pPos->m_Pos;
	}
	else
	{
		while (true)
		{
			if (!iTrg)
				OnCorrupted(); // not found

			TxKernel::Ptr& p = txve.m_vKernels[--iTrg];
			if (p->get_ID() == idKrn)
				break;
		}
	}

	if (pProof)
	{
		Merkle::FixedMmr mmr;
		mmr.Resize(txve.m_vKernels.size());

		for (const auto& p : txve.m_vKernels)
			mmr.Append(p->get_ID());

		mmr.get_Proof(*pProof, iTrg);

		if (Rules::get().IsPastFork_<3>(h))
		{
			struct MyProofBuilder
				:public ProofBuilder_PrevState
			{
				using ProofBuilder_PrevState::ProofBuilder_PrevState;

				bool get_Kernels(Merkle::Hash&) override { return false; }

				bool get_Logs(Merkle::Hash& hv) override
				{
					hv = m_StateExtra.m_hvLogs;
					return true;
				}
			};

			MyProofBuilder pb(*this, *pProof, sid);
			pb.GenerateProof();
		}
	}

	if (ppRes)
		*ppRes = std::move(txve.m_vKernels[iTrg]);

	return h;
}

bool NodeProcessor::get_ProofContractLog(Merkle::Proof& proof, const HeightPos& pos)
{
	if (!pos.m_Height)
		return false; // it's possible to include contracts in treasury, and have logs. But proof won't be available

	Merkle::FixedMmr lmmr;
	uint64_t iTrg = static_cast<uint64_t>(-1);

	{
		NodeDB::ContractLog::Walker wlk;
		for (m_DB.ContractLogEnum(wlk, HeightPos(pos.m_Height), HeightPos(pos.m_Height, static_cast<uint32_t>(-1))); wlk.MoveNext(); )
		{
			if (!IsContractVarStoredInMmr(wlk.m_Entry.m_Key))
				continue;

			if (pos.m_Pos == wlk.m_Entry.m_Pos.m_Pos)
				iTrg = lmmr.m_Count; // found!

			Merkle::Hash hv;
			Block::get_HashContractLog(hv, wlk.m_Entry.m_Key, wlk.m_Entry.m_Val, wlk.m_Entry.m_Pos.m_Pos);

			lmmr.Resize(lmmr.m_Count + 1);
			lmmr.Append(hv);
		}
	}

	if (lmmr.m_Count <= iTrg)
		return false;

	lmmr.get_Proof(proof, iTrg);

	NodeDB::StateID sid;
	FindAtivePastHeight(sid, pos.m_Height);

	struct MyProofBuilder
		:public ProofBuilder_PrevState
	{
		using ProofBuilder_PrevState::ProofBuilder_PrevState;

		Merkle::Hash m_hvKernels;

		bool get_Kernels(Merkle::Hash& hv) override
		{
			hv = m_hvKernels;
			return true;
		}

		bool get_Logs(Merkle::Hash& hv) override
		{
			return false;
		}
	};

	MyProofBuilder pb(*this, proof, sid);

	{
		TxVectors::Eternal txve;
		ReadKrns(sid.m_Row, txve);

		KrnFlyMmr fmmr(txve);
		fmmr.get_Hash(pb.m_hvKernels);
	}

	pb.GenerateProof();

	return true;
}

void NodeProcessor::EvaluatorEx::set_Kernels(const TxVectors::Eternal& txe)
{
	KrnFlyMmr fmmr(txe);
	fmmr.get_Hash(m_hvKernels);
}

void NodeProcessor::EvaluatorEx::set_Logs(const std::vector<Merkle::Hash>& v)
{
	struct MyMmr
		:public Merkle::FlyMmr
	{
		const Merkle::Hash* m_pArr;

		void LoadElement(Merkle::Hash& hv, uint64_t n) const override {
			hv = m_pArr[n];
		}

	} lmmr;

	lmmr.m_Count = v.size();
	if (lmmr.m_Count)
		lmmr.m_pArr = &v.front();

	lmmr.get_Hash(m_Comms.m_hvLogs);
}

void NodeProcessor::ExtractBlockWithExtra(const NodeDB::StateID& sid, Height h, std::vector<TxoInfo>& vIns, std::vector<TxoInfo>& vOuts, TxVectors::Eternal& txe, std::vector<ContractInvokeExtraInfo>& vC)
{
	{
		// kernels
		ByteBuffer bbE;
		m_DB.GetStateBlock(sid.m_Row, nullptr, &bbE, nullptr);

		Deserializer der;
		der.reset(bbE);
		der & txe;

		NodeDB::KrnInfo::Walker wlk;
		for (m_DB.KrnInfoEnum(wlk, h); wlk.MoveNext(); )
		{
			auto& info = vC.emplace_back();
			info.m_Cid = wlk.m_Entry.m_Cid;

			der.reset(wlk.m_Entry.m_Val.p, wlk.m_Entry.m_Val.n);
			der & info;
		}

	}

	{
		// inputs
		std::vector<NodeDB::StateInput> v;
		m_DB.get_StateInputs(sid.m_Row, v);

		vIns.resize(v.size());

		for (uint32_t i = 0; i < v.size(); i++)
		{
			TxoID txoID = v[i].get_ID();
			auto& dst = vIns[i];

			NodeDB::WalkerTxo wlk;
			m_DB.TxoGetValue(wlk, txoID);

			Deserializer der;
			der.reset(wlk.m_Value.p, wlk.m_Value.n);
			der & dst.m_Outp;

			dst.m_hSpent = h;

			FindHeightByTxoID(dst.m_hCreate, txoID);
		}
	}

	{
		// outputs
		TxoID id1 = get_TxosBefore(Block::Number(sid.m_Number.v + 1));
		TxoID id0 = get_TxosBefore(sid.m_Number);
		vOuts.reserve(id1 - id0);

		NodeDB::WalkerTxo wlk;
		for (m_DB.EnumTxos(wlk, id0); wlk.MoveNext(); )
		{
			if (wlk.m_ID >= id1)
				break;

			auto& dst = vOuts.emplace_back();

			Deserializer der;
			der.reset(wlk.m_Value.p, wlk.m_Value.n);
			der & dst.m_Outp;

			dst.m_hCreate = h;
			dst.m_hSpent = wlk.m_SpendHeight;
		}
	}
}

void NodeProcessor::ExtractTreasurykWithExtra(std::vector<TxoInfo>& vOuts)
{
	NodeDB::WalkerTxo wlk;
	for (m_DB.EnumTxos(wlk, 0); wlk.MoveNext(); )
	{
		if (wlk.m_ID >= m_Extra.m_TxosTreasury)
			break;

		auto& dst = vOuts.emplace_back();

		Deserializer der;
		der.reset(wlk.m_Value.p, wlk.m_Value.n);
		der & dst.m_Outp;

		dst.m_hCreate = 0;
		dst.m_hSpent = wlk.m_SpendHeight;
	}
}

TxoID NodeProcessor::get_TxosBefore(Block::Number num)
{
	if (!num.v)
		return 0;

	if (num.v == 1u)
		return m_Extra.m_TxosTreasury;

	TxoID id = m_DB.get_StateTxos(FindActiveAtStrict(Block::Number(num.v - 1)));
	if (MaxHeight == id)
		OnCorrupted();

	return id;
}

TxoID NodeProcessor::FindBlockByTxoID(NodeDB::StateID& sid, TxoID id0)
{
	if (id0 < m_Extra.m_TxosTreasury)
	{
		sid.SetNull();
		return m_Extra.m_TxosTreasury;
	}

	return m_DB.FindStateByTxoID(sid, id0);
}

TxoID NodeProcessor::FindHeightByTxoID(Height& h, TxoID id0)
{
	NodeDB::StateID sid;
	auto ret = FindBlockByTxoID(sid, id0);

	h = Num2Height(sid);
	return ret;
}

bool NodeProcessor::EnumTxos(ITxoWalker& wlk)
{
	Block::NumberRange nr;
	nr.m_Min.v = 0; // treasury included
	nr.m_Max = m_Cursor.m_Full.m_Number;
	return EnumTxos(wlk, nr);
}

bool NodeProcessor::EnumTxos(ITxoWalker& wlkTxo, const Block::NumberRange& nr)
{
	if (nr.IsEmpty())
		return true;
	assert(nr.m_Max.v <= m_Cursor.m_Full.m_Number.v);

	TxoID idBeg = get_TxosBefore(nr.m_Min);
	TxoID idEnd = get_TxosBefore(Block::Number(nr.m_Max.v + 1));
	assert(idBeg <= idEnd);

	if (idBeg == idEnd)
	{
		// there's an artificail gap in TxoID in each block, so this basically shouldn't happen
		// the only exception: Node state is empty, even Treasury wasn't processed yet
		assert(!nr.m_Min.v && !nr.m_Max.v && !IsTreasuryHandled());
		// ignore
	}

	TxoID id1 = idBeg;
	Height hLast = 0;

	if (wlkTxo.m_pLa)
		wlkTxo.m_pLa->SetTotal(idEnd - idBeg);

	NodeDB::WalkerTxo wlk;
	for (m_DB.EnumTxos(wlk, id1);  wlk.MoveNext(); )
	{
		if (wlk.m_ID >= id1)
		{
			if (wlk.m_ID >= idEnd)
				break;

			// update height and next boundary
			id1 = FindHeightByTxoID(hLast, wlk.m_ID);
			assert(wlk.m_ID < id1);

			if (wlkTxo.m_pLa &&
				!wlkTxo.m_pLa->OnProgress(wlk.m_ID - idBeg + 1))
				throw std::runtime_error("EnumTxos interrupted");
		}

		if (!wlkTxo.OnTxo(wlk, hLast))
			return false;
	}

	return true;
}

bool NodeProcessor::EnumKernels(IKrnWalker& wlkKrn, Block::NumberRange nr)
{
	if (nr.IsEmpty() || !IsTreasuryHandled())
		return true;
	assert(nr.m_Max.v <= m_Cursor.m_Full.m_Number.v);

	if (wlkKrn.m_pLa)
		wlkKrn.m_pLa->SetTotal(nr.m_Max.v - nr.m_Min.v + 1);

	if (!nr.m_Min.v)
	{
		// treasury
		if (Rules::get().TreasuryChecksum != Zero)
		{
			Treasury::Data td;
			{
				ByteBuffer buf;
				m_DB.ParamGet(NodeDB::ParamID::Treasury, nullptr, nullptr, &buf);

				Deserializer der;
				der.reset(buf);
				der & td;
			}

			// squash the treasury into a single tx
			TxVectors::Full txv;

			for (auto& g : td.m_vGroups)
			{
				if (txv.m_vKernels.empty())
					txv.m_vKernels.swap(g.m_Data.m_vKernels);
				else
				{
					g.m_Data.MoveInto(txv);
					txv.m_vInputs.clear();
					txv.m_vOutputs.clear();
				}
			}

			txv.NormalizeE();

			wlkKrn.m_Height = 0; // treasury
			wlkKrn.m_nKrnIdx = 0;

			NodeDB::StateID sid;
			sid.SetNull();
			if (!wlkKrn.ProcessBlock(sid, txv.m_vKernels))
				return false;

		}

		nr.m_Min.v++;
	}

	TxVectors::Eternal txve;

	NodeDB::StateID sid;

	for (sid.m_Number = nr.m_Min; sid.m_Number.v <= nr.m_Max.v; sid.m_Number.v++)
	{
		sid.m_Row = FindActiveAtStrict(sid.m_Number);
		wlkKrn.m_Height = Num2Height(sid);

		txve.m_vKernels.clear();
		ReadKrns(sid.m_Row, txve);

		wlkKrn.m_nKrnIdx = 0;
		if (!wlkKrn.ProcessBlock(sid, txve.m_vKernels))
			return false;

		if (wlkKrn.m_pLa &&
			!wlkKrn.m_pLa->OnProgress(sid.m_Number.v - nr.m_Min.v + 1))
			throw std::runtime_error("EnumKernels interrupted");
	}

	return true;
}

bool NodeProcessor::ITxoWalker::OnTxo(const NodeDB::WalkerTxo& wlk , Height hCreate)
{
	Deserializer der;
	der.reset(wlk.m_Value.p, wlk.m_Value.n);

	Output outp;
	der & outp;

	return OnTxo(wlk, hCreate, outp);
}

bool NodeProcessor::ITxoWalker::OnTxo(const NodeDB::WalkerTxo&, Height hCreate, Output&)
{
	assert(false);
	return false;
}

bool NodeProcessor::ITxoRecover::OnTxo(const NodeDB::WalkerTxo& wlk, Height hCreate)
{
	if (TxoIsNaked(wlk.m_Value))
		return true;

	return ITxoWalker::OnTxo(wlk, hCreate);
}

bool NodeProcessor::ITxoRecover::OnTxo(const NodeDB::WalkerTxo& wlk, Height hCreate, Output& outp)
{
	assert(m_pKey);
	CoinID cid;
	Output::User user;
	if (!outp.Recover(hCreate, *m_pKey, cid, &user))
		return true;

	return OnTxo(wlk, hCreate, outp, cid, user);
}

bool NodeProcessor::ITxoWalker_UnspentNaked::OnTxo(const NodeDB::WalkerTxo& wlk, Height hCreate)
{
	if (wlk.m_SpendHeight != MaxHeight)
		return true;

	uint8_t pNaked[s_TxoNakedMax];
	TxoToNaked(pNaked, Cast::NotConst(wlk).m_Value); // save allocation and deserialization of sig

	return ITxoWalker::OnTxo(wlk, hCreate);
}

bool NodeProcessor::ITxoWalker_Unspent::OnTxo(const NodeDB::WalkerTxo& wlk, Height hCreate)
{
	if (wlk.m_SpendHeight != MaxHeight)
		return true;

	return ITxoWalker::OnTxo(wlk, hCreate);
}

bool NodeProcessor::GetBlock(const NodeDB::StateID& sid, ByteBuffer* pEthernal, ByteBuffer* pPerishable, Block::Number n0, Block::Number nLo1, Block::Number nHi1, bool bActive)
{
	// h0 - current peer Height
	// hLo1 - HorizonLo that peer needs after the sync
	// hHi1 - HorizonL1 that peer needs after the sync
	if ((nLo1.v > nHi1.v) || (n0.v >= sid.m_Number.v))
		return false;

	// For every output:
	//	if SpendHeight > hHi1 (or null) then fully transfer
	//	if SpendHeight > hLo1 then transfer naked (remove Confidential, Public, Asset::ID)
	//	Otherwise - don't transfer

	// For every input (commitment only):
	//	if SpendHeight > hLo1 then transfer
	//	if CreateHeight <= h0 then transfer
	//	Otherwise - don't transfer

	beam::setmax(nHi1.v, sid.m_Number.v); // valid block can't spend its own output. Hence this means full block should be transferred
	beam::setmax(nLo1.v, sid.m_Number.v - 1);

	if (m_Extra.m_TxoHi.v > nHi1.v)
		return false;

	if (m_Extra.m_TxoLo.v > nLo1.v)
		return false;

	if (n0.v && (m_Extra.m_TxoLo.v > sid.m_Number.v))
		return false; // we don't have any info for the range [1, n0].

	// in case we're during sync - make sure we don't return non-full blocks as-is
	if (IsFastSync() && (sid.m_Number.v > m_Cursor.m_Full.m_Number.v))
		return false;

	bool bFullBlock = (sid.m_Number.v >= nHi1.v) && (sid.m_Number.v > nLo1.v);
	m_DB.GetStateBlock(sid.m_Row, bFullBlock ? pPerishable : nullptr, pEthernal, nullptr);

	if (!(pPerishable && pPerishable->empty()))
		return true;

	// re-create it from Txos
	if (!bActive && !(m_DB.GetStateFlags(sid.m_Row) & NodeDB::StateFlags::Active))
		return false; // only active states are supported

	TxoID idInpCut = get_TxosBefore(Block::Number(n0.v + 1));
	TxoID id0;

	TxoID id1 = m_DB.get_StateTxos(sid.m_Row);

	ByteBuffer bbBlob;
	TxBase txb;
	ReadOffset(txb.m_Offset, sid.m_Row);

	uint64_t rowid = sid.m_Row;
	if (m_DB.get_Prev(rowid))
	{
		ECC::Scalar offsPrev;
		ReadOffset(offsPrev, rowid);
		AdjustOffset(txb.m_Offset, offsPrev, false);
		id0 = m_DB.get_StateTxos(rowid);
	}
	else
		id0 = m_Extra.m_TxosTreasury;

	Serializer ser;
	ser & txb;

	uint32_t nCount = 0;

	// inputs
	std::vector<NodeDB::StateInput> v;
	m_DB.get_StateInputs(sid.m_Row, v);

	for (uint32_t iCycle = 0; ; iCycle++)
	{
		for (size_t i = 0; i < v.size(); i++)
		{
			TxoID id = v[i].get_ID();

			//	if SpendHeight > hLo1 then transfer
			//	if CreateHeight <= h0 then transfer
			//	Otherwise - don't transfer
			if ((sid.m_Number.v > nLo1.v) || (id < idInpCut))
			{
				if (iCycle)
				{
					const NodeDB::StateInput& si = v[i];

					// write
					Input inp;
					si.Get(inp.m_Commitment);
					ser & inp;
				}
				else
					nCount++;
			}
		}

		if (iCycle)
			break;

		ser & uintBigFrom(nCount);
	}

	nCount = 0;

	// outputs
	Height hLo1 = Num2Height(nLo1);
	Height hHi1 = Num2Height(nHi1);

	NodeDB::WalkerTxo wlk;
	for (m_DB.EnumTxos(wlk, id0); wlk.MoveNext(); )
	{
		if (wlk.m_ID >= id1)
			break;

		//	if SpendHeight > hHi1 (or null) then fully transfer
		//	if SpendHeight > hLo1 then transfer naked (remove Confidential, Public, Asset::ID)
		//	Otherwise - don't transfer

		if (wlk.m_SpendHeight <= hLo1)
			continue;

		uint8_t pNaked[s_TxoNakedMax];

		if (wlk.m_SpendHeight <= hHi1)
			TxoToNaked(pNaked, wlk.m_Value);

		nCount++;

		const uint8_t* p = reinterpret_cast<const uint8_t*>(wlk.m_Value.p);
		bbBlob.insert(bbBlob.end(), p, p + wlk.m_Value.n);
	}

	ser & uintBigFrom(nCount);
	ser.swap_buf(*pPerishable);
	pPerishable->insert(pPerishable->end(), bbBlob.begin(), bbBlob.end());
		
	return true;
}

} // namespace beam
