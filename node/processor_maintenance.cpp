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

namespace beam {

void NodeProcessor::DeleteBlocksInRange(const NodeDB::StateID& sidTop, Block::Number numStop)
{
	for (NodeDB::StateID sid = sidTop; sid.m_Number.v > numStop.v; )
	{
		DeleteBlock(sid.m_Row);

		if (!m_DB.get_Prev(sid))
			sid.SetNull();
	}
}

void NodeProcessor::DeleteBlock(uint64_t row)
{
	m_DB.DelStateBlockAll(row);
	m_DB.SetStateNotFunctional(row);
}

Height NodeProcessor::PruneOld()
{
	if (IsFastSync())
		return 0; // don't remove anything while in fast-sync mode

	Height hRet = 0;

	if (m_Cursor.m_Full.m_Number.v > m_Horizon.m_Branching)
	{
		Block::Number num(m_Cursor.m_Full.m_Number.v - m_Horizon.m_Branching);

		while (true)
		{
			uint64_t rowid;
			{
				NodeDB::WalkerState ws;
				m_DB.EnumTips(ws);
				if (!ws.MoveNext())
					break;
				if (ws.m_Sid.m_Number.v >= num.v)
					break;

				rowid = ws.m_Sid.m_Row;
			}

			do
			{
				if (!m_DB.DeleteState(rowid, rowid))
					break;
				hRet++;

			} while (rowid);
		}
	}

	if (IsBigger2(m_Cursor.m_Full.m_Number.v, m_Extra.m_Fossil.v, (uint64_t) Rules::get().MaxRollback))
		hRet += RaiseFossil(Block::Number(m_Cursor.m_Full.m_Number.v - Rules::get().MaxRollback));

	if (IsBigger2(m_Cursor.m_Full.m_Number.v, m_Extra.m_TxoLo.v, m_Horizon.m_Local.Lo))
		hRet += RaiseTxoLo(Block::Number(m_Cursor.m_Full.m_Number.v - m_Horizon.m_Local.Lo));

	if (IsBigger2(m_Cursor.m_Full.m_Number.v, m_Extra.m_TxoHi.v, m_Horizon.m_Local.Hi))
		hRet += RaiseTxoHi(Block::Number(m_Cursor.m_Full.m_Number.v - m_Horizon.m_Local.Hi));

	return hRet;
}

struct LongActionPlus
{
	LongAction m_La;
	bool m_Logging;

	LongActionPlus(const char* sz, Height h, Height hTrg, ILongAction* pExternalHandler)
	{
		assert(hTrg > h);
		auto dh = hTrg - h;
		if (dh < 10000)
		{
			m_Logging = false;
			return;
		}
		m_La.m_pExternal = pExternalHandler;
		m_Logging = true;
		m_La.Reset(sz, dh);
	}

	void OnProgress(Height h, Height hTrg)
	{
		if (m_Logging)
			m_La.OnProgress(m_La.m_Total + h - hTrg);
	}
};

Height NodeProcessor::RaiseFossil(Block::Number numTrg)
{
	if (numTrg.v <= m_Extra.m_Fossil.v)
		return 0;

	LongActionPlus la("Raising Fossil...", m_Extra.m_Fossil.v, numTrg.v, m_pExternalHandler);

	Height hRet = 0;

	while (m_Extra.m_Fossil.v < numTrg.v)
	{
		m_Extra.m_Fossil.v++;

		NodeDB::WalkerState ws;
		for (m_DB.EnumStatesAt(ws, m_Extra.m_Fossil); ws.MoveNext(); )
		{
			if (NodeDB::StateFlags::Active & m_DB.GetStateFlags(ws.m_Sid.m_Row))
				m_DB.DelStateBlockPPR(ws.m_Sid.m_Row);
			//else
			//	DeleteBlock(ws.m_Sid.m_Row);

			// Don't delete non-active blocks! For non-archieve nodes the whole abandoned branch will eventually be deleted.
			// For archieve node - keep abandoned blocks, to be able to analyze them later.

			hRet++;
		}
		la.OnProgress(m_Extra.m_Fossil.v, numTrg.v);

	}

	m_DB.ParamIntSet(NodeDB::ParamID::NumberFossil, m_Extra.m_Fossil.v);
	return hRet;
}

Height NodeProcessor::RaiseTxoLo(Block::Number numTrg)
{
	if (numTrg.v <= m_Extra.m_TxoLo.v)
		return 0;

	LongActionPlus la("Raising TxoLo...", m_Extra.m_TxoLo.v, numTrg.v, m_pExternalHandler);

	Height hRet = 0;
	std::vector<NodeDB::StateInput> v;

	while (m_Extra.m_TxoLo.v < numTrg.v)
	{
		++m_Extra.m_TxoLo.v;
		uint64_t rowid = FindActiveAtStrict(m_Extra.m_TxoLo);
		if (!m_DB.get_StateInputs(rowid, v))
			continue;

		size_t iRes = 0;
		for (size_t i = 0; i < v.size(); i++)
		{
			const NodeDB::StateInput& inp = v[i];
			TxoID id = inp.get_ID();
			if (id >= m_Extra.m_TxosTreasury)
				m_DB.TxoDel(id);
			else
			{
				if (iRes != i)
					v[iRes] = inp;
				iRes++;
			}
		}

		hRet += (v.size() - iRes);

		m_DB.set_StateInputs(rowid, &v.front(), iRes);

		la.OnProgress(m_Extra.m_TxoLo.v, numTrg.v);
	}

	m_Extra.m_TxoLo = numTrg;
	m_DB.ParamIntSet(NodeDB::ParamID::NumberTxoLo, m_Extra.m_TxoLo.v);

	return hRet;
}

Height NodeProcessor::RaiseTxoHi(Block::Number numTrg)
{
	if (numTrg.v <= m_Extra.m_TxoHi.v)
		return 0;

	LongActionPlus la("Raising TxoHi...", m_Extra.m_TxoHi.v, numTrg.v, m_pExternalHandler);


	Height hRet = 0;
	std::vector<NodeDB::StateInput> v;

	NodeDB::WalkerTxo wlk;

	while (m_Extra.m_TxoHi.v < numTrg.v)
	{
		++m_Extra.m_TxoHi.v;
		uint64_t rowid = FindActiveAtStrict(m_Extra.m_TxoHi);
		m_DB.get_StateInputs(rowid, v);

		for (size_t i = 0; i < v.size(); i++)
		{
			TxoID id = v[i].get_ID();

			m_DB.TxoGetValue(wlk, id);

			if (TxoIsNaked(wlk.m_Value))
				continue; //?!

			uint8_t pNaked[s_TxoNakedMax];
			TxoToNaked(pNaked, wlk.m_Value);

			m_DB.TxoSetValue(id, wlk.m_Value);
			hRet++;
		}

		la.OnProgress(m_Extra.m_TxoHi.v, numTrg.v);
	}

	m_DB.ParamIntSet(NodeDB::ParamID::NumberTxoHi, m_Extra.m_TxoHi.v);

	return hRet;
}

void NodeProcessor::TxoToNaked(uint8_t* pBuf, Blob& v)
{
	if (v.n < s_TxoNakedMin)
		OnCorrupted();

	const uint8_t* pSrc = reinterpret_cast<const uint8_t*>(v.p);
	v.p = pBuf;

	if (!(0x10 & pSrc[0]))
	{
		// simple case - just remove some flags and truncate.
		memcpy(pBuf, pSrc, s_TxoNakedMin);
		v.n = s_TxoNakedMin;
		pBuf[0] &= 3;

		return;
	}

	// complex case - the UTXO has Incubation period. Utxo must be re-read
	Deserializer der;
	der.reset(pSrc, v.n);

	Output outp;
	der & outp;

	outp.m_pConfidential.reset();
	outp.m_pPublic.reset();
	outp.m_pAsset.reset();

	StaticBufferSerializer<s_TxoNakedMax> ser;
	ser & outp;

	SerializeBuffer sb = ser.buffer();
	assert(sb.second <= s_TxoNakedMax);

	memcpy(pBuf, sb.first, sb.second);
	v.n = static_cast<uint32_t>(sb.second);
}

bool NodeProcessor::TxoIsNaked(const Blob& v)
{
	if (v.n < s_TxoNakedMin)
		OnCorrupted();

	const uint8_t* pSrc = reinterpret_cast<const uint8_t*>(v.p);

	return !(pSrc[0] & 0xc);

}

void NodeProcessor::InitializeUtxos()
{
	struct Walker
		:public ITxoWalker_UnspentNaked
	{
		NodeProcessor& m_This;
		Walker(NodeProcessor& x) :m_This(x) {}

		bool OnTxo(const NodeDB::WalkerTxo& wlk, Height hCreate) override
		{
			m_This.InitializeUtxosProgress(wlk.m_ID, m_pLa->m_Total);
			return ITxoWalker_UnspentNaked::OnTxo(wlk, hCreate);
		}

		bool OnTxo(const NodeDB::WalkerTxo& wlk, Height hCreate, Output& outp) override
		{
			m_This.m_Extra.m_Txos = wlk.m_ID;
			BlockInterpretCtx bic(m_This, hCreate, true);
			if (!bic.HandleBlockElement(outp))
				OnCorrupted();

			return true;
		}
	};

	LongAction la("Rebuilding mapped image...", 0, m_pExternalHandler);

	Walker wlk(*this);
	wlk.m_pLa = &la;

	EnumTxos(wlk);
}

void NodeProcessor::RebuildNonStd()
{
	Height h0 = Rules::get().pForks[2].m_Height;
	if (m_Cursor.m_hh.m_Height < h0)
		return; // no non-std data

	LongAction la("Rebuilding non-std data...", m_Cursor.m_Full.m_Number.v, m_pExternalHandler);

	// Delete all asset info, contracts, shielded, and replay everything
	m_Mapped.m_Contract.Clear();
	m_DB.ContractDataDelAll();
	m_DB.ContractLogDel(HeightPos(0), HeightPos(MaxHeight));
	m_DB.ShieldedOutpDelFrom(0);
	m_DB.ParamDelSafe(NodeDB::ParamID::ShieldedInputs);
	m_DB.AssetsDelAll(Rules::get().CA.ForeignEnd);
	m_DB.AssetEvtsDeleteFrom(0);
	m_DB.UniqueDeleteAll();
	m_DB.KrnInfoDelFrom(0);

	m_Mmr.m_Assets.ResizeTo(0);
	m_Mmr.m_Shielded.ResizeTo(0);
	m_Extra.m_ShieldedOutputs = 0;

	static_assert(NodeDB::StreamType::StatesMmr == 0);
	m_DB.StreamsDelAll(static_cast<NodeDB::StreamType::Enum>(1), NodeDB::StreamType::count);

	struct KrnWalkerRebuild
		:public IKrnWalker
	{
		NodeProcessor& m_This;
		BlockInterpretCtx* m_pBic = nullptr;
		IPbftHandler* m_pPbft = nullptr;
		std::vector<ContractInvokeExtraInfo>* m_pvC = nullptr;
		KrnWalkerRebuild(NodeProcessor& p) :m_This(p) {}

		ByteBuffer m_Rollback;

		bool ProcessBlock(const NodeDB::StateID& sid, const std::vector<TxKernel::Ptr>& v) override
		{
			BlockInterpretCtx bic(m_This, m_Height, true);
			m_pBic = &bic;
			BlockInterpretCtx::ChangesFlush cf(m_This);

			m_pBic->m_pvC = m_pvC;
			bic.m_AlreadyValidated = true;
			bic.m_Rollback.swap(m_Rollback); // optimization

			Process(v);
			bic.HandlePbftReward(v, m_pPbft);
		
			if (sid.m_Number.v > m_This.m_Extra.m_Fossil.v)
			{
				assert(sid.m_Row); // can't be treasury height, since it's above the fossil height

				// replace rollback data
				m_This.m_DB.set_StateRB(sid.m_Row, bic.m_Rollback);
			}

			bic.m_Rollback.swap(m_Rollback);
			m_Rollback.clear();

			cf.Do(m_This, m_Height);

			if (m_pvC)
			{
				Serializer ser;
				ser.swap_buf(m_Rollback);

				bic.AddKrnInfo(ser);

				ser.swap_buf(m_Rollback);

				m_Rollback.clear();
				m_pvC->clear();
			}

			return true;
		}

		bool OnKrn(const TxKernel& krn) override
		{
			m_pBic->m_nKrnIdx = m_nKrnIdx;

			if (!m_pBic->HandleKernelTypeAny(krn))
				OnCorrupted();

			return true;
		}

	} wlk(*this);

	if (Rules::Consensus::Pbft == Rules::get().m_Consensus)
	{
		wlk.m_pPbft = get_PbftHandler();
		if (!wlk.m_pPbft)
			OnCorrupted();

		wlk.m_pPbft->OnContractStoreReset();
	}

	std::vector<ContractInvokeExtraInfo> vC;
	wlk.m_pvC = m_DB.ParamIntGetDef(NodeDB::ParamID::RichContractInfo) ? &vC : nullptr;

	wlk.m_pLa = &la;

	Block::NumberRange nr;
	if (Rules::Consensus::Pbft == Rules::get().m_Consensus)
	{
		// start from the beginning, rebuild pbft data too
	}
	else
		nr.m_Min = FindAtivePastHeight(h0);

	nr.m_Max = m_Cursor.m_Full.m_Number;

	EnumKernels(wlk, nr);
}

} // namespace beam
