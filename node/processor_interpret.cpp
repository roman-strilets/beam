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

Height NodeProcessor::BlockInterpretCtx::FindVisibleKernel(const Merkle::Hash& id)
{
	assert(!m_AlreadyValidated);

	if (m_Temporary)
	{
		auto it = m_KrnIDs.find(id);
		if (m_KrnIDs.end() != it)
			return m_Height;
	}

	Height h = m_Proc.m_DB.FindKernel(id);
	if (h)
	{
		assert(h <= m_Height);

		const Rules& r = Rules::get();
		if (r.IsPastFork_<2>(m_Height) && (m_Height - h > r.MaxKernelValidityDH))
			return 0; // Starting from Fork2 - visibility horizon is limited
	}

	return h;
}

bool NodeProcessor::BlockInterpretCtx::HandleKernelType(const TxKernelStd& krn)
{
	if (m_Fwd && krn.m_pRelativeLock && !m_AlreadyValidated)
	{
		const TxKernelStd::RelativeLock& x = *krn.m_pRelativeLock;

		Height h0 = FindVisibleKernel(x.m_ID);
		if (!h0)
		{
			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << "RelLock not found " << x.m_ID;

			return false;
		}

		HeightAdd(h0, x.m_LockHeight);
		if (h0 > m_Height)
		{
			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << "RelLock too early " << x.m_ID << ", dH=" << x.m_LockHeight;

			return false;
		}
	}

	return true;
}

bool NodeProcessor::BlockInterpretCtx::HandleKernelType(const TxKernelShieldedOutput& krn)
{
	const ECC::Point& key = krn.m_Txo.m_Ticket.m_SerialPub;
	Blob blobKey(&key, sizeof(key));

	if (m_Fwd)
	{
		if (!m_AlreadyValidated)
		{
			if (m_ShieldedOuts >= Rules::get().Shielded.MaxOuts)
			{
				if (m_pTxErrorInfo)
					*m_pTxErrorInfo << "Shielded outp limit exceeded";

				m_LimitExceeded = true;
				return false;
			}

			if (!ValidateAssetRange(krn.m_Txo.m_pAsset))
				return false;
		}

		ShieldedOutpPacked sop;
		sop.m_Height = m_Height;
		sop.m_MmrIndex = m_Proc.m_Mmr.m_Shielded.m_Count;
		sop.m_TxoID = m_Proc.m_Extra.m_ShieldedOutputs;
		sop.m_Commitment = krn.m_Txo.m_Commitment;

		Blob blobVal(&sop, sizeof(sop));

		if (!ValidateUniqueNoDup(blobKey, &blobVal))
		{
			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << "Shielded outp duplicate";

			return false;
		}

		if (!m_Temporary)
		{
			ECC::Point::Native pt, pt2;
			pt.Import(krn.m_Txo.m_Commitment); // don't care if Import fails (kernels are not necessarily tested at this stage)
			pt2.Import(krn.m_Txo.m_Ticket.m_SerialPub);
			pt += pt2;

			ECC::Point::Storage pt_s;
			pt.Export(pt_s);

			// Append to cmList
			m_Proc.m_DB.ShieldedResize(m_Proc.m_Extra.m_ShieldedOutputs + 1, m_Proc.m_Extra.m_ShieldedOutputs);
			m_Proc.m_DB.ShieldedWrite(m_Proc.m_Extra.m_ShieldedOutputs, &pt_s, 1);

			// Append state hash
			ECC::Hash::Value hvState;
			if (m_Proc.m_Extra.m_ShieldedOutputs)
				m_Proc.m_DB.ShieldedStateRead(m_Proc.m_Extra.m_ShieldedOutputs - 1, &hvState, 1);
			else
				hvState = Zero;

			ShieldedTxo::UpdateState(hvState, pt_s);

			m_Proc.m_DB.ShieldedStateResize(m_Proc.m_Extra.m_ShieldedOutputs + 1, m_Proc.m_Extra.m_ShieldedOutputs);
			m_Proc.m_DB.ShieldedStateWrite(m_Proc.m_Extra.m_ShieldedOutputs, &hvState, 1);
		}

		if (!m_SkipDefinition)
		{
			ShieldedTxo::DescriptionOutp d;
			d.m_SerialPub = krn.m_Txo.m_Ticket.m_SerialPub;
			d.m_Commitment = krn.m_Txo.m_Commitment;
			d.m_ID = m_Proc.m_Extra.m_ShieldedOutputs;
			d.m_Height = m_Height;

			Merkle::Hash hv;
			d.get_Hash(hv);
			m_Proc.m_Mmr.m_Shielded.Append(hv);
		}

		m_Proc.m_Extra.m_ShieldedOutputs++;

		m_ShieldedOuts++; // ok

	}
	else
	{
		ValidateUniqueNoDup(blobKey, nullptr);

		if (!m_Temporary)
		{
			m_Proc.m_DB.ShieldedResize(m_Proc.m_Extra.m_ShieldedOutputs - 1, m_Proc.m_Extra.m_ShieldedOutputs);
			m_Proc.m_DB.ShieldedStateResize(m_Proc.m_Extra.m_ShieldedOutputs - 1, m_Proc.m_Extra.m_ShieldedOutputs);
		}

		if (!m_SkipDefinition)
			m_Proc.m_Mmr.m_Shielded.ShrinkTo(m_Proc.m_Mmr.m_Shielded.m_Count - 1);
		

		assert(m_ShieldedOuts);
		m_ShieldedOuts--;

		assert(m_Proc.m_Extra.m_ShieldedOutputs);
		m_Proc.m_Extra.m_ShieldedOutputs--;
	}

	return true;
}

bool NodeProcessor::BlockInterpretCtx::HandleKernelType(const TxKernelShieldedInput& krn)
{
	ECC::Point key = krn.m_SpendProof.m_SpendPk;
	key.m_Y |= 2;
	Blob blobKey(&key, sizeof(key));

	if (m_Fwd)
	{
		if (!m_AlreadyValidated)
		{
			if (!ValidateAssetRange(krn.m_pAsset))
				return false;

			if (m_ShieldedIns >= Rules::get().Shielded.MaxIns)
			{
				if (m_pTxErrorInfo)
					*m_pTxErrorInfo << "Shielded inp limit exceeded";

				m_LimitExceeded = true;
				return false;
			}

			if (!m_Proc.IsShieldedInPool(krn))
			{
				if (m_pTxErrorInfo)
					*m_pTxErrorInfo << "Shielded inp oob";

				return false; // references invalid pool window
			}
		}
 
		ShieldedInpPacked sip;
		sip.m_Height = m_Height;
		sip.m_MmrIndex = m_Proc.m_Mmr.m_Shielded.m_Count;

		Blob blobVal(&sip, sizeof(sip));
		if (!ValidateUniqueNoDup(blobKey, &blobVal))
		{
			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << "Shielded inp duplicate";
			return false;
		}

		if (!m_SkipDefinition)
		{
			ShieldedTxo::DescriptionInp d;
			d.m_SpendPk = krn.m_SpendProof.m_SpendPk;
			d.m_Height = m_Height;

			Merkle::Hash hv;
			d.get_Hash(hv);
			m_Proc.m_Mmr.m_Shielded.Append(hv);
		}

		m_ShieldedIns++; // ok

	}
	else
	{
		ValidateUniqueNoDup(blobKey, nullptr);

		if (!m_SkipDefinition)
			m_Proc.m_Mmr.m_Shielded.ShrinkTo(m_Proc.m_Mmr.m_Shielded.m_Count - 1);

		assert(m_ShieldedIns);
		m_ShieldedIns--;
	}

	return true;
}

bool NodeProcessor::BlockInterpretCtx::HandleValidatedTx(const TxVectors::Full& txv)
{
	size_t pN[3];

	bool bOk = true;
	if (m_Fwd)
	{
		m_vInpAux.reserve(m_vInpAux.size() + txv.m_vInputs.size());

		ZeroObject(pN);
		bOk =
			HandleElementVecFwd(txv.m_vInputs, pN[0]) &&
			HandleElementVecFwd(txv.m_vOutputs, pN[1]) &&
			HandleElementVecFwd(txv.m_vKernels, pN[2]);

		if (bOk)
			return true;

		m_Fwd = false; // rollback partial changes
	}
	else
	{
		// rollback all
		pN[0] = txv.m_vInputs.size();
		pN[1] = txv.m_vOutputs.size();
		pN[2] = txv.m_vKernels.size();
	}

	HandleElementVecBwd(txv.m_vKernels, pN[2]);
	HandleElementVecBwd(txv.m_vOutputs, pN[1]);
	HandleElementVecBwd(txv.m_vInputs, pN[0]);

	if (!bOk)
		m_Fwd = true; // restore it to prevent confuse

	return bOk;
}

bool NodeProcessor::BlockInterpretCtx::HandlePbftReward(const std::vector<TxKernel::Ptr>& v, IPbftHandler* pPbft)
{
	if (pPbft)
	{
		AmountBig::Number fees = Zero;
		TxKernel::AddFees(fees, v);

		if (fees != Zero)
		{
			if (m_Fwd)
			{
				m_Storage.PutVarsTerminator();

				if (!pPbft->AddReward(fees, m_Storage))
					return false;
			}
			else
				m_Storage.UndoVars();
		}
	}

	return true;
}

bool NodeProcessor::BlockInterpretCtx::HandleValidatedBlock(const Block::Body& block, IPbftHandler* pPbft)
{
	// make sure we adjust txo count, to prevent the same Txos for consecutive blocks after cut-through
	if (!m_Fwd)
	{
		assert(m_Proc.m_Extra.m_Txos);
		m_Proc.m_Extra.m_Txos--;

		HandlePbftReward(block.m_vKernels, pPbft);
	}

	if (!HandleValidatedTx(block))
		return false;

	// currently there's no extra info in the block that's needed

	if (m_Fwd)
	{
		if (!HandlePbftReward(block.m_vKernels, pPbft))
		{
			bool bFwd = false;
			TemporarySwap swp(bFwd, m_Fwd);
			if (!HandleValidatedTx(block))
				OnCorrupted();

			return false;
		}

		m_Proc.m_Extra.m_Txos++;
	}

	return true;
}

bool NodeProcessor::ExecInDependentContext(IWorker& wrk, const Merkle::Hash* pCtx, const TxPool::Dependent& txp)
{
	if (!pCtx)
		wrk.Do();
	else
	{
		if (m_Cursor.m_Full.m_Prev == *pCtx)
			wrk.Do();
		else
		{
			auto itCtx = txp.m_setContexts.find(*pCtx, TxPool::Dependent::Element::Context::Comparator());
			if (txp.m_setContexts.end() == itCtx)
				return false;

			BlockInterpretCtx bic(*this, m_Cursor.m_hh.m_Height + 1, true);
			bic.m_Temporary = true;
			bic.m_TxValidation = true;
			bic.m_SkipDefinition = true;
			bic.m_AlreadyValidated = true;
			bic.m_SkipInOuts = true;

			DependentContextSwitch dcs(bic);
			if (!dcs.Apply(&itCtx->get_ParentObj()))
				return false;

			wrk.Do();
		}
	}

	return true;
}

bool NodeProcessor::BlockInterpretCtx::HandleBlockElement(const Input& v)
{
	if (m_SkipInOuts)
		return true;

	if (m_Fwd)
	{
		struct Traveler :public UtxoTree::ITraveler {
			bool OnLeaf(const RadixTree::Leaf& x) override {
				return false; // stop iteration
			}
		} t;


		UtxoTree::Key kMin, kMax;

		UtxoTree::Key::Data d;
		d.m_Commitment = v.m_Commitment;
		d.m_Maturity = 0;
		kMin = d;
		d.m_Maturity = m_Height - 1;
		kMax = d;

		UtxoTree::Cursor cu;
		t.m_pCu = &cu;
		t.m_pBound[0] = kMin.V.m_pData;
		t.m_pBound[1] = kMax.V.m_pData;

		if (m_Proc.m_Mapped.m_Utxo.Traverse(t))
		{
			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << "not found Input " << v.m_Commitment;
			return false;
		}

		UtxoTree::MyLeaf* p = &Cast::Up<UtxoTree::MyLeaf>(cu.get_Leaf());

		d = p->m_Key;
		assert(d.m_Commitment == v.m_Commitment);
		assert(d.m_Maturity < m_Height);

		TxoID nID = p->m_ID;

		if (!p->IsExt())
			m_Proc.m_Mapped.m_Utxo.Delete(cu);
		else
		{
			nID = m_Proc.m_Mapped.m_Utxo.PopID(*p);
			cu.InvalidateElement();
			m_Proc.m_Mapped.m_Utxo.OnDirty();
		}

		auto& aux = m_vInpAux.emplace_back();
		aux.m_Maturity = d.m_Maturity;
		aux.m_ID = nID;

	} else
	{
		assert(!m_vInpAux.empty());
		auto aux = std::move(m_vInpAux.back());
		m_vInpAux.pop_back();

		m_Proc.UndoInput(v, aux);
	}

	return true;
}

void NodeProcessor::UndoInput(const Input& v, const InputAux& aux)
{
	UtxoTree::Key::Data d;
	d.m_Commitment = v.m_Commitment;
	d.m_Maturity = aux.m_Maturity;

	bool bCreate = true;
	UtxoTree::Key key;
	key = d;

	m_Mapped.m_Utxo.EnsureReserve();

	UtxoTree::Cursor cu;
	UtxoTree::MyLeaf* p = m_Mapped.m_Utxo.Find(cu, key, bCreate);

	if (bCreate)
		p->m_ID = aux.m_ID;
	else
	{
		m_Mapped.m_Utxo.PushID(aux.m_ID, *p);
		cu.InvalidateElement();
		m_Mapped.m_Utxo.OnDirty();
	}

}

bool NodeProcessor::BlockInterpretCtx::HandleBlockElement(const Output& v)
{
	if (m_SkipInOuts)
		return true;

	UtxoTree::Key::Data d;
	d.m_Commitment = v.m_Commitment;
	d.m_Maturity = m_SkipDefinition ?
		(m_Height - 1) : // allow this output to be spent in exactly this block. Won't happen in normal blocks (matching in/outs are not allowed), but ok when assembling a block, before cut-through
		v.get_MinMaturity(m_Height);

	UtxoTree::Key key;
	key = d;

	m_Proc.m_Mapped.m_Utxo.EnsureReserve();

	UtxoTree::Cursor cu;
	bool bCreate = true;
	UtxoTree::MyLeaf* p = m_Proc.m_Mapped.m_Utxo.Find(cu, key, bCreate);

	cu.InvalidateElement();
	m_Proc.m_Mapped.m_Utxo.OnDirty();

	if (m_Fwd)
	{
		if (!ValidateAssetRange(v.m_pAsset))
			return false;

		TxoID nID = m_Proc.m_Extra.m_Txos;

		if (bCreate)
			p->m_ID = nID;
		else
		{
			// protect again overflow attacks, though it's highly unlikely (Input::Count is currently limited to 32 bits, it'd take millions of blocks)
			Input::Count nCountInc = p->get_Count() + 1;
			if (!nCountInc)
			{
				if (m_pTxErrorInfo)
					*m_pTxErrorInfo << "overflow output " << v.m_Commitment;
				return false;
			}

			m_Proc.m_Mapped.m_Utxo.PushID(nID, *p);
		}

		m_Proc.m_Extra.m_Txos++;

	} else
	{
		assert(m_Proc.m_Extra.m_Txos);
		m_Proc.m_Extra.m_Txos--;

		if (!p->IsExt())
			m_Proc.m_Mapped.m_Utxo.Delete(cu);
		else
			m_Proc.m_Mapped.m_Utxo.PopID(*p);
	}

	return true;
}

void NodeProcessor::BlockInterpretCtx::ManageKrnID(const TxKernel& krn)
{
	if (!m_Height)
		return; // for historical reasons treasury kernels are ignored

	const auto& key = krn.get_ID();

	if (m_Temporary)
	{
		if (m_AlreadyValidated)
			return;

		if (m_Fwd)
			m_KrnIDs.insert(key);
		else
		{
			auto it = m_KrnIDs.find(key);
			assert(m_KrnIDs.end() != it);
			m_KrnIDs.erase(it);
		}
	}
	else
	{
		if (m_Fwd)
			m_Proc.m_DB.InsertKernel(key, m_Height);
		else
			m_Proc.m_DB.DeleteKernel(key, m_Height);
	}

}

bool NodeProcessor::BlockInterpretCtx::HandleBlockElement(const TxKernel& v)
{
	const Rules& r = Rules::get();
	if (m_Fwd && r.IsPastFork_<2>(m_Height) && !m_AlreadyValidated)
	{
		Height hPrev = FindVisibleKernel(v.get_ID());
		if (hPrev)
		{
			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << "Kernel ID=" << v.get_ID() << " duplicated at " << hPrev;

			return false; // duplicated
		}
	}

	if (!m_Fwd)
		ManageKrnID(v);

	if (!HandleKernel(v))
	{
		if (m_pTxErrorInfo)
			*m_pTxErrorInfo << " <- Kernel ID=" << v.get_ID();

		if (!m_Fwd)
			OnCorrupted();
		return false;
	}

	if (m_Fwd)
		ManageKrnID(v);

	return true;
}

bool NodeProcessor::BlockInterpretCtx::HandleKernel(const TxKernel& v)
{
	size_t n = 0;
	bool bOk = true;

	if (m_Fwd)
	{
		// nested
		for (; n < v.m_vNested.size(); n++)
		{
			if (!HandleKernel(*v.m_vNested[n]))
			{
				if (m_pTxErrorInfo)
					*m_pTxErrorInfo << " <- Nested Kernel " << n;

				bOk = false;
				break;
			}
		}
	}
	else
	{
		n = v.m_vNested.size();

		assert(m_nKrnIdx);
		m_nKrnIdx--;
	}

	if (bOk)
	{
		bOk = HandleKernelTypeAny(v);

		if (!bOk && m_pTxErrorInfo)
			*m_pTxErrorInfo << " <- Kernel Type " << (uint32_t) v.get_Subtype();
	}

	if (bOk)
	{
		if (m_Fwd)
			m_nKrnIdx++;
	}
	else
	{
		if (!m_Fwd)
			OnCorrupted();
		m_Fwd = false;
	}

	if (!m_Fwd)
	{
		// nested
		while (n--)
			if (!HandleKernel(*v.m_vNested[n]))
				OnCorrupted();
	}

	if (!bOk)
		m_Fwd = true; // restore it back

	return bOk;
}

bool NodeProcessor::BlockInterpretCtx::HandleKernelTypeAny(const TxKernel& krn)
{
	auto eType = krn.get_Subtype();

	bool bContextChange = !krn.m_CanEmbed && (TxKernel::Subtype::Std != eType);
	if (bContextChange && !m_Fwd && m_Temporary)
	{
		Der der(*this);
		der & m_hvDependentCtx;
	}

	switch (eType)
	{
#define THE_MACRO(id, name) \
	case TxKernel::Subtype::name: \
		if (!HandleKernelType(Cast::Up<TxKernel##name>(krn))) \
			return false; \
		break; 

	BeamKernelsAll(THE_MACRO)
#undef THE_MACRO

	default:
		assert(false); // should not happen!
	}

	if (bContextChange && m_Fwd)
	{
		const auto& hvPrev = m_DependentCtxSet ? m_hvDependentCtx : m_Proc.m_Cursor.m_Full.m_Prev;
		if (m_Temporary)
		{
			Ser ser(*this);
			ser & hvPrev;
		}

		DependentContext::get_Ancestor(m_hvDependentCtx, hvPrev, krn.get_ID());
		m_DependentCtxSet = true;
	}

	return true;
}

bool NodeProcessor::IsShieldedInPool(const Transaction& tx)
{
	struct Walker
		:public TxKernel::IWalker
	{
		NodeProcessor* m_pThis;
		bool OnKrn(const TxKernel& krn) override
		{
			if (krn.get_Subtype() != TxKernel::Subtype::ShieldedInput)
				return true;

			return m_pThis->IsShieldedInPool(Cast::Up<TxKernelShieldedInput>(krn));
		}
	} wlk;
	wlk.m_pThis = this;

	return wlk.Process(tx.m_vKernels);
}

bool NodeProcessor::IsShieldedInPool(const TxKernelShieldedInput& krn)
{
	const Rules& r = Rules::get();
	if (!r.Shielded.Enabled)
		return false;

	if (krn.m_WindowEnd > m_Extra.m_ShieldedOutputs)
		return false;

	if (!(krn.m_SpendProof.m_Cfg == r.Shielded.m_ProofMin))
	{
		if (!(krn.m_SpendProof.m_Cfg == r.Shielded.m_ProofMax))
			return false; // cfg not allowed

		if (m_Extra.m_ShieldedOutputs > krn.m_WindowEnd + r.Shielded.MaxWindowBacklog)
			return false; // large anonymity set is no more allowed, expired
	}

	return true;
}

void NodeProcessor::BlockInterpretCtx::AddKrnInfo(Serializer& ser)
{
	assert(m_pvC);
	auto& vC = *m_pvC;

	for (uint32_t i = 0; i < vC.size(); i++)
	{
		const auto& info = vC[i];

		NodeDB::KrnInfo::Entry x;
		x.m_Pos.m_Height = m_Height;
		x.m_Pos.m_Pos = i + 1;
		x.m_Cid = info.m_Cid;

		ser.reset();
		ser & info;

		SerializeBuffer sb = ser.buffer();
		x.m_Val = Blob(sb.first, static_cast<uint32_t>(sb.second));

		m_Proc.m_DB.KrnInfoInsert(x);
	}
}

NodeProcessor::BlockInterpretCtx::Ser::Ser(BlockInterpretCtx& bic)
	:m_This(bic)
{
	m_Pos = bic.m_Rollback.size();
	swap_buf(bic.m_Rollback);
}

NodeProcessor::BlockInterpretCtx::Ser::~Ser()
{
	if (!std::uncaught_exceptions())
	{
		Marker mk = static_cast<uint32_t>(buffer().second - m_Pos);
		*this & mk;
	}
	swap_buf(m_This.m_Rollback);
}

NodeProcessor::BlockInterpretCtx::Der::Der(BlockInterpretCtx& bic)
{
	ByteBuffer& buf = bic.m_Rollback; // alias

	Ser::Marker mk;
	SetBwd(buf, mk.nBytes);
	*this & mk;

	uint32_t n;
	mk.Export(n);
	SetBwd(buf, n);
}

void NodeProcessor::BlockInterpretCtx::Der::SetBwd(ByteBuffer& buf, uint32_t nPortion)
{
	if (buf.size() < nPortion)
		OnCorrupted();

	size_t nVal = buf.size() - nPortion;
	reset(&buf.front() + nVal, nPortion);

	if (nVal < buf.size()) // to avoid stringop-overflow warning
		buf.resize(nVal); // it's safe to call resize() while the buffer is being used, coz std::vector does NOT reallocate on shrink
}

bool NodeProcessor::BlockInterpretCtx::ValidateUniqueNoDup(const Blob& key, const Blob* pVal)
{
	if (m_Temporary)
	{
		if (m_AlreadyValidated)
			return true;

		auto* pE = m_Dups.Find(key);
		if (m_Fwd)
		{
			if (pE)
				return false; // duplicated

			NodeDB::Recordset rs;
			if (m_Proc.m_DB.UniqueFind(key, rs))
				return false; // duplicated

			pE = m_Dups.Create(key);
		}
		else
		{
			assert(pE);
			m_Dups.Delete(*pE);
		}
	}
	else
	{
		if (m_Fwd)
		{
			if (!m_Proc.m_DB.UniqueInsertSafe(key, pVal))
				return false; // duplicated
		}
		else
			m_Proc.m_DB.UniqueDeleteStrict(key);
	}

	return true;
}

Height NodeProcessor::GetInputMaturity(TxoID id)
{
	// awkward, but this is not used frequently.
	// NodeDB::StateInput doesn't contain the maturity of the spent UTXO. Hence we reconstruct it
	// We find the original UTXO height, and then decode the UTXO body, and check its additional maturity factors (coinbase, incubation)

	NodeDB::WalkerTxo wlk;
	m_DB.TxoGetValue(wlk, id);

	uint8_t pNaked[s_TxoNakedMax];
	Blob val = wlk.m_Value;

	TxoToNaked(pNaked, val);

	Deserializer der;
	der.reset(val.p, val.n);

	Output outp;
	der & outp;

	Height hCreate = 0;
	FindHeightByTxoID(hCreate, id); // relatively heavy operation: search for the original txo height

	return outp.get_MinMaturity(hCreate);
}

} // namespace beam
