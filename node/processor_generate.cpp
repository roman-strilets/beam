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

uint8_t NodeProcessor::ValidateTxContextEx(const Transaction& tx, const HeightRange& hr, bool bShieldedTested, uint32_t& nBvmCharge, TxPool::Dependent::Element* pParent, std::ostream* pExtraInfo, Merkle::Hash* pCtxNew)
{
	Height h = m_Cursor.m_hh.m_Height + 1;

	if (!hr.IsInRange(h))
	{
		if (pExtraInfo)
			*pExtraInfo << "Height range fail";
		return proto::TxStatus::InvalidContext;
	}

	BlockInterpretCtx bic(*this, h, true);

	bic.m_Temporary = true;
	bic.m_TxValidation = true;
	bic.m_SkipDefinition = true;
	bic.m_pTxErrorInfo = pExtraInfo;
	bic.m_AlreadyValidated = true;

	DependentContextSwitch dcs(bic);
	if (!dcs.Apply(pParent))
	{
		BEAM_LOG_WARNING() << "can't switch dependent context"; // normally should not happen
		return proto::TxStatus::DependentNoParent;
	}

	bool bNewVal = false;
	TemporarySwap ts(bic.m_AlreadyValidated, bNewVal);

	// Cheap tx verification. No need to update the internal structure, recalculate definition, or etc.

	// Ensure input UTXOs are present
	for (size_t i = 0; i < tx.m_vInputs.size(); i++)
	{
		Input::Count nCount = 1;
		const Input& v = *tx.m_vInputs[i];

		for (; i + 1 < tx.m_vInputs.size(); i++, nCount++)
			if (tx.m_vInputs[i + 1]->m_Commitment != v.m_Commitment)
				break;

		if (!ValidateInputs(v.m_Commitment, nCount))
		{
			if (pExtraInfo)
				*pExtraInfo << "Inputs missing";

			return proto::TxStatus::InvalidInput; // some input UTXOs are missing
		}
	}

	nBvmCharge = bic.m_ChargePerBlock;

	size_t n = 0;
	bool bOk = bic.HandleElementVecFwd(tx.m_vKernels, n);

	if (bOk && pCtxNew)
		*pCtxNew = bic.m_DependentCtxSet ? bic.m_hvDependentCtx : m_Cursor.m_Full.m_Prev;

	nBvmCharge -= bic.m_ChargePerBlock;

	if (!bic.m_ShieldedIns)
		bShieldedTested = true;

	bic.m_Fwd = false;
	bic.HandleElementVecBwd(tx.m_vKernels, n);

	if (!bOk)
	{
		if (proto::TxStatus::Unspecified != bic.m_TxStatus)
			return bic.m_TxStatus;

		if (bic.m_LimitExceeded)
			return proto::TxStatus::LimitExceeded;

		return proto::TxStatus::InvalidContext;
	}

	// Ensure output assets are in range
	for (size_t i = 0; i < tx.m_vOutputs.size(); i++)
		if (!bic.ValidateAssetRange(tx.m_vOutputs[i]->m_pAsset))
			return proto::TxStatus::InvalidContext;

	if (!bShieldedTested)
	{
		ECC::InnerProduct::BatchContextEx<4> bc;
		MultiShieldedContext msc;

		msc.Prepare(tx, *this, h);

		bool bValid = msc.IsValid(tx, h, bc, 0, 1, m_ValCache);
		if (bValid)
		{
			msc.Calculate(bc.m_Sum, *this);
			bValid = bc.Flush();
		}

		if (!bValid)
		{
			if (pExtraInfo)
				*pExtraInfo << "bad shielded input";
			return proto::TxStatus::InvalidInput;
		}

		msc.MoveToGlobalCache(m_ValCache);
	}

	return proto::TxStatus::Ok;
}

bool NodeProcessor::ValidateInputs(const ECC::Point& comm, Input::Count nCount /* = 1 */)
{
	struct Traveler :public UtxoTree::ITraveler
	{
		uint32_t m_Count;
		bool OnLeaf(const RadixTree::Leaf& x) override
		{
			const UtxoTree::MyLeaf& n = Cast::Up<UtxoTree::MyLeaf>(x);
			Input::Count nCount = n.get_Count();
			assert(m_Count && nCount);
			if (m_Count <= nCount)
				return false; // stop iteration

			m_Count -= nCount;
			return true;
		}
	} t;
	t.m_Count = nCount;


	UtxoTree::Key kMin, kMax;

	UtxoTree::Key::Data d;
	d.m_Commitment = comm;
	d.m_Maturity = 0;
	kMin = d;
	d.m_Maturity = m_Cursor.m_hh.m_Height;
	kMax = d;

	UtxoTree::Cursor cu;
	t.m_pCu = &cu;
	t.m_pBound[0] = kMin.V.m_pData;
	t.m_pBound[1] = kMax.V.m_pData;

	return !m_Mapped.m_Utxo.Traverse(t);
}

size_t NodeProcessor::BlockInterpretCtx::GenerateNewBlockInternal(BlockContext& bc)
{
	if (!HandleValidatedTx(bc.m_Block)) // pre-added elements
	{
		bc.m_Block.m_vInputs.clear();
		bc.m_Block.m_vOutputs.clear();
		bc.m_Block.m_vKernels.clear();
		return 0;
	}

	// Generate the block up to the allowed size.
	// All block elements are serialized independently, their binary size can just be added to the size of the "empty" block.

	SerializerSizeCounter ssc;
	ssc & bc.m_Block;

	const auto& r = Rules::get();
	auto& rbs = m_Proc.m_ReserveBlockSize; // alias
	if (rbs.m_Size && (Rules::Consensus::Pbft != r.m_Consensus) && (r.IsPastFork_<6>(m_Height) != r.IsPastFork_<6>(rbs.m_Height)))
		rbs.m_Size = 0; // no more relevant

	if (!rbs.m_Size)
	{
		SerializerSizeCounter ssc2;

		if (Rules::Consensus::Pbft != r.m_Consensus)
		{
			Output outp;
			outp.m_pPublic.reset(new ECC::RangeProof::Public);
			ZeroObject(*outp.m_pPublic);
			outp.m_pPublic->m_Value = static_cast<Amount>(-1); // pessimistic

			ssc2 & outp;

			if (!r.IsPastFork_<6>(m_Height))
			{
				// explicit fee UTXO
				outp.m_pPublic.reset();
				outp.m_pConfidential.reset(new ECC::RangeProof::Confidential);
				ZeroObject(*outp.m_pConfidential);
				outp.m_pAsset = std::make_unique<Asset::Proof>();
				outp.m_pAsset->InitArrays(r.CA.m_ProofCfg);
				outp.m_pAsset->m_Begin = static_cast<Asset::ID>(-1);

				ssc2 & outp;
			}

			TxKernelStd krn = {};
			krn.m_Height.m_Min = MaxHeight; // pessimistic
			ssc2 & krn;
		}
		else
		{
			// size of extra kernel
			TxKernelContractInvoke krn;
			krn.m_Height.m_Min = MaxHeight;
			krn.m_Commitment = Zero;
			ZeroObject(krn.m_Signature);
			krn.m_iMethod = std::numeric_limits<uint32_t>::max();
			krn.m_Args.resize(100); // excessive

			ssc2 & krn;
		}

		rbs.m_Size = ssc2.m_Counter.m_Value;
		rbs.m_Height = m_Height;
	}

	ssc.m_Counter.m_Value += rbs.m_Size; // pre-add it

	const size_t nSizeMax = r.MaxBodySize;
	if (ssc.m_Counter.m_Value > nSizeMax)
	{
		// the block may be non-empty (i.e. pre-selected txs)
		BEAM_LOG_WARNING() << "Block too large.";
		return 0; //
	}

	Amount feesReserve = static_cast<Amount>(-1);
	if ((Rules::Consensus::Pbft != r.m_Consensus) && r.IsPastFork_<6>(m_Height))
		feesReserve -= r.get_Emission(m_Height);

	Block::Builder bb(bc.m_SubIdx, bc.m_Coin, bc.m_Tag, m_Height);
	ECC::Scalar::Native offset = bc.m_Block.m_Offset;
	size_t nTxNum = 0;
	DependentContextSwitch::Vec vDependent;
	DependentContextSwitch::Convert(vDependent, bc.m_pParent);

	for (size_t i = 0; i < vDependent.size(); i++)
	{
		// Theoretically for dependent txs can set m_AlreadyValidated flag. But it's not good to mix validated and non-validated in the same pass (ManageKrnID would be confused).
		// For now - ignore this optimization

		const auto& x = *vDependent[i];
		Amount txFee = x.m_Fee;
		auto nSize = x.m_Size;
		if (x.m_pParent)
		{
			txFee -= x.m_pParent->m_Fee;
			nSize -= x.m_pParent->m_Size;
		}

		if (txFee > feesReserve)
			break; // huge fees are unsupported

		size_t nSizeNext = ssc.m_Counter.m_Value + nSize;
		if (nSizeNext > nSizeMax)
			break;

		Transaction& tx = *x.m_pValue;

		assert(!m_LimitExceeded);
		if (!HandleValidatedTx(tx))
		{
			m_LimitExceeded = false;
			break;
		}

		TxVectors::Writer(bc.m_Block, bc.m_Block).Dump(tx.get_Reader());

		feesReserve -= txFee;
		bc.m_Fees += txFee;
		ssc.m_Counter.m_Value = nSizeNext;
		offset += ECC::Scalar::Native(tx.m_Offset);
		++nTxNum;
	}

	for (TxPool::Fluff::ProfitSet::iterator it = bc.m_TxPool.m_setProfit.begin(); bc.m_TxPool.m_setProfit.end() != it; )
	{
		TxPool::Fluff::Element& x = (it++)->get_ParentObj();

		if (x.m_Profit.m_Stats.m_Fee > feesReserve)
			continue; // huge fees are unsupported

		size_t nSizeNext = ssc.m_Counter.m_Value + x.m_Profit.m_Stats.m_Size;
		if (nSizeNext > nSizeMax)
		{
			if (bc.m_Block.IsEmpty())
			{
				// won't fit in empty block
				BEAM_LOG_INFO() << "Tx is too big.";
				bc.m_TxPool.Delete(x);
			}
			continue;
		}

		Transaction& tx = *x.m_pValue;

		bool bDelete = !x.m_Profit.m_Stats.m_Hr.IsInRange(m_Height);
		if (!bDelete)
		{
			assert(!m_LimitExceeded);
			if (HandleValidatedTx(tx))
			{
				TxVectors::Writer(bc.m_Block, bc.m_Block).Dump(tx.get_Reader());

				feesReserve -= x.m_Profit.m_Stats.m_Fee;
				bc.m_Fees += x.m_Profit.m_Stats.m_Fee;

				ssc.m_Counter.m_Value = nSizeNext;
				offset += ECC::Scalar::Native(tx.m_Offset);
				++nTxNum;
			}
			else
			{
				if (m_LimitExceeded)
					m_LimitExceeded = false; // don't delete it, leave it for the next block
				else
					bDelete = true;
			}
		}

		if (bDelete)
		{
			x.m_Hist.m_Height = m_Proc.m_Cursor.m_hh.m_Height;
			bc.m_TxPool.SetState(x, TxPool::Fluff::State::Outdated); // isn't available in this context
		}
	}

	if (BlockContext::Mode::Assemble != bc.m_Mode)
	{
		size_t n0 = ssc.m_Counter.m_Value;
		ssc.m_Counter.m_Value -= rbs.m_Size; // was pre-added, now remove it

		if (Rules::Consensus::Pbft != r.m_Consensus)
		{
			Asset::Proof::Params::Override po(m_Proc.get_AidMax());
			Output::Ptr ppOutp[2];
			TxKernel::Ptr pKrn;
			bb.AddCoinbaseAndKrn(bc.m_Fees, ppOutp[0], ppOutp[1], pKrn);

			for (uint32_t i = 0; i < _countof(ppOutp); i++)
			{
				auto& pOutp = ppOutp[i];
				if (!pOutp)
					continue;

				if (!HandleBlockElement(*pOutp))
					return 0;

				ssc & (*pOutp);
				bc.m_Block.m_vOutputs.push_back(std::move(pOutp));
			}

			if (pKrn)
			{
				if (!HandleBlockElement(*pKrn))
					return 0;

				ssc & pKrn;
				bc.m_Block.m_vKernels.push_back(std::move(pKrn));
			}

			assert(ssc.m_Counter.m_Value <= n0);
			(n0); // suppress 'unused' warning in release build
		}


		bb.m_Offset = -bb.m_Offset;
		offset += bb.m_Offset;
	}

	BEAM_LOG_INFO() << "GenerateNewBlock: size of block = " << ssc.m_Counter.m_Value << "; num txs = " << nTxNum;

	bc.m_Block.m_Offset = offset;

	return ssc.m_Counter.m_Value;
}

void NodeProcessor::BlockInterpretCtx::GenerateNewHdr(BlockContext& bc)
{
#ifndef NDEBUG
	// kernels must be sorted already
	for (size_t i = 1; i < bc.m_Block.m_vKernels.size(); i++)
	{
		const TxKernel& krn0 = *bc.m_Block.m_vKernels[i - 1];
		const TxKernel& krn1 = *bc.m_Block.m_vKernels[i];
		assert(krn0 <= krn1);
	}
#endif // NDEBUG

	EvaluatorEx ev(m_Proc);
	ev.m_Number = bc.m_Hdr.m_Number;
	ev.m_Height = m_Height;
	ev.set_Kernels(bc.m_Block);
	ev.set_Logs(m_vLogs);

	ev.get_Definition(bc.m_Hdr.m_Definition);

	if (Rules::get().IsPastFork_<3>(ev.m_Height))
		m_Proc.get_Utxos().get_Hash(bc.m_Hdr.m_Kernels);
	else
		bc.m_Hdr.m_Kernels = ev.m_hvKernels;
}

NodeProcessor::BlockContext::BlockContext(TxPool::Fluff& txp, Key::Index nSubKey, Key::IKdf& coin, Key::IPKdf& tag)
	:m_TxPool(txp)
	,m_pParent(nullptr)
	,m_SubIdx(nSubKey)
	,m_Coin(coin)
	,m_Tag(tag)
{
	m_Fees = 0;
	m_Block.ZeroInit();
}

bool NodeProcessor::GenerateNewBlock(BlockContext& bc)
{
	bc.m_Hdr.m_Number.v = m_Cursor.m_Full.m_Number.v + 1;

	BlockInterpretCtx bic(*this, m_Cursor.m_hh.m_Height + 1, true);
	bic.m_Temporary = true;
	bic.m_SkipDefinition = true;

	size_t nSizeEstimated = 1;

	if (BlockContext::Mode::Finalize == bc.m_Mode)
	{
		if (!bic.HandleValidatedTx(bc.m_Block))
			return false;
	}
	else
		nSizeEstimated = bic.GenerateNewBlockInternal(bc);

	bic.m_Fwd = false;
    BEAM_VERIFY(bic.HandleValidatedTx(bc.m_Block)); // undo changes
	assert(bic.m_Rollback.empty());
	assert(bic.m_vInpAux.empty());

	if (!nSizeEstimated)
		return false;

	if (BlockContext::Mode::Assemble == bc.m_Mode)
		return true;

	size_t nCutThrough = bc.m_Block.Normalize(); // right before serialization
	nCutThrough; // remove "unused var" warning

	// The effect of the cut-through block may be different than it was during block construction, because the consumed and created UTXOs (removed by cut-through) could have different maturities.
	// Hence - we need to re-apply the block after the cut-throught, evaluate the definition, and undo the changes (once again).
	//
	// In addition to this, kernels reorder may also have effect: shielded outputs may get different IDs
	bic.m_Fwd = true;
	bic.m_AlreadyValidated = true;
	bic.m_SkipDefinition = false;

	IPbftHandler* pPbft = nullptr;

	const auto& r = Rules::get();
	if (Rules::Consensus::Pbft == r.m_Consensus)
	{
		pPbft = get_PbftHandler();
		if (!pPbft)
			return false;

		auto& d = Cast::Reinterpret<Block::Pbft::HdrData>(bc.m_Hdr.m_PoW);
		pPbft->get_Validators().get_Hash(d.m_hvVsBoth);
	}

	bool bOk = bic.HandleValidatedBlock(bc.m_Block, pPbft);
	if (!bOk)
	{
		BEAM_LOG_WARNING() << "couldn't apply block after cut-through!";
		ZeroObject(bc.m_Hdr);
		OnInvalidBlock(bc.m_Hdr, bc.m_Block);
		return false; // ?!
	}

	bc.m_Hdr.m_Prev = m_Cursor.m_hh.m_Hash;
	bc.m_Hdr.m_ChainWork = m_Cursor.m_Full.m_ChainWork;

	if (Rules::Consensus::Pbft == r.m_Consensus)
	{
		Merkle::Hash hvVsNext;
		pPbft->get_Validators().get_Hash(hvVsNext);

		auto& d = Cast::Reinterpret<Block::Pbft::HdrData>(bc.m_Hdr.m_PoW);
		Merkle::Interpret(d.m_hvVsBoth, hvVsNext, true);

		// Assume d.m_Time_ms and the hdr timestamp are already assigned by the caller

		if (m_Cursor.m_hh.m_Height)
		{
			d.m_Flags1 = bc.m_Block.IsEmpty() ? Block::Pbft::HdrData::Flags::Empty : 0;

			assert(bic.m_Height == m_Cursor.m_hh.m_Height + 1);
			uint32_t dh = 1;

			auto& d0 = Cast::Reinterpret<Block::Pbft::HdrData>(m_Cursor.m_Full.m_PoW);
			if (Block::Pbft::HdrData::Flags::Empty & d0.m_Flags1)
			{
				// cannibalize the previous empty block
				bc.m_Hdr.m_Prev = m_Cursor.m_Full.m_Prev;
				bc.m_Hdr.m_Number = m_Cursor.m_Full.m_Number;
				bc.m_Hdr.m_ChainWork -= m_Cursor.m_Full.m_PoW.m_Difficulty;

				dh += r.Difficulty2Span(m_Cursor.m_Full.m_PoW.m_Difficulty);
			}

			bc.m_Hdr.m_PoW.m_Difficulty = r.Span2Difficulty(dh);

		}
		else
		{
			d.m_Flags1 = 0; // very 1st block
			bc.m_Hdr.m_PoW.m_Difficulty = r.DA.Difficulty0;
		}
	}
	else
	{
		bc.m_Hdr.m_TimeStamp = getTimestamp();
		// Adjust the timestamp to be no less than the moving median (otherwise the block'll be invalid)
		Timestamp tm = get_MovingMedian() + 1;
		beam::setmax(bc.m_Hdr.m_TimeStamp, tm);

		bc.m_Hdr.m_PoW.m_Difficulty = m_Cursor.m_DifficultyNext;
	}

	bc.m_Hdr.m_ChainWork += bc.m_Hdr.m_PoW.m_Difficulty; // always, in either mode

	bic.GenerateNewHdr(bc);

	bic.m_Fwd = false;
    BEAM_VERIFY(bic.HandleValidatedBlock(bc.m_Block, pPbft)); // undo changes
	assert(bic.m_Rollback.empty());

	Serializer ser;

	ser.reset();
	ser & Cast::Down<Block::BodyBase>(bc.m_Block);
	ser & Cast::Down<TxVectors::Perishable>(bc.m_Block);
	ser.swap_buf(bc.m_Body.m_Perishable);

	ser.reset();
	ser & Cast::Down<TxVectors::Eternal>(bc.m_Block);
	ser.swap_buf(bc.m_Body.m_Eternal);

	size_t nSize = bc.m_Body.m_Perishable.size() + bc.m_Body.m_Eternal.size();

	if (BlockContext::Mode::SinglePass == bc.m_Mode)
	{
		// the actual block size may be less because of:
		// 1. Cut-through removed some data
		// 2. our size estimation is a little pessimistic because of extension of kernels. If all kernels are standard, then 1 bytes per kernel is saved
		assert(nCutThrough ?
			(nSize < nSizeEstimated) :
			(
				(nSize == nSizeEstimated) ||
				(nSize == nSizeEstimated - bc.m_Block.m_vKernels.size())
			)
		);
	}

	return nSize <= Rules::get().MaxBodySize;
}

} // namespace beam
