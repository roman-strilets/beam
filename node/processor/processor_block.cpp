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

bool NodeProcessor::ExtractTreasury(const Blob& blob, Treasury::Data& td)
{
	Deserializer der;
	der.reset(blob.p, blob.n);

	try {
		der & td;
	}
	catch (const std::exception&) {
		BEAM_LOG_WARNING() << "Treasury corrupt";
		return false;
	}

	return true;
}

bool NodeProcessor::HandleTreasury(const Blob& blob)
{
	assert(!IsTreasuryHandled());

	Treasury::Data td;
	if (!ExtractTreasury(blob, td))
		return false;

	if (!td.IsValid())
	{
		BEAM_LOG_WARNING() << "Treasury validation failed";
		return false;
	}

	{
		std::vector<Treasury::Data::Burst> vBursts = td.get_Bursts();

		std::ostringstream os;
		os << "Treasury check. Total bursts=" << vBursts.size();

		for (size_t i = 0; i < vBursts.size(); i++)
		{
			const Treasury::Data::Burst& b = vBursts[i];
			os << "\n\t" << "Height=" << b.m_Height << ", Aid=" << b.m_Aid << ", Value=" << AmountBig::Printable(b.m_Value);
		}

		BEAM_LOG_INFO() << os.str();
	}

	BlockInterpretCtx bic(*this, 0, true);
	BlockInterpretCtx::ChangesFlush cf(*this);

	std::ostringstream osErr;
	bic.m_pTxErrorInfo = &osErr;

	std::vector<ContractInvokeExtraInfo> vC;
	if (m_DB.ParamIntGetDef(NodeDB::ParamID::RichContractInfo))
		bic.m_pvC = &vC;


	for (size_t iG = 0; iG < td.m_vGroups.size(); iG++)
	{
		if (!bic.HandleValidatedTx(td.m_vGroups[iG].m_Data))
		{
			// undo partial changes
			bic.m_Fwd = false;
			while (iG--)
			{
				if (!bic.HandleValidatedTx(td.m_vGroups[iG].m_Data))
					OnCorrupted(); // although should not happen anyway
			}

			BEAM_LOG_WARNING() << "Treasury invalid";
			return false;
		}
	}

	cf.Do(*this, 0);

	if (bic.m_pvC)
	{
		Serializer ser;
		bic.AddKrnInfo(ser);
	}

	Serializer ser;
	TxoID id0 = 0;

	for (size_t iG = 0; iG < td.m_vGroups.size(); iG++)
	{
		for (size_t i = 0; i < td.m_vGroups[iG].m_Data.m_vOutputs.size(); i++, id0++)
		{
			ser.reset();
			ser & *td.m_vGroups[iG].m_Data.m_vOutputs[i];

			SerializeBuffer sb = ser.buffer();
			m_DB.TxoAdd(id0, Blob(sb.first, static_cast<uint32_t>(sb.second)));
		}
	}

	return true;
}

bool NodeProcessor::TestBlock(const HeightHash& id, const Block::SystemState::Full& s, const proto::BodyBuffers& bufs)
{
	MultiblockContext mbc(*this);

	if (!HandleBlockInternal(id, s, mbc, bufs, true, true, Zero, 0))
		return false;

	return mbc.Flush();
}

bool NodeProcessor::HandleBlock(const HeightHash& id, uint64_t row, const Block::SystemState::Full& s, MultiblockContext& mbc)
{
	proto::BodyBuffers bufs;
	m_DB.GetStateBlock(row, &bufs.m_Perishable, &bufs.m_Eternal, nullptr);

	PeerID pid = Zero;

	bool bFirstTime = (m_DB.get_StateTxos(row) == MaxHeight);
	if (bFirstTime)
		m_DB.get_Peer(row, pid);

	return HandleBlockInternal(id, s, mbc, bufs, bFirstTime, false, pid, row);
}

bool NodeProcessor::HandleBlockInternal(const HeightHash& id, const Block::SystemState::Full& s, MultiblockContext& mbc, const proto::BodyBuffers& bufs, bool bFirstTime, bool bTestOnly, const PeerID& pid, uint64_t row)
{
	if (s.m_Number.v == m_ManualSelection.m_Sid.m_Number.v)
	{
		if (!m_ManualSelection.IsAllowed(id.m_Hash))
			return false;
	}

	if (bFirstTime)
		mbc.OnNextBlockPid(pid);

	MultiblockContext::MyTask::SharedBlock::Ptr pShared = std::make_shared<MultiblockContext::MyTask::SharedBlock>(mbc, s.m_Number);
	Block::Body& block = pShared->m_Body;

	const auto& r = Rules::get();
	IPbftHandler* pPbft = nullptr;
	Merkle::Hash hvVs;

	Deserializer der;
	der.reset(bufs.m_Perishable);

	try {
		der & Cast::Down<Block::BodyBase>(block);
		der & Cast::Down<TxVectors::Perishable>(block);

		der.reset(bufs.m_Eternal);
		der & Cast::Down<TxVectors::Eternal>(block);

		if (Rules::Consensus::Pbft == r.m_Consensus)
		{
			Exc::CheckpointTxt cp("pbft block data");

			pPbft = get_PbftHandler();
			if (!pPbft)
				Exc::Fail();

			if (bFirstTime)
				pPbft->TestBlock(s, id, block, bTestOnly);

			pPbft->get_Validators().get_Hash(hvVs);
		}
	}
	catch (const std::exception& e) {
		BEAM_LOG_WARNING() << id << " Block deserialization failed " << e.what();
		return false;
	}

	if (bFirstTime)
	{
		// Protect against blocks with invalid offset (historically happened in explorer node)

		// Block with zero offset can be created in PBFT consensus algorithm, empty blocks are normal.
		// However only the last block can be empty (due to cannibalization).
		// Hence it's not applicable to Fast-Sync case.
		if ((s.m_Number.v <= m_SyncData.m_TxoLo.v) && (block.m_Offset.m_Value == Zero))
		{
			BEAM_LOG_WARNING() << id << " has zero offset";
			return false;
		}


		pShared->m_Size = bufs.m_Perishable.size() + bufs.m_Eternal.size();
		pShared->m_Ctx.m_Height = id.m_Height;

		mbc.OnBlock(pShared);

		if (Rules::Consensus::Pbft != r.m_Consensus)
		{
			// Chainwork test isn't really necessary, already tested in DB. Just for more safety.
			Difficulty::Raw wrk = m_Cursor.m_Full.m_ChainWork + s.m_PoW.m_Difficulty;

			if (wrk != s.m_ChainWork)
			{
				BEAM_LOG_WARNING() << id << " Chainwork expected=" << wrk.full() << ", actual=" << s.m_ChainWork.full();
				return false;
			}

			if (m_Cursor.m_DifficultyNext.m_Packed != s.m_PoW.m_Difficulty.m_Packed)
			{
				BEAM_LOG_WARNING() << id << " Difficulty expected=" << m_Cursor.m_DifficultyNext << ", actual=" << s.m_PoW.m_Difficulty;
				return false;
			}

			if (s.m_TimeStamp <= get_MovingMedian())
			{
				BEAM_LOG_WARNING() << id << " Timestamp inconsistent wrt median";
				return false;
			}
		}
	}

	TxoID id0 = m_Extra.m_Txos;

	BlockInterpretCtx bic(*this, id.m_Height, true);
	if (bTestOnly)
		bic.m_Temporary = true;

	BlockInterpretCtx::ChangesFlush cf(*this);
	if (!bFirstTime)
		bic.m_AlreadyValidated = true;

	// check contract signatures in the verification threads, along with the rest of the block
	std::vector<BlockInterpretCtx::ContractSig> vContractSigs;
	if (bFirstTime)
		bic.m_pvContractSigs = &vContractSigs;

	std::ostringstream osErr;
	bic.m_pTxErrorInfo = &osErr;

	std::vector<ContractInvokeExtraInfo> vC;
	if (m_DB.ParamIntGetDef(NodeDB::ParamID::RichContractInfo))
		bic.m_pvC = &vC;

	bool bOk = bic.HandleValidatedBlock(block, pPbft);
	if (!bOk)
	{
		assert(bFirstTime);
		assert(m_Extra.m_Txos == id0);
		BEAM_LOG_WARNING() << id << " invalid in its context: " << osErr.str();
	}
	else
	{
		assert(m_Extra.m_Txos > id0);
	}

	EvaluatorEx ev(*this);
	ev.set_Kernels(block);
	ev.set_Logs(bic.m_vLogs);

	Merkle::Hash hvDef;
	ev.m_Height = id.m_Height;
	ev.m_Number = s.m_Number;

	bool bPastFork3 = r.IsPastFork_<3>(id.m_Height);
	bool bPastFastSync = (s.m_Number.v >= m_SyncData.m_TxoLo.v);
	bool bDefinition = bPastFork3 || bPastFastSync;

	if (bDefinition)
		ev.get_Definition(hvDef);

	if (bOk)
	{
		if (bFirstTime)
		{
			if (bDefinition)
			{
				// check the validity of state description.
				if (s.m_Definition != hvDef)
				{
					BEAM_LOG_WARNING() << id << " Header Definition mismatch";
					bOk = false;
				}
			}

			if (bPastFork3)
			{
				if (bPastFastSync)
				{
					get_Utxos().get_Hash(hvDef);
					if (s.m_Kernels != hvDef)
					{
						BEAM_LOG_WARNING() << id << " Utxos mismatch";
						bOk = false;
					}
				}
			}
			else
			{
				if (s.m_Kernels != ev.m_hvKernels)
				{
					BEAM_LOG_WARNING() << id << " Kernel commitment mismatch";
					bOk = false;
				}
			}

			if (s.m_Number.v <= m_SyncData.m_TxoLo.v)
			{
				// make sure no spent txos above the requested h0
				for (size_t i = 0; i < bic.m_vInpAux.size(); i++)
				{
					if (bic.m_vInpAux[i].m_ID >= mbc.m_id0)
					{
						BEAM_LOG_WARNING() << id << " Invalid input in sparse block";
						bOk = false;
						break;
					}
				}
			}
		}

		if (bOk && pPbft)
			try
			{
				Merkle::Hash hvVsNext;
				pPbft->get_Validators().get_Hash(hvVsNext);
				Merkle::Interpret(hvVs, hvVsNext, true);

				if (hvVs != Cast::Reinterpret<Block::Pbft::HdrData>(s.m_PoW).m_hvVsBoth)
					Exc::Fail("vs.both");
			}
			catch (const std::exception& e) {
				BEAM_LOG_WARNING() << id << " " << e.what();
				bOk = false;
			}

		if (bOk && !vContractSigs.empty())
			mbc.OnContractSigs(std::move(vContractSigs));

		if (!bOk || bTestOnly)
		{
			bic.m_Fwd = false;
			BEAM_VERIFY(bic.HandleValidatedBlock(block, pPbft));
		}
	}

	if (bOk && !bTestOnly)
	{
		m_Cursor.m_hvKernels = ev.m_hvKernels;
		m_Cursor.m_bKernels = true;

		AdjustOffset(m_Cursor.m_StateExtra.m_TotalOffset, block.m_Offset, true);

		StateExtra::Comms& comms = m_Cursor.m_StateExtra; // downcast
		if (bDefinition)
			comms = ev.m_Comms;
		else
		{
			assert(!bPastFork3);
			ZeroObject(comms);
		}

		Blob blobExtra;
		blobExtra.p = &m_Cursor.m_StateExtra;
		if (bPastFork3)
		{
			blobExtra.n = sizeof(m_Cursor.m_StateExtra);

			// omit trailing hashes if they're zero. Make sure to always include offset
			for (; blobExtra.n > sizeof(m_Cursor.m_StateExtra.m_TotalOffset); blobExtra.n--)
				if (reinterpret_cast<const uint8_t*>(blobExtra.p)[blobExtra.n - 1])
					break;
		}
		else
			blobExtra.n = sizeof(m_Cursor.m_StateExtra.m_TotalOffset);

		Blob blobRB(bic.m_Rollback);
		m_DB.set_StateTxosAndExtra(row, &m_Extra.m_Txos, &blobExtra, &blobRB);

		std::vector<NodeDB::StateInput> v;
		v.reserve(block.m_vInputs.size());

		assert(block.m_vInputs.size() == bic.m_vInpAux.size());
		for (size_t i = 0; i < block.m_vInputs.size(); i++)
		{
			const Input& x = *block.m_vInputs[i];
			auto txoID = bic.m_vInpAux[i].m_ID;

			m_DB.TxoSetSpent(txoID, id.m_Height);
			v.emplace_back().Set(txoID, x.m_Commitment);
		}

		if (!v.empty())
			m_DB.set_StateInputs(row, &v.front(), v.size());

		Serializer ser;

		// recognize all
		MyRecognizer rec(*this);

		for (const auto& acc : m_vAccounts)
		{
			rec.m_Handler.m_pAccount = &acc;
			rec.m_Recognizer.m_Pos = id.m_Height;
			rec.m_Recognizer.RecognizeBlock(block, bic.m_ShieldedOuts);
		}

		bic.m_Rollback.clear();
		ser.swap_buf(bic.m_Rollback); // optimization

		for (size_t i = 0; i < block.m_vOutputs.size(); i++)
		{
			const Output& x = *block.m_vOutputs[i];

			ser.reset();
			ser & x;

			SerializeBuffer sb = ser.buffer();
			m_DB.TxoAdd(id0++, Blob(sb.first, static_cast<uint32_t>(sb.second)));
		}

		m_RecentStates.Push(row, s);

		cf.Do(*this, id.m_Height);

		if (bic.m_pvC)
			bic.AddKrnInfo(ser);
	}
	else
	{
		m_DB.AssetEvtsDeleteFrom(id.m_Height);

		if (!bOk)
			OnInvalidBlock(s, block);
	}

	return bOk;
}

void NodeProcessor::ReadOffset(ECC::Scalar& offs, uint64_t rowid)
{
	static_assert(sizeof(StateExtra::Base) == sizeof(offs));
	if (m_DB.get_StateExtra(rowid, &offs, sizeof(offs)) < sizeof(offs))
		OnCorrupted();
}

void NodeProcessor::AdjustOffset(ECC::Scalar& offs, const ECC::Scalar& offsPrev, bool bAdd)
{
	ECC::Scalar::Native s(offsPrev);
	if (!bAdd)
		s = -s;

	s += offs;
	offs = s;
}

} // namespace beam
