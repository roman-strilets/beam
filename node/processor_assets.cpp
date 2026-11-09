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

void NodeProcessor::AssetDataPacked::set_Strict(const Blob& blob)
{
	if (sizeof(*this) != blob.n)
		OnCorrupted();
	memcpy(this, blob.p, sizeof(*this));
}

bool NodeProcessor::InternalAssetAdd(Asset::Full& ai, bool bMmr)
{
	ai.m_Value = Zero;
	if (!m_DB.AssetAdd(ai))
		return false;

	assert(ai.m_ID);

	if (bMmr)
	{
		uint32_t nCount = ai.m_ID - Rules::get().CA.ForeignEnd;

		if (m_Mmr.m_Assets.m_Count < nCount)
			m_Mmr.m_Assets.ResizeTo(nCount);

		Merkle::Hash hv;
		ai.get_Hash(hv);
		m_Mmr.m_Assets.Replace(nCount - 1, hv);
	}

	return true;
}

void NodeProcessor::InternalAssetDel(Asset::ID nAssetID, bool bMmr)
{
	Asset::ID aidMax = m_DB.AssetDelete(nAssetID);

	if (bMmr)
	{
		Asset::ID aid0 = Rules::get().CA.ForeignEnd;
		uint32_t nCount = aidMax - aid0;
		assert(nCount <= m_Mmr.m_Assets.m_Count);

		if (nCount < m_Mmr.m_Assets.m_Count)
			m_Mmr.m_Assets.ResizeTo(nCount);
		else
		{
			assert(nAssetID < aidMax);
			m_Mmr.m_Assets.Replace(nAssetID - (aid0 + 1), Zero);
		}
	}
}

bool NodeProcessor::BlockInterpretCtx::HandleKernelType(const TxKernelAssetCreate& krn)
{
	Asset::ID aid;
	Amount valDeposit;
	return HandleAssetCreate(krn.m_Owner, nullptr, krn.m_MetaData, aid, valDeposit);
}

bool NodeProcessor::BlockInterpretCtx::HandleAssetCreate(const PeerID& pidOwner, const ContractID* pCid, const Asset::Metadata& md, Asset::ID& aid, Amount& valDeposit, uint32_t nSubIdx)
{
	if (m_Fwd)
	{
		if (!m_AlreadyValidated && m_Proc.m_DB.AssetFindByOwner(pidOwner))
		{
			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << "duplicate asset owner: " << pidOwner;
			return false;
		}

		Asset::Full ai;
		ai.m_ID = 0; // auto
		ai.m_Owner = pidOwner;
		ai.m_LockHeight = m_Height;
		ai.m_Deposit = valDeposit = Rules::get().get_DepositForCA(m_Height);
		ai.SetCid(pCid);
		ai.m_Metadata.m_Hash = md.m_Hash;

		{
			TemporarySwap<ByteBuffer> ts(Cast::NotConst(md).m_Value, ai.m_Metadata.m_Value);

			if (!m_Proc.InternalAssetAdd(ai, !m_SkipDefinition))
			{
				if (m_pTxErrorInfo)
					*m_pTxErrorInfo << "assets overflow";
				return false;
			}
		}

		Ser ser(*this);
		ser & ai.m_ID;

		if (!m_Temporary)
		{
			NodeDB::AssetEvt evt;
			evt.m_ID = ai.m_ID + Asset::s_MaxCount;

			ByteBuffer bufBlob;
			bufBlob.resize(sizeof(AssetCreateInfoPacked) + md.m_Value.size());
			auto* pAcip = reinterpret_cast<AssetCreateInfoPacked*>(&bufBlob.front());

			pAcip->m_OwnedByContract = !!pCid;
			Cast::Down<ECC::uintBig>(pAcip->m_Owner) = pAcip->m_OwnedByContract ? (*pCid) : Cast::Down<ECC::uintBig>(pidOwner);
			if (!md.m_Value.empty())
				memcpy(pAcip + 1, &md.m_Value.front(), md.m_Value.size());

			evt.m_Body = bufBlob;

			AssetEvtInsert(evt, nSubIdx);
		}

		aid = ai.m_ID;
	}
	else
	{
		Der der(*this);

		der & aid;

		m_Proc.InternalAssetDel(aid, !m_SkipDefinition);
	}

	return true;
}

bool NodeProcessor::BlockInterpretCtx::HandleKernelType(const TxKernelAssetDestroy& krn)
{
	Amount valDeposit = krn.get_Deposit();
	if (!HandleAssetDestroy(krn.m_Owner, nullptr, krn.m_AssetID, valDeposit, true))
		return false;

	return true;
}

bool NodeProcessor::BlockInterpretCtx::HandleAssetDestroy(const PeerID& pidOwner, const ContractID* pCid, Asset::ID aid, Amount& valDeposit, bool bDepositCheck, uint32_t nSubIdx)
{
	if (HandleAssetDestroy2(pidOwner, pCid, aid, valDeposit, bDepositCheck, nSubIdx))
		return true;

	if (m_pTxErrorInfo)
		*m_pTxErrorInfo << ", AssetID=" << aid;

	return false;
}

bool NodeProcessor::BlockInterpretCtx::HandleAssetDestroy2(const PeerID& pidOwner, const ContractID* pCid, Asset::ID aid, Amount& valDeposit, bool bDepositCheck, uint32_t nSubIdx)
{
	if (m_Fwd)
	{
		Asset::Full ai;
		ai.m_ID = aid;
		if (!m_Proc.m_DB.AssetGetSafe(ai))
		{
			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << "not found";
			return false;
		}

		if (!m_AlreadyValidated)
		{
			if (ai.m_Owner != pidOwner)
			{
				if (m_pTxErrorInfo)
					*m_pTxErrorInfo << "Not owned";
				return false;
			}

			if (ai.m_Value != Zero)
			{
				if (m_pTxErrorInfo)
					*m_pTxErrorInfo << "Value=" << AmountBig::Printable(ai.m_Value);
				return false;
			}

			if (ai.m_LockHeight + Rules::get().CA.LockPeriod > m_Height)
			{
				if (m_pTxErrorInfo)
					*m_pTxErrorInfo << "LockHeight=" << ai.m_LockHeight;

				return false;
			}

			if (bDepositCheck && (valDeposit != ai.m_Deposit))
			{
				if (m_pTxErrorInfo)
					*m_pTxErrorInfo << "Deposit expected=" << ai.m_Deposit << ", actual=" << valDeposit;
				return false;
			}
		}

		// looks good
		m_Proc.InternalAssetDel(aid, !m_SkipDefinition);

		Ser ser(*this);
		ser
			& ai.m_Metadata
			& ai.m_LockHeight;

		if (Rules::get().IsPastFork_<5>(m_Height))
			ser & ai.m_Deposit;
		else
			assert(Rules::get().CA.DepositForList2 == ai.m_Deposit);

		valDeposit = ai.m_Deposit;

		if (!m_Temporary)
		{
			NodeDB::AssetEvt evt;
			evt.m_ID = aid + Asset::s_MaxCount;
			ZeroObject(evt.m_Body);
			AssetEvtInsert(evt, nSubIdx);
		}
	}
	else
	{
		Asset::Full ai;
		ai.m_ID = aid;
		ai.SetCid(pCid);

		Der der(*this);
		der
			& ai.m_Metadata
			& ai.m_LockHeight;

		if (pCid)
			ai.m_Metadata.get_Owner(ai.m_Owner, *pCid);
		else
			ai.m_Owner = pidOwner;

		if (Rules::get().IsPastFork_<5>(m_Height))
			der & ai.m_Deposit;
		else
			ai.m_Deposit = Rules::get().CA.DepositForList2;

		if (!m_Proc.InternalAssetAdd(ai, !m_SkipDefinition))
			OnCorrupted();

		if (ai.m_ID != aid)
			OnCorrupted();
	}

	return true;
}

bool NodeProcessor::BlockInterpretCtx::HandleKernelType(const TxKernelAssetEmit& krn)
{
	return HandleAssetEmit(krn.m_Owner, krn.m_AssetID, krn.m_Value);
}

bool NodeProcessor::BlockInterpretCtx::HandleAssetEmit(const PeerID& pidOwner, Asset::ID aid, AmountSigned val, uint32_t nSubIdx)
{
	bool bRes = Rules::get().CA.IsForeign(aid) ?
		HandleAssetEmitForeign(pidOwner, aid, val, nSubIdx) :
		HandleAssetEmitLocal(pidOwner, aid, val, nSubIdx);

	if (bRes)
		return true;

	if (m_pTxErrorInfo)
		*m_pTxErrorInfo << ", AssetID=" << aid;

	return false;
}

bool NodeProcessor::BlockInterpretCtx::HandleAssetEmitLocal(const PeerID& pidOwner, Asset::ID aid, AmountSigned val, uint32_t nSubIdx)
{
	Asset::Full ai;
	ai.m_ID = aid;
	if (!m_Proc.m_DB.AssetGetSafe(ai))
	{
		if (m_pTxErrorInfo)
			*m_pTxErrorInfo << "not found";
		return false;
	}

	bool bAdd;
	Amount valUns = SplitAmountSigned(val, bAdd);

	if (m_Fwd)
	{
		if (!m_AlreadyValidated && (ai.m_Owner != pidOwner))
		{
			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << "not owned";
			return false; // as well
		}
	}
	else
		bAdd = !bAdd;

	auto val0 = ai.m_Value.ToNumber();
	AmountBig::Number valDelta = valUns;

	bool bWasZero = (ai.m_Value == Zero);

	if (bAdd)
	{
		val0 += valDelta;
		if (val0 < valDelta)
		{
			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << "too large";

			return false; // overflow (?!)
		}
	}
	else
	{
		if (val0 < valDelta)
		{
			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << "too low";

			return false; // not enough to burn
		}

		val0 -= valDelta;
	}

	ai.m_Value.FromNumber(val0);

	bool bZero = (val0 == Zero);
	if (bZero != bWasZero)
	{
		if (m_Fwd)
		{
			Ser ser(*this);
			ser & ai.m_LockHeight;

			ai.m_LockHeight = m_Height;
		}
		else
		{
			Der der(*this);
			der & ai.m_LockHeight;
		}
	}

	m_Proc.m_DB.AssetSetValue(ai.m_ID, ai.m_Value, ai.m_LockHeight);

	if (!m_SkipDefinition)
	{
		Merkle::Hash hv;
		ai.get_Hash(hv);

		m_Proc.m_Mmr.m_Assets.Replace(ai.m_ID - (Rules::get().CA.ForeignEnd + 1), hv);
	}

	if (m_Fwd && !m_Temporary)
	{
		AssetDataPacked adp;
		adp.m_Amount = ai.m_Value;
		adp.m_LockHeight = ai.m_LockHeight;

		NodeDB::AssetEvt evt;
		evt.m_ID = aid;
		evt.m_Body.p = &adp;
		evt.m_Body.n = sizeof(adp);

		AssetEvtInsert(evt, nSubIdx);
	}

	return true;
}

bool NodeProcessor::BridgeAddInfo(const PeerID& pidOwner, const HeightPos& pos, Asset::ID aid, Amount val)
{
	ForeignDetailsPacked fdp;
	fdp.m_Aid = aid;
	fdp.m_Amount = val;
	Blob blobVal(&fdp, sizeof(fdp));

	return m_DB.BridgeInsertSafe(pos, pidOwner, &blobVal);
}

bool NodeProcessor::FindExternalAssetEmit(const PeerID& pidOwner, bool bEmit, ForeignDetailsPacked& fdp)
{
	ECC::Point key;
	key.m_X = Cast::Down<ECC::uintBig>(pidOwner);
	key.m_Y = 4;

	if (!bEmit)
		key.m_Y |= 8;

	NodeDB::Recordset rs;
	Blob blob(&key, sizeof(key));
	if (!m_DB.UniqueFind(blob, rs))
		return false;

	const auto& fep = rs.get_As<ForeignEmitPacked>(0);
	fdp = fep.m_Details;

	return true;
}

bool NodeProcessor::BlockInterpretCtx::HandleAssetEmitForeign(const PeerID& pidOwner, Asset::ID aid, AmountSigned val, uint32_t nSubIdx)
{
	ECC::Point key;
	key.m_X = Cast::Down<ECC::uintBig>(pidOwner);
	key.m_Y = 4;

	bool bAdd;
	Amount valUns = SplitAmountSigned(val, bAdd);

	if (!bAdd)
		key.m_Y |= 8;
	Blob blobKey(&key, sizeof(key));

	ForeignEmitPacked fep;
	fep.m_Details.m_Aid = aid;
	fep.m_Details.m_Amount = valUns;

	if (m_Fwd && !m_AlreadyValidated && bAdd && !m_TxValidation && m_Temporary) // verify bridge ONLY when checking block proposals. But not when interpeting already signed blocks, or broadcasting txs
	{
		NodeDB::Recordset rs;
		Blob blobDetails;

		m_Proc.m_DB.BridgeFind(pidOwner, blobDetails, rs);
		if (sizeof(ForeignDetailsPacked) != blobDetails.n)
		{
			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << "bridge no key";
			return false;
		}

		const auto& fdp = *reinterpret_cast<const ForeignDetailsPacked*>(blobDetails.p);
		if ((fdp.m_Aid != fep.m_Details.m_Aid) || (fdp.m_Amount != fep.m_Details.m_Amount))
		{
			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << "bridge details mismatch";
			return false;
		}
	}

	fep.m_Height = m_Height;
	fep.m_nIdx = m_nKrnIdx;

	Blob blobVal(&fep, sizeof(fep));

	if (!ValidateUniqueNoDup(blobKey, &blobVal))
	{
		if (m_pTxErrorInfo)
			*m_pTxErrorInfo << "double-emit";
		return false;
	}

	if (m_Fwd && !m_Temporary)
	{
		// register the emission in evts
		AssetDataPacked adp;
		adp.m_LockHeight = Zero;
		adp.m_Amount = valUns;

		if (bAdd)
			adp.m_Amount.Negate();

		{
			NodeDB::WalkerAssetEvt wlk;
			m_Proc.m_DB.AssetEvtsEnumBwd(wlk, aid, m_Height);
			if (wlk.MoveNext())
			{
				AssetDataPacked adp0;
				adp0.set_Strict(wlk.m_Body);
				adp.m_Amount += adp0.m_Amount;
			}
		}

		NodeDB::AssetEvt evt;
		evt.m_ID = aid;
		evt.m_Body.p = &adp;
		evt.m_Body.n = sizeof(adp);

		AssetEvtInsert(evt, 0); // will include height+krnIdx
	}

	return true;
}

void NodeProcessor::BlockInterpretCtx::AssetEvtInsert(NodeDB::AssetEvt& evt, uint32_t nSubIdx)
{
	evt.m_Height = m_Height;
	evt.m_Index = get_AssetEvtIdx(m_nKrnIdx, nSubIdx);
	m_Proc.m_DB.AssetEvtsInsert(evt);
}

int NodeProcessor::get_AssetAt(Asset::Full& ai, Height h, bool bFindAid)
{
	assert(h <= m_Cursor.m_hh.m_Height);

	// search for create/destroy
	NodeDB::WalkerAssetEvt wlk;
	if (bFindAid)
		m_DB.AssetEvtsEnumBwd2(wlk, ai.m_ID + Asset::s_MaxCount, h);
	else
		m_DB.AssetEvtsEnumBwd(wlk, ai.m_ID + Asset::s_MaxCount, h);

	if (!wlk.MoveNext())
		return 0; // never existed

	if (!wlk.m_Body.n) // last was destroy
		return -1;

	get_AssetCreateInfo(ai, wlk);

	typedef std::pair<Height, uint64_t> HeightAndIndex;
	HeightAndIndex hiCreate(wlk.m_Height, wlk.m_Index);

	m_DB.AssetEvtsEnumBwd(wlk, ai.m_ID, h);
	if (wlk.MoveNext() && (HeightAndIndex(wlk.m_Height, wlk.m_Index) > hiCreate))
	{
		AssetDataPacked adp;
		adp.set_Strict(wlk.m_Body);

		ai.m_Value = adp.m_Amount;
		adp.m_LockHeight.Export(ai.m_LockHeight);

	}
	else
	{
		// wasn't ever emitted
		ai.m_LockHeight = wlk.m_Height;
		ai.m_Value = Zero;
	}

	return 1;
}

void NodeProcessor::get_AssetCreateInfo(Asset::CreateInfo& ai, const NodeDB::WalkerAssetEvt& wlk)
{
	if (wlk.m_Body.n < sizeof(AssetCreateInfoPacked))
		OnCorrupted();

	auto* pAcip = reinterpret_cast<const AssetCreateInfoPacked*>(wlk.m_Body.p);

	ai.m_Metadata.m_Value.resize(wlk.m_Body.n - sizeof(AssetCreateInfoPacked));
	if (!ai.m_Metadata.m_Value.empty())
		memcpy(&ai.m_Metadata.m_Value.front(), pAcip + 1, ai.m_Metadata.m_Value.size());
	ai.m_Metadata.UpdateHash();

	if (pAcip->m_OwnedByContract)
	{
		ai.SetCid(&pAcip->m_Owner);
		ai.m_Metadata.get_Owner(ai.m_Owner, ai.m_Cid);
	}
	else
	{
		ai.SetCid(nullptr);
		ai.m_Owner = pAcip->m_Owner;

	}

	ai.m_Deposit = Rules::get().get_DepositForCA(wlk.m_Height);
}

} // namespace beam
