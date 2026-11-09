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
#include "bvm2_internal.h"

namespace beam {
namespace bvm2 {

	//BVM_METHOD(LoadVarEx)
	//{
	//	return OnHost_LoadVarEx(nTag, get_AddrR(pKey, nKey), nKey, get_AddrW(pVal, nVal), nVal);
	//}
	//BVM_METHOD_HOST(LoadVarEx)
	//{
	//	Wasm::Test(m_pCid);
	//	Wasm::Test(nKey <= Limits::VarKeySize);

	//	VarKey vk;
	//	vk.Set(*m_pCid);
	//	vk.Append(nTag, Blob(pKey, nKey));

	//	LoadVar(vk, static_cast<uint8_t*>(pVal), nVal);
	//	return nVal;
	//}

	//BVM_METHOD(LoadAllVars)
	//{
	//	struct Marshaller
	//		:public ILoadVarCallback
	//	{
	//		ProcessorManager& m_This;
	//		Wasm::Word m_Object;
	//		Wasm::Word m_Method;
	//
	//		Marshaller(ProcessorManager& x) :m_This(x) {}
	//
	//		uint8_t OnVar(uint8_t nTag, const uint8_t* pKey, uint32_t nKey, const uint8_t* pVal, uint32_t nVal) override
	//		{
	//			Wasm::Word nAliasSp0 = m_This.m_Stack.m_BytesCurrent;
	//
	//			m_This.m_Stack.PushAlias(Blob(pKey, nKey));
	//			Wasm::Word pKey_ = m_This.m_Stack.get_AlasSp();
	//
	//			m_This.m_Stack.PushAlias(Blob(pVal, nVal));
	//			Wasm::Word pVal_ = m_This.m_Stack.get_AlasSp();
	//
	//			Wasm::Word nAliasSp1 = m_This.m_Stack.m_BytesCurrent;
	//			Wasm::Word nOperandSp = m_This.m_Stack.m_Pos;
	//
	//			m_This.m_Stack.Push(m_Object); // 'this' pointer
	//			m_This.m_Stack.Push(nTag);
	//			m_This.m_Stack.Push(pKey_);
	//			m_This.m_Stack.Push(nKey);
	//			m_This.m_Stack.Push(pVal_);
	//			m_This.m_Stack.Push(nVal);
	//
	//			m_This.Run(m_Method);
	//
	//			auto ret = m_This.m_Stack.Pop<Wasm::Word>();
	//
	//			Wasm::Test(nAliasSp1 == m_This.m_Stack.m_BytesCurrent); // alias stack must be restored
	//			Wasm::Test(nOperandSp == m_This.m_Stack.m_Pos); // operand stack must be restored
	//
	//			m_This.m_Stack.m_BytesCurrent = nAliasSp0;
	//
	//
	//			return !!ret;
	//		}
	//
	//	} m(*this);
	//
	//	m.m_Object = pCallback;
	//	m.m_Method = ReadVFunc(pCallback, 0);
	//
	//	return LoadAllVars(m);
	//}
	//
	//BVM_METHOD_HOST(LoadAllVars)
	//{
	//	Wasm::Test(pCallback);
	//	return LoadAllVars(*pCallback);
	//}
	BVM_METHOD(SelectContext)
	{
		return OnHost_SelectContext(bDependent, nChargeNeeded);
	}
	BVM_METHOD_HOST(SelectContext)
	{
		bool b = !!bDependent;
		if (MaxHeight != m_Context.m_Height)
		{
			bool b0 = !!m_Context.m_pParent;
			if (b == b0)
				return; // no change

			m_Context.Reset();
		}

		SelectContext(b, nChargeNeeded);
	}

	BVM_METHOD(Vars_Enum)
	{
		return OnHost_Vars_Enum(get_AddrR(pKey0, nKey0), nKey0, get_AddrR(pKey1, nKey1), nKey1);
	}
	BVM_METHOD_HOST(Vars_Enum)
	{
		if (!EnsureContext())
			return 0;

		IReadVars::Ptr pObj;
		VarsEnum(Blob(pKey0, nKey0), Blob(pKey1, nKey1), pObj);
		if (!pObj)
			return 0;

		ZeroObject(pObj->m_LastKey);
		ZeroObject(pObj->m_LastVal);

		uint32_t nKey = m_mapReadVars.empty() ? 1 : m_mapReadVars.rbegin()->m_Key + 1;
		pObj->m_Key = nKey;
		m_mapReadVars.insert(*pObj.release());

		return nKey;
	}

	BVM_METHOD(Vars_MoveNext)
	{
		auto& nKey_ = get_AddrAsW<Wasm::Word>(nKey);
		auto& nVal_ = get_AddrAsW<Wasm::Word>(nVal);

		auto nKeySize = Wasm::from_wasm(nKey_);
		auto nValSize = Wasm::from_wasm(nVal_);

		auto nRet = OnHost_Vars_MoveNext(iSlot, get_AddrW(pKey, nKeySize), nKeySize, get_AddrW(pVal, nValSize), nValSize, nRepeat);

		nKey_ = Wasm::to_wasm(nKeySize);
		nVal_ = Wasm::to_wasm(nValSize);

		return nRet;
	}
	BVM_METHOD_HOST(Vars_MoveNext)
	{
		auto it = m_mapReadVars.find(iSlot, IReadVars::Comparator());
		Exc::Test(m_mapReadVars.end() != it);

		auto& x = *it;
		if (!nRepeat && !x.MoveNext())
			return 0;

		memcpy(pKey, x.m_LastKey.p, std::min(nKey, x.m_LastKey.n));
		memcpy(pVal, x.m_LastVal.p, std::min(nVal, x.m_LastVal.n));

		nKey = x.m_LastKey.n;
		nVal = x.m_LastVal.n;
		return 1;
	}

	BVM_METHOD(Vars_Close)
	{
		return OnHost_Vars_Close(iSlot);
	}
	BVM_METHOD_HOST(Vars_Close)
	{
		auto it = m_mapReadVars.find(iSlot, IReadVars::Comparator());
		Exc::Test(m_mapReadVars.end() != it);
		m_mapReadVars.Delete(*it);
	}

	const HeightPos* Processor::FromWasmOpt(Wasm::Word pPos, HeightPos& buf)
	{
		if (!pPos)
			return nullptr;

		auto& pos_ = get_AddrAsR<HeightPos>(pPos);
		buf.m_Height = Wasm::from_wasm(pos_.m_Height);
		buf.m_Pos = Wasm::from_wasm(pos_.m_Pos);

		return &buf;
	}

	BVM_METHOD(Logs_Enum)
	{
		HeightPos posMin, posMax;
		return OnHost_Logs_Enum(get_AddrR(pKey0, nKey0), nKey0, get_AddrR(pKey1, nKey1), nKey1, FromWasmOpt(pPosMin, posMin), FromWasmOpt(pPosMax, posMax));
	}
	BVM_METHOD_HOST(Logs_Enum)
	{
		if (!EnsureContext())
			return 0;

		IReadLogs::Ptr pObj;
		LogsEnum(Blob(pKey0, nKey0), Blob(pKey1, nKey1), pPosMin, pPosMax, pObj);
		if (!pObj)
			return 0;

		ZeroObject(pObj->m_LastKey);
		ZeroObject(pObj->m_LastVal);
		ZeroObject(pObj->m_LastPos);

		uint32_t nKey = m_mapReadLogs.empty() ? 1 : m_mapReadLogs.rbegin()->m_Key + 1;
		pObj->m_Key = nKey;
		m_mapReadLogs.insert(*pObj.release());

		return nKey;
	}

	BVM_METHOD(Logs_MoveNext)
	{
		auto& nKey_ = get_AddrAsW<Wasm::Word>(nKey);
		auto& nVal_ = get_AddrAsW<Wasm::Word>(nVal);

		auto nKeySize = Wasm::from_wasm(nKey_);
		auto nValSize = Wasm::from_wasm(nVal_);

		auto nRet = OnHost_Logs_MoveNext(iSlot, get_AddrW(pKey, nKeySize), nKeySize, get_AddrW(pVal, nValSize), nValSize, get_AddrAsW<HeightPos>(pos), nRepeat);

		nKey_ = Wasm::to_wasm(nKeySize);
		nVal_ = Wasm::to_wasm(nValSize);

		return nRet;
	}
	BVM_METHOD_HOST(Logs_MoveNext)
	{
		auto it = m_mapReadLogs.find(iSlot, IReadLogs::Comparator());
		Exc::Test(m_mapReadLogs.end() != it);

		auto& x = *it;
		if (!nRepeat && !x.MoveNext())
			return 0;

		memcpy(pKey, x.m_LastKey.p, std::min(nKey, x.m_LastKey.n));
		memcpy(pVal, x.m_LastVal.p, std::min(nVal, x.m_LastVal.n));
		pos = x.m_LastPos;

		nKey = x.m_LastKey.n;
		nVal = x.m_LastVal.n;
		return 1;
	}

	BVM_METHOD(Logs_Close)
	{
		return OnHost_Logs_Close(iSlot);
	}
	BVM_METHOD_HOST(Logs_Close)
	{
		auto it = m_mapReadLogs.find(iSlot, IReadLogs::Comparator());
		Exc::Test(m_mapReadLogs.end() != it);
		m_mapReadLogs.Delete(*it);
	}

	BVM_METHOD(Assets_Enum)
	{
		return OnHost_Assets_Enum(aid0, h);
	}
	BVM_METHOD_HOST(Assets_Enum)
	{
		IReadAssets::Ptr pObj;
		AssetsEnum(aid0, h, pObj);
		if (!pObj)
			return 0;

		pObj->m_aiLast.m_ID = 0;

		uint32_t nKey = m_mapReadAssets.empty() ? 1 : m_mapReadAssets.rbegin()->m_Key + 1;
		pObj->m_Key = nKey;
		m_mapReadAssets.insert(*pObj.release());

		return nKey;
	}

	BVM_METHOD(Assets_MoveNext)
	{
		auto& nMetadata_ = get_AddrAsW<Wasm::Word>(nMetadata);
		auto nMetadataSize = Wasm::from_wasm(nMetadata_);

		auto ret = OnHost_Assets_MoveNext(iSlot, get_AddrAsW<AssetInfo>(res), get_AddrW(pMetadata, nMetadataSize), nMetadataSize, nRepeat);

		nMetadata_ = Wasm::to_wasm(nMetadataSize);
		return ret;
	}
	BVM_METHOD_HOST(Assets_MoveNext)
	{
		auto it = m_mapReadAssets.find(iSlot, IReadAssets::Comparator());
		Exc::Test(m_mapReadAssets.end() != it);

		auto& x = *it;
		if (nRepeat)
		{
			if (!x.m_aiLast.m_ID)
				return 0; // MoveNext never called
		}
		else
		{
			if (!x.MoveNext())
				return 0;
		}

		nMetadata = ProcessorPlus::CvtAssetInfo(res, x.m_aiLast, pMetadata, nMetadata);
		return x.m_aiLast.m_ID;
	}

	BVM_METHOD(Assets_Close)
	{
		return OnHost_Assets_Close(iSlot);
	}
	BVM_METHOD_HOST(Assets_Close)
	{
		auto it = m_mapReadAssets.find(iSlot, IReadAssets::Comparator());
		Exc::Test(m_mapReadAssets.end() != it);
		m_mapReadAssets.Delete(*it);
	}

	uint32_t ProcessorManager::VarGetProofInternal(const void* pKey, uint32_t nKey, Wasm::Word& pVal, Wasm::Word& nVal, Wasm::Word& pProof)
	{
		if (!EnsureContext())
			return 0;

		ByteBuffer val;
		beam::Merkle::Proof proof;

		Blob key(pKey, nKey);
		if (!VarGetProof(key, val, proof))
		{
			FreeAuxAllocGuarded();
			return 0;
		}

		uint32_t nSizeVal = static_cast<uint32_t>(val.size());
		uint32_t nProof = static_cast<uint32_t>(proof.size());
		uint32_t nSizeProof = nProof * static_cast<uint32_t>(sizeof(proof[0]));

		uint8_t* pDst = ResizeAux(nSizeVal + nSizeProof);

		nVal = nSizeVal;
		pVal = m_AuxAlloc.m_pPtr;
		if (nSizeVal)
			memcpy(pDst, &val.front(), nSizeVal);

		auto* pProofDst = reinterpret_cast<Merkle::Node*>(pDst + nSizeVal);
		pProof = m_AuxAlloc.m_pPtr + nSizeVal;
		if (nProof)
			memcpy((void*)pProofDst, &proof.front(), nSizeProof);

		return nProof;
	}

	BVM_METHOD(VarGetProof)
	{
		Wasm::Word pVal, nVal, pProof;
		uint32_t nRet = VarGetProofInternal(get_AddrR(pKey, nKey), nKey, pVal, nVal, pProof);

		if (nRet)
		{
			Wasm::to_wasm(get_AddrW(pnVal, sizeof(Wasm::Word)), nVal);
			Wasm::to_wasm(get_AddrW(ppVal, sizeof(Wasm::Word)), pVal);
			Wasm::to_wasm(get_AddrW(ppProof, sizeof(Wasm::Word)), pProof);
		}

		return nRet;
	}

	BVM_METHOD_HOST(VarGetProof)
	{
		Wasm::Word pVal, pProof;
		uint32_t nRet = VarGetProofInternal(pKey, nKey, pVal, *pnVal, pProof);

		if (nRet)
		{
			const uint8_t* p = get_AddrR(m_AuxAlloc.m_pPtr, 1);
			*ppVal = p + pVal;
			*ppProof = reinterpret_cast<const Merkle::Node*>(p + pProof);
		}

		return nRet;
	}

	uint32_t ProcessorManager::LogGetProofInternal(const HeightPos& hp, Wasm::Word& pProof)
	{
		if (!EnsureContext())
			return 0;

		ByteBuffer val;
		beam::Merkle::Proof proof;

		if (!LogGetProof(hp, proof))
		{
			FreeAuxAllocGuarded();
			return 0;
		}

		uint32_t nProof = static_cast<uint32_t>(proof.size());
		uint32_t nSizeProof = nProof * static_cast<uint32_t>(sizeof(proof[0]));

		uint8_t* pDst = ResizeAux(nSizeProof);

		auto* pProofDst = reinterpret_cast<Merkle::Node*>(pDst);
		pProof = m_AuxAlloc.m_pPtr;
		if (nProof)
			memcpy((void*) pProofDst, &proof.front(), nSizeProof);

		return nProof;
	}

	BVM_METHOD(LogGetProof)
	{
		const auto& pos_ = get_AddrAsR<HeightPos>(pos);

		HeightPos hp;
		hp.m_Height = Wasm::from_wasm(pos_.m_Height);
		hp.m_Pos = Wasm::from_wasm(pos_.m_Pos);

		Wasm::Word pProof;
		uint32_t nRet = LogGetProofInternal(hp, pProof);

		if (nRet)
			Wasm::to_wasm(get_AddrW(ppProof, sizeof(Wasm::Word)), pProof);

		return nRet;
	}

	BVM_METHOD_HOST(LogGetProof)
	{
		Wasm::Word pProof;
		uint32_t nRet = LogGetProofInternal(pos, pProof);

		if (nRet)
		{
			const uint8_t* p = get_AddrR(m_AuxAlloc.m_pPtr, 1);
			*ppProof = reinterpret_cast<const Merkle::Node*>(p + pProof);
		}

		return nRet;
	}

} // namespace bvm2
} // namespace beam
