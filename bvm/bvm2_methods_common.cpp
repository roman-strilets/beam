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

	template<>
	const char* ProcessorPlus::ToHost<const char*, Wasm::Word>(Wasm::Word sz)
	{
		return RealizeStr(sz);
	}

	BVM_METHOD(Write)
	{
		OnHost_Write(get_AddrR(pData, nData), nData, iStream);
	}
	BVM_METHOD_HOST(Write)
	{
		if (nData)
			WriteStream(Blob(pData, nData), iStream);
	}

	BVM_METHOD(Memcpy)
	{
		OnHost_Memcpy(get_AddrW(pDst, size), get_AddrR(pSrc, size), size);
		return pDst;
	}
	BVM_METHOD_HOST(Memcpy)
	{
		DischargeMemOp(size);
		// prefer to use memmove
		return memmove(pDst, pSrc, size);
	}

	BVM_METHOD(Memset)
	{
		OnHost_Memset(get_AddrW(pDst, size), val, size);
		return pDst;
	}
	BVM_METHOD_HOST(Memset)
	{
		DischargeMemOp(size);
		return memset(pDst, val, size);
	}

	BVM_METHOD(Memcmp)
	{
		return OnHost_Memcmp(get_AddrR(p1, size), get_AddrR(p2, size), size);
	}
	BVM_METHOD_HOST(Memcmp)
	{
		DischargeMemOp(size);
		return static_cast<int32_t>(memcmp(p1, p2, size));
	}

	BVM_METHOD(Memis0)
	{
		return OnHost_Memis0(get_AddrR(p, size), size);
	}
	BVM_METHOD_HOST(Memis0)
	{
		DischargeMemOp(size);
		bool bRes = memis0(p, size);
		return !!bRes;
	}

	const char* Processor::RealizeStr(Wasm::Word sz, uint32_t& nLenOut)
	{
		Exc::CheckpointTxt cp("string/realize");

		uint32_t n;
		auto sz_ = reinterpret_cast<const char*>(get_AddrExVar(sz, n, false));

		auto* p = reinterpret_cast<const char*>(memchr(sz_, 0, n));
		Exc::Test(p);
		nLenOut = static_cast<uint32_t>(p - sz_);

		DischargeMemOp(nLenOut + 1);

		return sz_;
	}

	const char* Processor::RealizeStr(Wasm::Word sz)
	{
		uint32_t nLenOut;
		return RealizeStr(sz, nLenOut);
	}

	BVM_METHOD(Strlen)
	{
		uint32_t n;
		RealizeStr(sz, n);
		return n;
	}
	BVM_METHOD_HOST(Strlen)
	{
		return static_cast<uint32_t>(strlen(sz));
	}

	BVM_METHOD_VIA_HOST(Strcmp)
	BVM_METHOD_HOST(Strcmp)
	{
		return static_cast<int32_t>(strcmp(sz1, sz2));
	}

	BVM_METHOD(StackAlloc)
	{
		m_Stack.AliasAlloc(size);
		return m_Stack.get_AlasSp();
	}
	BVM_METHOD_HOST(StackAlloc)
	{
		m_Stack.AliasAlloc(size);
		return m_Stack.get_AliasPtr();
	}

	BVM_METHOD(StackFree)
	{
		m_Stack.AliasFree(size);
	}
	BVM_METHOD_HOST_AUTO(StackFree)

	void ProcessorContract::SetVarKeyFromShader(VarKey& vk, uint8_t nTag, const Blob& key, bool bW)
	{
		Exc::Test(key.n <= Limits::VarKeySize);
		SetVarKey(vk, nTag, key);

		if (bW)
		{
			switch (nTag)
			{
			case VarKey::Tag::Internal:
			case VarKey::Tag::InternalStealth:
				break; // ok

			default:
				Exc::Fail(); // not allowed to modify/emit
			}
		}
	}

	BVM_METHOD(LoadVar)
	{
		uint32_t ret = OnHost_LoadVar(get_AddrR(pKey, nKey), nKey, get_AddrW(pVal, nVal), nVal, nType);

		DischargeUnits(Limits::Cost::LoadVar_For(std::min(nVal, ret)));

		return ret;
	}
	BVM_METHOD_HOST(LoadVar)
	{
		Blob key(pKey, nKey);
		VarKey vk;

		if (Kind::Contract == get_Kind())
		{
			((ProcessorPlusEnv_Contract*) this)->SetVarKeyFromShader(vk, nType, key, false);
			key = vk.ToBlob();
		}
		else
		{
			// impose the same restriction for apps
			Exc::Test(key.n <= Limits::VarKeySize);
		}

		Blob res;
		get_Storage().LoadVar(key, res);

		memcpy(pVal, res.p, std::min(nVal, res.n));
		return res.n;
	}

	BVM_METHOD(LoadVarEx)
	{
		auto& nKey_ = get_AddrAsW<Wasm::Word>(nKey);
		auto& nVal_ = get_AddrAsW<Wasm::Word>(nVal);

		auto nKeySize = Wasm::from_wasm(nKey_);
		auto nValSize = Wasm::from_wasm(nVal_);
		auto nValSize0 = nValSize;

		OnHost_LoadVarEx(get_AddrW(pKey, nKeyBufSize), nKeySize, nKeyBufSize, get_AddrW(pVal, nValSize), nValSize, nType, nSearchFlag);

		DischargeUnits(Limits::Cost::LoadVar_For(std::min(nValSize, nValSize0)));

		nKey_ = Wasm::to_wasm(nKeySize);
		nVal_ = Wasm::to_wasm(nValSize);
	}
	BVM_METHOD_HOST(LoadVarEx)
	{
		Blob key(pKey, nKey);
		VarKey vk;

		bool bContract = (Kind::Contract == get_Kind());
		if (bContract)
		{
			Exc::Test(IsPastFork_<4>());

			beam::setmin(nKeyBufSize, Limits::VarKeySize);
			Exc::Test(nKey <= nKeyBufSize);

			((ProcessorPlusEnv_Contract*) this)->SetVarKeyFromShader(vk, nType, key, false);
			key = vk.ToBlob();
		}
		else
		{
			// impose the same restriction for apps
			Exc::Test(key.n <= Limits::VarKeySize);
		}

		Blob res;
		get_Storage().LoadVarEx(key, res,
			!!(Shaders::KeySearchFlags::Exact & nSearchFlag),
			!!(Shaders::KeySearchFlags::Bigger & nSearchFlag));

		if (bContract)
		{
			// make sure it's still the current contract+type
			bool bMatch = (key.n > ContractID::nBytes);
			if (bMatch)
			{
				auto pK = (const uint8_t*)key.p;
				const auto& cid = ((ProcessorPlusEnv_Contract*) this)->m_FarCalls.m_Stack.back().m_Cid;

				if (!memcmp(pK, cid.m_pData, cid.nBytes) && (nType == pK[cid.nBytes]))
				{
					// ok
					key.n -= (ContractID::nBytes + 1);
					((const uint8_t*&) key.p) += (ContractID::nBytes + 1);
				}
				else
					bMatch = false;
			}

			if (!bMatch)
			{
				key.n = 0;
				res.n = 0;
			}
		}

		nKey = key.n;
		memcpy(pKey, key.p, std::min(nKey, nKeyBufSize));

		memcpy(pVal, res.p, std::min(nVal, res.n));
		nVal = res.n;
	}

	BVM_METHOD(SaveVar)
	{
		DischargeUnits(Limits::Cost::SaveVar_For(nVal));
		return OnHost_SaveVar(get_AddrR(pKey, nKey), nKey, get_AddrR(pVal, nVal), nVal, nType);
	}
	BVM_METHOD_HOST(SaveVar)
	{
		Blob key(pKey, nKey);
		VarKey vk;

		if (Kind::Contract == get_Kind())
		{
			((ProcessorPlusEnv_Contract*) this)->TestVarSize(nVal);
			((ProcessorPlusEnv_Contract*) this)->TestCanWrite();

			((ProcessorPlusEnv_Contract*) this)->SetVarKeyFromShader(vk, nType, key, true);
			key = vk.ToBlob();
		}
		else
		{
			Exc::Test(key.n <= Limits::VarKeySize);
			Exc::Test(nVal <= Limits::VarSize_4);
		}

		return get_Storage().SaveVar(key, Blob(pVal, nVal));
	}

	BVM_METHOD(get_Height)
	{
		return get_Height();
	}
	BVM_METHOD_HOST_AUTO(get_Height)

	BVM_METHOD(get_HdrInfo)
	{
		DischargeUnits(Limits::Cost::LoadVar);
		auto& hdr_ = get_AddrAsW<BlockHeader::Info>(hdr); // currently ignore alignment
		hdr_.m_Height = Wasm::from_wasm(hdr_.m_Height);
		OnHost_get_HdrInfo(hdr_);
		hdr_.Convert<true>();
	}
	BVM_METHOD_HOST(get_HdrInfo)
	{
		Block::SystemState::Full s;
		Exc::Test(get_HdrAt(s, hdr.m_Height));

		CvtHdr(hdr, s);
		s.get_Hash(hdr.m_Hash);
	}

	BVM_METHOD(get_HdrFull)
	{
		DischargeUnits(Limits::Cost::LoadVar);
		auto& hdr_ = get_AddrAsW<BlockHeader::Full>(hdr); // currently ignore alignment
		hdr_.m_Height = Wasm::from_wasm(hdr_.m_Height);
		OnHost_get_HdrFull(hdr_);
		hdr_.Convert<true>();
	}
	BVM_METHOD_HOST(get_HdrFull)
	{
		Block::SystemState::Full s;
		Exc::Test(get_HdrAt(s, hdr.m_Height));

		CvtHdr(hdr, s);
		hdr.m_Prev = s.m_Prev;
		hdr.m_ChainWork = s.m_ChainWork;

		Cast::Reinterpret<Block::PoW>(hdr.m_PoW) = s.m_PoW;
	}

	void Processor::CvtHdr(Shaders::BlockHeader::InfoBase& hdr, const Block::SystemState::Full& s)
	{
		hdr.m_Timestamp = s.m_TimeStamp;
		hdr.m_Kernels = s.m_Kernels;
		hdr.m_Definition = s.m_Definition;
	}

	BVM_METHOD(get_RulesCfg)
	{
		auto& res_ = get_AddrAsW<Shaders::HashValue>(res); // currently ignore alignment
		return OnHost_get_RulesCfg(h, res_);
	}
	BVM_METHOD_HOST(get_RulesCfg)
	{
		const auto& r = Rules::get();
		auto iFork = r.FindFork(h);
		res = r.pForks[iFork].m_Hash;
		return r.pForks[iFork].m_Height;
	}

	BVM_METHOD(get_ForkHeight)
	{
		if (Kind::Contract == get_Kind())
			Exc::Test(IsPastFork_<5>());

		const Rules& r = Rules::get();
		return (iFork >= _countof(r.pForks)) ? MaxHeight : r.pForks[iFork].m_Height;
	}

	BVM_METHOD_HOST_AUTO(get_ForkHeight)

	BVM_METHOD(get_AssetInfo)
	{
		if (Kind::Contract == get_Kind())
		{
			Exc::Test(IsPastFork_<6>());
			DischargeUnits(Limits::Cost::LoadVar);
		}

		return OnHost_get_AssetInfo(aid, get_AddrAsW<AssetInfo>(res), get_AddrW(pMetadata, nMetadata), nMetadata);
	}

	BVM_METHOD_HOST(get_AssetInfo)
	{
		ZeroObject(res);

		Asset::Full ai;
		ai.m_ID = aid;
		if (get_AssetInfo(ai))
			return ProcessorPlus::CvtAssetInfo(res, ai, pMetadata, nMetadata);

		return 0;
	}

	uint32_t ProcessorPlus::CvtAssetInfo(AssetInfo& res, const Asset::Info& ai, void* pMetadata, uint32_t nMetadata)
	{
		res.m_ValueLo = AmountBig::get_Lo(ai.m_Value);
		res.m_ValueHi = AmountBig::get_Hi(ai.m_Value);
		res.m_Owner = Cast::Down<HashValue>(ai.m_Owner);
		res.m_Cid = ai.m_Cid;
		res.m_LockHeight = ai.m_LockHeight;
		res.m_Deposit = ai.m_Deposit;

		Blob md(ai.m_Metadata.m_Value);
		memcpy(pMetadata, md.p, std::min(nMetadata, md.n));

		return md.n;
	}

} // namespace bvm2
} // namespace beam
