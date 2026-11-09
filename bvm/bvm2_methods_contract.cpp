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

	BVM_METHOD(EmitLog)
	{
		DischargeUnits(Limits::Cost::Log_For(nVal));
		return OnHost_EmitLog(get_AddrR(pKey, nKey), nKey, get_AddrR(pVal, nVal), nVal, nType);
	}
	BVM_METHOD_HOST(EmitLog)
	{
		TestVarSize(nVal);

		VarKey vk;
		SetVarKeyFromShader(vk, nType, Blob(pKey, nKey), true);

		return get_Storage().OnLog(vk.ToBlob(), Blob(pVal, nVal));
	}

	BVM_METHOD(CallFar)
	{
		// make sure the pArgs is not malicious.
		// Attacker may try to cause the target shader to overwrite its future stack or heap operands, or const data in its data section.
		//
		// Note: using 'Global' (heap) is not allowed either, since there's no reliable and simple way to verify it.
		// The attacker may pass a global pointer which is assumed to point to arguments, but partially belongs to unallocated heap, which may be allocated later by the callee.

		auto nCalleeStackMax = m_Stack.m_BytesCurrent;
		uint8_t* pArgsPtr = nullptr;

		bool bPastHF6 = IsPastFork_<6>();
		if (!bPastHF6)
		{
			nFlags &= 0xff; // before HF6 it was uint8_t
			if (nFlags)
				nFlags = CallFarFlags::InheritContext;
		}

		if (!(CallFarFlags::InheritContext & nFlags))
		{
			Exc::Test(iMethod >= 2); // c'tor and d'tor calls are not allowed

			if (nArgs)
			{
				bool bIsStackPtr = ((Wasm::MemoryType::Mask & pArgs) == Wasm::MemoryType::Stack);
				if (bIsStackPtr)
				{
					// for historical reasons the caller is allowed to specify greater nArgs (though it's not recommended), we truncate it

					// Make sure that the pointer is valid (i.e. between current and max stack pointers)
					uint32_t nSize;
					get_AddrExVar(pArgs, nSize, false); // ensures it's a valid alias stack pointer.
					beam::setmin(nArgs, nSize);

					// Note: the above does NOT ensure it's not below current stack pointer, it has less strict validation criteria (see comments in its implementation for more info).
					// Hence - the following is necessary.
					Exc::Test(pArgs >= m_Stack.get_AlasSp());
				}

				if (bPastHF6)
				{
					// we allow any args pointer
					if (pArgs != m_Stack.get_AlasSp())
					{
						// copy on stack at current position
						pArgsPtr = get_AddrW(pArgs, nArgs);

						m_Stack.PushAlias(Blob(pArgsPtr, nArgs));
						pArgs = m_Stack.get_AlasSp();
					}
				}
				else
					// Only stack pointer is allowed
					Exc::Test(bIsStackPtr);

				if (!pArgsPtr)
					nCalleeStackMax = (pArgs & ~Wasm::MemoryType::Stack) + nArgs; // restrict callee from accessing anything beyond
			}
			else
				pArgs = m_Stack.get_AlasSp();
		}

		auto nDepth = m_FarCalls.m_Stack.size(); // see if depth increases. If it doesn't - the call was hijacked, performed natively. No need to adjust m_BytesMax
		CallFar(get_AddrAsR<ContractID>(cid), iMethod, pArgs, nArgs, nFlags);

		if (!(CallFarFlags::InheritContext & nFlags) && (m_FarCalls.m_Stack.size() > nDepth))
		{
			if (bPastHF6)
			{
				auto& x = m_FarCalls.m_Stack.back();
				x.m_Flags |= FarCalls::Flags::s_HeapSwap;
				m_Heap.swap(x.m_Heap);
				ZeroObject(m_LinearMem);

				if (pArgsPtr)
				{
					x.m_Args.p = pArgsPtr;
					x.m_Args.n = nArgs;
				}
			}

			m_Stack.m_BytesMax = nCalleeStackMax;
		}
	}
	BVM_METHOD_HOST(CallFar)
	{
		Exc::Fail(); // not supported in this form. Proper host code would invoke CallFar_T
	}

	BVM_METHOD(get_CallDepth)
	{
		return OnHost_get_CallDepth();
	}

	BVM_METHOD_HOST(get_CallDepth)
	{
		return static_cast<uint32_t>(m_FarCalls.m_Stack.size());
	}

	BVM_METHOD(get_CallerCid)
	{
		return OnHost_get_CallerCid(iCaller, get_AddrAsW<HashValue>(cid));
	}

	BVM_METHOD_HOST(get_CallerCid)
	{
		for (auto it = m_FarCalls.m_Stack.rbegin(); ; it++)
		{
			Exc::Test(m_FarCalls.m_Stack.rend() != it);
			if (!iCaller--)
			{
				cid = it->m_Cid;
				break;
			}
		}
	}

	BVM_METHOD(UpdateShader)
	{
		OnHost_UpdateShader(get_AddrR(pVal, nVal), nVal);
	}

	BVM_METHOD_HOST(UpdateShader)
	{
		Exc::Test(IsPastFork_<4>());
		TestCanWrite();

		TestVarSize(nVal);
		DischargeUnits(Limits::Cost::UpdateShader_For(nVal));

		const auto& cid = m_FarCalls.m_Stack.back().m_Cid;
		AddRemoveShader(cid, nullptr, false);

		Blob blob(pVal, nVal);
		AddRemoveShader(cid, &blob, true);
	}

	BVM_METHOD(Halt)
	{
		struct MyCheckpoint :public Exc::Checkpoint
		{
			void Dump(std::ostream& os) override {
				os << "Explicit Halt";
			}

			uint32_t get_Type() override {
				return ErrorSubType::Internal;
			}

		} cp;

		Exc::Fail();
	}
	BVM_METHOD_HOST_AUTO(Halt)

	BVM_METHOD(AddSig)
	{
		DischargeUnits(Limits::Cost::AddSig);
		return OnHost_AddSig(get_AddrAsR<ECC::Point>(pubKey));
	}
	BVM_METHOD_HOST(AddSig)
	{
		m_vSigs.push_back(pubKey);
	}

	BVM_METHOD(FundsLock)
	{
		HandleAmount(amount, aid, true);
	}
	BVM_METHOD_HOST_AUTO(FundsLock)

	BVM_METHOD(FundsUnlock)
	{
		HandleAmount(amount, aid, false);
	}
	BVM_METHOD_HOST_AUTO(FundsUnlock)

	BVM_METHOD(RefAdd)
	{
		DischargeUnits(Limits::Cost::Refs);
		return OnHost_RefAdd(get_AddrAsR<ContractID>(cid));
	}
	BVM_METHOD_HOST(RefAdd)
	{
		return ProcessorPlus_Contract::From(*this).HandleRef(cid, true);
	}

	BVM_METHOD(RefRelease)
	{
		DischargeUnits(Limits::Cost::Refs);
		return OnHost_RefRelease(get_AddrAsR<ContractID>(cid));
	}
	BVM_METHOD_HOST(RefRelease)
	{
		return ProcessorPlus_Contract::From(*this).HandleRef(cid, false);
	}

	BVM_METHOD(AssetCreate)
	{
		return OnHost_AssetCreate(get_AddrR(pMeta, nMeta), nMeta);
	}
	BVM_METHOD_HOST(AssetCreate)
	{
		Exc::Test(nMeta && (nMeta <= Asset::Info::s_MetadataMaxSize));
		DischargeUnits(Limits::Cost::AssetManage);

		Asset::Metadata md;
		Blob(pMeta, nMeta).Export(md.m_Value);
		md.UpdateHash();

		AssetVar av;
		md.get_Owner(av.m_Owner, m_FarCalls.m_Stack.back().m_Cid);

		Amount valDeposit;
		Asset::ID ret = AssetCreate(md, av.m_Owner, valDeposit);
		if (ret)
		{
			ProcessorPlus_Contract::From(*this).HandleAmountOuter(valDeposit, Zero, true);

			SetAssetKey(av, ret);
			SaveVarInternal(av.m_vk.ToBlob(), av.m_Owner);
		}

		return ret;
	}

	void ProcessorContract::SetAssetKey(AssetVar& av, Asset::ID aid)
	{
		SetVarKey(av.m_vk);
		av.m_vk.Append(VarKey::Tag::OwnedAsset, uintBigFrom(aid));
	}

	void ProcessorContract::get_AssetStrict(AssetVar& av, Asset::ID aid)
	{
		SetAssetKey(av, aid);

		Blob res;
		get_Storage().LoadVar(av.m_vk.ToBlob(), res);

		Exc::Test(av.m_Owner.nBytes == res.n);
		memcpy(av.m_Owner.m_pData, res.p, res.n);
	}

	BVM_METHOD(AssetEmit)
	{
		DischargeUnits(Limits::Cost::AssetEmit);

		AmountSigned valS(amount);
		Exc::Test(valS >= 0);

		if (!bEmit)
		{
			valS = -valS;
			Exc::Test(valS <= 0);
		}

		AssetVar av;

		const auto& r = Rules::get();
		if (r.CA.IsForeign(aid))
		{
			// bridged asset
			// av.m_Owner from the most recent AddSig
			ECC::Point pt;
			if (m_vSigs.empty())
				return false;

			Cast::Down<ECC::uintBig>(av.m_Owner) = m_vSigs.back().m_X;
		}
		else
			get_AssetStrict(av, aid);

		bool b = AssetEmit(aid, av.m_Owner, valS);
		if (b)
			HandleAmountInner(amount, aid, bEmit);

		return !!b;
	}
	BVM_METHOD_HOST_AUTO(AssetEmit)

	BVM_METHOD(AssetDestroy)
	{
		DischargeUnits(Limits::Cost::AssetManage);

		AssetVar av;
		get_AssetStrict(av, aid);

		Amount valDeposit;
		bool b = AssetDestroy(aid, av.m_Owner, valDeposit);
		if (b)
		{
			HandleAmountOuter(valDeposit, Zero, false);
			SaveVarInternal(av.m_vk.ToBlob(), Blob(nullptr, 0));
		}

		return !!b;
	}
	BVM_METHOD_HOST_AUTO(AssetDestroy)

} // namespace bvm2
} // namespace beam
