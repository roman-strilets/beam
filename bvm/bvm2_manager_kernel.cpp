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

	void ProcessorManager::Call(Wasm::Word addr)
	{
		Call(addr, get_Ip());
	}

	void ProcessorManager::Call(Wasm::Word addr, Wasm::Word retAddr)
	{
		m_Stack.Push(retAddr);
		OnCall(addr);
	}

	BVM_METHOD(GenerateKernel)
	{
		if (!EnsureContext())
			return;

#pragma pack (push, 1)
		struct SigRequest {
			Wasm::Word m_pID;
			Wasm::Word m_nID;
		};
#pragma pack (pop)

		const FundsChange* pFunds_ = get_ArrayAddrAsR<FundsChange>(pFunds, nFunds);
		auto& v = GenerateKernel(pCid ? &get_AddrAsR<ContractID>(pCid) : nullptr, iMethod, Blob(get_AddrR(pArg, nArg), nArg), pFunds_, nFunds, true, RealizeStr(szComment), nCharge);

		auto* pSig_ = get_ArrayAddrAsR<SigRequest>(pSig, nSig);
		v.m_vSig.reserve(nSig);

		for (uint32_t i = 0; i < nSig; i++)
		{
			const auto& x_ = pSig_[i];
			SigRequest x;
			x.m_pID = Wasm::from_wasm(x_.m_pID);
			x.m_nID = Wasm::from_wasm(x_.m_nID);
			DeriveKeyPreimage(v.m_vSig.emplace_back(), Blob(get_AddrR(x.m_pID, x.m_nID), x.m_nID));
		}

	}
	BVM_METHOD_HOST(GenerateKernel)
	{
		if (!EnsureContext())
			return;

		auto& v = GenerateKernel(pCid, iMethod, Blob(pArg, nArg), pFunds, nFunds, false, szComment, nCharge);

		v.m_vSig.reserve(nSig);

		for (uint32_t i = 0; i < nSig; i++)
		{
			const auto& x = pSig[i];
			DeriveKeyPreimage(v.m_vSig.emplace_back(), Blob(x.m_pID, x.m_nID));
		}
	}

	BVM_METHOD(GetMaxSpend)
	{
		return get_MaxSpend(get_ArrayAddrAsW<FundsChange>(pFc, nFc), nFc, true);
	}

	BVM_METHOD_HOST(GetMaxSpend)
	{
		return get_MaxSpend(pFc, nFc, false);
	}

	BVM_METHOD(SetMaxSpend)
	{
		set_MaxSpend(get_ArrayAddrAsR<FundsChange>(pFc, nFc), nFc, true);
	}

	BVM_METHOD_HOST(SetMaxSpend)
	{
		set_MaxSpend(pFc, nFc, false);
	}

	uint32_t ProcessorManager::get_MaxSpend(FundsChange* pFc, uint32_t nFc, bool bCvt)
	{
		uint32_t i = 0;
		for (const auto& x : m_fmSpendMax)
		{
			if (i == nFc)
				break;

			auto& trg = pFc[i++];
			trg.m_Aid = x.first;
			trg.m_Consume = (x.second > 0);

			trg.m_Amount = trg.m_Consume ? x.second : (-x.second);

			if (bCvt)
			{
				trg.m_Aid = Wasm::to_wasm(trg.m_Aid);
				trg.m_Amount = Wasm::to_wasm(trg.m_Amount);
			}
		}
		
		return (uint32_t) m_fmSpendMax.size();
	}

	void ProcessorManager::set_MaxSpend(const FundsChange* pFc, uint32_t nFc, bool bCvt)
	{
		m_fmSpendMax.clear();

		for (uint32_t i = 0; i < nFc; i++)
		{
			const auto& src = pFc[i];
			Amount val = bCvt ? Wasm::from_wasm(src.m_Amount) : src.m_Amount;
			AmountSigned valSigned = src.m_Consume ? val : (0 - val);

			if (!valSigned)
				continue;

			Asset::ID aid = bCvt ? Wasm::from_wasm(src.m_Aid) : src.m_Aid;
			m_fmSpendMax[aid] = valSigned;
		}
	}


	BVM_METHOD(GetApiVersion)
	{
		return m_ApiVersion;
	}

	BVM_METHOD_HOST_AUTO(GetApiVersion)

	BVM_METHOD(SetApiVersion)
	{
		Exc::Test(nVer <= Shaders::ApiVersion::Current);
		m_ApiVersion = nVer;
	}

	BVM_METHOD_HOST_AUTO(SetApiVersion)

	void ProcessorManager::RunOnce()
	{
		assert(!IsSuspended());
		auto nIp = get_Ip();
		auto nSp = m_Stack.m_Pos;

		Processor::RunOnce();

		if (IsSuspended())
		{
			Jmp(nIp); // restore
			m_Stack.m_Pos = nSp;
		}
	}

	Height ProcessorManager::get_Height()
	{
		EnsureContext();
		return m_Context.m_Height;
	}

	bool ProcessorManager::EnsureContext()
	{
		if (MaxHeight != m_Context.m_Height)
			return true;

		SelectContext(false, 0);

		if (MaxHeight != m_Context.m_Height)
			return true;

		Exc::Test(IsSuspended());
		return false;
	}

	ContractInvokeEntry& ProcessorManager::GenerateKernel(const ContractID* pCid, uint32_t iMethod, const Blob& args, const Shaders::FundsChange* pFunds, uint32_t nFunds, bool bCvtFunds, const char* szComment, uint32_t nCharge)
	{
		bool bFirst = m_InvokeData.m_vec.empty();
		auto& v = m_InvokeData.m_vec.emplace_back();

		if (iMethod)
		{
			Exc::Test(pCid != nullptr); // only c'tor can be invoked without cid
			v.m_Cid = *pCid;
		}
		else
		{
			v.m_Cid = Zero;
			get_ContractShader(v.m_Data);
		}

		v.m_iMethod = iMethod;
		args.Export(v.m_Args);
		v.m_sComment = szComment;
		v.m_Charge = nCharge;
		v.m_Flags = 0;

		if (m_Context.m_pParent && bFirst)
		{
			v.m_Flags |= ContractInvokeEntry::Flags::Dependent;
			v.m_ParentCtx.m_Height = m_Context.m_Height + 1;
			v.m_ParentCtx.m_Hash = *m_Context.m_pParent;
		}

		SetFunds(v.m_Spend, pFunds, nFunds, bCvtFunds);

		return v;
	}

	void ProcessorManager::SetFunds(FundsMap& fm, const Shaders::FundsChange* pFunds, uint32_t nFunds, bool bCvtFunds)
	{
		for (uint32_t i = 0; i < nFunds; i++)
		{
			auto x = pFunds[i];
			if (bCvtFunds)
				x.Convert<false>();

			AmountSigned val = x.m_Amount;
			if (!x.m_Consume)
				val = -val;

			fm.AddSpend(x.m_Aid, val);
		}
	}

	void ProcessorManager::SetMultisignedTx(const void* pID, uint32_t nID, uint8_t bIsSender, const PubKey* pPeers, uint32_t nPeers,
		const bool* pIsMultisigned, uint32_t nIsMultisigned, const Shaders::FundsChange* pFunds, uint32_t nFunds, bool bCvtFunds)
	{
		Exc::Test(bIsSender || (1 == nPeers)); // co-signer must specify the sender as its only peer

		m_InvokeData.m_IsSender = !!bIsSender;
		m_InvokeData.m_vPeers.assign(pPeers, pPeers + nPeers);

		DeriveKeyPreimage(m_InvokeData.m_hvKey, Blob(pID, nID));

		Exc::Test(nIsMultisigned <= m_InvokeData.m_vec.size());
		for (uint32_t i = 0; i < nIsMultisigned; i++)
		{
			if (!pIsMultisigned[i])
				continue;

			auto& v = m_InvokeData.m_vec[i];
			Exc::Test(v.IsAdvanced());
			v.m_Flags |= ContractInvokeEntry::Flags::Multisigned;
		}

		m_InvokeData.m_SpendExtra.clear();
		SetFunds(m_InvokeData.m_SpendExtra, pFunds, nFunds, bCvtFunds);
	}

	void ProcessorManager::SetKernelAdv(Height hMin, Height hMax, const PubKey& ptFullBlind, const PubKey& ptFullNonce, const ECC::Scalar& skForeignSig, uint32_t iSlotBlind, uint32_t iSlotNonce,
		const PubKey* pSig, uint32_t nSig, ECC::Scalar* pE)
	{
		assert(!m_InvokeData.m_vec.empty());
		auto& v = m_InvokeData.m_vec.back();

		v.m_Adv.m_Height.m_Min = hMin;
		v.m_Adv.m_Height.m_Max = hMax;

		v.m_Adv.m_Fee = v.get_FeeMin(hMin);
		v.m_Flags |= ContractInvokeEntry::Flags::Adv;

		Key::IKdf* pKdf = nullptr;
		ECC::Hash::Value hvNonce;


		if (!pE)
		{
			Exc::Test(m_pKdf != nullptr);
			pKdf = m_pKdf.get();

			get_SlotPreimageInternal(v.m_Adv.m_hvSk, iSlotBlind);
			SlotErase(iSlotBlind);

			get_SlotPreimageInternal(hvNonce, iSlotNonce);
			SlotErase(iSlotNonce);
		}

		v.GenerateAdv(pKdf, pE, ptFullBlind, ptFullNonce, &hvNonce, &skForeignSig, pSig, nSig);

		if (pE)
			m_InvokeData.m_vec.pop_back(); // no more needed
	}

	BVM_METHOD(GenerateKernelAdvanced)
	{
		if (!EnsureContext())
			return;

		const FundsChange* pFunds_ = get_ArrayAddrAsR<FundsChange>(pFunds, nFunds);
		GenerateKernel(pCid ? &get_AddrAsR<ContractID>(pCid) : nullptr, iMethod, Blob(get_AddrR(pArg, nArg), nArg), pFunds_, nFunds, true, RealizeStr(szComment), nCharge);

		const PubKey* pSig_ = get_ArrayAddrAsR<PubKey>(pSig, nSig);
		ECC::Scalar* pE = pChallenges ? get_ArrayAddrAsW<ECC::Scalar>(pChallenges, nSig) : nullptr;

		SetKernelAdv(hMin, hMax,  get_AddrAsR<PubKey>(ptFullBlind), get_AddrAsR<PubKey>(ptFullNonce), get_AddrAsR<ECC::Scalar>(skForeignSig), iSlotBlind, iSlotNonce, pSig_, nSig, pE);
	}

	BVM_METHOD_HOST(GenerateKernelAdvanced)
	{
		if (!EnsureContext())
			return;

		GenerateKernel(pCid, iMethod, Blob(pArg, nArg), pFunds, nFunds, false, szComment, nCharge);
		SetKernelAdv(hMin, hMax, ptFullBlind, ptFullNonce, skForeignSig, iSlotBlind, iSlotNonce, pSig, nSig, pChallenges);
	}

	BVM_METHOD(SetMultisignedTx)
	{
		SetMultisignedTx(get_AddrR(pID, nID), nID, bIsSender, get_ArrayAddrAsR<ECC::Point>(pPeers, nPeers), nPeers,
			get_ArrayAddrAsR<bool>(pIsMultisigned, nIsMultisigned), nIsMultisigned, get_ArrayAddrAsR<FundsChange>(pFundsExtra, nFundsExtra), nFundsExtra, true);
	}

	BVM_METHOD_HOST(SetMultisignedTx)
	{
		SetMultisignedTx(pID, nID, bIsSender, pPeers, nPeers, pIsMultisigned, nIsMultisigned, pFundsExtra, nFundsExtra, false);
	}

	BVM_METHOD(GenerateRandom)
	{
		OnHost_GenerateRandom(get_AddrW(pBuf, nSize), nSize);
	}
	BVM_METHOD_HOST(GenerateRandom)
	{
		ECC::GenRandom(pBuf, nSize);
	}

} // namespace bvm2
} // namespace beam
