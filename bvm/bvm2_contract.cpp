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

	void ProcessorContract::InitStackPlus(uint32_t nStackBytes)
	{
		InitBase(Limits::StackSize + nStackBytes);
	}

	void ProcessorContract::CallFar(const ContractID& cid, uint32_t iMethod, Wasm::Word pArgs, uint32_t nArgs, uint32_t nFlags)
	{
		struct MyCheckpoint :public Exc::Checkpoint
		{
			const ContractID& m_Cid;
			uint32_t m_iMethod;

			MyCheckpoint(const ContractID& cid, uint32_t iMethod)
				:m_Cid(cid)
				,m_iMethod(iMethod)
			{
			}

			void Dump(std::ostream& os) override {
				os << "CallFar cid=" << m_Cid << ", iMethod=" << m_iMethod;
			}
		} cp(cid, iMethod);

		DischargeUnits(Limits::Cost::CallFar);
		Exc::Test(m_FarCalls.m_Stack.size() < Limits::FarCallDepth);

		auto nRetAddr = get_Ip();
		const ContractID* pCidContext = &cid;

		uint32_t nFlagsDst = 0;

		if (CallFarFlags::GlobalLockRO & nFlags)
			nFlagsDst |= FarCalls::Flags::s_GlobalRO;

		if (!m_FarCalls.m_Stack.empty())
		{
			if (CallFarFlags::SelfBlock & nFlags)
				nFlagsDst |= FarCalls::Flags::s_CallerBlocked;
			if (CallFarFlags::SelfLockRO & nFlags)
				nFlagsDst |= FarCalls::Flags::s_CallerLockedRO;

			auto it = m_FarCalls.m_Stack.rbegin();

			{
				FarCalls::Frame& f = *it;

				if (CallFarFlags::InheritContext & nFlags)
					pCidContext = &f.m_Cid;

				if (FarCalls::Flags::s_GlobalRO & f.m_Flags)
					nFlagsDst |= FarCalls::Flags::s_GlobalRO; // propagate it
			}

			// check re-entry flags
			for (uint32_t nFlagsSrc = nFlagsDst; ; )
			{
				FarCalls::Frame& f = *it;

				if (f.m_Cid == *pCidContext)
				{
					if (FarCalls::Flags::s_CallerBlocked & nFlagsSrc)
						Exc::Fail("reentry blocked");

					if (FarCalls::Flags::s_CallerLockedRO & nFlagsSrc)
						nFlagsDst |= FarCalls::Flags::s_LockedRO;

					if (FarCalls::Flags::s_LockedRO & f.m_Flags)
						nFlagsDst |= FarCalls::Flags::s_LockedRO;

					break;
				}

				if (m_FarCalls.m_Stack.rend() == ++it)
					break;

				nFlagsSrc = f.m_Flags;
			}
		}

		auto& x = *m_FarCalls.m_Stack.Create_back();
		x.m_Cid = *pCidContext;
		x.m_FarRetAddr = nRetAddr;
		x.m_StackBytesMax = m_Stack.m_BytesMax;
		x.m_StackBytesRet = m_Stack.m_BytesCurrent;
		x.m_Flags = nFlagsDst;
		ZeroObject(x.m_Args);

		x.m_StackPosMin = m_Stack.m_PosMin;
		m_Stack.m_PosMin = m_Stack.m_Pos;

		get_Storage().LoadVar(cid, m_Code);
		m_Code.Export(x.m_Body);
		m_Code = x.m_Body; // important! Use our local copy to access the code

		const Header& hdr = ParseMod();
		Exc::Test(iMethod < ByteOrder::from_le(hdr.m_NumMethods));

		m_Stack.Push(pArgs);
		m_Stack.Push(0); // retaddr, set dummy for far call

		uint32_t nAddr = ByteOrder::from_le(hdr.m_pMethod[iMethod]);
		OnCall(nAddr);
	}

	void ProcessorContract::OnRet(Wasm::Word nRetAddr)
	{
		auto& x = m_FarCalls.m_Stack.back();

		if (m_FarCalls.m_SaveLocal)
			x.m_Debug.OnRet();

		if (nRetAddr)
			Processor::OnRet(nRetAddr);
		else
			OnRetFar();
	}

	void ProcessorContract::OnRetFar()
	{
		Exc::Test(m_Stack.m_Pos == m_Stack.m_PosMin);

		auto& x = m_FarCalls.m_Stack.back();
		Exc::Test(m_Stack.m_BytesCurrent == x.m_StackBytesRet);

		Wasm::Word nRetAddr = x.m_FarRetAddr;

		m_Stack.m_PosMin = x.m_StackPosMin;
		m_Stack.m_BytesMax = x.m_StackBytesMax;

		if (FarCalls::Flags::s_HeapSwap & x.m_Flags)
		{
			m_Heap.swap(x.m_Heap);
			m_LinearMem = m_Heap.m_vMem;
		}

		if (x.m_Args.n)
		{
			memcpy(Cast::NotConst(x.m_Args.p), m_Stack.get_AliasPtr(), x.m_Args.n);
			m_Stack.AliasFree(x.m_Args.n);
		}

		m_FarCalls.m_Stack.Delete(x);

		if (!m_FarCalls.m_Stack.empty())
		{
			m_Code = m_FarCalls.m_Stack.back().m_Body;
			ParseMod(); // restore code/data sections

			Processor::OnRet(nRetAddr);
		}
	}

	void ProcessorContract::OnCall(Wasm::Word nAddr)
	{
		if (m_FarCalls.m_SaveLocal)
		{
			auto& x = m_FarCalls.m_Stack.back();
			Wasm::Word nRetAddr = x.m_Debug.m_v.empty() ? x.m_FarRetAddr : get_Ip();
			x.m_Debug.OnCall(nAddr, nRetAddr);
		}

		Processor::OnCall(nAddr);
	}

	void ProcessorContract::DumpCallstack(std::ostream& os) const
	{
		Wasm::Word ip = get_Ip();

		for (auto itF = m_FarCalls.m_Stack.rbegin(); m_FarCalls.m_Stack.rend() != itF; itF++)
		{
			const auto& fr = *itF;

			bvm2::ShaderID sid;
			bvm2::get_ShaderID(sid, fr.m_Body); // theoretically m_Body may be different, the contract code may modify itself. Never mind.

			os << std::endl << "Cid=" << fr.m_Cid << ", Sid=" << sid;

			fr.m_Debug.Dump(os, ip, get_DbgInfo(sid));
		}
	}

	uint32_t ProcessorContract::get_HeapLimit()
	{
		return Limits::HeapSize;
	}

	void ProcessorContract::SetVarKey(VarKey& vk)
	{
		vk.Set(m_FarCalls.m_Stack.back().m_Cid);
	}

	void ProcessorContract::SetVarKey(VarKey& vk, uint8_t nTag, const Blob& blob)
	{
		SetVarKey(vk);
		vk.Append(nTag, blob);
	}

	void ProcessorContract::DischargeUnits(uint32_t n)
	{
		if (m_Charge < n)
		{
			struct MyCheckpoint :public Exc::Checkpoint
			{
				void Dump(std::ostream& os) override {
					os << "Discharge";
				}

				uint32_t get_Type() override {
					return ErrorSubType::NoCharge;
				}

			} cp;

			Exc::Fail("no Charge");
		}

		m_Charge -= n;
	}

	uint32_t ProcessorContract::get_WasmVersion()
	{
		return IsPastFork_<6>() ? 1 : 0;
	}

	/////////////////////////////////////////////
	// Other funcs

	bool ProcessorContract::LoadFixedOrZero(const VarKey& vk, uint8_t* pVal, uint32_t n)
	{
		Blob res;
		get_Storage().LoadVar(vk.ToBlob(), res);

		if (n == res.n)
		{
			memcpy(pVal, res.p, n);
			return true;
		}

		memset0(pVal, n);
		return false;
	}

	void ProcessorContract::TestCanWrite()
	{
		assert(!m_FarCalls.m_Stack.empty());

		uint32_t nFlags = m_FarCalls.m_Stack.back().m_Flags;
		if ((FarCalls::Flags::s_LockedRO | FarCalls::Flags::s_GlobalRO) & nFlags)
			Exc::Fail("write lock");
	}


	uint32_t ProcessorContract::SaveVarInternal(const Blob& key, const Blob& val)
	{
		TestCanWrite();
		return get_Storage().SaveVar(key, val);
	}

	uint32_t ProcessorContract::SaveNnz(const VarKey& vk, const uint8_t* pVal, uint32_t n)
	{
		return SaveVarInternal(vk.ToBlob(), Blob(pVal, memis0(pVal, n) ? 0 : n));
	}

	void ProcessorContract::HandleAmount(Amount amount, Asset::ID aid, bool bLock)
	{
		DischargeUnits(Limits::Cost::FundsLock);

		HandleAmountInner(amount, aid, bLock);
		HandleAmountOuter(amount, aid, bLock);
	}

	void ProcessorContract::HandleAmountInner(Amount amount, Asset::ID aid, bool bLock)
	{
		struct MyCheckpoint :public Exc::Checkpoint
		{
			void Dump(std::ostream& os) override {
				os << "Funds I/O";
			}

			uint32_t get_Type() override {
				return ErrorSubType::FundsIO;
			}

		} cp;

		VarKey vk;
		SetVarKey(vk, VarKey::Tag::LockedAmount, uintBigFrom(aid));

		AmountBig::Number val0;
		Load_T(vk, val0);

		auto val = MultiWord::From(amount);

		if (bLock)
		{
			val0 += val;
			Exc::Test(val0 >= val); // overflow test
		}
		else
		{
			Exc::Test(val0 >= val); // overflow test
			val0 -= val;
		}

		Save_T(vk, val0);
	}

	void ProcessorContract::HandleAmountOuter(Amount amount, Asset::ID aid, bool bLock)
	{
		if (m_pFundsIO)
			m_pFundsIO->Add(amount, aid, bLock);
	}

	bool ProcessorContract::HandleRefRaw(const VarKey& vk, bool bAdd)
	{
		uintBig_t<4> refs; // more than enough
		Load_T(vk, refs);

		bool ret = false;

		if (bAdd)
		{
			ret = (refs == Zero);
			refs.Inc();
			Exc::Test(refs != Zero);
		}
		else
		{
			Exc::Test(refs != Zero);
			refs.Negate();
			refs.Inv();
			ret = (refs == Zero);
		}

		Save_T(vk, refs);
		return ret;
	}

	uint8_t ProcessorContract::HandleRef(const ContractID& cid, bool bAdd)
	{
		VarKey vk;
		SetVarKey(vk, VarKey::Tag::Refs, cid);

		if (HandleRefRaw(vk, bAdd))
		{
			// z/nnz flag changed.
			VarKey vk2;
			vk2.Set(cid);

			if (bAdd)
			{
				// make sure the target contract exists
				Blob res;
				get_Storage().LoadVar(vk2.ToBlob(), res);

				if (!res.n)
				{
					HandleRefRaw(vk, false); // undo
					return 0;
				}
			}

			vk2.Append(VarKey::Tag::Refs, Blob(nullptr, 0));
			HandleRefRaw(vk2, bAdd);
		}

		return 1;
	}

	/////////////////////////////////////////////
	// Signature
	void ProcessorContract::CheckSigs(const ECC::Point& pt, const ECC::Signature& sig)
	{
		if (!m_pSigValidate || !m_pFundsIO)
			return;

		CheckSigsEx(*m_pSigValidate, m_vSigs, *m_pFundsIO, pt, sig);
	}

	void ProcessorContract::CheckSigsEx(ECC::Hash::Processor& hp, const std::vector<ECC::Point>& vSigs, const FundsChangeMap& fundsIO, const ECC::Point& pt, const ECC::Signature& sig)
	{
		struct MyCheckpoint :public Exc::Checkpoint
		{
			const std::vector<ECC::Point>& m_vSigs;
			const FundsChangeMap& m_FundsIO;
			MyCheckpoint(const std::vector<ECC::Point>& vSigs, const FundsChangeMap& fundsIO) :m_vSigs(vSigs), m_FundsIO(fundsIO) {}

			void Dump(std::ostream& os) override
			{
				os << "CheckSigs";

				if (!m_vSigs.empty())
				{
					os << ", Keys=[";
					for (const auto& pk : m_vSigs)
						os << pk << ", ";
					os << "]";
				}

				if (!m_FundsIO.m_Map.empty())
				{
					os << ", Funds=[";
					for (auto it = m_FundsIO.m_Map.begin(); ; )
					{
						auto val = it->second;
						bool bSpend = !!val.get_Msb();
						if (bSpend)
							val = -val;

						os << (bSpend ? '+' : '-');
						os << it->first << ':';
						AmountBig::Print(os, val);

						if (m_FundsIO.m_Map.end() == ++it)
							break;

						os << ", ";
					}
					os << ']';
				}
			}

			uint32_t get_Type() override {
				return ErrorSubType::BadSignature;
			}

		} cp(vSigs, fundsIO);

		std::vector<ECC::Point::Native> vPts;
		vPts.reserve(vSigs.size() + 1);

		auto fnAddSig = [&hp, &vPts](const ECC::Point& pk)
		{
			hp << pk;

			auto& ret = vPts.emplace_back();
			Exc::Test(ret.ImportNnz(pk));
		};

		for (const auto& pk : vSigs)
			fnAddSig(pk);

		fnAddSig(pt);


		ECC::Point::Native ptFunds;
		fundsIO.ToCommitment(ptFunds);
		vPts.back() += ptFunds;

		ECC::Hash::Value hv;
		hp >> hv;

		ECC::SignatureBase::Config cfg = ECC::Context::get().m_Sig.m_CfgG1; // copy
		cfg.m_nKeys = static_cast<uint32_t>(vPts.size());

		Exc::Test(Cast::Down<ECC::SignatureBase>(sig).IsValid(cfg, hv, &sig.m_k, &vPts.front()));
	}

	/////////////////////////////////////////////
	// Shader aux
	void ProcessorContract::AddRemoveShader(const ContractID& cid, const Blob* pCode)
	{
		AddRemoveShader(cid, pCode, IsPastFork_<4>());
	}

	void ProcessorContract::AddRemoveShader(const ContractID& cid, const Blob* pCode, bool bFireEvent)
	{
		ShaderID sid;

		auto& s = get_Storage();
		if (pCode)
		{
			s.SaveVar(cid, *pCode);
			get_ShaderID(sid, *pCode);
		}
		else
		{
			Blob res;
			s.LoadVar(cid, res);

			get_ShaderID(sid, res);
			s.SaveVar(cid, Blob(nullptr, 0));
		}

		ToggleSidEntry(sid, cid, !!pCode);

		if (bFireEvent)
		{
			VarKey vk;
			vk.Set(cid);

			vk.Append(VarKey::Tag::ShaderChange, Blob(nullptr, 0));
			get_Storage().OnLog(vk.ToBlob(), pCode ? Blob(sid) : Blob(nullptr, 0));
		}
	}

	void ProcessorContract::ToggleSidEntry(const ShaderID& sid, const ContractID& cid, bool bSet)
	{
		uint8_t pKey[ContractID::nBytes * 2 + ShaderID::nBytes + 1];
		memset0(pKey, ContractID::nBytes);
		pKey[ContractID::nBytes] = VarKey::Tag::SidCid;
		memcpy(pKey + ContractID::nBytes + 1, sid.m_pData, sid.nBytes);
		memcpy(pKey + ContractID::nBytes + 1 + ShaderID::nBytes, cid.m_pData, cid.nBytes);

		Blob blob(pKey, ContractID::nBytes * 2 + bvm2::ShaderID::nBytes + 1);

		auto& s = get_Storage();
		if (bSet)
		{
			auto h = uintBigFrom(get_Height() + 1);
			s.SaveVar(blob, h);
		}
		else
			s.SaveVar(blob, Blob(nullptr, 0));
	}

} // namespace bvm2
} // namespace beam
