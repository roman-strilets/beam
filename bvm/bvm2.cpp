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

	void get_ShaderID(ShaderID& sid, const Blob& data)
	{
		ECC::Hash::Processor()
			<< "bvm.shader.id"
			<< data.n
			<< data
			>> sid;
	}

	void get_CidViaSid(ContractID& cid, const ShaderID& sid, const Blob& args)
	{
		ECC::Hash::Processor()
			<< "bvm.cid"
			<< sid
			<< args.n
			<< args
			>> cid;
	}

	void get_Cid(ContractID& cid, const Blob& data, const Blob& args)
	{
		ShaderID sid;
		get_ShaderID(sid, data);
		get_CidViaSid(cid, sid, args);
	}

	/////////////////////////////////////////////
	// Processor
	void Processor::InitBase(uint32_t nStackBytes)
	{
		ZeroObject(m_Code);
		ZeroObject(m_Data);
		ZeroObject(m_LinearMem);
		m_Instruction.m_p0 = m_Instruction.m_p1 = nullptr;

		m_vStack.resize((nStackBytes + sizeof(Wasm::Word) - 1) / sizeof(Wasm::Word), 0);

		m_Stack.m_pPtr = m_vStack.empty() ? nullptr : &m_vStack.front();
		m_Stack.m_BytesMax = nStackBytes;
		m_Stack.m_BytesCurrent = m_Stack.m_BytesMax;
		m_Stack.m_Pos = 0;
		m_Stack.m_PosMin = 0;

		m_Heap.Clear();

		m_DataProcessor.m_Map.Clear();
		m_Secp.m_Point.m_Map.Clear();
		m_Secp.m_Scalar.m_Map.Clear();
	}

	const Processor::Header& Processor::ParseMod()
	{
		Exc::Test(m_Code.n >= sizeof(Header));
		const Header& hdr = *reinterpret_cast<const Header*>(m_Code.p);

		Exc::Test(ByteOrder::from_le(hdr.m_Version) == hdr.s_Version);
		uint32_t nMethods = ByteOrder::from_le(hdr.m_NumMethods);
		Exc::Test((nMethods - Header::s_MethodsMin <= Header::s_MethodsMax - Header::s_MethodsMin));

		uint32_t nHdrSize = sizeof(Header) + sizeof(Wasm::Word) * (nMethods - Header::s_MethodsMin);
		Exc::Test(nHdrSize <= m_Code.n);

		m_Data.n = m_Code.n - nHdrSize;
		m_Data.p = reinterpret_cast<const uint8_t*>(m_Code.p) + nHdrSize;
		m_prData0 = ByteOrder::from_le(hdr.m_hdrData0);

		m_prTable0 = ByteOrder::from_le(hdr.m_hdrTable0);
		Exc::Test(m_prTable0 <= m_Code.n);

		return hdr;
	}

	void Processor::VarKey::Set(const ContractID& cid)
	{
		memcpy(m_p, cid.m_pData, ContractID::nBytes);
		m_Size = ContractID::nBytes;
	}

	void Processor::VarKey::Append(uint8_t nTag, const Blob& blob)
	{
		m_p[m_Size++] = nTag;

		assert(m_Size + blob.n <= _countof(m_p));
		memcpy(m_p + m_Size, blob.p, blob.n);
		m_Size += blob.n;
	}

	void Processor::DischargeMemOp(uint32_t size)
	{
		// don't care about overflow. Assume avail max mem size multiplied by cost won't overflow
		DischargeUnits(size * Limits::Cost::MemOpPerByte);
	}

	template <typename T>
	typename uintBigFor<T>::Type LogFrom(T x) {
		return typename uintBigFor<T>::Type(x);
	}

	uint32_t LogFrom(uint8_t x) {
		return x;
	}

	// test there're no collisions
	void NeverCalled_VerifyNoIdCollisions(uint32_t nBinding)
	{
#define THE_MACRO(id, ret, name) case id:

		// must ensure no ID collisions within Common, Contract, Manager, and also combined {Common, Contract} and {Common, Manager}
		// It's not necessary that {Common, Contract, Manager} all be unique (we can assign same ID to a method in Contract and other method in Manager), but we prefer to avoid this too
		// This is to ease in the future the promotion of a method from Contract/Manager into Common

		switch (nBinding)
		{
		BVMOpsAll_Common(THE_MACRO)
		BVMOpsAll_Contract(THE_MACRO)
		BVMOpsAll_Manager(THE_MACRO)
			break;
		}

#undef THE_MACRO
	}

#define PAR_PASS(type, name) m_##name.V
#define PAR_DECL(type, name) ParamWrap<type> m_##name;
#define PAR_ASSIGN(type, name) args.m_##name =
#define PAR_DUMP(type, name) << "," #name "=" << LogFrom(m_##name.V)

#ifdef WASM_INTERPRETER_DEBUG
#	define WASM_LOG_EXTCALL(name) if (m_Dbg.m_ExtCall) *m_Dbg.m_pOut << "  " #name << std::endl;
#else // WASM_INTERPRETER_DEBUG
#	define WASM_LOG_EXTCALL(name)
#endif // WASM_INTERPRETER_DEBUG

#define THE_MACRO(id, ret, name) \
		case id: { \
			WASM_LOG_EXTCALL(name) \
			struct Args :public Exc::Checkpoint { \
				BVMOp_##name(PAR_DECL, MACRO_NOP) \
				RetType_##name Call(TProcessor& me) const { return me.OnMethod_##name(BVMOp_##name(PAR_PASS, MACRO_COMMA)); } \
				void Dump(std::ostream& os) override { \
					os << #name BVMOp_##name(PAR_DUMP, MACRO_NOP); \
				} \
			} args; \
			BVMOp_##name(PAR_ASSIGN, MACRO_NOP) *this; \
			Caller<RetType_##name>::Call(*this, args); \
		} break;

	void ProcessorPlus::InvokeExtPlus(uint32_t nBinding)
	{
		switch (nBinding)
		{
		BVMOpsAll_Common(THE_MACRO)
		default:
			Wasm::Processor::InvokeExt(nBinding);
		}
	}

	void ProcessorPlus_Contract::InvokeExtPlus(uint32_t nBinding)
	{
		switch (nBinding)
		{
		BVMOpsAll_Contract(THE_MACRO)
		default:
			ProcessorPlus::From(*this).InvokeExtPlus(nBinding);
		}
	}

	void ProcessorPlus_Manager::InvokeExtPlus(uint32_t nBinding)
	{
		switch (nBinding)
		{
		BVMOpsAll_Manager(THE_MACRO)
		default:
			ProcessorPlus::From(*this).InvokeExtPlus(nBinding);
		}
	}

#undef THE_MACRO
#undef PAR_PASS
#undef PAR_DECL
#undef PAR_ASSIGN

	void Processor::InvokeExt(uint32_t nBinding)
	{
		ProcessorPlus::From(*this).InvokeExtPlus(nBinding);
	}

	void ProcessorContract::InvokeExt(uint32_t nBinding)
	{
		ProcessorPlus_Contract::From(*this).InvokeExtPlus(nBinding);
	}

	void ProcessorManager::InvokeExt(uint32_t nBinding)
	{
		ProcessorPlus_Manager::From(*this).InvokeExtPlus(nBinding);
	}

} // namespace bvm2
} // namespace beam
