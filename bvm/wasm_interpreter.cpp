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

#include "wasm_internal.h"

namespace beam {
namespace Wasm {

	/////////////////////////////////////////////
	// Reader

	void Reader::Ensure(uint32_t n)
	{
		Exc::Test(static_cast<size_t>(m_p1 - m_p0) >= n);
	}

	const uint8_t* Reader::Consume(uint32_t n)
	{
		Ensure(n);

		const uint8_t* pRet = m_p0;
		m_p0 += n;

		return pRet;
	}

	/////////////////////////////////////////////
	// Processor
	Word Processor::Stack::Pop1()
	{
		Exc::Test(m_Pos > m_PosMin);
		return m_pPtr[--m_Pos];
	}

	void Processor::Stack::Push1(Word x)
	{
		Exc::Test(m_Pos < m_BytesCurrent / sizeof(Word));
		m_pPtr[m_Pos++] = x;
	}

	// for well-formed wasm program we don't need to care about multi-word types bits words order, since there should be no type mixing (i.e. push as uint64, pop as uint32).
	// But the attacker may violate this rule, and cause different behavior on different machines. So we do the proper conversion
	uint64_t Processor::Stack::Pop2()
	{
		uint64_t ret = Pop1(); // loword
		return ret | static_cast<uint64_t>(Pop1()) << 32; // hiword
	}

	void Processor::Stack::Push2(uint64_t x)
	{
		Push1(static_cast<Word>(x >> 32)); // hiword
		Push1(static_cast<Word>(x)); // loword
	}

#ifdef WASM_INTERPRETER_DEBUG
	template <typename T>
	void Processor::Stack::Log(T x, bool bPush)
	{
		if (get_ParentObj().m_Dbg.m_Stack)
			*get_ParentObj().m_Dbg.m_pOut << (bPush ? "  <- " : "  -> ") << uintBigFrom(x) << std::endl;
	}

	template void Processor::Stack::Log(uint32_t, bool);
	template void Processor::Stack::Log(uint64_t, bool);
#endif // WASM_INTERPRETER_DEBUG

	Word Processor::Stack::get_AlasSp() const
	{
		assert(AlignUp(m_BytesCurrent) == m_BytesCurrent);
		return MemoryType::Stack | m_BytesCurrent;
	}

	void Processor::Stack::set_AlasSp(Word x)
	{
		Exc::Test((MemoryType::Mask & x) == MemoryType::Stack);
		m_BytesCurrent = (x & ~MemoryType::Mask);
		TestSelf();
	}

	void Processor::Stack::AliasAlloc(Word nSize)
	{
		nSize = AlignUp(nSize);
		Exc::Test(nSize <= m_BytesCurrent);
		m_BytesCurrent -= nSize;
		TestSelf();
	}

	void Processor::Stack::AliasFree(Word nSize)
	{
		nSize = AlignUp(nSize);
		m_BytesCurrent += nSize;
		Exc::Test(nSize <= m_BytesCurrent); // no overflow
		TestSelf();
	}

	uint8_t* Processor::Stack::get_AliasPtr() const
	{
		return reinterpret_cast<uint8_t*>(m_pPtr) + m_BytesCurrent;
	}

	void Processor::Stack::PushAlias(const Blob& b)
	{
		AliasAlloc(b.n);
		memcpy(get_AliasPtr(), b.p, b.n);
	}

	void Processor::Stack::TestSelf() const
	{
		// Test(m_Pos >= m_PosMin); - this test is not needed
		Exc::Test(m_BytesCurrent <= m_BytesMax);
		Exc::Test(m_Pos <= m_BytesCurrent / sizeof(Word));
		assert(AlignUp(m_BytesCurrent) == m_BytesCurrent);
	}

	Word Processor::Stack::AlignUp(Word n)
	{
		return (n + s_Alignment-1) & ~(s_Alignment-1);
	}

	void Processor::Stack::TestAlignmentPower(uint32_t n)
	{
		static_assert(s_Alignment == (1 << 4));
		Exc::Test(n <= 4);
	}

	struct ProcessorPlus
		:public Processor
	{
		template <typename T>
		T FixShiftCount(T val)
		{
			const T nMaxBits = sizeof(T) * 8;
			if (val >= nMaxBits)
			{
				if (!get_WasmVersion())
					Exc::Fail("Incompatible shift operand");

				val %= nMaxBits;
			}

			return val;
		}

#define THE_MACRO_unop(name) \
		template <typename TOut, typename TIn> \
		void On_##name() \
		{ \
			m_Stack.Push<TOut>(Eval_##name<TOut, TIn>(m_Stack.Pop<TIn>())); \
		}

#define THE_MACRO_binop(name) \
		template <typename TOut, typename TIn> \
		void On_##name() \
		{ \
			TIn b = m_Stack.Pop<TIn>(); \
			TIn a = m_Stack.Pop<TIn>(); \
			m_Stack.Push<TOut>(Eval_##name<TOut, TIn>(a, b)); \
		}


#define THE_MACRO(name, id32, id64) THE_MACRO_unop(name)
		WasmInstructions_unop_Polymorphic_32(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(name, id32, id64) THE_MACRO_binop(name)
		WasmInstructions_binop_Polymorphic_32(THE_MACRO)
		WasmInstructions_binop_Polymorphic_x(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(id, name) void On_##name();
		WasmInstructions_CustomPorted(THE_MACRO)
		WasmInstructions_Proprietary(THE_MACRO)
#undef THE_MACRO

#define UNOP(name) template <typename TOut, typename TIn> TOut Eval_##name(TIn x)
#define BINOP(name) template <typename TOut, typename TIn> TOut Eval_##name(TIn a, TIn b)

		UNOP(eqz) { return x == 0; }
		BINOP(eq) { return a == b; }
		BINOP(ne) { return a != b; }
		BINOP(lt_s) { return Type::SignedFrom(a) < Type::SignedFrom(b); }
		BINOP(gt_s) { return Type::SignedFrom(a) > Type::SignedFrom(b); }
		BINOP(le_s) { return Type::SignedFrom(a) <= Type::SignedFrom(b); }
		BINOP(ge_s) { return Type::SignedFrom(a) >= Type::SignedFrom(b); }
		BINOP(lt_u) { return a < b; }
		BINOP(gt_u) { return a > b; }
		BINOP(le_u) { return a <= b; }
		BINOP(ge_u) { return a >= b; }
		BINOP(add) { return a + b; }
		BINOP(sub) { return a - b; }
		BINOP(mul) { return a * b; }
		BINOP(div_s) {
			auto a_s = Type::SignedFrom(a);
			auto b_s = Type::SignedFrom(b);
			Exc::Test(b_s != 0);
			Exc::Test((b_s != -1) || (a_s != std::numeric_limits<typename Type::ToFlexible<TIn, true>::T>::min()));
			return a_s / b_s;
		}
		BINOP(div_u) { Exc::Test(b);  return a / b; }
		BINOP(rem_s) {
			auto b_s = Type::SignedFrom(b);
			Exc::Test(b_s != 0);
			if (b_s == -1)
				return 0;
			return Type::SignedFrom(a) % Type::SignedFrom(b);
		}
		BINOP(rem_u) { Exc::Test(b);  return a % b; }
		BINOP(and) { return a & b; }
		BINOP(or) { return a | b; }
		BINOP(xor) { return a ^ b; }
		BINOP(shl) { return a << FixShiftCount(b); }
		BINOP(shr_s) { return Type::SignedFrom(a) >> FixShiftCount(b); }
		BINOP(shr_u) { return a >> FixShiftCount(b); }
		BINOP(rotl) { return std::rotl(a, static_cast<int>(FixShiftCount(b))); }
		BINOP(rotr) { return std::rotr(a, static_cast<int>(FixShiftCount(b))); }


		Word ReadAddr()
		{
			return from_wasm<Word>(m_Instruction.Consume(sizeof(Word)));
		}

		void OnLocal(bool bSet, bool bGet)
		{
			uint32_t nOffset = m_Instruction.Read<uint32_t>();

			uint8_t nType = Type::s_Base + static_cast<uint8_t>((sizeof(Word) - 1) & (nOffset - Type::s_Base));
			uint8_t nWords = Type::Words(nType);

			nOffset /= sizeof(Word);

			Exc::Test((nOffset >= nWords) && (nOffset <= m_Stack.m_Pos - m_Stack.m_PosMin));

			uint32_t* pSrc = m_Stack.m_pPtr + m_Stack.m_Pos;
			uint32_t* pDst = pSrc - nOffset;

			if (!bSet)
			{
				std::swap(pDst, pSrc);
				m_Stack.m_Pos += nWords;
				Exc::Test(m_Stack.m_Pos <= m_Stack.m_BytesCurrent / sizeof(Word));
			}
			else
			{
				pSrc -= nWords;

				if (!bGet)
					m_Stack.m_Pos -= nWords;
			}

			for (uint32_t i = 0; i < nWords; i++)
				pDst[i] = pSrc[i];
		}

		void OnGlobal(bool bGet)
		{
			Exc::Fail();
		}

		void OnGlobalImp(bool bGet)
		{
			auto iVar = m_Instruction.Read<uint32_t>();
			OnGlobalVar(iVar, bGet);
		}

		uint8_t* MemArgEx(uint32_t nSize, bool bW)
		{
			auto nAlign = m_Instruction.Read<Word>();
			Stack::TestAlignmentPower(nAlign);

			auto nOffs = m_Instruction.Read<Word>();
			nOffs += m_Stack.Pop<Word>();

			return get_AddrEx(nOffs, nSize, bW);
		}

		uint8_t* MemArgW(uint32_t nSize) {
			return MemArgEx(nSize, true);
		}

		const uint8_t* MemArgR(uint32_t nSize) {
			return MemArgEx(nSize, false);
		}

		void RunOncePlus()
		{
			struct MyCheckpoint :public Exc::Checkpoint {
				Word m_Ip;
				void Dump(std::ostream& os) override {
					os << "wasm/Run, Ip=" << uintBigFrom(m_Ip);
				}
			} cp;
			cp.m_Ip = get_Ip();

			typedef Instruction I;
			I nInstruction = (I) m_Instruction.Read1();

#ifdef WASM_INTERPRETER_DEBUG
			if (m_Dbg.m_Instructions)
				*m_Dbg.m_pOut << "ip=" << uintBigFrom(cp.m_Ip) << ", sp=" << uintBigFrom(m_Stack.m_Pos) << ' ';

#	define WASM_LOG_INSTRUCTION(name) if (m_Dbg.m_Instructions) (*m_Dbg.m_pOut) << #name << std::endl;
#else // WASM_INTERPRETER_DEBUG
#	define WASM_LOG_INSTRUCTION(name)
#endif // WASM_INTERPRETER_DEBUG

			switch (nInstruction)
			{
#define THE_CASE(name) case I::name: WASM_LOG_INSTRUCTION(name)

#define THE_MACRO(id, name) THE_CASE(name) On_##name(); break;
			WasmInstructions_CustomPorted(THE_MACRO)
			WasmInstructions_Proprietary(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(name, id32, id64) \
			THE_CASE(i32_##name) On_##name<uint32_t, uint32_t>(); break; \
			THE_CASE(i64_##name) On_##name<uint32_t, uint64_t>(); break;

			WasmInstructions_unop_Polymorphic_32(THE_MACRO)
			WasmInstructions_binop_Polymorphic_32(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(name, id32, id64) \
			THE_CASE(i32_##name) On_##name<uint32_t, uint32_t>(); break; \
			THE_CASE(i64_##name) On_##name<uint64_t, uint64_t>(); break;

			WasmInstructions_binop_Polymorphic_x(THE_MACRO)
#undef THE_MACRO


#define THE_MACRO(id, type, name, tmem) \
			THE_CASE(type##_##name) { \
				tmem val1 = from_wasm<Type::ToFlexible<tmem, false>::T>(MemArgR(sizeof(tmem))); \
				auto valExt = Type::Extend<Type::Code2Type<Type::type>::T, tmem>(val1); \
				m_Stack.Push(valExt); \
			} break;

			WasmInstructions_Load(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(id, type, name, tmem) \
			THE_CASE(type##_##name) { \
				auto val = m_Stack.Pop<Type::Code2Type<Type::type>::T>(); \
				to_wasm(MemArgW(sizeof(tmem)), static_cast<tmem>(val)); \
			} break;

			WasmInstructions_Store(THE_MACRO)
#undef THE_MACRO

			default:
				Exc::Fail();
			}

		}
	};

	Word Processor::get_Ip() const
	{
		return static_cast<Word>(m_Instruction.m_p0 - (const uint8_t*)m_Code.p);
	}

	uint8_t* Processor::get_AddrExVar(uint32_t nOffset, uint32_t& nSizeOut, bool bW) const
	{
		Exc::CheckpointTxt cp("mem/probe");

		Blob blob;

		Word nMemType = MemoryType::Mask & nOffset;
		nOffset &= ~MemoryType::Mask;

		switch (nMemType)
		{
		case MemoryType::Global:
			blob = m_LinearMem;
			break;

		case MemoryType::Data:
			//Test(!bW); Enable write to data. This enables normal use of global variables.
			blob = m_Data;
			nOffset -= m_prData0; // data va start at specific offset
			break;

		case MemoryType::Stack:
			// should be no access below current stack pointer
			if (nOffset < m_Stack.m_BytesCurrent)
			{
				// sometimes the compiler may omit updating the stack pointer yet write below it (currently this happens in debug build empty function with a single parameter)
				// We allow it, as long as it's above wasm operand stack
				Exc::Test(m_Stack.m_Pos <= nOffset / sizeof(Word));
			}

			blob.p = m_Stack.m_pPtr;
			blob.n = m_Stack.m_BytesMax;
			break;

		default:
			Exc::Fail();
		}

		Exc::Test(nOffset <= blob.n);
		nSizeOut = blob.n - nOffset;
		return reinterpret_cast<uint8_t*>(Cast::NotConst(blob.p)) + nOffset;
	}

	uint8_t* Processor::get_AddrEx(uint32_t nOffset, uint32_t nSize, bool bW) const
	{
		if (!nSize)
			return nullptr;

		uint32_t nSizeOut;
		auto pRet = get_AddrExVar(nOffset, nSizeOut, bW);

		Exc::CheckpointTxt cp("mem/bounds");
		Exc::Test(nSize <= nSizeOut);

		return pRet;
	}

	void Processor::RunOnce()
	{
		auto& p = Cast::Up<ProcessorPlus>(*this);
		static_assert(sizeof(p) == sizeof(*this));
		p.RunOncePlus();
	}

	void Processor::InvokeExt(uint32_t)
	{
		Exc::Fail(); // unresolved binding
	}

	void Processor::OnGlobalVar(uint32_t iVar, bool bGet)
	{
		switch (static_cast<VariableType>(iVar))
		{
		case VariableType::StackPointer:
			if (bGet)
				m_Stack.Push(m_Stack.get_AlasSp());
			else
			{
				Word val = m_Stack.Pop<Word>();
				Exc::Test(m_Stack.AlignUp(val) == val);
				m_Stack.set_AlasSp(val);
			}
			break;

		default:
			Exc::Fail();
		}

	}

	void Processor::Jmp(uint32_t ip)
	{
		Exc::Test(ip < m_Code.n);

		m_Instruction.m_p0 = reinterpret_cast<const uint8_t*>(m_Code.p) + ip;
		m_Instruction.m_p1 = m_Instruction.m_p0 + m_Code.n - ip;
	}

	void ProcessorPlus::On_local_get()
	{
		OnLocal(false, true);
	}

	void ProcessorPlus::On_local_set()
	{
		OnLocal(true, false);
	}

	void ProcessorPlus::On_local_tee()
	{
		OnLocal(true, true);
	}

	void ProcessorPlus::On_global_get()
	{
		OnGlobal(true);
	}

	void ProcessorPlus::On_global_set()
	{
		OnGlobal(false);
	}

	void ProcessorPlus::On_global_get_imp()
	{
		OnGlobalImp(true);
	}

	void ProcessorPlus::On_global_set_imp()
	{
		OnGlobalImp(false);
	}

	void ProcessorPlus::On_drop()
	{
		uint32_t nWords = Type::Words(m_Instruction.Read1());
		Exc::Test(m_Stack.m_Pos - m_Stack.m_PosMin >= nWords);
		m_Stack.m_Pos -= nWords;
	}

	void ProcessorPlus::On_select()
	{
		uint32_t nWords = Type::Words(m_Instruction.Read1());
		auto nSel = m_Stack.Pop<Word>();

		Exc::Test(m_Stack.m_Pos - m_Stack.m_PosMin >= (nWords << 1)); // must be at least 2 such operands
		m_Stack.m_Pos -= nWords;

		if (!nSel)
		{
			for (uint32_t i = 0; i < nWords; i++)
				m_Stack.m_pPtr[m_Stack.m_Pos + i - nWords] = m_Stack.m_pPtr[m_Stack.m_Pos + i];
		}
	}

	void ProcessorPlus::On_i32_wrap_i64()
	{
		auto val = m_Stack.Pop<uint64_t>();
		m_Stack.Push(static_cast<uint32_t>(val));
	}

	void ProcessorPlus::On_i64_extend_i32_s()
	{
		m_Stack.Push(Type::Extend<uint64_t>(m_Stack.Pop<int32_t>()));
	}

	void ProcessorPlus::On_i64_extend_i32_u()
	{
		m_Stack.Push(Type::Extend<uint64_t>(m_Stack.Pop<uint32_t>()));
	}

	void ProcessorPlus::On_br()
	{
		Jmp(ReadAddr());
	}

	void ProcessorPlus::On_br_if()
	{
		Word addr = ReadAddr();
		if (m_Stack.Pop<Word>())
			Jmp(addr);
	}

	void ProcessorPlus::On_br_table()
	{
		uint32_t nLabels;
		m_Instruction.Read(nLabels);

		Word nOperand = m_Stack.Pop<Word>();
		beam::setmin(nOperand, nLabels); // fallback to 'def' if out-of-range

		nLabels++;
		uint32_t nSize = sizeof(Word) * nLabels;
		Exc::Test(nSize / sizeof(Word) == nLabels); // overflow check

		auto* pAddrs = reinterpret_cast<const Word*>(m_Instruction.Consume(nSize));
		Jmp(from_wasm<Word>(pAddrs[nOperand]));
	}

	void ProcessorPlus::On_call()
	{
		Word nAddr = ReadAddr();
		Word nRetAddr = get_Ip();
		m_Stack.Push(nRetAddr);
		OnCall(nAddr);
	}

	void ProcessorPlus::On_call_ext()
	{
		uint32_t iExt = m_Instruction.Read<uint32_t>();

		struct MyCheckpoint :public Exc::Checkpoint {
			uint32_t m_iExt;
			void Dump(std::ostream& os) override {
				if (static_cast<uint32_t>(-1) == m_iExt)
					os << "Unresolved binding";
				else
					os << "InvokeExt=" << m_iExt;
			}

		} cp;
		cp.m_iExt = iExt;

		InvokeExt(iExt);
	}

	Word Processor::ReadTable(Word iItem) const
	{
		iItem--; // it's 1-based
		assert(m_prTable0 <= m_Code.n); // must be checked during setup
		Exc::Test(iItem < (m_Code.n - m_prTable0) / sizeof(Word));
		return from_wasm<Word>(static_cast<const uint8_t*>(m_Code.p) + m_prTable0 + sizeof(Word) * iItem);
	}

	Word Processor::ReadVFunc(Word pObject, Word iFunc) const
	{
		Word pVTable = from_wasm<Word>(get_AddrR(pObject, sizeof(Word)));
		Word iFuncIdx = from_wasm<Word>(get_AddrR(pVTable + sizeof(Word) * iFunc, sizeof(Word)));

		return ReadTable(iFuncIdx);
	}

	void ProcessorPlus::On_call_indirect()
	{
		Word iFunc = m_Stack.Pop<Word>();
		Word nAddr = ReadTable(iFunc);

		Word nRetAddr = get_Ip();
		m_Stack.Push(nRetAddr);
		OnCall(nAddr);
	}

	void ProcessorPlus::On_i32_const()
	{
		m_Stack.Push<uint32_t>(m_Instruction.Read<int32_t>());
	}

	void ProcessorPlus::On_i64_const()
	{
		m_Stack.Push<uint64_t>(m_Instruction.Read<int64_t>());
	}

	void ProcessorPlus::On_prolog()
	{
		auto nWords = m_Instruction.Read<uint32_t>();
		while (nWords--)
			m_Stack.Push1(0); // for more safety - zero-init locals. This way we don't need initial stack initialization 
	}

	void ProcessorPlus::On_ret()
	{
		auto nRets = m_Instruction.Read<uint32_t>();
		auto nLocals = m_Instruction.Read<uint32_t>();
		auto nArgs = m_Instruction.Read<uint32_t>();

		// stack layout
		// ...
		// args
		// retaddr (4 bytes)
		// locals
		// retval

		uint32_t nPosRetSrc = m_Stack.m_Pos - nRets;
		Exc::Test(nPosRetSrc <= m_Stack.m_Pos);

		uint32_t nPosAddr = nPosRetSrc - (nLocals + 1);
		Exc::Test(nPosAddr < nPosRetSrc);

		uint32_t nPosRetDst = nPosAddr - nArgs;
		Exc::Test(nPosRetDst <= nPosAddr);
		Exc::Test(nPosRetDst >= m_Stack.m_PosMin);

		Word nRetAddr = m_Stack.m_pPtr[nPosAddr];

		for (uint32_t i = 0; i < nRets; i++)
			m_Stack.m_pPtr[nPosRetDst + i] = m_Stack.m_pPtr[nPosRetSrc + i];


		m_Stack.m_Pos = nPosRetDst + nRets;
		OnRet(nRetAddr);
	}

	void Processor::OnCall(Word nAddr)
	{
		Jmp(nAddr);
	}

	void Processor::OnRet(Word nRetAddr)
	{
		Jmp(nRetAddr);
	}

	void Processor::DebugCallstack::OnCall(Wasm::Word nAddr, Wasm::Word nRetAddr)
	{
		if (m_v.size() < s_MaxEntries)
		{
			auto& fl = m_v.emplace_back();
			fl.m_Addr = nAddr;
			fl.m_CallerIp = nRetAddr;
		}
		else
			m_Missing++;
	}

	void Processor::DebugCallstack::OnRet()
	{
		if (m_Missing)
			m_Missing--;
		else
		{
			if (!m_v.empty())
				m_v.pop_back();
		}
	}

	void Processor::DebugCallstack::Dump(std::ostream& os, Wasm::Word& ip, const Wasm::Compiler::DebugInfo* pDbgInfo) const
	{
		for (uint32_t i = (uint32_t) m_v.size(); i--; )
		{
			const auto& x = m_v[i];

			os << std::endl << "Ip=" << uintBigFrom(x.m_Addr) << "+" << uintBigFrom(ip - x.m_Addr);

			if (pDbgInfo)
			{
				auto* pF = pDbgInfo->Find(ip);
				if (pF)
				{
					size_t iL = pF->Find(ip);
					if (iL < pF->m_vOps.size())
						os << std::endl << "\tFunc = " << pF->m_sName << ", Line = " << iL;
				}
			}

			ip = x.m_CallerIp;
		}
	}

	std::ostream& operator << (std::ostream& os, const Compiler::PerImport& x)
	{
		os.write(x.m_sMod.p, x.m_sMod.n);
		os << '/';
		os.write(x.m_sName.p, x.m_sName.n);
		return os;
	}

	std::ostream& operator << (std::ostream& os, const Compiler::PerImportFunc& x)
	{
		return operator << (os, Cast::Down<Compiler::PerImport>(x));
	}

} // namespace Wasm
} // namespace beam
