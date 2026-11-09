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
	// Compiler
	uint32_t Compiler::PerFunction::Locals::get_SizeWords() const
	{
		return m_v.empty() ? 0 : m_v.back().m_PosWords + m_v.back().m_SizeWords;
	}

	void Compiler::PerFunction::Locals::Add(uint8_t nType)
	{
		uint32_t nPosWords = get_SizeWords();

		auto& v = m_v.emplace_back();
		v.m_Type = nType;
		v.m_PosWords = nPosWords;
		v.m_SizeWords = Type::Words(nType);
	}

	void TestBinding(const Compiler::PerImport& x)
	{
		Exc::Test(x.m_Binding != static_cast<uint32_t>(-1));
	}

	struct Compiler::Context
	{

		Compiler& m_This;
		const uint8_t* m_p0;
		uint32_t m_iFunc = 0;
		Reader m_Code;
		bool m_BuildDependency;

		struct Block
		{
			PerType m_Type;
			std::vector<uint8_t> m_OperandsAtExit;
			uint32_t m_iLabel;
			bool m_Loop = false;
		};

		std::vector<Block> m_Blocks;
		std::vector<uint8_t> m_Operands;
		uint32_t m_WordsOperands = 0;
		bool m_Unreachable = false;

		Block& get_B() {
			Exc::Test(!m_Blocks.empty());
			return m_Blocks.back();
		}

		void Push(uint8_t x) {
			m_Operands.push_back(x);
			m_WordsOperands += Type::Words(x);
		}

		uint8_t Pop() {
			Exc::Test(!m_Operands.empty());
			uint8_t ret = m_Operands.back();
			m_Operands.pop_back();

			m_WordsOperands -= Type::Words(ret);
			return ret;
		}

		void Pop(uint8_t nType) {
			uint8_t x = Pop();
			Exc::Test(x == nType);
		}

		void Pop(const Vec<uint8_t>& vArgs)
		{
			for (uint32_t i = vArgs.n; i--; )
				Pop(vArgs.p[i]);
		}

		void TestOperands(const Vec<uint8_t>& v)
		{
			if (!v.n)
				return;

			Exc::Test(m_Operands.size() >= v.n);
			Exc::Test(!memcmp(&m_Operands.front() + m_Operands.size() - v.n, v.p, sizeof(*v.p) * v.n));
		}

		void BlockOpen(const PerType& tp)
		{
			auto& b = m_Blocks.emplace_back();
			size_t nOperandsAtExit = m_Operands.size();

			if (1 != m_Blocks.size())
			{
				TestOperands(tp.m_Args); // for most outer function block the args are not on the stack
				nOperandsAtExit -= tp.m_Args.n;

				b.m_iLabel = static_cast<uint32_t>(m_This.m_Labels.m_Items.size());
				m_This.m_Labels.m_Items.push_back(0);
			}

			b.m_OperandsAtExit.resize(nOperandsAtExit + tp.m_Rets.n);
			std::copy(m_Operands.begin(), m_Operands.begin() + nOperandsAtExit, b.m_OperandsAtExit.begin());
			std::copy(tp.m_Rets.p, tp.m_Rets.p + tp.m_Rets.n, b.m_OperandsAtExit.begin() + nOperandsAtExit);

			b.m_Type = tp;
		}

		void BlockOpen()
		{
			auto nType = m_Code.Read<uint32_t>();
			Exc::Test(0x40 == nType);
			PerType tp = { { 0 } };
			BlockOpen(tp);

			m_p0 = nullptr; // don't write
		}

		void TestBlockCanClose(const Block& b)
		{
			Exc::Test(m_Operands == b.m_OperandsAtExit);
		}

		void UpdTopBlockLabel()
		{
			m_This.m_Labels.m_Items[m_Blocks.back().m_iLabel] = static_cast<uint32_t>(m_This.m_Result.size());
		}

		static uint32_t WordsOfVars(const Vec<uint8_t>& v)
		{
			uint32_t nWords = 0;
			for (uint32_t i = 0; i < v.n; i++)
				nWords += Type::Words(v.p[i]);
			return nWords;
		}

		void WriteRet()
		{
			WriteRes(Instruction::ret);

			// stack layout
			// ...
			// args
			// retaddr (4 bytes)
			// locals
			// retval

			// to allow for correct return we need to provide the following:

			uint32_t nWordsLocal = m_This.m_Functions[m_iFunc].m_Locals.get_SizeWords(); // sizeof(args) + sizeof(locals)

			const auto& tp = m_Blocks.front().m_Type;
			uint32_t nWordsArgs = WordsOfVars(tp.m_Args);

			WriteResU(WordsOfVars(tp.m_Rets)); // sizeof(retval)
			WriteResU(nWordsLocal - nWordsArgs); // sizeof(locals), excluding args
			WriteResU(nWordsArgs);
		}

		void BlockClose()
		{
			auto& b = get_B();

			if (m_Unreachable)
			{
				if (m_Operands != b.m_OperandsAtExit)
				{
					// sometimes the compiler won't bother to restore stack operands past unconditional return. Ignore this.
					m_Operands.swap(b.m_OperandsAtExit); // sometimes the compiler won't bother to restore stack operands past unconditional return. Ignore this.

					m_WordsOperands = 0;
					for (size_t i = 0; i < m_Operands.size(); i++)
						m_WordsOperands += Type::Words(m_Operands[i]);
				}
			}
			else
				TestBlockCanClose(b);

			if (1 == m_Blocks.size())
				WriteRet(); // end of function
			else
			{
				if (!b.m_Loop)
					UpdTopBlockLabel();
			}

			m_Blocks.pop_back();
			m_Unreachable = false;

			m_p0 = nullptr; // don't write
		}

		void PutLabelTrg(uint32_t iLabel)
		{
			auto& lbl = m_This.m_Labels.m_Targets.emplace_back();
			lbl.m_iItem = iLabel;
			lbl.m_Pos = static_cast<uint32_t>(m_This.m_Result.size());

			Word n = to_wasm(iLabel);
			WriteRes(reinterpret_cast<uint8_t*>(&n), sizeof(n));
		}

		void PutBranchLabel(uint32_t nLabel)
		{
			Exc::Test(nLabel + 1 < m_Blocks.size());
			assert(nLabel < m_This.m_Labels.m_Items.size());

			auto iBlock = m_Blocks.size() - (nLabel + 1);
			auto& b = m_Blocks[iBlock];

			size_t nOperands = b.m_OperandsAtExit.size();
			if (b.m_Loop)
			{
				assert(iBlock); // function block can't be loop
				nOperands += b.m_Type.m_Args.n - b.m_Type.m_Rets.n;
			}

			TestOperands(b.m_Type.m_Args);
			Exc::Test(m_Operands.size() == nOperands);

			PutLabelTrg(m_Blocks[m_Blocks.size() - (nLabel + 1)].m_iLabel);
		}

		void OnBranch()
		{
			WriteRes(*m_p0); // opcode
			m_p0 = nullptr;

			auto nLabel = m_Code.Read<uint32_t>();
			PutBranchLabel(nLabel);
		}

		uint8_t OnLocalVar()
		{
			WriteInstruction();

			const auto& f = m_This.m_Functions[m_iFunc];
			auto iVar = m_Code.Read<uint32_t>();

			Exc::Test(iVar < f.m_Locals.m_v.size());
			const auto var = f.m_Locals.m_v[iVar];


			// add type and position of the local wrt current stack

			// stack layout
			// ...
			// args
			// retaddr (4 bytes)
			// locals
			// current operand stack

			uint32_t nOffsWords = m_WordsOperands + f.m_Locals.get_SizeWords() - var.m_PosWords;

			const auto& fType = m_This.m_Types[f.m_TypeIdx];
			if (iVar < fType.m_Args.n)
				nOffsWords++; // retaddr

			assert(static_cast<size_t>(var.m_Type) - static_cast<size_t>(Type::s_Base) < sizeof(Word));

			uint32_t nValue = (nOffsWords * sizeof(Word)) | (var.m_Type - Type::s_Base);
			WriteResU(nValue);

			return var.m_Type;
		}

		void On_local_get() {
			Push(OnLocalVar());
		}
		void On_local_set() {
			Pop(OnLocalVar());
		}

		void On_local_tee() {
			auto nType = OnLocalVar();
			Pop(nType);
			Push(nType);
		}

		void On_drop() {
			WriteInstruction();
			WriteRes(Pop());
		}

		void On_select() {
			WriteInstruction();

			Pop(Type::i32);
			uint8_t nType = Pop();
			Pop(nType); // must be the same
			Push(nType); // result

			WriteRes(nType);
		}

		uint32_t ReadOffsAndPadding()
		{
			auto nAlign = m_Code.Read<uint32_t>();
			Processor::Stack::TestAlignmentPower(nAlign);
			return m_Code.Read<uint32_t>();
		}

		void On_i32_wrap_i64() {
			Pop(Type::i64);
			Push(Type::i32);
		}

		void On_i64_extend_i32_s() {
			Pop(Type::i32);
			Push(Type::i64);
		}

		void On_i64_extend_i32_u() {
			On_i64_extend_i32_s();
		}

		void On_block() {
			BlockOpen();
		}

		void On_loop() {
			BlockOpen();
			get_B().m_Loop = true;
			UpdTopBlockLabel();
		}

		void On_nop() {
			m_p0 = nullptr;
		}

		void On_unreachable() {
			m_p0 = nullptr;
			m_Unreachable = true;
		}

		void On_end_block() {
			BlockClose();
		}

		void On_br() {
			OnBranch();
		}

		void On_br_if() {
			Pop(Type::i32); // conditional
			OnBranch();
		}

		void On_br_table()
		{
			Pop(Type::i32); // operand

			WriteRes(*m_p0); // opcode
			m_p0 = nullptr;

			uint32_t nLabels;
			m_Code.Read(nLabels);
			WriteResU(nLabels);

			// the following loop includes the 'def' label too
			do
			{
				auto nLabel = m_Code.Read<uint32_t>();
				PutBranchLabel(nLabel);

			} while (nLabels--);

		}

		void On_ret()
		{
			TestBlockCanClose(m_Blocks.front()); // if the return is from a nested block - assume the necessary unwind has already been done
			WriteRet(); // end of function
			m_Unreachable = true; // ignore operand stack state until the next block closes.
		}

		void On_i32_const() {
			[[maybe_unused]] auto val = m_Code.Read<int32_t>();
			//val;
			Push(Type::i32);
		}

		void On_i64_const() {
			[[maybe_unused]] auto val = m_Code.Read<int64_t>();
			//val;
			Push(Type::i64);
		}

		void OnGlobal(bool bGet)
		{
			auto iVar = m_Code.Read<uint32_t>();
			bool bImported = (iVar < m_This.m_ImportGlobals.size());
			if (bImported)
			{
				const auto& x = m_This.m_ImportGlobals[iVar];

				if (bGet) {
					Push(x.m_Type);
					WriteRes(Instruction::global_get_imp);
				}
				else {
					Pop(x.m_Type);
					WriteRes(Instruction::global_set_imp);
				}

				TestBinding(x);
				WriteResU(x.m_Binding);
			}
			else
			{
				Exc::Fail(); // not supported atm
			}

			m_p0 = nullptr; // don't write
		}

		void On_global_get()
		{
			OnGlobal(true);
		}

		void On_global_set()
		{
			OnGlobal(false);
		}

		void OnCallType(uint32_t iTypeIdx)
		{
			const auto& tp = m_This.m_Types[iTypeIdx];

			Pop(tp.m_Args);

			for (uint32_t i = 0; i < tp.m_Rets.n; i++)
				Push(tp.m_Rets.p[i]);
		}

		void OnDep(uint32_t iTrg)
		{
			if (m_BuildDependency)
				m_This.m_Functions[m_iFunc].m_Dep.m_Set.insert(iTrg);
		}

		void On_call()
		{
			auto iFunc = m_Code.Read<uint32_t>();

			bool bImported = (iFunc < m_This.m_ImportFuncs.size());
			if (!bImported)
			{
				iFunc -= static_cast<uint32_t>(m_This.m_ImportFuncs.size());
				Exc::Test(iFunc < m_This.m_Functions.size());

				OnDep(iFunc);
			}

			uint32_t iTypeIdx = bImported ? m_This.m_ImportFuncs[iFunc].m_TypeIdx : m_This.m_Functions[iFunc].m_TypeIdx;
			OnCallType(iTypeIdx);

			m_p0 = nullptr; // don't write

			if (bImported)
			{
				WriteRes(Instruction::call_ext);

				const auto& f = m_This.m_ImportFuncs[iFunc];
				//TestBinding(f); // allow code generation for unbound function calls. Catch it during runtime
				WriteResU(f.m_Binding);
			}
			else
			{
				WriteRes(Instruction::call);
				PutLabelTrg(iFunc);
			}
		}

		void On_call_indirect()
		{
			auto iType = m_Code.Read<uint32_t>();
			Exc::Test(iType < m_This.m_Types.size());

			auto iTable = m_Code.Read<uint32_t>();
			Exc::Test(!iTable);

			Pop(Type::i32); // func index
			OnCallType(iType);

			m_p0 = nullptr; // don't write

			WriteRes(Instruction::call_indirect);

			OnDep(Dependency::s_IdxIndirect);
		}

		void CompileFunc();


		Context(Compiler& x) :m_This(x) {}

		void WriteRes(uint8_t x) {
			m_This.m_Result.push_back(x);
		}

		void Write(Instruction x) {
			WriteRes(static_cast<uint8_t>(x));
		}

		void WriteRes(const uint8_t* p, uint32_t n);
		void WriteResU(uint64_t);
		void WriteResS(int64_t);

		void WriteInstruction();
	};

	void Compiler::Context::WriteInstruction()
	{
		if (m_p0)
		{
			WriteRes(m_p0, static_cast<uint32_t>(m_Code.m_p0 - m_p0));
			m_p0 = nullptr;
		}
	}

	void Compiler::Context::WriteRes(const uint8_t* p, uint32_t n)
	{
		if (n)
		{
			size_t n0 = m_This.m_Result.size();
			m_This.m_Result.resize(n0 + n);
			memcpy(&m_This.m_Result.front() + n0, p, n);
		}
	}

	void Compiler::Context::WriteResU(uint64_t x)
	{
		while (true)
		{
			uint8_t n = static_cast<uint8_t>(x);
			x >>= 7;

			if (!x)
			{
				assert(!(n & 0x80));
				WriteRes(n);
				break;
			}

			WriteRes(n | 0x80);
		}
	}

	void Compiler::Context::WriteResS(int64_t x)
	{
		while (true)
		{
			uint8_t n = static_cast<uint8_t>(x);
			x >>= 6; // sign bit is propagated

			if (!x)
			{
				assert((n & 0xC0) == 0);
				WriteRes(n);
				break;
			}

			if (-1 == x)
			{
				assert((n & 0xC0) == 0xC0);
				WriteRes(n & ~0x80); // sign bit must remain
				break;
			}

			WriteRes(n | 0x80);
			x >>= 1;
		}
	}

	template <typename TObject, typename TCmp>
	size_t FindPastMatch(const TCmp& comparator, const TObject* p0, size_t n)
	{
		size_t n0 = 0;
		while (n0 < n)
		{
			size_t mid = (n0 + n) / 2;

			if (comparator.IsMatch(p0[mid]))
				n0 = mid + 1;
			else
				n = mid;
		}

		return n0;
	}

	const Compiler::DebugInfo::Function* Compiler::DebugInfo::Find(Wasm::Word nAddr) const
	{
		if (!m_vFuncs.empty())
		{
			struct MyCmp
			{
				Wasm::Word m_Addr;
				bool IsMatch(const Function& f) const {
					return f.m_Pos <= m_Addr;
				}
			};

			MyCmp myCmp;
			myCmp.m_Addr = nAddr;

			auto idx = FindPastMatch(myCmp, &m_vFuncs.front(), m_vFuncs.size());
			if (idx)
				return &m_vFuncs[idx - 1];
		}
		return nullptr;
	}

	size_t Compiler::DebugInfo::Function::Find(Wasm::Word nAddr) const
	{
		if (m_vOps.empty())
			return 0;

		struct MyCmp
		{
			Wasm::Word m_Pos;
			bool IsMatch(const Entry& e) const {
				return e.m_Pos < m_Pos;
			}
		};

		MyCmp myCmp;
		myCmp.m_Pos = nAddr - m_Pos; // relative addr

		return FindPastMatch(myCmp, &m_vOps.front(), m_vOps.size());
	}

	void Compiler::Build()
	{
		Exc::CheckpointTxt cp("wasm/Compiler/build");

		auto n0 = m_Result.size();

		BuildPass(false);

		uint32_t nIncluded = 0, nTotal = 0;
		CalcDependencies(nIncluded, nTotal);

		if (nIncluded < nTotal)
		{
			m_Result.resize(n0);
			BuildPass(true);
		}
	}

	void Compiler::BuildPass(bool bDependentOnly)
	{
		if (m_pDebugInfo)
		{
			m_pDebugInfo->m_vFuncs.clear();
			m_pDebugInfo->m_vFuncs.reserve(m_Functions.size());
		}

		m_Labels.m_Items.resize(m_Functions.size()); // function labels
		m_Labels.m_Targets.clear();

		for (uint32_t i = 0; i < m_Functions.size(); i++)
		{
			Context ctx(*this);
			m_Labels.m_Items[i] = static_cast<uint32_t>(m_Result.size());
			ctx.m_iFunc = i;
			ctx.m_BuildDependency = !bDependentOnly;
			ctx.CompileFunc();
		}

		m_cmplTable0 = static_cast<uint32_t>(m_Result.size());

		if (!bDependentOnly || m_IndirectFuncs.m_Dep.m_Include)
		{
			for (uint32_t i = 0; i < m_IndirectFuncs.m_vec.size(); i++)
			{
				m_Result.resize(m_Result.size() + sizeof(Word));
				to_wasm(&m_Result.back() - (sizeof(Word) - 1), m_Labels.m_Items[m_IndirectFuncs.m_vec[i]]);
			}
		}

		for (uint32_t i = 0; i < m_Labels.m_Targets.size(); i++)
		{
			auto& trg = m_Labels.m_Targets[i];

			to_wasm(&m_Result.front() + trg.m_Pos, m_Labels.m_Items[trg.m_iItem]);
		}
	}

	void Compiler::CalcDependencies(uint32_t& nIncluded, uint32_t& nTotal)
	{
		std::vector<uint32_t> vec;
		vec.reserve(m_Functions.size() + 1);

		for (uint32_t i = 0; i < m_Functions.size(); i++)
		{
			auto& func = m_Functions[i];
			if (func.m_Dep.m_Include)
				vec.push_back(i);
		}

		nTotal += (uint32_t) m_Functions.size();

		if (!m_IndirectFuncs.m_vec.empty())
		{
			nTotal++;
			for (uint32_t i = 0; i < m_IndirectFuncs.m_vec.size(); i++)
				m_IndirectFuncs.m_Dep.m_Set.insert(m_IndirectFuncs.m_vec[i]);
		}

		assert(!m_IndirectFuncs.m_Dep.m_Include);

		for (uint32_t i = 0; i < vec.size(); i++)
		{
			nIncluded++;

			auto iIdx = vec[i];
			auto& dep = (Dependency::s_IdxIndirect == iIdx) ? m_IndirectFuncs.m_Dep : m_Functions[iIdx].m_Dep;
			assert(dep.m_Include);

			for (auto it = dep.m_Set.begin(); dep.m_Set.end() != it; it++)
			{
				auto iIdx2 = *it;
				auto& dep2 = (Dependency::s_IdxIndirect == iIdx2) ? m_IndirectFuncs.m_Dep : m_Functions[iIdx2].m_Dep;
				if (!dep2.m_Include)
				{
					dep2.m_Include = true;
					vec.push_back(iIdx2);
				}
			}
		}
	}

	void Compiler::Context::CompileFunc()
	{
		struct MyCheckpoint :public Exc::Checkpoint {
			uint32_t m_iFunc;
			uint32_t m_Line = 0;
			void Dump(std::ostream& os) override {
				os << "iFunc=" << m_iFunc << ", Line=" << m_Line;
			}

		} cp;
		cp.m_iFunc = m_iFunc;

		auto& func = m_This.m_Functions[m_iFunc];

		if (!m_BuildDependency && !func.m_Dep.m_Include)
			return; // skip it

		DebugInfo::Function* pDbg = nullptr;
		if (m_This.m_pDebugInfo)
		{
			pDbg = &m_This.m_pDebugInfo->m_vFuncs.emplace_back();
			pDbg->m_sName.assign(func.m_sName.p, func.m_sName.n);
			pDbg->m_Pos = (uint32_t) m_This.m_Result.size();
		}

		m_Code = func.m_Expression;

		const auto& tp = m_This.m_Types[func.m_TypeIdx];
		BlockOpen(tp);

		typedef Instruction I;

		const auto& v = func.m_Locals.m_v;
		assert(tp.m_Args.n <= v.size());
		if (tp.m_Args.n < v.size())
		{
			uint32_t nLocalVarsSize = func.m_Locals.get_SizeWords() - v[tp.m_Args.n].m_PosWords;

			WriteRes(I::prolog);
			WriteResU(nLocalVarsSize);
		}

		for ( ; !m_Blocks.empty(); cp.m_Line++)
		{
			m_p0 = m_Code.m_p0;

			I nInstruction = (I) m_Code.Read1();

			if (pDbg)
			{
				auto& de = pDbg->m_vOps.emplace_back();
				de.m_Opcode = (uint8_t) nInstruction;
				de.m_Pos = (uint32_t) m_This.m_Result.size();
				de.m_Pos -= pDbg->m_Pos; // to rel
			}

			switch (nInstruction)
			{
#define THE_MACRO(id, name) \
			case I::name: \
				On_##name(); \
				break;

			WasmInstructions_CustomPorted(THE_MACRO)
			WasmInstructions_NotPorted(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(id, type, name, tmem) \
			case I::type##_##name: { \
				ReadOffsAndPadding(); \
				Pop(Type::i32); \
				Push(Type::type); \
			} break;

			WasmInstructions_Load(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(id, type, name, tmem) \
			case I::type##_##name: { \
				ReadOffsAndPadding(); \
				Pop(Type::type); \
				Pop(Type::i32); \
			} break;

			WasmInstructions_Store(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO_ID32(name, id32, id64) case id32:
#define THE_MACRO_ID64(name, id32, id64) case id64:

			WasmInstructions_unop_Polymorphic_32(THE_MACRO_ID32)
			{
				Pop(Type::i32);
				Push(Type::i32);

			} break;

			WasmInstructions_unop_Polymorphic_32(THE_MACRO_ID64)
			{
				Pop(Type::i64);
				Push(Type::i32);

			} break;

			WasmInstructions_binop_Polymorphic_32(THE_MACRO_ID32)
			WasmInstructions_binop_Polymorphic_x(THE_MACRO_ID32)
			{
				Pop(Type::i32);
				Pop(Type::i32);
				Push(Type::i32);

			} break;

			WasmInstructions_binop_Polymorphic_32(THE_MACRO_ID64)
			{
				Pop(Type::i64);
				Pop(Type::i64);
				Push(Type::i32);

			} break;

			WasmInstructions_binop_Polymorphic_x(THE_MACRO_ID64)
			{
				Pop(Type::i64);
				Pop(Type::i64);
				Push(Type::i64);

			} break;

#undef THE_MACRO_ID32
#undef THE_MACRO_ID64

			default:
				Exc::Fail();
			}

			WriteInstruction(); // unless already written
		}

		Exc::Test(m_Code.m_p0 == m_Code.m_p1);

	}

} // namespace Wasm
} // namespace beam
