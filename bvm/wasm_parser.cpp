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

	struct CompilerPlus
		:public Compiler
	{
#define WasmParserSections(macro) \
		macro(0, Custom) \
		macro(1, Type) \
		macro(2, Import) \
		macro(3, Funcs) \
		macro(4, Table) \
		macro(6, Global) \
		macro(7, Export) \
		macro(9, Element) \
		macro(10, Code) \
		macro(11, Data) \
		macro(12, DataCount) \

#define THE_MACRO(id, name) void OnSection_##name(Reader&);
		WasmParserSections(THE_MACRO)
#undef THE_MACRO

		void ParsePlus(Reader);

		int32_t ReadI32Initializer(Reader& inp)
		{
			// initialization expression for the global variable. Ignore it, we don't really support globals, it's just needed for a non-imported stack pointer worakround.
			Exc::Test(Instruction::i32_const == inp.Read1());
			auto val = inp.Read<int32_t>();
			Exc::Test(Instruction::end_block == inp.Read1());
			return val;
		}
	};

	void Compiler::Parse(const Reader& inp)
	{
		auto& c = Cast::Up<CompilerPlus>(*this);
		static_assert(sizeof(c) == sizeof(*this));
		c.ParsePlus(inp);

	}

	void CompilerPlus::ParsePlus(Reader inp)
	{
		Exc::CheckpointTxt cp("wasm/parse");

		static const uint8_t pMagic[] = { 0, 'a', 's', 'm' };
		Exc::Test(!memcmp(pMagic, inp.Consume(sizeof(pMagic)), sizeof(pMagic)));

		static const uint8_t pVer[] = { 1, 0, 0, 0 };
		Exc::Test(!memcmp(pVer, inp.Consume(sizeof(pVer)), sizeof(pVer)));

		m_cmplData0 = 0;

		for (uint8_t nPrevSection = 0; inp.m_p0 < inp.m_p1; )
		{
			auto nSection = inp.Read1();
			bool bIgnoreOrder = !nSection || (12 == nSection);
			Exc::Test(!nPrevSection || bIgnoreOrder || (nSection > nPrevSection));

			auto nLen = inp.Read<uint32_t>();

			Reader inpSection;
			inpSection.m_p0 = inp.Consume(nLen);
			inpSection.m_p1 = inpSection.m_p0 + nLen;

			switch (nSection)
			{
#define THE_MACRO(id, name) case id: OnSection_##name(inpSection); Exc::Test(inpSection.m_p0 == inpSection.m_p1); break;
			WasmParserSections(THE_MACRO)
#undef THE_MACRO

			}

			if (!bIgnoreOrder)
				nPrevSection = nSection;
		}
	}

#define STR_MATCH(vec, txt) ((vec.n == sizeof(txt)-1) && !memcmp(vec.p, txt, sizeof(txt)-1))

	void CompilerPlus::OnSection_Custom(Reader& inp)
	{
		Vec<char> sName;
		sName.Read(inp);

		if (STR_MATCH(sName, "name"))
		{
			auto nType = inp.Read1();
			auto nSize = inp.Read<uint32_t>();

			Reader r;
			r.m_p0 = inp.Consume(nSize);
			r.m_p1 = r.m_p0 + nSize;

			if (1 == nType)
			{
				// function names
				auto nCount = r.Read<uint32_t>();
				for (uint32_t i = 0; i < nCount; i++)
				{
					auto iFunc = r.Read<uint32_t>();
					sName.Read(r);

					// the convention looks to be: first all the imported names, then the internal
					iFunc -= (uint32_t) m_ImportFuncs.size();

					if (iFunc < m_Functions.size())
						m_Functions[iFunc].m_sName = sName;
				}
			}
		}

		inp.m_p0 = inp.m_p1; // skip the rest
	}

	void CompilerPlus::OnSection_Type(Reader& inp)
	{
		auto nCount = inp.Read<uint32_t>();
		m_Types.resize(nCount);

		for (uint32_t i = 0; i < nCount; i++)
		{
			auto& x = m_Types[i];

			Exc::Test(inp.Read1() == 0x60);

			x.m_Args.Read(inp);
			x.m_Rets.Read(inp);

			Exc::Test(x.m_Rets.n <= 1);
		}
	}

	void CompilerPlus::OnSection_Import(Reader& inp)
	{
		auto nCount = inp.Read<uint32_t>();

		for (uint32_t i = 0; i < nCount; i++)
		{
			PerImport pi;
			pi.m_sMod.Read(inp);
			pi.m_sName.Read(inp);
			pi.m_Binding = static_cast<uint32_t>(-1);

			uint8_t nKind = inp.Read1();
			switch (nKind)
			{
			case 0: {
				auto& x = m_ImportFuncs.emplace_back();
				Cast::Down<PerImport>(x) = pi;
				x.m_TypeIdx = inp.Read<uint32_t>();
				Exc::Test(x.m_TypeIdx < m_Types.size());
			}
			break;

			case 1: {
				// table. Ignore.
				[[maybe_unused]] uint32_t lim;
				lim = inp.Read<uint32_t>();
				lim = inp.Read<uint32_t>();
				//lim;

				[[maybe_unused]] uint8_t nElemType = inp.Read1();
				//nElemType;


			} break;

			case 2: {

				// mem type. Ignore.
				[[maybe_unused]] uint32_t lim;
				lim = inp.Read<uint32_t>();
				lim = inp.Read<uint32_t>();
				//lim;
			}
			break;

			case 3: {
				// global
				auto& x = m_ImportGlobals.emplace_back();
				Cast::Down<PerImport>(x) = pi;
				x.m_Type = inp.Read1();
				x.m_IsVariable = inp.Read1();
			}
			break;

			default:
				Exc::Fail();

			}

		}
	}

	void CompilerPlus::OnSection_Funcs(Reader& inp)
	{
		auto nCount = inp.Read<uint32_t>();
		m_Functions.resize(nCount);

		for (uint32_t i = 0; i < nCount; i++)
		{
			auto& x = m_Functions[i];
			ZeroObject(x.m_Expression);
			ZeroObject(x.m_sName);

			x.m_TypeIdx = inp.Read<uint32_t>();
			Exc::Test(x.m_TypeIdx < m_Types.size());
		}
	}

	void CompilerPlus::OnSection_Table(Reader& inp)
	{
		auto nCount = inp.Read<uint32_t>();

		for (uint32_t i = 0; i < nCount; i++)
		{
			auto tblType = inp.Read<uint32_t>(); 
			Exc::Test(0x70 == tblType); // must by anyfunc

			auto nFlags = inp.Read<uint32_t>();
			[[maybe_unused]] auto nLimitMin = inp.Read<uint32_t>();
			//nLimitMin;
			[[maybe_unused]] auto nLimitMax = nLimitMin;

			if (1 & nFlags)
				nLimitMax = inp.Read<uint32_t>(); // max

			//nLimitMax;
		}
	}

	void CompilerPlus::OnSection_Global(Reader& inp)
	{
		auto nCount = inp.Read<uint32_t>();
		m_Globals.resize(nCount);

		for (uint32_t i = 0; i < nCount; i++)
		{
			auto& x = m_Globals[i];

			x.m_Type = inp.Read1();
			x.m_IsVariable = inp.Read1();

			// initialization expression for the global variable. Ignore it, we don't really support globals, it's just needed for a non-imported stack pointer worakround.
			ReadI32Initializer(inp);
		}
	}

	void CompilerPlus::OnSection_Export(Reader& inp)
	{
		auto nCount = inp.Read<uint32_t>();
		m_Exports.resize(nCount);

		for (uint32_t i = 0; i < nCount; i++)
		{
			auto& x = m_Exports[i];
			x.m_sName.Read(inp);
			x.m_Kind = inp.Read1();
			x.m_Idx = inp.Read<uint32_t>();

			if (!x.m_Kind)
			{
				x.m_Idx -= static_cast<uint32_t>(m_ImportFuncs.size());
				Exc::Test(x.m_Idx < m_Functions.size());
			}
		}
	}

	void CompilerPlus::OnSection_Element(Reader& inp)
	{
		auto nCount = inp.Read<uint32_t>();
		Exc::Test(1 == nCount);

		for (uint32_t i = 0; i < nCount; i++)
		{
			auto iTable = inp.Read<uint32_t>();
			Exc::Test(0 == iTable);
			uint32_t nOffset = ReadI32Initializer(inp);
			Exc::Test(1 == nOffset); // seems irrelevant

			auto nFuncs = inp.Read<uint32_t>();
			m_IndirectFuncs.m_vec.resize(nFuncs);

			for (uint32_t iFunc = 0; iFunc < nFuncs; iFunc++)
			{
				auto val = inp.Read<uint32_t>();
				val -= static_cast<uint32_t>(m_ImportFuncs.size());
				Exc::Test(val < m_Functions.size());

				m_IndirectFuncs.m_vec[iFunc] = val;
			}
		}
	}

	void CompilerPlus::OnSection_Code(Reader& inp)
	{
		auto nCount = inp.Read<uint32_t>();
		Exc::Test(nCount == m_Functions.size());

		for (uint32_t i = 0; i < nCount; i++)
		{
			auto& x = m_Functions[i];

			auto nSize = inp.Read<uint32_t>();
			Reader inpFunc;
			inpFunc.m_p0 = inp.Consume(nSize);
			inpFunc.m_p1 = inpFunc.m_p0 + nSize;

			const auto& funcType = m_Types[x.m_TypeIdx];

			for (uint32_t iArg = 0; iArg < funcType.m_Args.n; iArg++)
				x.m_Locals.Add(funcType.m_Args.p[iArg]);

			auto nBlocks = inpFunc.Read<uint32_t>();
			for (uint32_t iBlock = 0; iBlock < nBlocks; iBlock++)
			{
				auto nVarsCount = inpFunc.Read<uint32_t>();
				uint8_t nType = inpFunc.Read1();

				while (nVarsCount--)
					x.m_Locals.Add(nType);
			}

			// the rest is the function body
			x.m_Expression = inpFunc;
		}
	}

	void CompilerPlus::OnSection_Data(Reader& inp)
	{
		uint32_t nCount = inp.Read<uint32_t>();

		for (uint32_t i = 0; i < nCount; i++)
		{
			uint32_t nAddr = inp.Read<uint32_t>();

			uint32_t nOffset = ReadI32Initializer(inp);
			nAddr += nOffset;

			Vec<uint8_t> data;
			data.Read(inp);

			if (i) {
				// assume data blocks are sorted w.r.t. their virtual offset, and no overlap
				Exc::Test(nAddr >= m_cmplData0 + m_Data.size());
			}
			else
				m_cmplData0 = nAddr;

			if (data.n)
			{
				nAddr -= m_cmplData0; // to rel

				size_t n = nAddr + data.n;
				if (m_Data.size() < n)
					m_Data.resize(n);

				memcpy(&m_Data.front() + nAddr, data.p, data.n);
			}
		}
	}

	void CompilerPlus::OnSection_DataCount(Reader& inp)
	{
		[[maybe_unused]] uint32_t nCount = inp.Read<uint32_t>();
		//nCount;
	}

} // namespace Wasm
} // namespace beam
