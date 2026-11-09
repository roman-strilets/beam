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

	/////////////////////////////////////////////
	// Compilation

	void Processor::Compile(ByteBuffer& res, const Blob& src, Kind kind, Wasm::Compiler::DebugInfo* pDbgInfo /* = nullptr */)
	{
		Compiler c;
		c.Compile(res, src, kind, pDbgInfo);
	}

#define STR_MATCH(vec, txt) ((vec.n == sizeof(txt)-1) && !memcmp(vec.p, txt, sizeof(txt)-1))

	int32_t Processor::Compiler::get_PublicMethodIdx(const Wasm::Compiler::Vec<char>& sName)
	{
		if (STR_MATCH(sName, "Ctor"))
			return 0;
		if (STR_MATCH(sName, "Dtor"))
			return 1;

		static const char szPrefix[] = "Method_";
		if (sName.n < sizeof(szPrefix) - 1)
			return -1;

		if (memcmp(sName.p, szPrefix, sizeof(szPrefix) - 1))
			return -1;

		// TODO - rewrite this
		char szTxt[11];
		size_t nLen = std::min(sizeof(szTxt) - 1, sName.n - (sizeof(szPrefix) - 1));
		memcpy(szTxt, sName.p + sizeof(szPrefix) - 1, nLen);
		szTxt[nLen] = 0;

		return atoi(szTxt);
	}

	void Processor::Compiler::Compile(ByteBuffer& res, const Blob& src, Kind kind, Wasm::Compiler::DebugInfo* pDbgInfo /* = nullptr */)
	{
		Exc::CheckpointTxt cp("Wasm/compile");

		Wasm::Reader inp;
		inp.m_p0 = reinterpret_cast<const uint8_t*>(src.p);
		inp.m_p1 = inp.m_p0 + src.n;

		Wasm::Compiler c;
		c.m_pDebugInfo = pDbgInfo;
		c.Parse(inp);

		ResolveBindings(c, kind);

		typedef std::map<uint32_t, uint32_t> MethodMap; // method num -> func idx
		MethodMap hdrMap;

		for (uint32_t i = 0; i < c.m_Exports.size(); i++)
		{
			auto& x = c.m_Exports[i];
			if (x.m_Kind)
				continue; // not a function

			int32_t iMethod = get_PublicMethodIdx(x.m_sName);
			if (iMethod < 0)
				continue;

			Exc::Test(hdrMap.end() == hdrMap.find(iMethod)); // duplicates check
			hdrMap[iMethod] = x.m_Idx;
		}

		uint32_t nNumMethods = static_cast<uint32_t>(hdrMap.size());

		Exc::Test(nNumMethods >= Header::s_MethodsMin);
		Exc::Test(hdrMap.rbegin()->first == nNumMethods - 1); // should be no gaps

		for (auto it = hdrMap.begin(); hdrMap.end() != it; it++)
		{
			uint32_t iFunc = it->second;
			c.m_Functions[iFunc].m_Dep.m_Include = true;
		}

		// header
		uint32_t nSizeHdr = sizeof(Header) + sizeof(Wasm::Word) * (nNumMethods - Header::s_MethodsMin);
		c.m_Result.resize(nSizeHdr);
		auto* pHdr = reinterpret_cast<Header*>(&c.m_Result.front());

		pHdr->m_Version = ByteOrder::to_le(Header::s_Version);
		pHdr->m_NumMethods = ByteOrder::to_le(nNumMethods);

		// the methods themselves are not assigned yet

		c.Build();

		pHdr = reinterpret_cast<Header*>(&c.m_Result.front());
		pHdr->m_hdrTable0 = ByteOrder::to_le(c.m_cmplTable0);

		for (auto it = hdrMap.begin(); hdrMap.end() != it; it++)
		{
			uint32_t iMethod = it->first;
			uint32_t iFunc = it->second;
			pHdr->m_pMethod[iMethod] = ByteOrder::to_le(c.m_Labels.m_Items[iFunc]);
		}

		// Put data segment at the end
		if (c.m_Data.empty())
			pHdr->m_hdrData0 = 0; // irrelevant
		else
		{
			uint32_t nPos = static_cast<uint32_t>(c.m_Result.size());

			Wasm::Word val = c.m_cmplData0 + nSizeHdr - nPos; // overflow is ok
			pHdr->m_hdrData0 = ByteOrder::to_le(val);

			c.m_Result.resize(nPos + c.m_Data.size());
			memcpy(&c.m_Result.front() + nPos, &c.m_Data.front(), c.m_Data.size());
		}

		res = std::move(c.m_Result);
	}

	void TestStackPtr(const Wasm::Compiler::GlobalVar& x)
	{
		Exc::Test(x.CanBeStackPtr());
	}

	bool Processor::Compiler::ResolveBinding(Wasm::Compiler& c, Wasm::Compiler::PerImportFunc& x, Kind kind)
	{
		if (!STR_MATCH(x.m_sMod, "env"))
			return false; // imports from other modules are not supported

		const auto& tp = c.m_Types[x.m_TypeIdx];

#define PAR_TYPECODE(type, name) ParamWrap<type>::s_Code,
#define THE_MACRO(id, ret, name) \
		if (STR_MATCH(x.m_sName, #name)) { \
			x.m_Binding = id; \
			/* verify signature */ \
			const uint8_t pSig[] = { BVMOp_##name(PAR_TYPECODE, MACRO_NOP) 0 }; \
			Exc::Test(tp.m_Args.n == _countof(pSig) - 1); \
			Exc::Test(!memcmp(tp.m_Args.p, pSig, sizeof(pSig) - 1)); \
			ParamWrap<ret>::TestRetType(tp.m_Rets); \
			return true; \
		}

		BVMOpsAll_Common(THE_MACRO)

		if (Kind::Contract == kind)
		{
			BVMOpsAll_Contract(THE_MACRO)
		}

		if (Kind::Manager == kind)
		{
			BVMOpsAll_Manager(THE_MACRO)
		}


#undef THE_MACRO
#undef PAR_TYPECODE

		return false; // not found
	}

	void Processor::Compiler::ResolveBindings(Wasm::Compiler& c, Kind kind)
	{
		Exc::CheckpointTxt cp("Bindings");

		for (uint32_t i = 0; i < c.m_ImportFuncs.size(); i++)
		{
			auto& x = c.m_ImportFuncs[i];

			struct MyCheckpoint
				:public Exc::Checkpoint
			{
				const Wasm::Compiler::PerImportFunc& m_Func;
				MyCheckpoint(const Wasm::Compiler::PerImportFunc& f) :m_Func(f) {}

				void Dump(std::ostream& os) override
				{
					os << m_Func;
				}

			} cp2(x);

			if (ResolveBinding(c, x, kind))
				continue;

			if (Kind::Manager != kind)
				Exc::Fail();

			OnBindingMissing(x);
		}

		bool bStackPtrImported = false;
		for (uint32_t i = 0; i < c.m_ImportGlobals.size(); i++)
		{
			auto& x = c.m_ImportGlobals[i];

			if (STR_MATCH(x.m_sMod, "env"))
			{
				if (STR_MATCH(x.m_sName, "__stack_pointer"))
				{
					TestStackPtr(x);
					x.m_Binding = static_cast<uint32_t>(Wasm::VariableType::StackPointer);
					bStackPtrImported = true;
				}
			}

			// ignore unrecognized variables, they may not be used
		}

		if (!c.m_Globals.empty())
		{
			// we don't support globals, but it could be the stack pointer if the module wasn't built as a "shared" lib.
			Exc::Test(!bStackPtrImported && (1 <= c.m_Globals.size()));

			for (auto& g : c.m_Globals)
			{
				// work-around
				auto& x = c.m_ImportGlobals.emplace_back();
				ZeroObject(x);
				if (!bStackPtrImported && g.CanBeStackPtr())
				{
					x.m_Binding = static_cast<uint32_t>(Wasm::VariableType::StackPointer);
					bStackPtrImported = true;
				}
				else
				{
					x.m_Binding = static_cast<uint32_t>(-1); // invalidate all other globals, we don't support them, compilation fails if we try to use them
				}

				Cast::Down<Wasm::Compiler::GlobalVar>(x) = g;
			}
			c.m_Globals.clear();
		}

	}

} // namespace bvm2
} // namespace beam
