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

// Internal header shared by the bvm2_*.cpp sources: parameter marshalling,
// the ProcessorPlus wrappers that implement the BVM methods, and the
// BVM_METHOD macros used to define them.
#pragma once
#define _CRT_SECURE_NO_WARNINGS // sprintf
#include "bvm2_impl.h"
#include <sstream>
#include <iomanip>
#include <string>
#include <string_view>
#include <algorithm>

namespace beam {
namespace bvm2 {

	/////////////////////////////////////////////
	// Compiled module header
#pragma pack (push, 1)
	struct Processor::Header
	{
		static const uint32_t s_Version = 2;

		uint32_t m_Version;
		uint32_t m_NumMethods;
		uint32_t m_hdrData0;
		uint32_t m_hdrTable0;

		static const uint32_t s_MethodsMin = 2; // c'tor and d'tor
		static const uint32_t s_MethodsMax = 1U << 28; // would be much much less of course, this is just to ensure no-overflow for offset calculation

		uint32_t m_pMethod[s_MethodsMin]; // var size
	};
#pragma pack (pop)

	/////////////////////////////////////////////
	// Redirection of calls

	template <typename T>
	struct ParamWrap {
		typedef T Type;
		Type V;
		Wasm::Processor& operator = (Wasm::Processor& p) {
			V = p.m_Stack.Pop<Type>();
			return p;
		}

		void Set(Type v) {
			V = v;
		}

		static_assert(std::numeric_limits<T>::is_integer);
		static_assert(sizeof(T) <= sizeof(Wasm::Word) * 2);

		static const uint8_t s_Code = sizeof(T) <= sizeof(Wasm::Word) ?
			Wasm::TypeCode::i32 :
			Wasm::TypeCode::i64;

		static void TestRetType(const Wasm::Compiler::Vec<uint8_t>& v) {
			Exc::Test((1 == v.n) && (s_Code == *v.p));
		}
	};

	template <typename T>
	struct ParamWrap<T*>
		:public ParamWrap<Wasm::Word>
	{
		Wasm::Processor& operator = (Wasm::Processor& p) {
			return ParamWrap<Wasm::Word>::operator = (p);
		}
	};

	template <typename T>
	struct ParamWrap<T&>
		:public ParamWrap<Wasm::Word>
	{
		Wasm::Processor& operator = (Wasm::Processor& p) {
			return ParamWrap<Wasm::Word>::operator = (p);
		}
	};

	template <>
	struct ParamWrap<void>
	{
		typedef void Type;

		static void TestRetType(const Wasm::Compiler::Vec<uint8_t>& v) {
			Exc::Test(!v.n);
		}
	};

	template <typename TRes> struct Caller {
		template <typename TArgs, typename TProcessor>
		static void Call(TProcessor& me, const TArgs& args) {
			auto ret = args.Call(me);
			if (!me.IsSuspended())
				me.m_Stack.template Push<TRes>(ret);
		}
	};
	template <> struct Caller<void> {
		template <typename TArgs, typename TProcessor>
		static void Call(TProcessor& me, const TArgs& args) {
			args.Call(me);
		}
	};


#define MACRO_NOP
#define MACRO_COMMA ,
#define PAR_DECL(type, name) ParamWrap<type>::Type name
#define THE_MACRO(id, ret, name) \
		typedef ParamWrap<ret>::Type RetType_##name; \
		typedef ret RetTypeHost_##name; \
		RetType_##name OnMethod_##name(BVMOp_##name(PAR_DECL, MACRO_COMMA));

	typedef ECC::Point PubKey;
	typedef Asset::ID AssetID;

	struct ProcessorPlus
		:public ProcessorPlusEnv
	{
		typedef ProcessorPlus TProcessor;
		static TProcessor& From(Processor& p)
		{
			static_assert(sizeof(TProcessor) == sizeof(p));
			return Cast::Up<TProcessor>(p);
		}

		template <typename TOut, typename TIn>
		TOut ToHost(TIn x) { return x; }

		template <typename TOut, typename TIn>
		TOut ToMethod(TIn x) { return x; }

		void InvokeExtPlus(uint32_t nBinding);

		static uint32_t CvtAssetInfo(AssetInfo& res, const Asset::Info&, void* pMetadata, uint32_t nMetadata);

		BVMOpsAll_Common(THE_MACRO)
	};

	// Explicit specializations, defined in the corresponding source files
	template<> const char* ProcessorPlus::ToHost<const char*, Wasm::Word>(Wasm::Word sz); // bvm2_methods_common.cpp
	template<> Wasm::Word ProcessorPlus::ToMethod<Wasm::Word, HashObj*>(HashObj* p); // bvm2_hash.cpp
	template<> Wasm::Word ProcessorPlus::ToMethod<Wasm::Word, const Secp_scalar&>(const Secp_scalar& s); // bvm2_secp.cpp
	template<> Wasm::Word ProcessorPlus::ToMethod<Wasm::Word, Secp_scalar&>(Secp_scalar& s); // bvm2_secp.cpp
	template<> Wasm::Word ProcessorPlus::ToMethod<Wasm::Word, const Secp_point&>(const Secp_point& p); // bvm2_secp.cpp
	template<> Wasm::Word ProcessorPlus::ToMethod<Wasm::Word, Secp_point&>(Secp_point& p); // bvm2_secp.cpp

	struct ProcessorPlus_Contract
		:public ProcessorPlusEnv_Contract
	{
		typedef ProcessorPlus_Contract TProcessor;
		static TProcessor& From(ProcessorContract& p)
		{
			static_assert(sizeof(TProcessor) == sizeof(p));
			return Cast::Up<TProcessor>(p);
		}

		void InvokeExtPlus(uint32_t nBinding);
		BVMOpsAll_Contract(THE_MACRO)
	};

	struct ProcessorPlus_Manager
		:public ProcessorPlusEnv_Manager
	{
		typedef ProcessorPlus_Manager TProcessor;
		static TProcessor& From(ProcessorManager& p)
		{
			static_assert(sizeof(TProcessor) == sizeof(p));
			return Cast::Up<TProcessor>(p);
		}

		void InvokeExtPlus(uint32_t nBinding);
		BVMOpsAll_Manager(THE_MACRO)
	};

#undef THE_MACRO
#undef PAR_DECL

	/////////////////////////////////////////////
	// Methods
	namespace ProcessorFromMethod {
#define THE_MACRO(id, ret, name) typedef ProcessorPlus name##_Type; typedef ProcessorPlusEnv name##_TypeEnv;
		BVMOpsAll_Common(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(id, ret, name) typedef ProcessorPlus_Contract name##_Type; typedef ProcessorPlusEnv_Contract name##_TypeEnv;
		BVMOpsAll_Contract(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(id, ret, name) typedef ProcessorPlus_Manager name##_Type; typedef ProcessorPlusEnv_Manager name##_TypeEnv;
		BVMOpsAll_Manager(THE_MACRO)
#undef THE_MACRO
	}

#define BVM_METHOD_PAR_DECL(type, name) ParamWrap<type>::Type name
#define BVM_METHOD_PAR_DECL_HOST(type, name) type name
#define BVM_METHOD_PAR_PASS_TO_METHOD(type, name) ProcessorPlus::From(*this).ToMethod<ParamWrap<type>::Type, type>(name)
#define BVM_METHOD_PAR_PASS_TO_HOST(type, name) ProcessorPlus::From(*this).ToHost<type>(name)
#define BVM_METHOD(name) ProcessorFromMethod::name##_Type::RetType_##name ProcessorFromMethod::name##_Type::OnMethod_##name(BVMOp_##name(BVM_METHOD_PAR_DECL, MACRO_COMMA))
#define BVM_METHOD_HOST(name) ProcessorFromMethod::name##_Type::RetTypeHost_##name ProcessorFromMethod::name##_TypeEnv::OnHost_##name(BVMOp_##name(BVM_METHOD_PAR_DECL_HOST, MACRO_COMMA))
#define BVM_METHOD_AUTO_INVOKE(name) ProcessorFromMethod::name##_Type::From(*this).OnMethod_##name(BVMOp_##name(BVM_METHOD_PAR_PASS_TO_METHOD, MACRO_COMMA));
#define BVM_METHOD_HOST_AUTO(name) BVM_METHOD_HOST(name)  { return BVM_METHOD_AUTO_INVOKE(name); }

#define BVM_METHOD_VIA_HOST(name) BVM_METHOD(name) { return OnHost_##name(BVMOp_##name(BVM_METHOD_PAR_PASS_TO_HOST, MACRO_COMMA)); }

} // namespace bvm2
} // namespace beam
