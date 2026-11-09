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
	// Secp
	uint32_t Processor::Secp::Scalar::From(const Secp_scalar& s)
	{
		return static_cast<uint32_t>(reinterpret_cast<size_t>(&s));
	}

	template<> Wasm::Word ProcessorPlus::ToMethod<Wasm::Word, const Secp_scalar&>(const Secp_scalar& s) {
		return Secp::Scalar::From(s);
	}
	template<> Wasm::Word ProcessorPlus::ToMethod<Wasm::Word, Secp_scalar&>(Secp_scalar& s) {
		return Secp::Scalar::From(s);
	}

	Processor::Secp::Scalar::Item& Processor::Secp::Scalar::FindStrict(uint32_t key)
	{
		auto it = m_Map.find(key, Item::Comparator());
		Exc::Test(m_Map.end() != it);
		return *it;
	}

	BVM_METHOD(Secp_Scalar_alloc)
	{
		auto& x = m_Secp.m_Scalar.m_Map;

		if ((Kind::Contract == get_Kind()) && (x.size() >= Limits::SecScalars))
			return 0;

		uint32_t ret = x.empty() ? 1 : (x.rbegin()->m_Key + 1);
		x.Create(ret);
		return ret;
	}

	BVM_METHOD_HOST(Secp_Scalar_alloc)
	{
		uint32_t val = ProcessorPlus::From(*this).OnMethod_Secp_Scalar_alloc();
		return reinterpret_cast<Secp_scalar*>((size_t) val);
	}

	BVM_METHOD(Secp_Scalar_free)
	{
		m_Secp.m_Scalar.m_Map.Delete(m_Secp.m_Scalar.FindStrict(s));
	}
	BVM_METHOD_HOST_AUTO(Secp_Scalar_free)

	BVM_METHOD(Secp_Scalar_import)
	{
		return !m_Secp.m_Scalar.FindStrict(s).m_Val.Import(get_AddrAsR<ECC::Scalar>(data));
	}

	BVM_METHOD_HOST(Secp_Scalar_import)
	{
		return !m_Secp.m_Scalar.FindStrict(Secp::Scalar::From(s)).m_Val.Import(data);
	}

	BVM_METHOD(Secp_Scalar_export)
	{
		return m_Secp.m_Scalar.FindStrict(s).m_Val.Export(get_AddrAsW<ECC::Scalar>(data));
	}

	BVM_METHOD_HOST(Secp_Scalar_export)
	{
		return m_Secp.m_Scalar.FindStrict(Secp::Scalar::From(s)).m_Val.Export(data);
	}

	BVM_METHOD(Secp_Scalar_neg)
	{
		m_Secp.m_Scalar.FindStrict(dst).m_Val = -m_Secp.m_Scalar.FindStrict(src).m_Val;
	}
	BVM_METHOD_HOST_AUTO(Secp_Scalar_neg)

	BVM_METHOD(Secp_Scalar_add)
	{
		m_Secp.m_Scalar.FindStrict(dst).m_Val =
			m_Secp.m_Scalar.FindStrict(a).m_Val +
			m_Secp.m_Scalar.FindStrict(b).m_Val;
	}
	BVM_METHOD_HOST_AUTO(Secp_Scalar_add)

	BVM_METHOD(Secp_Scalar_mul)
	{
		m_Secp.m_Scalar.FindStrict(dst).m_Val =
			m_Secp.m_Scalar.FindStrict(a).m_Val *
			m_Secp.m_Scalar.FindStrict(b).m_Val;
	}
	BVM_METHOD_HOST_AUTO(Secp_Scalar_mul)

	BVM_METHOD(Secp_Scalar_inv)
	{
		DischargeUnits(Limits::Cost::Secp_ScalarInv);
		m_Secp.m_Scalar.FindStrict(dst).m_Val.SetInv(m_Secp.m_Scalar.FindStrict(src).m_Val);
	}
	BVM_METHOD_HOST_AUTO(Secp_Scalar_inv)

	BVM_METHOD(Secp_Scalar_set)
	{
		m_Secp.m_Scalar.FindStrict(dst).m_Val = val;
	}
	BVM_METHOD_HOST_AUTO(Secp_Scalar_set)


	uint32_t Processor::Secp::Point::From(const Secp_point& p)
	{
		return static_cast<uint32_t>(reinterpret_cast<size_t>(&p));
	}

	template<> Wasm::Word ProcessorPlus::ToMethod<Wasm::Word, const Secp_point&>(const Secp_point& p) {
		return Secp::Point::From(p);
	}
	template<> Wasm::Word ProcessorPlus::ToMethod<Wasm::Word, Secp_point&>(Secp_point& p) {
		return Secp::Point::From(p);
	}

	Processor::Secp::Point::Item& Processor::Secp::Point::FindStrict(uint32_t key)
	{
		auto it = m_Map.find(key, Item::Comparator());
		Exc::Test(m_Map.end() != it);
		return *it;
	}

	BVM_METHOD(Secp_Point_alloc)
	{
		auto& x = m_Secp.m_Point.m_Map;

		if ((Kind::Contract == get_Kind()) && (x.size() >= Limits::SecPoints))
			return 0;

		uint32_t ret = x.empty() ? 1 : (x.rbegin()->m_Key + 1);
		x.Create(ret);
		return ret;
	}

	BVM_METHOD_HOST(Secp_Point_alloc)
	{
		uint32_t val = ProcessorPlus::From(*this).OnMethod_Secp_Point_alloc();
		return reinterpret_cast<Secp_point*>((size_t) val);
	}

	BVM_METHOD(Secp_Point_free)
	{
		m_Secp.m_Point.m_Map.Delete(m_Secp.m_Point.FindStrict(p));
	}
	BVM_METHOD_HOST_AUTO(Secp_Point_free)

	BVM_METHOD(Secp_Point_Import)
	{
		DischargeUnits(Limits::Cost::Secp_Point_Import);
		return !!m_Secp.m_Point.FindStrict(p).m_Val.Import(get_AddrAsR<ECC::Point>(pk));
	}

	BVM_METHOD_HOST(Secp_Point_Import)
	{
		return !!m_Secp.m_Point.FindStrict(Secp::Point::From(p)).m_Val.Import(pk);
	}

	BVM_METHOD(Secp_Point_Export)
	{
		DischargeUnits(Limits::Cost::Secp_Point_Export);
		m_Secp.m_Point.FindStrict(p).m_Val.Export(get_AddrAsW<ECC::Point>(pk));
	}

	BVM_METHOD_HOST(Secp_Point_Export)
	{
		m_Secp.m_Point.FindStrict(Secp::Point::From(p)).m_Val.Export(pk);
	}

	BVM_METHOD(Secp_Point_ExportEx)
	{
		if (Kind::Contract == get_Kind())
			Exc::Test(IsPastFork_<4>());

		DischargeUnits(Limits::Cost::Secp_Point_Export);
		m_Secp.m_Point.FindStrict(p).m_Val.Export(get_AddrAsW<ECC::Point::Storage>(res));
	}

	BVM_METHOD_HOST(Secp_Point_ExportEx)
	{
		m_Secp.m_Point.FindStrict(Secp::Point::From(p)).m_Val.Export(res);
	}

	BVM_METHOD(Secp_Point_neg)
	{
		m_Secp.m_Point.FindStrict(dst).m_Val = -m_Secp.m_Point.FindStrict(src).m_Val;
	}
	BVM_METHOD_HOST_AUTO(Secp_Point_neg)

	BVM_METHOD(Secp_Point_add)
	{
		m_Secp.m_Point.FindStrict(dst).m_Val =
			m_Secp.m_Point.FindStrict(a).m_Val +
			m_Secp.m_Point.FindStrict(b).m_Val;
	}
	BVM_METHOD_HOST_AUTO(Secp_Point_add)

	BVM_METHOD(Secp_Point_mul)
	{
		DischargeUnits(Limits::Cost::Secp_Point_Multiply);

		ECC::Mode::Scope mode(ECC::Mode::Fast);

		m_Secp.m_Point.FindStrict(dst).m_Val =
			m_Secp.m_Point.FindStrict(p).m_Val *
			m_Secp.m_Scalar.FindStrict(s).m_Val;
	}
	BVM_METHOD_HOST_AUTO(Secp_Point_mul)

	BVM_METHOD(Secp_Point_IsZero)
	{
		bool bRet = m_Secp.m_Point.FindStrict(p).m_Val == Zero;
		return !!bRet;
	}
	BVM_METHOD_HOST_AUTO(Secp_Point_IsZero)

	BVM_METHOD(Secp_Point_mul_G)
	{
		DischargeUnits(Limits::Cost::Secp_Point_Multiply);

		ECC::Mode::Scope mode(ECC::Mode::Fast);

		m_Secp.m_Point.FindStrict(dst).m_Val =
			ECC::Context::get().G *
			m_Secp.m_Scalar.FindStrict(s).m_Val;
	}
	BVM_METHOD_HOST_AUTO(Secp_Point_mul_G)

	BVM_METHOD(Secp_Point_mul_J)
	{
		DischargeUnits(Limits::Cost::Secp_Point_Multiply);

		ECC::Mode::Scope mode(ECC::Mode::Fast);

		m_Secp.m_Point.FindStrict(dst).m_Val =
			ECC::Context::get().J *
			m_Secp.m_Scalar.FindStrict(s).m_Val;
	}
	BVM_METHOD_HOST_AUTO(Secp_Point_mul_J)

	BVM_METHOD(Secp_Point_mul_H)
	{
		DischargeUnits(Limits::Cost::Secp_Point_Multiply);

		ECC::Mode::Scope mode(ECC::Mode::Fast);

		auto& dst_ = m_Secp.m_Point.FindStrict(dst).m_Val;
		auto& s_ = m_Secp.m_Scalar.FindStrict(s).m_Val;

		CoinID::Generator gen(aid);
		if (gen.m_hGen == Zero)
			dst_ = ECC::Context::get().H * s_;
		else
			dst_ = gen.m_hGen * s_;
	}
	BVM_METHOD_HOST_AUTO(Secp_Point_mul_H)

} // namespace bvm2
} // namespace beam
