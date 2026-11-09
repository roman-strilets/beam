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

#include "core/crypto/ecc_internal.h"

namespace ECC {

	/////////////////////
	// Key::ID
	void Key::ID::get_Hash(Hash::Value& hv) const
	{
		Hash::Processor()
			<< "kid"
			<< m_Idx
			<< m_Type.V
			<< m_SubIdx
			>> hv;
	}

	void Key::ID::operator = (const Packed& x)
	{
		x.m_Idx.Export(m_Idx);
		x.m_Type.Export(m_Type.V);
		x.m_SubIdx.Export(m_SubIdx);
	}

	void Key::ID::Packed::operator = (const ID& x)
	{
		m_Idx = x.m_Idx;
		m_Type = x.m_Type.V;
		m_SubIdx = x.m_SubIdx;
	}

	void Key::IKdf::DeriveKey(Scalar::Native& out, const Key::ID& kid)
	{
		Hash::Value hv; // the key hash is not secret
		kid.get_Hash(hv);
		DeriveKey(out, hv);
	}

	bool Key::IPKdf::IsSame(IPKdf& x)
	{
		Hash::Value hv = Zero;

		Scalar::Native k0, k1;
		DerivePKey(k0, hv);
		x.DerivePKey(k1, hv);

		k0 += -k1;
		return (k0 == Zero); // not secret, constant-time guarantee isn't requied
	}

	int Key::ID::cmp(const ID& x) const
	{
		if (m_Type < x.m_Type)
			return -1;
		if (m_Type > x.m_Type)
			return 1;
		if (m_SubIdx < x.m_SubIdx)
			return -1;
		if (m_SubIdx > x.m_SubIdx)
			return 1;
		if (m_Idx < x.m_Idx)
			return -1;
		if (m_Idx > x.m_Idx)
			return 1;
		return 0;
	}

	/////////////////////
	// HKdf
	HKdf::Generator::Generator()
	{
		m_Secret.V = Zero;
	}

	void HKdf::Generator::Generate(Scalar::Native& out, const Hash::Value& hv) const
	{
		NonceGenerator("beam-Key")
			<< m_Secret.V
			<< hv
			>> out;
	}

	HKdf::HKdf()
	{
		m_kCoFactor = 1U; // by default
	}

	HKdf::~HKdf()
	{
	}

	void HKdf::Generate(const Hash::Value& hv)
	{
		NonceGenerator nonceGen1("beam-HKdf");
		nonceGen1 << hv;
		NonceGenerator nonceGen2 = nonceGen1;

		nonceGen1.SetContext("gen") >> m_Generator.m_Secret.V;
		nonceGen2.SetContext("coF") >> m_kCoFactor;
	}

	void HKdf::Create(Ptr& pRes, const Hash::Value& hv)
	{
		std::shared_ptr<HKdf> pVal = std::make_shared<HKdf>();
		pVal->Generate(hv);
		pRes = std::move(pVal);
	}

	void HKdf::GenerateChild(Key::IKdf& kdf, Key::Index iKdf)
	{
		Scalar::Native sk;
		kdf.DeriveKey(sk, Key::ID(iKdf, Key::Type::ChildKey));

		NoLeak<Scalar> sk_;
		sk_.V = sk;
		Generate(sk_.V.m_Value);
	}

	void HKdf::GenerateChildParallel(Key::IKdf& kdf, const Hash::Value& hv)
	{
		Scalar::Native sk;
		kdf.DerivePKey(sk, hv);

		NoLeak<Scalar> sk_;
		sk_.V = sk;
		Generate(sk_.V.m_Value);

		m_kCoFactor *= sk;
	}

	void HKdf::CreateChild(Ptr& pRes, Key::IKdf& kdf, Key::Index iKdf)
	{
		std::shared_ptr<HKdf> pVal = std::make_shared<HKdf>();
		pVal->GenerateChild(kdf, iKdf);
		pRes = std::move(pVal);
	}

	void HKdf::DeriveKey(Scalar::Native& out, const Hash::Value& hv)
	{
		m_Generator.Generate(out, hv);
		out *= m_kCoFactor;
	}

	void HKdf::DerivePKey(Scalar::Native& out, const Hash::Value& hv)
	{
		m_Generator.Generate(out, hv);
	}

	void Key::IKdf::DerivePKeyG(Point::Native& out, const Hash::Value& hv)
	{
		Scalar::Native sk;
		DeriveKey(sk, hv);
		out = Context::get().G * sk;
	}

	void Key::IKdf::DerivePKeyJ(Point::Native& out, const Hash::Value& hv)
	{
		Scalar::Native sk;
		DeriveKey(sk, hv);
		out = Context::get().J * sk;
	}

	HKdfPub::HKdfPub()
	{
	}

	HKdfPub::~HKdfPub()
	{
	}

	void HKdfPub::DerivePKey(Scalar::Native& out, const Hash::Value& hv)
	{
		m_Generator.Generate(out, hv);
	}

	void HKdfPub::DerivePKeyG(Point::Native& out, const Hash::Value& hv)
	{
		Scalar::Native sk;
		m_Generator.Generate(sk, hv);
		out = m_PkG * sk;
	}

	void HKdfPub::DerivePKeyJ(Point::Native& out, const Hash::Value& hv)
	{
		Scalar::Native sk;
		m_Generator.Generate(sk, hv);
		out = m_PkJ * sk;
	}

	uint32_t HKdf::ExportP(void* p) const
	{
		HKdfPub pkdf;
		if (p)
			pkdf.GenerateFrom(*this);
		return pkdf.ExportP(p);
	}

	uint32_t HKdf::ExportS(void* p) const
	{
		if (p)
			Export(*reinterpret_cast<Packed*>(p));
		return sizeof(Packed);
	}

	void HKdf::Export(Packed& v) const
	{
		v.m_Secret = m_Generator.m_Secret.V;
		v.m_kCoFactor = m_kCoFactor;
	}

	bool HKdf::Import(const Packed& v)
	{
		m_Generator.m_Secret.V = v.m_Secret;
		return !m_kCoFactor.Import(v.m_kCoFactor);
	}

	uint32_t HKdfPub::ExportP(void* p) const
	{
		if (p)
			Export(*reinterpret_cast<Packed*>(p));
		return sizeof(Packed);
	}

	void HKdfPub::Export(Packed& v) const
	{
		v.m_Secret = m_Generator.m_Secret.V;
		v.m_PkG = m_PkG;
		v.m_PkJ = m_PkJ;
	}

	bool HKdfPub::Import(const Packed& v)
	{
		m_Generator.m_Secret.V = v.m_Secret;
		return
			m_PkG.ImportNnz(v.m_PkG) &&
			m_PkJ.ImportNnz(v.m_PkJ);
	}

	void HKdfPub::GenerateFrom(const HKdf& v)
	{
		m_Generator = v.m_Generator;
		m_PkG = Context::get().G * v.m_kCoFactor;
		m_PkJ = Context::get().J * v.m_kCoFactor;
	}

	void HKdfPub::GenerateChildParallel(Key::IPKdf& kdf, const Hash::Value& hv)
	{
		Scalar::Native sk;
		kdf.DerivePKey(sk, hv);

		NoLeak<Scalar> sk_;
		sk_.V = sk;

		HKdf hkdf;
		hkdf.Generate(sk_.V.m_Value);
		GenerateFrom(hkdf);

		m_PkG = m_PkG * sk;
		m_PkJ = m_PkJ * sk;
	}

} // namespace ECC
