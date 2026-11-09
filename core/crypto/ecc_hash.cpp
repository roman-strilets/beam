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
	// Hash
	Hash::Processor::Processor()
	{
		Reset();
	}

	Hash::Processor::~Processor()
	{
		if (m_bInitialized)
			SecureErase(*this);
	}

	void Hash::Processor::Reset()
	{
		secp256k1_sha256_initialize(this);
		m_bInitialized = true;
	}

	void Hash::Processor::Reset(beam::Uninitialized_)
	{
		if (m_bInitialized)
		{
			SecureErase(*this);
			m_bInitialized = false;
		}
	}

	void Hash::Processor::Write(const void* p, uint32_t n)
	{
		assert(m_bInitialized);
		secp256k1_sha256_write(this, (const uint8_t*) p, n);
	}

	void Hash::Processor::Finalize(Value& v)
	{
		assert(m_bInitialized);
		secp256k1_sha256_finalize(this, v.m_pData);
		
		m_bInitialized = false;
	}

	void Hash::Processor::FinalizeTruncated(uint8_t* p, uint32_t nSize)
	{
		assert(nSize < Value::nBytes);

		Value hv;
		Finalize(hv);

		memcpy(p, hv.m_pData, nSize);
	}

	void Hash::Processor::Write(const beam::Blob& v)
	{
		Write(v.p, v.n);
	}

	void Hash::Processor::Write(bool b)
	{
		uint8_t n = (false != b);
		Write(n);
	}

	void Hash::Processor::Write(uint8_t n)
	{
		Write(&n, sizeof(n));
	}

	void Hash::Processor::Write(const Scalar& v)
	{
		Write(v.m_Value);
	}

	void Hash::Processor::Write(const Scalar::Native& v)
	{
		NoLeak<Scalar> s_;
		s_.V = v;
		Write(s_.V);
	}

	void Hash::Processor::Write(const Point& v)
	{
		Write(v.m_X);
		Write(v.m_Y);
	}

	void Hash::Processor::Write(const Point::Native& v)
	{
		Write(Point(v));
	}

	void Hash::Mac::Reset(const void* pSecret, uint32_t nSecret)
	{
		secp256k1_hmac_sha256_initialize(this, (uint8_t*)pSecret, nSecret);
	}

	void Hash::Mac::Write(const void* p, uint32_t n)
	{
		secp256k1_hmac_sha256_write(this, (uint8_t*)p, n);
	}

	void Hash::Mac::Finalize(Value& hv)
	{
		secp256k1_hmac_sha256_finalize(this, hv.m_pData);
	}

	/////////////////////
	// NonceGenerator
	void NonceGenerator::Reset()
	{
		ZeroObject(m_Context);
		m_bFirstTime = true;
		m_Counter = Zero;
	}

	void NonceGenerator::WriteIkm(const beam::Blob& b)
	{
		assert(m_bFirstTime);
		m_HMac.Write(b.p, b.n);
	}

	const Hash::Value& NonceGenerator::get_Okm()
	{
		if (m_bFirstTime)
			// Extract
			m_HMac >> m_Prk;

		// Expand
		m_HMac.Reset(m_Prk.m_pData, m_Prk.nBytes);

		if (m_bFirstTime)
			m_bFirstTime = false;
		else
			m_HMac.Write(m_Okm.m_pData, m_Okm.nBytes);

		m_HMac.Write(m_Context.p, m_Context.n);

		m_Counter.Inc();
		m_HMac.Write(m_Counter.m_pData, m_Counter.nBytes);

		m_HMac >> m_Okm;
		return m_Okm;
	}

	NonceGenerator& NonceGenerator::operator >> (Hash::Value& hv)
	{
		hv = get_Okm();
		return *this;
	}

	NonceGenerator& NonceGenerator::operator >> (Scalar::Native& sk)
	{
		static_assert(sizeof(Scalar) == sizeof(m_Okm), "");
		const Scalar& s = reinterpret_cast<const Scalar&>(m_Okm);

		do
			get_Okm();
		while (!sk.ImportNnz(s));

		return *this;
	}

	/////////////////////
	// Oracle
	void Oracle::Reset()
	{
		m_hp.Reset();
	}

	void Oracle::operator >> (Hash::Value& out)
	{
		Hash::Processor(m_hp) >> out;
		operator << (out);
	}

	void Oracle::operator >> (Scalar::Native& out)
	{
		NoLeak<Scalar> s;

		do
			operator >> (s.V.m_Value);
		while (!out.ImportNnz(s.V));
	}

} // namespace ECC
