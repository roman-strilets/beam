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
	// Scalar
	bool Scalar::IsValid() const
	{
		return m_Value < s_Order;
	}

	void Scalar::TestValid() const
	{
		if (!IsValid())
			throw std::runtime_error("invalid scalar");
	}

	Scalar& Scalar::operator = (const Native& v)
	{
		v.Export(*this);
		return *this;
	}

	Scalar& Scalar::operator = (const Zero_&)
	{
		m_Value = Zero;
		return *this;
	}

	std::string Scalar::str() const
	{
		return m_Value.str();
	}

	Scalar::Native::Native()
    {
        secp256k1_scalar_clear(this);
    }

	Scalar::Native& Scalar::Native::operator = (Zero_)
	{
		secp256k1_scalar_clear(this);
		return *this;
	}

	bool Scalar::Native::operator == (Zero_) const
	{
		return secp256k1_scalar_is_zero(this) != 0; // constant time guaranteed
	}

    bool Scalar::Native::operator != (Zero_ v) const
    {
        return !(operator == (v));
    }

	bool Scalar::Native::operator == (const Native& v) const
	{
		// Used in tests only, but implemented with constant mem-time guarantee
		Native x = - *this;
		x += v;
		return x == Zero;
	}

    bool Scalar::Native::operator != (const Native& v) const
    {
        return !(operator == (v));
    }

	Scalar::Native& Scalar::Native::operator = (Minus v)
	{
		secp256k1_scalar_negate(this, &v.x);
		return *this;
	}

	bool Scalar::Native::Import(const Scalar& v)
	{
		int overflow;
		secp256k1_scalar_set_b32(this, v.m_Value.m_pData, &overflow);
		return overflow != 0;
	}

	bool Scalar::Native::ImportNnz(const Scalar& v)
	{
		return
			!Import(v) &&
			!(*this == Zero);

	}

	void Scalar::Native::GenRandomNnz()
	{
		NoLeak<Scalar> s;
		do
			GenRandom(s.V.m_Value);
		while (!ImportNnz(s.V));
	}

	Scalar::Native& Scalar::Native::operator = (const Scalar& v)
	{
		Import(v);
		return *this;
	}

	void Scalar::Native::Export(Scalar& v) const
	{
		secp256k1_scalar_get_b32(v.m_Value.m_pData, this);
	}

	Scalar::Native& Scalar::Native::operator = (uint32_t v)
	{
		secp256k1_scalar_set_int(this, v);
		return *this;
	}

	Scalar::Native& Scalar::Native::operator = (uint64_t v)
	{
#ifdef SECP256K1_WIDEMUL_INT128
		static_assert(sizeof(d[0]) == sizeof(v), "");
		d[0] = v;
#else // SECP256K1_WIDEMUL_INT128
		static_assert(sizeof(d[0]) * 2 == sizeof(v), "");
		d[0] = static_cast<uint32_t>(v);
		d[1] = static_cast<uint32_t>(v >> 32u);
#endif // SECP256K1_WIDEMUL_INT128

		memset0(d + sizeof(v) / sizeof(d[0]), sizeof(d) - sizeof(v));
		return *this;
	}

	Scalar::Native& Scalar::Native::operator = (Plus v)
	{
		secp256k1_scalar_add(this, &v.x, &v.y);
		return *this;
	}

    Scalar::Native& Scalar::Native::operator = (Minus2 v)
    {
        Scalar::Native temp = -v.y;
        secp256k1_scalar_add(this, &v.x, &temp);
        return *this;
    }

	Scalar::Native& Scalar::Native::operator = (Mul v)
	{
		secp256k1_scalar_mul(this, &v.x, &v.y);
		return *this;
	}

	void Scalar::Native::SetInv(const Native& v)
	{
		secp256k1_scalar_inverse(this, &v);
	}

	void Scalar::Native::Inv()
	{
		SetInv(*this);
	}

	/////////////////////
	// ScalarGenerator
	void ScalarGenerator::Initialize(const Scalar::Native& x)
	{
		Scalar::Native pos = x;
		for (uint32_t iLevel = 0; ; )
		{
			PerLevel& lev = m_pLevel[iLevel];

			lev.m_pVal[0] = pos;
			for (uint32_t i = 1; i < _countof(lev.m_pVal); i++)
				lev.m_pVal[i] = lev.m_pVal[i - 1] * pos;

			if (++iLevel == nLevels)
				break;

			for (uint32_t i = 0; i < nBitsPerLevel; i++)
				secp256k1_scalar_mul(&pos.get_Raw(), &pos.get(), &pos.get());
		}
	}

	void ScalarGenerator::Calculate(Scalar::Native& trg, const Scalar& pwr) const
	{
		trg = 1U;

		const uint32_t nWordBits = sizeof(pwr.m_Value.m_pData[0]) << 3;
		const uint32_t nLevelsPerWord = nWordBits / nBitsPerLevel;
		static_assert(nLevelsPerWord * nBitsPerLevel == nWordBits, "");

		uint32_t iLevel = 0;
		for (uint32_t iWord = _countof(pwr.m_Value.m_pData); iWord--; )
		{
			unsigned int val = pwr.m_Value.m_pData[iWord];

			for (uint32_t j = 0; ; )
			{
				const PerLevel& lev = m_pLevel[iLevel++];

				unsigned int idx = val & _countof(lev.m_pVal);
				if (idx)
					trg *= lev.m_pVal[idx - 1];

				if (++j == nLevelsPerWord)
					break;

				val >>= nBitsPerLevel;
			}
		}
	}

} // namespace ECC
