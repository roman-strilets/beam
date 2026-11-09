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
	// Point
	int Point::cmp(const Point& v) const
	{
		int n = m_X.cmp(v.m_X);
		if (n)
			return n;

		if (m_Y < v.m_Y)
			return -1;
		if (m_Y > v.m_Y)
			return 1;

		return 0;
	}

	Point& Point::operator = (const Native& v)
	{
		v.Export(*this);
		return *this;
	}

	Point& Point::operator = (const Point& v)
	{
		m_X = v.m_X;
		m_Y = v.m_Y;
		return *this;
	}

	Point& Point::operator = (const Commitment& v)
	{
		return operator = (Native(v));
	}

	Point::Native::Native()
    {
        secp256k1_gej_set_infinity(this);
    }

	bool Point::Native::ImportNnz(const Point& v, Storage* pS /* = nullptr */)
	{
		if (v.m_Y > 1)
			return false; // should always be well-formed

		NoLeak<secp256k1_fe> nx;
		if (!secp256k1_fe_set_b32_limit(&nx.V, v.m_X.m_pData))
			return false;

		NoLeak<secp256k1_ge> ge;
		if (!secp256k1_ge_set_xo_var(&ge.V, &nx.V, v.m_Y))
			return false;

		secp256k1_gej_set_ge(this, &ge.V);

		if (pS)
			pS->FromNnz(ge.V);

		return true;
	}

	void Point::Storage::FromNnz(secp256k1_ge& ge)
	{
		// normalization seems unnecessary, but it's minor
		secp256k1_fe_normalize(&ge.x);
		secp256k1_fe_normalize(&ge.y);

		secp256k1_fe_get_b32(m_X.m_pData, &ge.x);
		secp256k1_fe_get_b32(m_Y.m_pData, &ge.y);
	}

	bool Point::Native::Import(const Point& v, Storage* pS /* = nullptr */)
	{
		if (ImportNnz(v, pS))
			return true;

		*this = Zero;
		if (pS)
			ZeroObject(*pS);

		return memis0(&v, sizeof(v));
	}

	void Point::Native::Fail()
	{
		beam::Exc::Fail("Bad EC Point");
	}

	void Point::Native::ExportNnz(secp256k1_ge& ge) const
	{
		NoLeak<secp256k1_gej> dup;
		dup.V = *this;
		secp256k1_ge_set_gej(&ge, &dup.V);
	}

	bool Point::Native::Export(Point& v) const
	{
		if (*this == Zero)
		{
			ZeroObject(v);
			return false;
		}

		NoLeak<secp256k1_ge> ge;
		ExportNnz(ge.V);

		// seems like normalization can be omitted (already done by secp256k1_ge_set_gej), but not guaranteed according to docs.
		// But this has a negligible impact on the performance
		secp256k1_fe_normalize(&ge.V.x);
		secp256k1_fe_normalize(&ge.V.y);

		ExportEx(v, ge.V);

		return true;
	}

	void Point::Native::ExportEx(Point& v, const secp256k1_ge& ge)
	{
		secp256k1_fe_get_b32(v.m_X.m_pData, &ge.x);
		v.m_Y = (secp256k1_fe_is_odd(&ge.y) != 0);
	}

	void Point::Native::Export(Storage& v) const
	{
		if (*this == Zero)
			ZeroObject(v);
		else
		{
			secp256k1_ge ge;
			ExportNnz(ge);
			v.FromNnz(ge);
		}
	}

	bool Point::Native::Import(const Storage& v, bool bVerify)
	{
		if (memis0(&v, sizeof(v)))
			*this = Zero;
		else
		{
			secp256k1_ge ge;
			ZeroObject(ge);

			secp256k1_fe_set_b32_mod(&ge.x, v.m_X.m_pData);
			secp256k1_fe_set_b32_mod(&ge.y, v.m_Y.m_pData);

			if (bVerify && !secp256k1_ge_is_valid_var(&ge))
			{
				*this = Zero;
				return false;
			}

			secp256k1_gej_set_ge(this, &ge);
		}

		return true;
	}

	void Point::Native::BatchNormalizer::Normalize()
	{
		secp256k1_fe zDenom;
		NormalizeInternal(zDenom, true);
	}

	void Point::Native::BatchNormalizer::ToCommonDenominator(secp256k1_fe& zDenom)
	{
		secp256k1_fe_set_int(&zDenom, 1);
		NormalizeInternal(zDenom, false);
	}

	void secp256k1_gej_rescale_XY(secp256k1_gej& gej, const secp256k1_fe& z)
	{
		// equivalent of secp256k1_gej_rescale, but doesn't change z coordinate
		// A bit more effective when the value of z is know in advance (such as when normalizing)
		secp256k1_fe zz;
		secp256k1_fe_sqr(&zz, &z);

		secp256k1_fe_mul(&gej.x, &gej.x, &zz);
		secp256k1_fe_mul(&gej.y, &gej.y, &zz);
		secp256k1_fe_mul(&gej.y, &gej.y, &z);
	}

	void Point::Native::BatchNormalizer::NormalizeInternal(secp256k1_fe& zDenom, bool bNormalize)
	{
		bool bEmpty = true;
		Element elPrev = { 0 }; // init not necessary, just suppress the warning
		Element el = { 0 };

		for (Reset(); ; )
		{
			if (!MoveNext(el))
				break;

			if (bEmpty)
			{
				bEmpty = false;
				*el.m_pFe = el.m_pPoint->z;
			}
			else
				secp256k1_fe_mul(el.m_pFe, elPrev.m_pFe, &el.m_pPoint->z);

			elPrev = el;
		}

		if (bEmpty)
			return;

		if (bNormalize)
			secp256k1_fe_inv(&zDenom, elPrev.m_pFe); // the only expensive call

		while (true)
		{
			bool bFetched = MovePrev(el);
			if (bFetched) 
				secp256k1_fe_mul(elPrev.m_pFe, el.m_pFe, &zDenom);
			else
				*elPrev.m_pFe = zDenom;

			secp256k1_fe_mul(&zDenom, &zDenom, &elPrev.m_pPoint->z);

			secp256k1_gej_rescale_XY(*elPrev.m_pPoint, *elPrev.m_pFe);
			elPrev.m_pPoint->z = zDenom;

			if (!bFetched)
				break;

			elPrev = el;
		}
	}

	void Point::Native::BatchNormalizer::get_As(secp256k1_ge& ge, const Point::Native& ptNormalized)
	{
		ge.x = ptNormalized.x;
		ge.y = ptNormalized.y;
		ge.infinity = ptNormalized.infinity;
	}

	void Point::Native::BatchNormalizer::get_As(secp256k1_ge_storage& ge_s, const Point::Native& ptNormalized)
	{
		secp256k1_ge ge;
		get_As(ge, ptNormalized);
		secp256k1_ge_to_storage(&ge_s, &ge);
	}

	void Point::Native::BatchNormalizer_Arr::get_At(Element& el, uint32_t iIdx)
	{
		el.m_pPoint = m_pPts + iIdx;
		el.m_pFe = m_pFes + iIdx;
	}

	void Point::Native::BatchNormalizer_Arr::Reset()
	{
		m_iIdx = 0;
	}

	bool Point::Native::BatchNormalizer_Arr::MoveNext(Element& el)
	{
		if (m_iIdx == m_Size)
			return false;

		get_At(el, m_iIdx);
		m_iIdx++;
		return true;
	}

	bool Point::Native::BatchNormalizer_Arr::MovePrev(Element& el)
	{
		if (m_iIdx < 2)
			return false;

		m_iIdx--;
		get_At(el, m_iIdx - 1);
		return true;
	}

	Point::Native& Point::Native::operator = (Zero_)
	{
		secp256k1_gej_set_infinity(this);
		return *this;
	}

	bool Point::Native::operator == (Zero_) const
	{
		return secp256k1_gej_is_infinity(this) != 0;
	}

    bool Point::Native::operator != (Zero_ v) const
    {
        return !(operator == (v));
    }

	bool Point::Native::operator == (const Native& v) const
	{
		Native val = -v;
		val += *this;
		return val == Zero;
	}

	bool Point::Native::operator == (const Point& v) const
	{
		// There's a fast way to compare the X coordinate (without normalizing the gej).
		// But there is no known way to check the oddity of the Y coordinate
		// Hence - we'll do the normalization.

		// Import/Export seems to be the same complexity
		ECC::Point v2;
		Export(v2);
		return v2 == v;
	}

	bool Point::operator == (const Native& pt) const
	{
		return pt == *this;
	}

	Point::Native& Point::Native::operator = (Minus v)
	{
		secp256k1_gej_neg(this, &v.x);
		return *this;
	}

	Point::Native& Point::Native::operator = (Plus v)
	{
		secp256k1_gej_add_var(this, &v.x, &v.y, NULL);
		return *this;
	}

    Point::Native& Point::Native::operator = (Minus2 v)
    {
        Point::Native temp = -v.y;
        secp256k1_gej_add_var(this, &v.x, &temp, NULL);
        return *this;
    }

	Point::Native& Point::Native::operator = (Double v)
	{
		secp256k1_gej_double_var(this, &v.x, NULL);
		return *this;
	}

    secp256k1_pubkey ConvertPointToPubkey(const Point& point)
    {
        Point::Native native;

        native.Import(point);

        NoLeak<secp256k1_gej> gej;
        NoLeak<secp256k1_ge> ge;

        gej.V = native.get_Raw();
        secp256k1_ge_set_gej(&ge.V, &gej.V);

        const size_t dataSize = 64;
        secp256k1_pubkey pubkey;

        if constexpr (sizeof(secp256k1_ge_storage) == dataSize)
        {
            secp256k1_ge_storage s;
            secp256k1_ge_to_storage(&s, &ge.V);
            memcpy(&pubkey.data[0], &s, dataSize);
        }
        else 
        {
            assert(false && "Unsupported case");
        }

        return pubkey;
    }

    std::vector<uint8_t> SerializePubkey(const secp256k1_pubkey& pubkey)
    {
        size_t dataSize = 65;
        std::vector<uint8_t> data(dataSize);

        secp256k1_context* context = secp256k1_context_create(SECP256K1_CONTEXT_SIGN | SECP256K1_CONTEXT_VERIFY);

        secp256k1_ec_pubkey_serialize(context, data.data(), &dataSize, &pubkey, SECP256K1_EC_UNCOMPRESSED);

        secp256k1_context_destroy(context);

        return data;
    }

	Point::Compact::Converter::Converter()
	{
		m_Batch.m_Size = 0;
	}

	void Point::Compact::Converter::Flush()
	{
		m_Batch.Normalize();

		for (uint32_t i = 0; i < m_Batch.m_Size; i++)
			m_Batch.get_As(*m_ppC[i], m_Batch.m_pPts[i]);

		m_Batch.m_Size = 0;
	}

	void Point::Compact::Converter::set_Deferred(Compact& trg, const Point::Native& src)
	{
		if (m_Batch.m_Size == N)
			Flush();

		m_ppC[m_Batch.m_Size] = &trg;
		m_Batch.m_pPts[m_Batch.m_Size] = src;
		m_Batch.m_Size++;
	}

	void Point::Compact::Assign(secp256k1_ge& ge) const
	{
		secp256k1_ge_from_storage(&ge, this);
	}

	void Point::Compact::Assign(Point::Native& p, bool bSet) const
	{
		NoLeak<secp256k1_ge> ge;
		Assign(ge.V);

		if (bSet)
			secp256k1_gej_set_ge(&p.get_Raw(), &ge.V);
		else
		{
			if (Mode::Secure == ECC::g_Mode)
				secp256k1_gej_add_ge(&p.get_Raw(), &p.get_Raw(), &ge.V);
			else
				secp256k1_gej_add_ge_var(&p.get_Raw(), &p.get_Raw(), &ge.V, nullptr);
		}
	}

	/////////////////////
	// Commitment
	void Commitment::Assign(Point::Native& res, bool bSet) const
	{
		(Context::get().G * k).Assign(res, bSet);
		res += Context::get().H * val;
	}

	/////////////////////
	// Tag
	namespace Tag {

		bool IsCustom(const Point::Native* pHGen)
		{
			return pHGen && !(*pHGen == Zero);
		}

		void AddValue(Point::Native& out, const Point::Native* pHGen, Amount v)
		{
			Mode::Scope scope(Mode::Fast);

			if (IsCustom(pHGen))
				out += *pHGen * v;
			else
				out += Context::get().H * v;
		}

	} // namespace Tag

} // namespace ECC
