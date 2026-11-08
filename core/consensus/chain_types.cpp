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

#include <ctime>
#include <chrono>
#include "core/consensus/chain_types.h"
#include "utility/logger.h"
#include "core/crypto/keccak.h"

namespace beam
{
	/////////////
	// PeerID
	bool PeerID::ExportNnz(ECC::Point::Native& pt) const
	{
		ECC::Point pk;
		pk.m_X = Cast::Down<ECC::uintBig>(*this);
		pk.m_Y = 0;

		return pt.ImportNnz(pk);
	}

	bool PeerID::Import(const ECC::Point::Native& pt)
	{
		ECC::Point pk = pt;
		*this = pk.m_X;
		return !pk.m_Y;
	}

	bool PeerID::FromSk(ECC::Scalar::Native& sk)
	{
		ECC::Point::Native pt = ECC::Context::get().G * sk;
		if (Import(pt))
			return true;

		sk = -sk;
		return false;
	}

	bool PeerID::CheckSignature(const Merkle::Hash& msg, const ECC::Signature& sig) const
	{
		ECC::Point::Native pt;
		return
			ExportNnz(pt) &&
			sig.IsValid(msg, pt);
	}

	/////////////
	// Evm::Address
	void Evm::Address::FromPubKey(const ECC::Point::Storage& pk)
	{
		KeccakProcessor<Word::nBits> hp;

		hp.Write(pk.m_X);
		hp.Write(pk.m_Y);

		Word w;
		hp.Read(w.m_pData);

		*this = W2A(w);
	}

	bool Evm::Address::FromPubKey(const ECC::Point& pk)
	{
		ECC::Point::Native pt_n;
		ECC::Point::Storage pt_s;
		if (!pt_n.ImportNnz(pk, &pt_s))
			return false;

		FromPubKey(pt_s);
		return true;
	}

	bool Evm::Address::FromPubKey(const PeerID& pid)
	{
		ECC::Point pt;
		pt.m_X = Cast::Down<ECC::uintBig>(pid);
		pt.m_Y = 0;
		return FromPubKey(pt);
	}

	void Evm::Address::FromPubKey(const ECC::Point::Native& pt)
	{
		ECC::Point::Storage pt_s;
		pt.Export(pt_s);
		FromPubKey(pt_s);
	}

	/////////////
	// HeightRange
	void HeightRange::Reset()
	{
		m_Min = 0;
		m_Max = MaxHeight;
	}

	void HeightRange::Intersect(const HeightRange& x)
	{
		beam::setmax(m_Min, x.m_Min);
		beam::setmin(m_Max, x.m_Max);
	}

	bool HeightRange::IsEmpty() const
	{
		return m_Min > m_Max;
	}

	bool HeightRange::IsInRange(Height h) const
	{
		return IsInRangeRelative(h - m_Min);
	}

	bool HeightRange::IsInRangeRelative(Height dh) const
	{
		return dh <= (m_Max - m_Min);
	}

	/////////////
	// MasterKey
	Key::IKdf::Ptr MasterKey::get_Child(Key::IKdf& kdf, Key::Index iSubkey)
	{
		Key::IKdf::Ptr pRes;
		ECC::HKdf::CreateChild(pRes, kdf, iSubkey);
		return pRes;
	}

	void HeightAdd(Height& trg, Height val)
	{
		trg += val;
		if (trg < val)
			trg = Height(-1);
	}

	Amount SplitAmountSigned(AmountSigned val, bool& isPositive)
	{
		isPositive = (val >= 0);
		if (isPositive)
			return val;

		return static_cast<Amount>(-val);
	}

	/////////////
	// AmountBig
	namespace AmountBig
	{
		Amount get_Lo(const Number& x)
		{
			static_assert(sizeof(x) == sizeof(Amount) * 2);
			return x.get_Element<Amount, 0>();
		}

		Amount get_Hi(const Number& x)
		{
			return x.get_Element<Amount, 1>();
		}

		void AddTo(ECC::Point::Native& res, const Number& x)
		{
			ECC::Mode::Scope scope(ECC::Mode::Fast);

			if (get_Hi(x))
			{
				ECC::Scalar s;
				s.m_Value.FromNumber(x);
				res += ECC::Context::get().H * ECC::Scalar::Native(s);
			}
			else
			{
				Amount lo = get_Lo(x);
				if (lo)
					res += ECC::Context::get().H * lo;
			}
		}

		void AddTo(ECC::Point::Native& res, const Number& x, const ECC::Point::Native& hGen)
        {
            ECC::Mode::Scope scope(ECC::Mode::Fast);
            ECC::Scalar s;
            s.m_Value.FromNumber(x);
			res += hGen * s;
        }

		uint32_t Text::Print(char* sz, const Number& x, bool bTrimGroths)
		{
			auto out = MultiWord::Factorization::MakePrintOut<10>(sz, nLenMax);
			x.DecomposeEx<10>(out, true);
			return Expand(sz, out.m_pE, nLenMax - out.get_Reserve(), bTrimGroths);
		}

		uint32_t Text::Print(char* sz, Amount val, bool bTrimGroths)
		{
			auto out = MultiWord::Factorization::MakePrintOut<10>(sz, nLenMax);
			MultiWord::From(val).DecomposeEx<10>(out, true);
			return Expand(sz, out.m_pE, nLenMax - out.get_Reserve(), bTrimGroths);
		}

		void ExpandGroths(char*& szPos, const char* szSrc, uint32_t nSrc, bool bTrim)
		{
			assert(nSrc <= Text::nGrothDigits);
			uint32_t nZeroes = Text::nGrothDigits - nSrc;

			if (bTrim)
			{
				for (; ; nSrc--)
				{
					if (!nSrc)
						return;
					if ('0' != szSrc[nSrc - 1])
						break;
				}
			}

			*szPos++ = '.';
			while (nZeroes--)
				*szPos++ = '0';

			memmove(szPos, szSrc, nSrc);
			szPos += nSrc;
		}

		uint32_t Text::Expand(char* szDst, const char* szSrc, uint32_t nSrc, bool bTrimGroths)
		{
			assert(nSrc <= nLenUndecorated);
			char* szPos = szDst;

			if (nSrc > nGrothDigits)
			{
				uint32_t nDelta = nSrc - nGrothDigits;
				szPos += Group3Base::Expand(szPos, szSrc, nDelta);

				ExpandGroths(szPos, szSrc + nDelta, nGrothDigits, bTrimGroths);
			}
			else
			{
				*szPos++ = '0';
				ExpandGroths(szPos, szSrc, nSrc, bTrimGroths);
			}

			*szPos = 0;
			return static_cast<uint32_t>(szPos - szDst);
		}

		void Print(std::ostream& os, const Number& x, bool bTrim /* = true */)
		{
			char sz[Text::nLenMax + 1];
			Text::Print(sz, x, bTrim);
			os << sz;
		}

		void Print(std::ostream& os, Amount x, bool bTrim /* = true */)
		{
			char sz[Text::nLenMax + 1];
			Text::Print(sz, x, bTrim);
			os << sz;
		}

	} // namespace AmountBig

	uint32_t Group3Base::Expand(char* szDst, const char* szSrc, uint32_t nSrc)
	{
		char* szPos = szDst;
		if (nSrc)
		{
			while (true)
			{
				*szPos++ = *szSrc++;

				if (!--nSrc)
					break;
				if (!(nSrc % 3))
					*szPos++ = ',';
			}
		}

		*szPos = 0;
		return static_cast<uint32_t>(szPos - szDst);
	}

	/////////////
	// Misc
	Timestamp getTimestamp()
	{
		return beam::Timestamp(std::chrono::seconds(std::time(nullptr)).count());
	}

	uint32_t GetTime_ms()
	{
#ifdef WIN32
		return GetTickCount();
#else // WIN32
		// platform-independent analogue of GetTickCount
		using namespace std::chrono;
		return (uint32_t)duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
#endif // WIN32
	}

	uint32_t GetTimeNnz_ms()
	{
		uint32_t ret = GetTime_ms();
		return ret ? ret : 1;
	}

	void LongAction::Reset(const char* sz, uint64_t nTotal)
	{
		m_Total = nTotal;
		m_Last_ms = GetTime_ms();
		BEAM_LOG_INFO() << sz;
		if (m_pExternal)
			m_pExternal->Reset(sz, nTotal);
	}

	void LongAction::SetTotal(uint64_t nTotal)
	{
		m_Total = nTotal;
		if (m_pExternal)
			m_pExternal->SetTotal(nTotal);
	}

	bool LongAction::OnProgress(uint64_t pos)
	{
		uint32_t dt_ms = GetTime_ms() - m_Last_ms;

		const uint32_t nWindow_ms = 10000; // 10 sec
		uint32_t n = dt_ms / nWindow_ms;
		if (n)
		{
			m_Last_ms += n * nWindow_ms;

			uint32_t nDone = 0;

			if (m_Total)
			{
				if (pos >= m_Total)
					nDone = 100;
				else
					nDone = (uint32_t) (pos * 100ull / m_Total);
			}

			BEAM_LOG_INFO() << "\t" << nDone << "%...";
		}
		if (m_pExternal)
			return m_pExternal->OnProgress(pos);

		return true;
	}

} // namespace beam
