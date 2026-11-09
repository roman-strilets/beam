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
	// MultiMac
	Point::Native& Point::Native::operator = (Mul v)
	{
		MultiMac::Casual mc;
		mc.Init(v.x);

		MultiMac mm;
		mm.m_pCasual = &mc;
		mm.m_Casual = 1;
		mm.m_pKCasual = Cast::NotConst(&v.y);
		mm.Calculate(*this);

		return *this;
	}

	Point::Native& Point::Native::operator += (Mul v)
	{
		return operator += (Native(v));
	}

	void MultiMac::Prepared::Initialize(Oracle& oracle, Hash::Processor& hpRes, Point::Compact::Converter& cpc)
	{
		Point::Native val;
		Generator::CreatePointNnz(val, oracle, &hpRes);
		Initialize(val, oracle, cpc);
	}

	void MultiMac::Prepared::Initialize(Point::Native& val, Oracle& oracle, Point::Compact::Converter& cpc)
	{
		Point::Native npos = val, nums = val * Two;

		for (unsigned int i = 0; i < _countof(m_Fast.m_pPt); i++)
		{
			if (i)
				npos += nums;

			cpc.set_Deferred(m_Fast.m_pPt[i], npos);
		}

		while (true)
		{
			Generator::CreatePointNnz(nums, oracle, NULL);
			oracle >> m_Secure.m_Scalar;

			npos = nums;
			bool bOk = true;

			for (int i = 0; ; )
			{
				if (npos == Zero)
					bOk = false;

				cpc.set_Deferred(m_Secure.m_pPt[i], npos);

				if (++i == _countof(m_Secure.m_pPt))
					break;

				npos += val;
			}

			cpc.Flush(); // we use fast data in secure init

			assert(Mode::Fast == g_Mode);
			MultiMac mm;

			const Prepared* ppPrep[] = { this };
			mm.m_ppPrepared = ppPrep;
			mm.m_pKPrep = &m_Secure.m_Scalar;
			Prepared::Fast::Wnaf wnaf;
			mm.m_pWnafPrepared = &wnaf;
			mm.m_Prepared = 1;

			mm.Calculate(npos);

			npos += nums;
			for (int i = ECC::nBits / Secure::nBits; --i; )
			{
				for (int j = Secure::nBits; j--; )
					nums = nums * Two;
				npos += nums;
			}

			if (npos == Zero)
				bOk = false;

			if (bOk)
			{
				npos = -npos;
				cpc.set_Deferred(m_Secure.m_Compensation, npos);
				break;
			}
		}
	}

	void MultiMac::Prepared::Assign(Point::Native& out, bool bSet) const
	{
		m_Fast.m_pPt[0].Assign(out, bSet);
	}

	void MultiMac::Casual::Init(const Point::Native& p)
	{
		if (Mode::Fast == g_Mode)
		{
			Fast& f = U.F.get();
			f.m_pPt[0] = p;
		}
		else
		{
			Secure& s = U.S.get();
			s.m_pPt[0] = Context::get().m_Casual.m_Nums;

			for (unsigned int i = 1; i < Secure::nCount; i++)
			{
				s.m_pPt[i] = s.m_pPt[i - 1];
				s.m_pPt[i] += p;
			}
		}
	}

	void MultiMac::Reset()
	{
		m_Casual = 0;
		m_Prepared = 0;
		m_ReuseFlag = Reuse::None;
	}

	unsigned int GetPortion(const Scalar::Native& k, unsigned int iWord, unsigned int iBitInWord, unsigned int nBitsWnd)
	{
		const Scalar::Native::uint& n = k.get().d[iWord];

		return (n >> (iBitInWord & ~(nBitsWnd - 1))) & ((1 << nBitsWnd) - 1);
	}

	struct MultiMac::WnafBase::Context
	{
		const Scalar::Native::uint* m_p;
		unsigned int m_Flag;
		unsigned int m_iBit;
		unsigned int m_Carry;

		unsigned int NextOdd();

		void OnBit(unsigned int& res, unsigned int x)
		{
			if (x)
			{
				res |= m_Flag;
				m_Carry = 0;
			}
		}
	};

	unsigned int MultiMac::WnafBase::Context::NextOdd()
	{
		const unsigned int nWordBits = sizeof(*m_p) << 3;
		const unsigned int nMsk = nWordBits - 1;

		unsigned int res = 0;

		while (true)
		{
			if (m_iBit >= ECC::nBits)
			{
				OnBit(res, m_Carry);

				if (!res)
					break;
			}
			else
			{
				unsigned int n = (unsigned int) (m_p[m_iBit / nWordBits] >> (m_iBit & nMsk));
				OnBit(res, (1 & n) != m_Carry);
			}

			m_iBit++;
			res >>= 1;

			if (1 & res)
				break;
		}

		return res;
	}

	void MultiMac::WnafBase::Shared::Reset()
	{
		ZeroObject(m_pTable);
	}

	unsigned int MultiMac::WnafBase::Shared::Add(Entry* pTrg, const Scalar::Native& k, unsigned int nWndBits, WnafBase& wnaf, unsigned int iElement)
	{
		const unsigned int nWndConsume = nWndBits + 1;

		Context ctx;
		ctx.m_p = k.get().d;
		ctx.m_iBit = 0;
		ctx.m_Carry = 0;
		ctx.m_Flag = 1 << nWndConsume;

		unsigned int iEntry = 0;

		for ( ; ; iEntry++)
		{
			unsigned int nOdd = ctx.NextOdd();
			if (!nOdd)
				break;

			assert(!ctx.m_Carry);
			assert(1 & nOdd);
			assert(!(nOdd >> nWndConsume));

			assert(ctx.m_iBit >= nWndConsume);
			unsigned int iIdx = ctx.m_iBit - nWndConsume;
			assert(iIdx < _countof(m_pTable));

			Entry& x = pTrg[iEntry];
			x.m_iBit = static_cast<uint16_t>(iIdx);

			if (nOdd >> nWndBits)
			{
				// hi bit is ON
				nOdd ^= (1 << nWndConsume) - 2;

				assert(1 & nOdd);
				assert(!(nOdd >> nWndBits));

				x.m_Odd = static_cast<int16_t>(nOdd) | Entry::s_Negative;

				ctx.m_Carry = 1;
			}
			else
				x.m_Odd = static_cast<int16_t>(nOdd);
		}

		unsigned int ret = iEntry--;
		if (ret)
		{
			// add the highest bit
			const Entry& x = pTrg[iEntry];
			Link& lnk = m_pTable[x.m_iBit];
			
			wnaf.m_Next = lnk;
			lnk.m_iElement = iElement;
			lnk.m_iEntry = iEntry;
		}

		return ret;
	}

	unsigned int MultiMac::WnafBase::Shared::Fetch(unsigned int iBit, WnafBase& wnaf, const Entry* pE, bool& bNeg)
	{
		Link& lnkTop = m_pTable[iBit]; // alias
		Link lnkThis = lnkTop;

		const Entry& e = pE[lnkThis.m_iEntry];
		assert(e.m_iBit == iBit);

		lnkTop = wnaf.m_Next; // pop this entry

		if (lnkThis.m_iEntry)
		{
			// insert next entry
			lnkThis.m_iEntry--;

			const Entry& e2 = pE[lnkThis.m_iEntry];
			assert(e2.m_iBit < iBit);

			Link& lnkTop2 = m_pTable[e2.m_iBit];
			wnaf.m_Next = lnkTop2;
			lnkTop2 = lnkThis;
		}

		unsigned int nOdd = e.m_Odd;
		assert(1 & nOdd);

		bNeg = !!(e.m_Odd & e.s_Negative);

		if (bNeg)
			nOdd &= ~e.s_Negative;

		return nOdd;
	}

	struct MultiMac::Normalizer
		:public Point::Native::BatchNormalizer
	{
		const MultiMac& m_This;

		Normalizer(const MultiMac& mm) :m_This(mm) {}

		struct Cursor
		{
			int m_iElement;
			uint32_t m_iEntry;
		};

		Cursor m_Cursor;

		void FromCursor(Element& el, const Cursor& cu) const;
		bool MoveBkwd(Cursor& cu) const;

		void Reset() override;
		bool MoveNext(Element& el) override;

		bool MovePrev(Element& el) override;
	};

	void MultiMac::Normalizer::FromCursor(Element& el, const Cursor& cu) const
	{
		Casual& x = m_This.m_pCasual[cu.m_iElement];
		Casual::Fast& f = x.U.F.get();

		el.m_pPoint = f.m_pPt + cu.m_iEntry;
		el.m_pFe = f.m_pFe + cu.m_iEntry;
	}

	bool MultiMac::Normalizer::MoveBkwd(Cursor& cu) const
	{
		while (true)
		{
			if (cu.m_iElement < m_This.m_Casual)
			{
				Casual& x = m_This.m_pCasual[cu.m_iElement];
				Casual::Fast& f = x.U.F.get();

				assert(cu.m_iEntry <= f.m_nNeeded);
				f; // suppress warning in release build

				if (cu.m_iEntry)
				{
					cu.m_iEntry--;
					break;
				}
			}

			if (!cu.m_iElement)
				return false;

			cu.m_iElement--;

			Casual& x = m_This.m_pCasual[cu.m_iElement];
			Casual::Fast& f = x.U.F.get();

			cu.m_iEntry = f.m_nNeeded;
		}

		return true;
	}

	void MultiMac::Normalizer::Reset()
	{
		ZeroObject(m_Cursor);
	}

	bool MultiMac::Normalizer::MoveNext(Element& el)
	{
		while (true)
		{
			if (m_Cursor.m_iElement == m_This.m_Casual)
				return false;

			Casual& x = m_This.m_pCasual[m_Cursor.m_iElement];
			Casual::Fast& f = x.U.F.get();

			if (m_Cursor.m_iEntry < f.m_nNeeded)
				break;

			m_Cursor.m_iElement++;
			m_Cursor.m_iEntry = 0;

		}

		FromCursor(el, m_Cursor);
		m_Cursor.m_iEntry++;

		return true;
	}

	bool MultiMac::Normalizer::MovePrev(Element& el)
	{
		Cursor cu1 = m_Cursor;
		if (!MoveBkwd(cu1))
			return false;

		Cursor cu2 = cu1;
		if (!MoveBkwd(cu2))
			return false;

		m_Cursor = cu1;
		FromCursor(el, cu2);
		return true;
	}

	void MultiMac::Calculate(Point::Native& res) const
	{
		const unsigned int nBitsPerWord = sizeof(Scalar::Native::uint) << 3;

		static_assert(!(nBitsPerWord % Casual::Secure::nBits), "");
		static_assert(!(nBitsPerWord % Prepared::Secure::nBits), "");

		res = Zero;

		NoLeak<secp256k1_ge> ge;
		NoLeak<Point::Compact> ge_s;
		secp256k1_fe zDenom;
		bool bDenomSet = false;

		WnafBase::Shared wsP, wsC;

		unsigned int iBit = ECC::nBits;

		if (Mode::Fast == g_Mode)
		{
			iBit++; // extra bit may be necessary because of interleaving
			assert(iBit == _countof(wsP.m_pTable));

			wsP.Reset();
			wsC.Reset();

			for (int iEntry = 0; iEntry < m_Prepared; iEntry++)
			{
				unsigned int nEntries = m_pWnafPrepared[iEntry].Init(wsP, m_pKPrep[iEntry], iEntry + 1);
				assert(nEntries <= _countof(m_pWnafPrepared[iEntry].m_pVals));
				nEntries; // suppress warning in release build
			}

			for (int iEntry = 0; iEntry < m_Casual; iEntry++)
			{
				Casual& x = m_pCasual[iEntry];
				Casual::Fast& f = x.U.F.get();

				Point::Native& pt = f.m_pPt[0];
				if (pt == Zero)
				{
					f.m_nNeeded = 0;
					continue;
				}

				unsigned int nEntries = f.m_Wnaf.Init(wsC, m_pKCasual[iEntry], iEntry + 1);
				assert(nEntries <= _countof(f.m_Wnaf.m_pVals));

				if (Reuse::UseGenerated == m_ReuseFlag)
				{
					if (!bDenomSet)
					{
						bDenomSet = true;
						zDenom = pt.get_Raw().z;
					}

					continue;
				}

				if (Reuse::Generate == m_ReuseFlag)
					f.m_nNeeded = Casual::Fast::nCount; // all
				else
				{
					// Find highest needed element, calculate all the needed ones
					f.m_nNeeded = 0;
					for (unsigned int i = 0; i < nEntries; i++)
					{
						const WnafBase::Entry& e = f.m_Wnaf.m_pVals[i];

						unsigned int nOdd = e.m_Odd & ~e.s_Negative;
						assert(nOdd & 1);

						unsigned int nElem = (nOdd >> 1);
						beam::setmax(f.m_nNeeded, nElem + 1);
					}
					assert(f.m_nNeeded <= Casual::Fast::nCount);

					if (f.m_nNeeded <= 1)
						continue;
				}

				// Calculate all the needed elements
				Point::Native ptX2 = pt * Two;

				for (uint32_t nPrepared = 1; nPrepared < f.m_nNeeded; nPrepared++)
					f.m_pPt[nPrepared] = f.m_pPt[nPrepared - 1] + ptX2;
			}

			if (Reuse::UseGenerated == m_ReuseFlag)
			{
				if (!bDenomSet)
					secp256k1_fe_set_int(&zDenom, 1);
			}
			else
			{
				// Bring everything to the same denominator
				Normalizer nrm(*this);
				nrm.ToCommonDenominator(zDenom);
			}
		}
		else
		{
			for (int iEntry = 0; iEntry < m_Prepared; iEntry++)
				m_pKPrep[iEntry] += m_ppPrepared[iEntry]->m_Secure.m_Scalar;
		}

		while (iBit--)
		{
			if (!(res == Zero))
				res = res * Two;

			if (Mode::Fast == g_Mode)
			{
				WnafBase::Link& lnkC = wsC.m_pTable[iBit]; // alias
				while (lnkC.m_iElement)
				{
					Casual& x = m_pCasual[lnkC.m_iElement - 1];
					Casual::Fast& f = x.U.F.get();
					Casual::Fast::Wnaf& wnaf = f.m_Wnaf;

					bool bNeg;
					unsigned int nOdd = wnaf.Fetch(wsC, iBit, bNeg);

					unsigned int nElem = (nOdd >> 1);
					assert(nElem < f.m_nNeeded);

					Point::Native::BatchNormalizer::get_As(ge.V, f.m_pPt[nElem]);

					if (bNeg)
						secp256k1_ge_neg(&ge.V, &ge.V);

					secp256k1_gej_add_ge_var(&res.get_Raw(), &res.get_Raw(), &ge.V, nullptr);
				}

				WnafBase::Link& lnkP = wsP.m_pTable[iBit]; // alias
				while (lnkP.m_iElement)
				{
					unsigned int iElement = lnkP.m_iElement - 1;

					Prepared::Fast::Wnaf& wnaf = m_pWnafPrepared[iElement];

					bool bNeg;
					unsigned int nOdd = wnaf.Fetch(wsP, iBit, bNeg);

					unsigned int nElem = (nOdd >> 1);
					assert(nElem < Prepared::Fast::nCount);

					const Point::Compact& ptC = m_ppPrepared[iElement]->m_Fast.m_pPt[nElem];

					secp256k1_ge_from_storage(&ge.V, &ptC);

					if (bNeg)
						secp256k1_ge_neg(&ge.V, &ge.V);

					secp256k1_gej_add_zinv_var(&res.get_Raw(), &res.get_Raw(), &ge.V, &zDenom);
				}
			}
			else
			{
				unsigned int iWord = iBit / nBitsPerWord;
				unsigned int iBitInWord = iBit & (nBitsPerWord - 1);

				// secure mode
				if (!(iBit & (Casual::Secure::nBits - 1)))
				{
					for (int iEntry = 0; iEntry < m_Casual; iEntry++)
					{
						Casual& x = m_pCasual[iEntry];

						unsigned int nVal = GetPortion(m_pKCasual[iEntry], iWord, iBitInWord, Casual::Secure::nBits);

						//Point::Native ptVal;
						//for (unsigned int i = 0; i < Casual::Secure::nCount; i++)
						//	object_cmov(ptVal, x.m_pPt[nVal], i == nVal);
						//
						//res += ptVal;

						res += x.U.S.get().m_pPt[nVal]; // cmov seems not needed, since the table is relatively small, and not in global mem (less predicatble addresses)
						// The version with cmov (commented-out above) is ~15% slower
					}
				}

				if (!(iBit & (Prepared::Secure::nBits - 1)))
				{
					for (int iEntry = 0; iEntry < m_Prepared; iEntry++)
					{
						const Prepared::Secure& x = m_ppPrepared[iEntry]->m_Secure;

						unsigned int nVal = GetPortion(m_pKPrep[iEntry], iWord, iBitInWord, Prepared::Secure::nBits);

						for (unsigned int i = 0; i < _countof(x.m_pPt); i++)
							object_cmov(ge_s.V, x.m_pPt[i], i == nVal);

						res += ge_s.V;
					}
				}
			}
		}

		if (Mode::Secure == g_Mode)
		{
			for (int iEntry = 0; iEntry < m_Prepared; iEntry++)
			{
				const Prepared::Secure& x = m_ppPrepared[iEntry]->m_Secure;

				res += x.m_Compensation;
			}

			for (int iEntry = 0; iEntry < m_Casual; iEntry++)
				res += Context::get().m_Casual.m_Compensation;
		}
		else
		{
			// fix denominator
			secp256k1_fe_mul(&res.get_Raw().z, &res.get_Raw().z, &zDenom);
		}
	}

	void MultiMac_Dyn::Prepare(uint32_t nMaxCasual, uint32_t nMaxPrepared)
	{
		if (nMaxCasual)
		{
			m_vCasual.resize(nMaxCasual);
			m_vKCasual.resize(nMaxCasual);

			m_pCasual = &m_vCasual.front();
			m_pKCasual = &m_vKCasual.front();
		}

		if (nMaxPrepared)
		{
			m_vpPrepared.resize(nMaxPrepared);
			m_vKPrepared.resize(nMaxPrepared);
			m_vWnafPrepared.resize(nMaxPrepared);

			m_ppPrepared = &m_vpPrepared.front();
			m_pKPrep = &m_vKPrepared.front();
			m_pWnafPrepared = &m_vWnafPrepared.front();
		}
	}

} // namespace ECC
