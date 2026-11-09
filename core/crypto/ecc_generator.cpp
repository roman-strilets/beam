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
	// Generator
	namespace Generator
	{
		void Simple::Initialize(const Point::Native& p, Point::Compact::Converter& cpc)
		{
			Point::Native pLevel[nPointsPerLevel];
			pLevel[0] = p;

			auto* pDst = m_pPts;

			for (uint32_t iLevel = 0; ; )
			{
				for (uint32_t i = 1; i < nPointsPerLevel; i++)
				{
					if (1u & i)
						pLevel[i] = pLevel[i >> 1] * Two;
					else
						pLevel[i] = pLevel[i - 1] + pLevel[0];
				}

				for (uint32_t i = 0; i < nPointsPerLevel; i++)
					cpc.set_Deferred(pDst[i], pLevel[i]);

				if (++iLevel == s_nLevels)
					break;

				pLevel[0] = pLevel[nPointsPerLevel >> 1] * Two;
				pDst += nPointsPerLevel;
			}
		}

		void Simple::SetMul(Point::Native& res, bool bSet, const Scalar::Native::uint* p, uint32_t nWords) const
		{
			static_assert(8 % nBitsPerLevel == 0, "");
			const uint32_t nLevelsPerWord = (sizeof(Scalar::Native::uint) << 3) / nBitsPerLevel;
			static_assert(!(nLevelsPerWord & (nLevelsPerWord - 1)), "should be power-of-2");

			const auto* pPts = m_pPts;

			for (uint32_t iWord = 0; iWord < nWords; iWord++)
			{
				Scalar::Native::uint n = p[iWord];

				for (uint32_t j = 0; j < nLevelsPerWord; j++, pPts += nPointsPerLevel, n >>= nBitsPerLevel)
				{
					uint32_t nSel = nPointsPerLevel & n;
					if (nSel)
					{
						const auto& pt = pPts[nSel - 1];

						if (bSet)
						{
							bSet = false;
							res = pt;
						}
						else
							res += pt;
					}
				}
			}

			if (bSet)
				res = Zero;
		}

		void CreatePointNnz(Point::Native& out, Oracle& oracle, Hash::Processor* phpRes)
		{
			Point pt;
			pt.m_Y = 0;

			do
				oracle >> pt.m_X;
			while (!out.ImportNnz(pt));

			if (phpRes)
				*phpRes << pt;
		}

		void Obscured::Initialize(const Point::Native& p, Oracle& oracle, Point::Compact::Converter& cpc)
		{
			while (!InitializeTry(p, oracle, cpc))
				; // very unlikely to happen
		}

		bool Obscured::InitializeTry(const Point::Native& p, Oracle& oracle, Point::Compact::Converter& cpc)
		{
			Point::Native nums;
			CreatePointNnz(nums, oracle, nullptr);

			oracle >> m_AddScalar;

			const auto* pAddSrc = m_AddScalar.get().d;
			const uint32_t nWordsBits = sizeof(*pAddSrc) * 8;

			Point::Native pLevel[nPointsPerLevel - 1];
			pLevel[0] = p;

			Point::Native pt2, ptAdd(Zero);

			auto* pDst = m_pPts;

			for (uint32_t iBit = 0; ; )
			{
				for (uint32_t i = 1; i < nPointsPerLevel - 1; i++)
				{
					if (1u & i)
						pLevel[i] = pLevel[i >> 1] * Two;
					else
						pLevel[i] = pLevel[i - 1] + pLevel[0];
				}

				uint32_t nAddMsk = (pAddSrc[iBit / nWordsBits] >> (iBit & (nWordsBits - 1))) & (nPointsPerLevel - 1);
				if (nAddMsk)
					ptAdd += pLevel[nAddMsk - 1];

				iBit += nBitsPerLevel;
				bool bLast = (iBit == nBits);
				if (bLast)
				{
					nums = -ptAdd;
					if (nums == Zero)
						return false;
				}

				for (uint32_t i = 0; i < nPointsPerLevel; i++)
				{
					pt2 = nums;
					if (i)
					{
						pt2 += pLevel[i - 1];
						if (pt2 == Zero)
							return false;
					}

					cpc.set_Deferred(pDst[i], pt2);
				}

				if (bLast)
					break;

				pLevel[0] = pLevel[(nPointsPerLevel - 1) >> 1] * Two;
				ptAdd += nums;
				pDst += nPointsPerLevel;
			}

			return true;
		}

		void Obscured::SetMul(Point::Native& res, bool bSet, const Scalar::Native& sk) const
		{
			Scalar::Native sk2 = sk + m_AddScalar;

			const Scalar::Native::uint* p = sk2.get().d;
			const uint32_t nWords = _countof(sk2.get().d);

			static_assert(8 % nBitsPerLevel == 0, "");
			const uint32_t nLevelsPerWord = (sizeof(Scalar::Native::uint) << 3) / nBitsPerLevel;
			static_assert(!(nLevelsPerWord & (nLevelsPerWord - 1)), "should be power-of-2");

			const auto* pPts = m_pPts;
			NoLeak<Point::Compact> ge_s;

			for (uint32_t iWord = 0; iWord < nWords; iWord++)
			{
				Scalar::Native::uint n = p[iWord];

				for (uint32_t j = 0; j < nLevelsPerWord; j++, pPts += nPointsPerLevel, n >>= nBitsPerLevel)
				{
					uint32_t nSel = (nPointsPerLevel - 1) & n;

					/** This uses a conditional move to avoid any secret data in array indexes.
					*   _Any_ use of secret indexes has been demonstrated to result in timing
					*   sidechannels, even when the cache-line access patterns are uniform.
					*  See also:
					*   "A word of warning", CHES 2013 Rump Session, by Daniel J. Bernstein and Peter Schwabe
					*    (https://cryptojedi.org/peter/data/chesrump-20130822.pdf) and
					*   "Cache Attacks and Countermeasures: the Case of AES", RSA 2006,
					*    by Dag Arne Osvik, Adi Shamir, and Eran Tromer
					*    (http://www.tau.ac.il/~tromer/papers/cache.pdf)
					*/

					const Point::Compact* pSel;
					if (Mode::Secure == g_Mode)
					{
						pSel = &ge_s.V;
						for (uint32_t i = 0; i < nPointsPerLevel; i++)
							object_cmov(ge_s.V, pPts[i], i == nSel);
					}
					else
						pSel = pPts + nSel;

					if (bSet)
					{
						bSet = false;
						res = *pSel;
					}
					else
						res += *pSel;
				}
			}
		}

	} // namespace Generator

} // namespace ECC
