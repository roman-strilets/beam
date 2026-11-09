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
	// Signature
	void SignatureBase::Expose(Oracle& oracle, const Hash::Value& msg) const
	{
		oracle
			<< m_NoncePub
			<< msg;
	}

	void SignatureBase::get_Challenge(Scalar::Native& out, const Hash::Value& msg) const
	{
		Oracle oracle;
		Expose(oracle, msg);
		oracle >> out;
	}

	bool SignatureBase::IsValid(const Config& cfg, const Hash::Value& msg, const Scalar* pK, const Point::Native* pPk) const
	{
		Point::Native noncePub;
		if (!noncePub.Import(m_NoncePub))
			return false;

		return IsValidPartial(cfg, msg, pK, pPk, noncePub);
	}

	bool SignatureBase::IsValidPartial(const Config& cfg, const Hash::Value& msg, const Scalar* pK, const Point::Native* pPk, const Point::Native& noncePub) const
	{
		// TODO: remove this limitation, get adequate MultiMac instance by vcall to cfg, which would invoke us back
		const uint32_t nMaxG = 2;
		const uint32_t nMaxK = 2;
		if ((cfg.m_nG <= nMaxG) && (cfg.m_nKeys <= nMaxK))
		{
			MultiMac_WithBufs<nMaxK, nMaxG> mm;
			return IsValidPartialInternal(cfg, mm, msg, pK, pPk, noncePub);
		}

		MultiMac_Dyn mmDyn;
		if (!InnerProduct::BatchContext::s_pInstance)
			mmDyn.Prepare(cfg.m_nKeys, cfg.m_nG); // otherwise mm won't be used
		return IsValidPartialInternal(cfg, mmDyn, msg, pK, pPk, noncePub);
	}

	bool SignatureBase::RecoverPubKey(const Config& cfg, const Hash::Value& msg, const Scalar* pK, const Point::Native* pPk, uint32_t iNum, Point::Native& res) const
	{
		if (iNum >= cfg.m_nKeys)
			return false;

		MultiMac_Dyn mm;
		mm.Prepare(cfg.m_nKeys, cfg.m_nG);
		mm.m_Prepared = cfg.m_nG;
		mm.m_Casual = cfg.m_nKeys;

		Mode::Scope scope(Mode::Fast);

		Oracle oracle;
		Expose(oracle, msg);

		Scalar::Native& eTrg = mm.m_pKCasual[iNum];

		for (uint32_t iK = 0; iK < cfg.m_nKeys; iK++)
		{
			oracle >> mm.m_pKCasual[iK];

			if (iK == iNum)
			{
				if (!res.Import(m_NoncePub))
					return false;

				mm.m_pCasual[iK].Init(res);
			}
			else
				mm.m_pCasual[iK].Init(*pPk++);
		}

		eTrg.Inv();
		eTrg = -eTrg;

		for (uint32_t iK = 0; iK < cfg.m_nKeys; iK++)
		{
			if (iK != iNum)
				mm.m_pKCasual[iK] *= eTrg;
		}

		for (uint32_t iG = 0; iG < cfg.m_nG; iG++)
		{
			mm.m_ppPrepared[iG] = cfg.m_pG[iG].m_pGenPrep;
			mm.m_pKPrep[iG] = pK[iG];
			mm.m_pKPrep[iG] *= eTrg;
		}

		mm.Calculate(res);
		return true;
	}

	bool SignatureBase::IsValidPartialInternal(const Config& cfg, MultiMac& mm, const Hash::Value& msg, const Scalar* pK, const Point::Native* pPk, const Point::Native& noncePub) const
	{
		Mode::Scope scope(Mode::Fast);

		Oracle oracle;
		Expose(oracle, msg);

		InnerProduct::BatchContext* pBc = InnerProduct::BatchContext::s_pInstance;
		if (pBc)
			pBc->EquationBegin();
		else
		{
			mm.m_Prepared = cfg.m_nG;
			mm.m_Casual = cfg.m_nKeys;
		}

		for (uint32_t iK = 0; iK < cfg.m_nKeys; iK++)
		{
			Scalar::Native e;
			oracle >> e;

			if (pBc)
				pBc->AddCasual(pPk[iK], e);
			else
			{
				mm.m_pCasual[iK].Init(pPk[iK]);
				mm.m_pKCasual[iK] = e;
			}
		}

		for (uint32_t iG = 0; iG < cfg.m_nG; iG++)
		{
			if (pBc)
				pBc->AddPrepared(cfg.m_pG[iG].m_nBatchIdx, pK[iG]);
			else
			{
				mm.m_ppPrepared[iG] = cfg.m_pG[iG].m_pGenPrep;
				mm.m_pKPrep[iG] = pK[iG];
			}
		}

		if (pBc)
		{
			pBc->AddCasual(noncePub, pBc->m_Multiplier, true);
			return true;
		}

		Point::Native pt;
		mm.Calculate(pt);

		pt += noncePub;
		return pt == Zero;
	}

	void SignatureBase::SetNoncePub(const Config& cfg, const Scalar::Native* pNonce)
	{
		Point::Native pubNonce(Zero);

		for (uint32_t iG = 0; iG < cfg.m_nG; iG++)
		{
			const auto& g = cfg.m_pG[iG];
			if (g.m_pGen_s)
				pubNonce += (*g.m_pGen_s) * pNonce[iG];
			else
				pubNonce += (*g.m_pGen_f) * pNonce[iG];
		}

		m_NoncePub = pubNonce;
	}

	void SignatureBase::CreateNonces(const Config& cfg, const Hash::Value& msg, const Scalar::Native* pSk, Scalar::Native* pRes)
	{
		NonceGenerator nonceGen("beam-Schnorr");

		NoLeak<Scalar> s_;

		for (uint32_t i = 0; i < cfg.m_nG * cfg.m_nKeys; i++)
		{
			s_.V = pSk[i];
			nonceGen << s_.V.m_Value;
		}

		nonceGen << msg;

		GenRandom(s_.V.m_Value); // add extra randomness to the nonce, so it's derived from both deterministic and random parts
		nonceGen
			<< s_.V.m_Value;

		for (uint32_t iG = 0; iG < cfg.m_nG; iG++)
			nonceGen >> pRes[iG];
	}

	void SignatureBase::Sign(const Config& cfg, const Hash::Value& msg, Scalar* pK, const Scalar::Native* pSk, Scalar::Native* pRes)
	{
		CreateNonces(cfg, msg, pSk, pRes);
		SetNoncePub(cfg, pRes);
		SignRaw(cfg, msg, pK, pSk, pRes);
	}

	void SignatureBase::SignRaw(const Config& cfg, const Hash::Value& msg, Scalar* pK, const Scalar::Native* pSk, Scalar::Native* pRes) const
	{
		Oracle oracle;
		Expose(oracle, msg);

		Scalar::Native e, k;

		for (uint32_t iK = 0; iK < cfg.m_nKeys; iK++, pSk += cfg.m_nG)
		{
			oracle >> e; // challenge

			for (uint32_t iG = 0; iG < cfg.m_nG; iG++)
			{
				k = pSk[iG] * e;
				pRes[iG] += k;
			}
		}

		for (uint32_t iG = 0; iG < cfg.m_nG; iG++)
		{
			pRes[iG] = -pRes[iG];
			pK[iG] = pRes[iG];
		}
	}

	void SignatureBase::SignPartial(const Config& cfg, const Hash::Value& msg, Scalar* pK, const Scalar::Native* pSk, const Scalar::Native* pNonce, Scalar::Native* pRes) const
	{
		for (uint32_t iG = 0; iG < cfg.m_nG; iG++)
			pRes[iG] = pNonce[iG];

		SignRaw(cfg, msg, pK, pSk, pRes);
	}

	const SignatureBase::Config& Signature::get_Config()
	{
		return ECC::Context::get().m_Sig.m_CfgG1;
	}

	void Signature::SignPartial(const Hash::Value& msg, const Scalar::Native& sk, const Scalar::Native& nonce)
	{
		Scalar::Native res;
		SignatureBase::SignPartial(get_Config(), msg, &m_k, &sk, &nonce, &res);
	}

	void Signature::Sign(const Hash::Value& msg, const Scalar::Native& sk)
	{
		Scalar::Native res;
		SignatureBase::Sign(get_Config(), msg, &m_k, &sk, &res);
	}

	bool Signature::IsValidPartial(const Hash::Value& msg, const Point::Native& pubNonce, const Point::Native& pk) const
	{
		return SignatureBase::IsValidPartial(get_Config(), msg, &m_k, &pk, pubNonce);
	}

	bool Signature::IsValid(const Hash::Value& msg, const Point::Native& pk) const
	{
		return SignatureBase::IsValid(get_Config(), msg, &m_k, &pk);
	}

	int Signature::cmp(const Signature& x) const
	{
		int n = m_NoncePub.cmp(x.m_NoncePub);
		if (n)
			return n;

		return m_k.cmp(x.m_k);
	}

} // namespace ECC
