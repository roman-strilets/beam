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

#include "core/consensus/asset.h"
#include "core/consensus/serialization_adapters.h"

namespace beam
{
	/////////////
	// Asset
	const PeerID Asset::s_InvalidOwnerID = Zero;

	void Asset::Base::get_Generator(ECC::Point::Native& res, ECC::Point::Storage& res_s) const
	{
		assert(m_ID);

		ECC::Point pt;
		pt.m_Y = 0;

		ECC::Oracle oracle;
		oracle
			<< "B.Asset.Gen.V1"
			<< m_ID;

		do
			oracle >> pt.m_X;
		while (!res.ImportNnz(pt, &res_s));
	}

	void Asset::Base::get_Generator(ECC::Point::Native& res) const
	{
		ECC::Point::Storage res_s;
		get_Generator(res, res_s);
	}

	void Asset::Base::get_Generator(ECC::Point::Storage& res_s) const
	{
		ECC::Point::Native res;
		get_Generator(res, res_s);
	}

	void Asset::Info::Reset()
	{
		m_Value = Zero;
		m_Owner = Zero;
		m_Cid = Zero;
		m_LockHeight = 0;
		m_Metadata.Reset();
	}

	void Asset::CreateInfo::SetCid(const ContractID* pCid)
	{
		if (pCid)
			m_Cid = *pCid;
		else
			m_Cid = Zero;
	}

	bool Asset::Info::IsEmpty() const
	{
	    return m_Value == Zero && m_Owner == Zero && m_LockHeight == Zero && m_Metadata.m_Value.empty();
	}

	bool Asset::Info::IsValid() const
    {
	    return m_Owner != Zero && m_LockHeight != Zero;
    }

	bool Asset::CreateInfo::IsDefDeposit() const
	{
		return (Rules::get().CA.DepositForList2 == m_Deposit);
	}

	void Asset::Full::get_Hash(ECC::Hash::Value& hv) const
	{
		ECC::Hash::Processor hp;
		hp
			<< "B.Asset.V1"
			<< m_ID
			<< m_Value
			<< m_Owner
			<< m_LockHeight
			<< m_Metadata.m_Hash;

		if (!IsDefDeposit())
		{
			hp
				<< "deposit"
				<< m_Deposit;
		}

		hp >> hv;
	}

	void Asset::Metadata::Reset()
	{
		m_Value.clear();
		UpdateHash();
	}

	void Asset::Metadata::UpdateHash()
	{
		if (m_Value.empty())
		{
			m_Hash = Zero;
		}
		else
		{
			ECC::Hash::Processor()
				<< "B.AssetMeta"
				<< m_Value.size()
				<< Blob(m_Value)
				>> m_Hash;
		}
	}

	void Asset::Metadata::set_String(const std::string& s, bool bLegacy)
	{
		if (bLegacy)
		{
			Serializer ser;
			ser & s;
			ser.swap_buf(m_Value);
		}
		else
		{
			m_Value.resize(s.size());
			if (!s.empty())
				memcpy(&m_Value.front(), &s.front(), s.size());
		}
	}

	void Asset::Metadata::get_String(std::string& s) const
	{
		s.clear();
		if (m_Value.empty())
			return;

		// historically the metadata was a serialized string, which is [size][string]
		// The [size] was either a single byte with MSB on, or multi-byte encoding with leading byte in range [0x01 - 0x08]
		// Currently we just assign it to the string itself.
		uint8_t x = m_Value.front();
		bool bDeserialize = (0x80 & x) || (x <= 8);
		if (bDeserialize)
		{
			try
			{
				Deserializer der;
				der.reset(m_Value);
				der & s;

				return; // ok
			}
			catch (const std::exception&) {
				// ignore error, fallback to standard case
				s.clear();
			}
		}

		s.assign((const char*) &m_Value.front(), m_Value.size());
	}

	void Asset::Metadata::get_Owner(PeerID& res, Key::IPKdf& pkdf) const
	{
		ECC::Point::Native pt;
		pkdf.DerivePKeyG(pt, m_Hash);
		res.Import(pt);
	}

	void Asset::Metadata::get_Owner(PeerID& res, const ContractID& cid) const
	{
		ECC::Hash::Processor()
			<< "bvm.a.own"
			<< cid
			<< m_Hash
			>> res;
	}

	bool Asset::CreateInfo::Recognize(Key::IPKdf& pkdf) const
	{
		PeerID pid;
		m_Metadata.get_Owner(pid, pkdf);
		return pid == m_Owner;
	}

	const ECC::Point::Compact& Asset::Base::get_H()
	{
		return ECC::Context::get().m_Ipp.H_.m_Fast.m_pPt[0];
	}

	void Asset::Base::get_GeneratorSafe(ECC::Point::Storage& pt_s) const
	{
		if (m_ID)
			get_Generator(pt_s);
		else
		{
			secp256k1_ge ge;
			get_H().Assign(ge);
			pt_s.FromNnz(ge);
		}
	}

	thread_local Asset::ID Asset::Proof::Params::s_AidMax_Override = 0;

	bool Asset::Proof::Params::IsNeeded(Asset::ID aid, Height hScheme)
	{
		if (aid)
			return true;

		if (!Rules::get().IsEnabledCA(hScheme))
			return false;

		return true;
	}


	struct Asset::Proof::CmList
		:public Sigma::CmList
	{
		Asset::ID m_Aid0;
		bool m_IsPastHF6;

		CmList(const Rules& r, Height hScheme)
		{
			m_IsPastHF6 = r.IsPastFork_<6>(hScheme);
		}

		bool get_At(ECC::Point::Storage& pt_s, uint32_t iIdx) override
		{
			Asset::ID aid = (m_IsPastHF6 && !iIdx) ? 0 : (m_Aid0 + iIdx);
			Base(aid).get_GeneratorSafe(pt_s);
			return true;
		}

		static void MakeRandom(ECC::Hash::Value&, const ECC::Scalar::Native& skGen);
		void SelectWindow(Asset::ID, const Rules&, const ECC::Scalar::Native& skGen);
		uint32_t SelectOffset(uint32_t n, uint32_t nMax, uint32_t N, ECC::Hash::Value&, const ECC::Scalar::Native& skGen, bool b);
	};

	void Asset::Proof::Create(Height hScheme, ECC::Point::Native& genBlinded, ECC::Scalar::Native& skInOut, Amount val, Asset::ID aid, const ECC::Hash::Value* phvSeed)
	{
		ECC::Point::Native gen;
		if (aid)
			Base(aid).get_Generator(gen);

		Create(hScheme, genBlinded, skInOut, val, aid, gen, phvSeed);
	}

	void Asset::Proof::Create(Height hScheme, ECC::Point::Native& genBlinded, ECC::Scalar::Native& skInOut, Amount val, Asset::ID aid, const ECC::Point::Native& gen, const ECC::Hash::Value* phvSeed)
	{
		ECC::NonceGenerator nonceGen("out-sk-asset");
		ECC::NoLeak<ECC::Scalar> k;

		k.V = skInOut;
		nonceGen << k.V.m_Value;

		ECC::Hash::Processor hp;
		hp
			<< aid
			<< val;
		if (phvSeed)
			hp << (*phvSeed);

		hp >> k.V.m_Value;
		nonceGen << k.V.m_Value;

		ECC::Scalar::Native skAsset;
		nonceGen >> skAsset;

		ModifySk(skInOut, skAsset, val);

		Create(hScheme, genBlinded, skAsset, aid, gen);
	}

	void Asset::Proof::Create(Height hScheme, ECC::Point::Native& genBlinded, const ECC::Scalar::Native& skGen, Asset::ID aid, const ECC::Point::Native& gen)
	{
		if (aid)
			genBlinded = gen;
		else
			Base::get_H().Assign(genBlinded, true); // not always specified explicitly for aid==0

		genBlinded += ECC::Context::get().G * skGen;
		m_hGen = genBlinded;

		const Rules& r = Rules::get();
		CmList lst(r, hScheme);

		lst.SelectWindow(aid, r, skGen);
		m_Begin = lst.m_Aid0;

		Sigma::Prover prover(lst, r.CA.m_ProofCfg, *this);
		prover.m_Witness.m_L = aid ? (aid - lst.m_Aid0) : 0; // should be correct, with both schemes
		prover.m_Witness.m_R = -skGen;

		ECC::Hash::Value hvSeed;
		ECC::Hash::Processor()
			<< "asset-pr-gen"
			<< skGen
			>> hvSeed;

		ECC::Oracle oracle;
		oracle << m_hGen;
		prover.Generate(hvSeed, oracle, genBlinded);
	}

	void Asset::Proof::ModifySk(ECC::Scalar::Native& skInOut, const ECC::Scalar::Native& skGen, Amount val)
	{
		// modify the blinding factor, to keep the original commitment
		ECC::Scalar::Native k = skGen* val;
		k = -k;
		skInOut += k;
	}

	void Asset::Proof::CmList::MakeRandom(ECC::Hash::Value& hv, const ECC::Scalar::Native& skGen)
	{
		ECC::Hash::Processor() << skGen >> hv; // pseudo-random
	}

	uint32_t Asset::Proof::CmList::SelectOffset(uint32_t n, uint32_t nMax, uint32_t N, ECC::Hash::Value& hv, const ECC::Scalar::Native& skGen, bool b)
	{
		assert(N);
		assert(n);

		if (nMax <  n)
			beam::setmax(nMax, n + N / 2); // guess

		if (nMax < N)
			return 0;

		// the whole range [0, nMax] is larger than N, means there's a degree of freedom to choose the window
		if (!b)
			MakeRandom(hv, skGen);

		// select the pos of our element within the window as uniform random
		uint32_t nPos;
		hv.ExportWord<0>(nPos);

		if (m_IsPastHF6)
			nPos = 1u + (nPos % (N - 1)); // our element position within [1, N)
		else
			nPos %= N; // our element position within [0, N)

		if (n <= nPos)
			return 0; // start saturated

		if (n - nPos + (N - 1) >= nMax)
			return nMax - (N - 1); // end saturated

		return n - nPos;
	}

	void Asset::Proof::CmList::SelectWindow(Asset::ID aid, const Rules& r, const ECC::Scalar::Native& skGen)
	{
		// Randomize m_Aid0
		m_Aid0 = 0;

		// 2 nuances to take into consideration:
		// (1) after HF6 the 0th element of the list is always the default asset (aid==0), the window affects only remaining elements
		// (2) For bridged network with foreign assets, there's a gap (usually very large). So basically we have 2 distinct ranges.

		uint32_t N = r.CA.m_ProofCfg.get_N();
		if (N <= 1)
			return; // should not happen

		uint32_t nCountForeign = std::min(N - 1, r.CA.ForeignEnd); // we have no reliable information, this is a guess
		uint32_t nCountLocal = (Params::s_AidMax_Override > r.CA.ForeignEnd) ? (Params::s_AidMax_Override - r.CA.ForeignEnd) : 0;

		ECC::Hash::Value hv;
		bool bHv = false;

		if (!aid)
		{
			if (!m_IsPastHF6)
				return;

			if (!nCountLocal && (nCountForeign < N))
				; // leave it as-is
			else
			{
				if (!nCountForeign && (nCountLocal < N))
					aid = r.CA.ForeignEnd;
				else
				{
					// choose a pseudo-random aid from either foreign or local ranges
					uint32_t nCountTotal = nCountForeign + nCountLocal; // won't overflow
					assert(nCountTotal);

					MakeRandom(hv, skGen);
					bHv = true;

					hv.ExportWord<1>(aid);
					aid %= nCountTotal;

					if (aid >= nCountForeign)
						aid += (r.CA.ForeignEnd - nCountForeign);
				}
			}

			aid++;
		}

		if (aid < r.CA.ForeignEnd)
			m_Aid0 = SelectOffset(aid, nCountForeign, N, hv, skGen, bHv);
		else
		{
			aid -= r.CA.ForeignEnd;
			m_Aid0 = SelectOffset(aid, nCountLocal, N, hv, skGen, bHv);
			m_Aid0 += r.CA.ForeignEnd;
		}
	}

	bool Asset::Proof::IsValidPrepare(ECC::Point::Native& hGen, ECC::InnerProduct::BatchContext& bc, ECC::Scalar::Native* pKs) const
	{
		ECC::Oracle oracle;
		oracle << m_hGen;

		ECC::Scalar::Native kBias;
		if (!Cast::Down<Sigma::Proof>(*this).IsValid(bc, oracle, Rules::get().CA.m_ProofCfg, pKs, kBias))
			return false;

		if (!hGen.ImportNnz(m_hGen))
			return false;

		bc.AddCasual(hGen, kBias, true); // the deferred part from m_Signature, plus the needed bias
		return true;
	}

	bool Asset::Proof::IsValid(Height hScheme, ECC::Point::Native& hGen) const
	{
		if (BatchContext::s_pInstance)
			return BatchContext::s_pInstance->IsValid(hScheme, hGen, *this);

		ECC::Mode::Scope scope(ECC::Mode::Fast);

		ECC::InnerProduct::BatchContextEx<1> bc;
		std::vector<ECC::Scalar::Native> vKs;

		const Rules& r = Rules::get();
		const Sigma::Cfg& cfg = r.CA.m_ProofCfg;
		uint32_t N = cfg.get_N();
		assert(N);
		vKs.resize(N);

		if (!IsValidPrepare(hGen, bc, &vKs.front()))
			return false;

		CmList lst(r, hScheme);
		lst.m_Aid0 = m_Begin;

		lst.Calculate(bc.m_Sum, 0, N, &vKs.front());

		return bc.Flush();
	}

	void Asset::Proof::Clone(Ptr& p) const
	{
		p = std::make_unique<Proof>();
		*p = *this;
	}

	void Asset::Proof::Expose(ECC::Oracle& oracle, Height hScheme, const Ptr& p)
	{
		if (Rules::get().IsPastFork_<3>(hScheme))
		{
			bool bAsset = !!p;
			oracle << bAsset;
			if (p)
				oracle << p->m_hGen;
		}
	}

	thread_local Asset::Proof::BatchContext* Asset::Proof::BatchContext::s_pInstance = nullptr;

	/////////////////////////////////////////////
	// FundsChangeMap
	void FundsChangeMap::Add(Amount val, Asset::ID aid, bool bSpend)
	{
		if (!val)
			return;

		AmountBig::Number valBig(val);
		if (bSpend)
			valBig = -valBig;

		Add(valBig, aid);
	}

	void FundsChangeMap::Add(const AmountBig::Number& valBig, Asset::ID aid)
	{
		assert(valBig != Zero);

		auto it = m_Map.find(aid);
		if (m_Map.end() == it)
			m_Map[aid] = valBig;
		else
		{
			auto& dst = it->second;
			dst += valBig;

			if (dst == Zero)
				m_Map.erase(it);
		}
	}

	void FundsChangeMap::ToCommitment(ECC::Point::Native& res) const
	{
		if (m_Map.empty())
			res = Zero;
		else
		{
			ECC::MultiMac_Dyn mm;
			mm.Prepare(static_cast<uint32_t>(m_Map.size()), 1);

			for (auto it = m_Map.begin(); m_Map.end() != it; it++)
			{
				ECC::Scalar::Native* pK;
				if (it->first)
				{
					CoinID::Generator gen(it->first);

					pK = mm.m_pKCasual + mm.m_Casual;
					mm.m_pCasual[mm.m_Casual++].Init(gen.m_hGen);
				}
				else
				{
					pK = mm.m_pKPrep + mm.m_Prepared;
					mm.m_ppPrepared[mm.m_Prepared++] = &ECC::Context::get().m_Ipp.H_;
				}

				ECC::Scalar s;

				auto& valBig = it->second;
				auto bNegative = valBig.get_Msb();
				if (bNegative)
				{
					AmountBig::Number dup = -valBig;
					s.m_Value.FromNumber(dup);
				}
				else
					s.m_Value.FromNumber(valBig);

				pK->Import(s);
				if (bNegative)
					*pK = -*pK;
			}

			mm.Calculate(res);
		}
	}

} // namespace beam
