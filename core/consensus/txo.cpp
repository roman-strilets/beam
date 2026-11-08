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

#include "core/consensus/txo.h"
#include "core/consensus/rules.h"
#include "core/consensus/impl_helpers.h"

namespace beam
{
	/////////////
	// TxStats
	void TxStats::Reset()
	{
		ZeroObject(*this);
	}

	void TxStats::operator += (const TxStats& s)
	{
		m_Fee				+= s.m_Fee;

		m_Kernels			+= s.m_Kernels;
		m_KernelsNonStd		+= s.m_KernelsNonStd;
		m_Inputs			+= s.m_Inputs;
		m_Outputs			+= s.m_Outputs;
		m_InputsShielded	+= s.m_InputsShielded;
		m_OutputsShielded	+= s.m_OutputsShielded;
		m_Contract			+= s.m_Contract;
		m_ContractSizeExtra	+= s.m_ContractSizeExtra;
	}

	/////////////
	// Input
	int TxElement::cmp(const TxElement& v) const
	{
		CMP_MEMBER_EX(m_Commitment)
		return 0;
	}

	Input& Input::operator = (const Input& v)
	{
		Cast::Down<TxElement>(*this) = v;
		return *this;
	}

	Input& Input::operator = (Input&& v) noexcept
	{
		if (*this != v)
		{
			Cast::Down<TxElement>(*this) = std::move(v);
		}
		return *this;
	}

	void Input::AddStats(TxStats& s) const
	{
		s.m_Inputs++;
	}

	/////////////
	// CoinID
	CoinID::Generator::Generator(Asset::ID aid)
	{
		if (aid)
			Asset::Base(aid).get_Generator(m_hGen);
	}

	void CoinID::Generator::AddValue(ECC::Point::Native& comm, Amount val) const
	{
		ECC::Tag::AddValue(comm, &m_hGen, val);
	}

	CoinID::Worker::Worker(const CoinID& cid)
		:Generator(cid.m_AssetID)
		,m_Cid(cid)
	{
	}

	bool CoinID::get_ChildKdfIndex(Key::Index& idx) const
	{
		Key::Index iSubkey = get_Subkey();
		Key::Index iScheme = get_Scheme();

		if (iScheme < Scheme::V3)
		{
			if (!iSubkey)
				return false; // by convention: before V3 , Subkey=0 - is a master key

			if (Scheme::BB21 == iScheme)
				return false; // BB2.1 workaround
		}

		if ((Scheme::V_Miner0 == iScheme) && !iSubkey)
			return false;

		idx = iSubkey;
		return true;
	}

	Key::IKdf::Ptr CoinID::get_ChildKdf(const Key::IKdf::Ptr& pMasterKdf) const
	{
		Key::Index iIdx;
		if (!get_ChildKdfIndex(iIdx))
			return pMasterKdf;

		return MasterKey::get_Child(*pMasterKdf, iIdx);
	}

	void CoinID::Worker::get_sk1(ECC::Scalar::Native& res, const ECC::Point::Native& comm0, const ECC::Point::Native& sk0_J)
	{
		ECC::Oracle()
			<< comm0
			<< sk0_J
			>> res;
	}

	void CoinID::Worker::AddValue(ECC::Point::Native& comm) const
	{
		Generator::AddValue(comm, m_Cid.m_Value);
	}

	void CoinID::get_Hash(ECC::Hash::Value& hv) const
	{
		Key::Index nScheme = get_Scheme();
		switch (nScheme)
		{
		case Scheme::V0:
			Cast::Down<Key::ID>(*this).get_Hash(hv); // legacy
			break;

		case Scheme::BB21:
			{
				// BB2.1 workaround
				CoinID cid2 = *this;
				cid2.set_Subkey(get_Subkey(), Scheme::V0);
				cid2.get_Hash(hv);
			}
			break;

		case Scheme::V_Miner0:
			{
				// miner0 workaround
				CoinID cid2 = *this;
				cid2.set_Subkey(get_Subkey(), Scheme::V3);
				cid2.get_Hash(hv);
			}
			break;

		default:
			{
				// newer scheme - account for the Value.
				// Make it infeasible to tamper with value or asset for unknown blinding factor
				ECC::Hash::Processor hp;
				hp
					<< "kidv-1"
					<< m_Idx
					<< m_Type.V
					<< m_SubIdx
					<< m_Value;

				if (m_AssetID)
				{
					hp
						<< "asset"
						<< m_AssetID;
				}

				hp >> hv;
			}
		}
	}

	std::ostream& operator << (std::ostream& s, const CoinID& x)
	{
		s
			<< "Key=" << x.m_Type
			<< "-" << x.get_Scheme()
			<< ":" << x.get_Subkey()
			<< ":" << x.m_Idx
			<< ", Value=" << AmountBig::Printable(x.m_Value);

		if (x.m_AssetID)
			s << ", AssetID=" << x.m_AssetID;

		return s;
	}

	void CoinID::Worker::CreateInternal(ECC::Scalar::Native& sk, ECC::Point::Native& comm, bool bComm, Key::IKdf& kdf) const
	{
		ECC::Hash::Value hv;
		m_Cid.get_Hash(hv);
		kdf.DeriveKey(sk, hv);

		comm = ECC::Context::get().G * sk;
		AddValue(comm);

		ECC::Point::Native sk0_J = ECC::Context::get().J * sk;

		ECC::Scalar::Native sk1;
		get_sk1(sk1, comm, sk0_J);

		sk += sk1;
		if (bComm)
			comm += ECC::Context::get().G * sk1;
	}

	void CoinID::Worker::Create(ECC::Scalar::Native& sk, Key::IKdf& kdf) const
	{
		ECC::Point::Native comm;
		CreateInternal(sk, comm, false, kdf);
	}

	void CoinID::Worker::Create(ECC::Scalar::Native& sk, ECC::Point::Native& comm, Key::IKdf& kdf) const
	{
		CreateInternal(sk, comm, true, kdf);
	}

	void CoinID::Worker::Create(ECC::Scalar::Native& sk, ECC::Point& comm, Key::IKdf& kdf) const
	{
		ECC::Point::Native comm2;
		Create(sk, comm2, kdf);
		comm = comm2;
	}

	void CoinID::Worker::Recover(ECC::Point::Native& res, Key::IPKdf& pkdf) const
	{
		ECC::Hash::Value hv;
		m_Cid.get_Hash(hv);

		ECC::Point::Native sk0_J;
		pkdf.DerivePKeyJ(sk0_J, hv);
		pkdf.DerivePKeyG(res, hv);

		Recover(res, sk0_J);
	}

	void CoinID::Worker::Recover(ECC::Point::Native& pkG_in_res_out, const ECC::Point::Native& pkJ) const
	{
		AddValue(pkG_in_res_out);

		ECC::Scalar::Native sk1;
		get_sk1(sk1, pkG_in_res_out, pkJ);

		pkG_in_res_out += ECC::Context::get().G * sk1;
	}

	/////////////
	// Output
	bool Output::IsValid(Height hScheme, ECC::Point::Native& comm) const
	{
		if (!comm.Import(m_Commitment))
			return false;

		if (!m_pAsset)
			return IsValid2(hScheme, comm, nullptr);

		if (!Rules::get().IsEnabledCA(hScheme))
			return false;

		ECC::Point::Native hGen;
		return
			m_pAsset->IsValid(hScheme, hGen) &&
			IsValid2(hScheme, comm, &hGen);
	}

	bool Output::IsValid2(Height hScheme, ECC::Point::Native& comm, const ECC::Point::Native* pGen) const
	{
		ECC::Oracle oracle;
		Prepare(oracle, hScheme);

		if (m_pConfidential)
		{
			if (m_Coinbase)
				return false; // coinbase must have visible amount

			if (m_pPublic)
				return false;

			return m_pConfidential->IsValid(comm, Rules::get().get_BpScheme(hScheme), oracle, pGen);
		}

		if (!m_pPublic)
			return false;

		if (m_Coinbase)
		{
			if (pGen)
				return false; // should contain unobscured default asset
		}
		else
		{
			if (!Rules::get().AllowPublicUtxos)
				return false;
		}

		return m_pPublic->IsValid(comm, oracle, pGen);
	}

	Output& Output::operator = (const Output& v)
	{
		Cast::Down<TxElement>(*this) = v;
		m_Coinbase = v.m_Coinbase;
		m_Incubation = v.m_Incubation;
		ClonePtr(m_pConfidential, v.m_pConfidential);
		ClonePtr(m_pPublic, v.m_pPublic);
		ClonePtr(m_pAsset, v.m_pAsset);
		return *this;
	}

	int Output::cmp(const Output& v) const
	{
		{
			int n = Cast::Down<TxElement>(*this).cmp(v);
			if (n)
				return n;
		}

		CMP_MEMBER(m_Coinbase)
		CMP_MEMBER(m_Incubation)
		CMP_MEMBER_PTR(m_pConfidential)
		CMP_MEMBER_PTR(m_pPublic)
		//CMP_MEMBER_PTR(m_pAsset)

		return 0;
	}

	void Output::AddStats(TxStats& s) const
	{
		s.m_Outputs++;
	}

#pragma pack (push, 1)
	struct Output::PackedKA
	{
		uintBigFor<Asset::ID>::Type m_AssetID;
		Key::ID::Packed m_Kid; // for historical reasons: Key::ID should be last. All new data should be added above.
	};
#pragma pack (pop)

	void Output::Create(Height hScheme, ECC::Scalar::Native& sk, Key::IKdf& coinKdf, const CoinID& cid, Key::IPKdf& tagKdf, OpCode::Enum eOp, const User* pUser)
	{
		CoinID::Worker wrk(cid);

		bool bUseCoinKdf = true;
		switch (eOp)
		{
		case OpCode::Mpc_1:
		case OpCode::Mpc_2:
			bUseCoinKdf = false;
			break;

		default:
			wrk.Create(sk, m_Commitment, coinKdf);
		}

		bool isPublic = (OpCode::Public == eOp) || m_Coinbase;

		ECC::Scalar::Native skSign = sk;
		if (!isPublic && Asset::Proof::Params::IsNeeded(cid.m_AssetID, hScheme))
		{
			ECC::Hash::Value hv;
			if (!bUseCoinKdf)
				cid.get_Hash(hv);

			m_pAsset = std::make_unique<Asset::Proof>();
			m_pAsset->Create(hScheme, wrk.m_hGen, skSign, cid.m_Value, cid.m_AssetID, wrk.m_hGen, bUseCoinKdf ? nullptr : &hv);
		}

		ECC::Oracle oracle;
		Prepare(oracle, hScheme);

		ECC::RangeProof::Params::Create cp;
		cp.m_Value = cid.m_Value;
		GenerateSeedKid(cp.m_Seed.V, m_Commitment, tagKdf);

		if (isPublic)
		{
			Key::ID::Packed kid;
			cp.m_Blob.p = &kid;
			cp.m_Blob.n = sizeof(kid);
			kid = cid;

			m_pPublic.reset(new ECC::RangeProof::Public);
			m_pPublic->m_Value = cid.m_Value;
			m_pPublic->Create(skSign, cp, oracle);
		}
		else
		{
			PackedKA kida;
			cp.m_Blob.p = &kida;
			cp.m_Blob.n = sizeof(kida);
			kida.m_Kid = cid;
			kida.m_AssetID = cid.m_AssetID;

			if (OpCode::Mpc_2 != eOp)
				m_pConfidential.reset(new ECC::RangeProof::Confidential);
			else
				assert(m_pConfidential);

			ECC::Scalar::Native pKExtra[_countof(pUser->m_pExtra)];
			if (pUser)
			{
				for (uint32_t i = 0; i < static_cast<uint32_t>(_countof(pUser->m_pExtra)); i++)
					pKExtra[i] = pUser->m_pExtra[i];

				cp.m_pExtra = pKExtra;
			}

			auto iVersion = Rules::get().get_BpScheme(hScheme);
			if (bUseCoinKdf)
				m_pConfidential->Create(skSign, cp, iVersion, oracle, &wrk.m_hGen);
			else
			{
				ECC::RangeProof::Confidential::Nonces nonces; // not required, leave it zero (set in c'tor)

				if (OpCode::Mpc_1 == eOp)
				{
					ZeroObject(m_pConfidential->m_Part2);
					m_pConfidential->CoSign(nonces, skSign, cp, iVersion, oracle, ECC::RangeProof::Confidential::Phase::Step2, &wrk.m_hGen); // stop after Part2
				}
				else
				{
					// by now Part2 and Part3 
					m_pConfidential->CoSign(nonces, skSign, cp, iVersion, oracle, ECC::RangeProof::Confidential::Phase::Finalize, &wrk.m_hGen);
				}
			}
		}
	}

	void Output::GenerateSeedKid(ECC::uintBig& seed, const ECC::Point& commitment, Key::IPKdf& tagKdf)
	{
		ECC::Hash::Processor() << commitment >> seed;

		ECC::Scalar::Native sk;
		tagKdf.DerivePKey(sk, seed);

		ECC::Hash::Processor() << sk >> seed;
	}

	void Output::Prepare(ECC::Oracle& oracle, Height hScheme) const
	{
		oracle << m_Incubation;

		if (Rules::get().IsPastFork_<1>(hScheme))
		{
			oracle << m_Commitment;
			Asset::Proof::Expose(oracle, hScheme, m_pAsset);
		}
	}

	bool Output::Recover(Height hScheme, Key::IPKdf& tagKdf, CoinID& cid, User* pUser) const
	{
		ECC::RangeProof::Params::Recover cp;
		GenerateSeedKid(cp.m_Seed.V, m_Commitment, tagKdf);

		ECC::Oracle oracle;
		Prepare(oracle, hScheme);

		if (m_pConfidential)
		{
			PackedKA kida;
			cp.m_Blob.p = &kida;
			cp.m_Blob.n = sizeof(kida);

			ECC::Scalar::Native pKExtra[_countof(pUser->m_pExtra)];
			if (pUser)
				cp.m_pExtra = pKExtra;

			if (!m_pConfidential->Recover(Rules::get().get_BpScheme(hScheme), oracle, cp))
				return false;

			Cast::Down<Key::ID>(cid) = kida.m_Kid;
			kida.m_AssetID.Export(cid.m_AssetID);

			if (pUser)
			{
				for (uint32_t i = 0; i < static_cast<uint32_t>(_countof(pUser->m_pExtra)); i++)
					pUser->m_pExtra[i] = pKExtra[i];
			}
		}
		else
		{
			if (!m_pPublic)
				return false;

			Key::ID::Packed kid;
			cp.m_Blob.p = &kid;
			cp.m_Blob.n = sizeof(kid);

			if (!m_pPublic->Recover(cp))
				return false;

			Cast::Down<Key::ID>(cid) = kid;
			cid.m_AssetID = 0; // can't be recovered atm

			if (pUser)
				ZeroObject(*pUser); // can't be recovered
		}


		// Skip further verification, assuming no need to fully reconstruct the commitment
		cid.m_Value = cp.m_Value;

		return true;
	}

	bool Output::VerifyRecovered(Key::IPKdf& coinKdf, const CoinID& cid) const
	{
		// reconstruct the commitment
		ECC::Mode::Scope scope(ECC::Mode::Fast);

		ECC::Point::Native comm, comm2;
		if (!comm2.Import(m_Commitment))
			return false;

		CoinID::Worker(cid).Recover(comm, coinKdf);

		comm = -comm;
		comm += comm2;
		
		return (comm == Zero);
	}

	Height Output::get_MinMaturity(Height h) const
	{
		if (m_Coinbase)
			HeightAdd(h, Rules::get().Maturity.Coinbase);
		HeightAdd(h, m_Incubation);
		return h;
	}

	void ShieldedTxo::DescriptionOutp::get_Hash(Merkle::Hash& hv) const
	{
		ECC::Hash::Processor()
			<< "stxo-out"
			<< m_ID
			<< m_Height
			<< m_SerialPub
			<< m_Commitment
			>> hv;
	}

	void ShieldedTxo::DescriptionInp::get_Hash(Merkle::Hash& hv) const
	{
		ECC::Hash::Processor()
			<< "stxo-in"
			<< m_Height
			<< m_SpendPk
			>> hv;
	}

} // namespace beam
