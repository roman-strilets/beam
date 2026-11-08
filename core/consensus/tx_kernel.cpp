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

#include "core/consensus/tx_kernel.h"
#include "core/consensus/serialization_adapters.h"
#include "core/consensus/transaction.h"
#include "core/consensus/rules.h"
#include "core/consensus/impl_helpers.h"

namespace beam
{
	/////////////
	// TxKernel
	const ECC::Hash::Value& TxKernelStd::HashLock::get_Image(ECC::Hash::Value& hv) const
	{
		if (m_IsImage)
			return m_Value;

		ECC::Hash::Processor() << m_Value >> hv;
		return hv;
	}

	void TxKernel::HashBase(ECC::Hash::Processor& hp) const
	{
		hp	<< m_Fee
			<< m_Height.m_Min
			<< m_Height.m_Max;
	}

	void TxKernel::HashNested(ECC::Hash::Processor& hp) const
	{
		for (auto it = m_vNested.begin(); ; ++it)
		{
			bool bBreak = (m_vNested.end() == it);
			hp << bBreak;

			if (bBreak)
				break;

			hp << (*it)->get_ID();
		}
	}

	bool TxKernelStd::HasNonStd() const
	{
		for (auto it = m_vNested.begin(); m_vNested.end() != it; ++it)
		{
			const TxKernel& v = *(*it);
			if (v.HasNonStd())
				return true;
		}

		return false;
	}

	void TxKernelStd::CalculateID() const
	{
		ECC::Hash::Processor hp;
		HashBase(hp);

		uint8_t nFlags =
			(m_pHashLock ? 1 : 0) |
			(m_pRelativeLock ? 2 : 0) |
			(m_CanEmbed ? 4 : 0);

		hp	<< m_Commitment
			<< Amount(0) // former m_AssetEmission
			<< nFlags;

		if (m_pHashLock)
		{
			ECC::Hash::Value hv;
			hp << m_pHashLock->get_Image(hv);
		}

		if (m_pRelativeLock)
		{
			hp
				<< m_pRelativeLock->m_ID
				<< m_pRelativeLock->m_LockHeight;
		}

		HashNested(hp);
		hp >> m_Lazy_ID.m_Value;
		m_Lazy_ID.m_Valid = true;
	}

	bool TxKernel::IsValid(Height hScheme, std::string* psErr /* = nullptr */) const
	{
		try {
			ECC::Point::Native exc;
			TestValid(hScheme, exc);
		} catch (const std::exception& e) {
			if (psErr)
				(*psErr) = e.what();
			return false;
		}

		return true;
	}

	HeightRange TxKernel::get_EffectiveHeightRange() const
	{
		HeightRange hr = m_Height;
		if (!hr.IsEmpty())
		{
			auto& r = Rules::get();
			if (r.IsPastFork_<2>(hr.m_Min) && (hr.m_Max - hr.m_Min > r.MaxKernelValidityDH))
				hr.m_Max = hr.m_Min + r.MaxKernelValidityDH;
		}

		return hr;
	}

	void TxKernel::AddFees(AmountBig::Number& res) const
	{
		res += AmountBig::Number(m_Fee);
		AddFees(res, m_vNested);
	}

	void TxKernel::AddFees(AmountBig::Number& res, const std::vector<Ptr>& v)
	{
		for (auto it = v.begin(); v.end() != it; ++it)
			 (*it)->AddFees(res);
	}

	void TxKernel::TestValidBase(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent, ECC::Point::Native* pComm) const
	{
		const Rules& r = Rules::get(); // alias
		if (m_CanEmbed)
			r.TestForkAtLeast_<1>(hScheme);

		if (pParent)
		{
			if (!m_CanEmbed)
				Exc::Fail();

			// nested kernel restrictions
			if ((m_Height.m_Min > pParent->m_Height.m_Min) ||
				(m_Height.m_Max < pParent->m_Height.m_Max))
				Exc::Fail(); // parent Height range must be contained in ours.
		}

		if (!m_vNested.empty())
		{
			ECC::Point::Native excNested(Zero);

			const TxKernel* p0Krn = nullptr;
			for (auto it = m_vNested.begin(); m_vNested.end() != it; ++it)
			{
				const TxKernel& v = *(*it);
				TxKernel::Checkpoint cp(v);

				// sort for nested kernels is not important. But for 'historical' reasons it's enforced up to Fork2
				// Remove this code once Fork2 is reached iff no multiple nested kernels
				if (!r.IsPastFork_<2>(hScheme) && p0Krn && (*p0Krn > v))
					TxBase::Fail_Order();
				p0Krn = &v;

				v.TestValid(hScheme, excNested, this);
			}

			if (!r.IsPastFork_<2>(hScheme))
			{
				// Prior to Fork2 the parent commitment was supposed to include the nested. But nested kernels are unlikely to be seen up to Fork2.
				// Remove this code once Fork2 is reached iff no such kernels exist
				if (!pComm)
					Exc::Fail();
				excNested = -excNested;
				(*pComm) += excNested;
			}
			else
				exc += excNested;
		}
	}

#define THE_MACRO(id, name) \
	TxKernel::Subtype::Enum TxKernel##name::get_Subtype() const \
	{ \
		return Subtype::name; \
	}

	BeamKernelsAll(THE_MACRO)
#undef THE_MACRO


	void TxKernelStd::TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent /* = nullptr */) const
	{
		const Rules& r = Rules::get(); // alias
		if (m_pRelativeLock)
		{
			r.TestForkAtLeast_<1>(hScheme);

			if (r.IsPastFork_<2>(hScheme) && !m_pRelativeLock->m_LockHeight)
				Exc::Fail(); // zero m_LockHeight makes no sense, but allowed prior to Fork2
		}

		ECC::Point::Native pt;
		pt.ImportNnzStrict(m_Commitment);

		exc += pt;

		TestValidBase(hScheme, exc, pParent, &pt);

		if (!m_Signature.IsValid(get_ID(), pt))
			TxBase::Fail_Signature();
	}

	void TxKernel::AddStats(TxStats& s) const
	{
		s.m_Kernels++;
		s.m_Fee += MultiWord::From(m_Fee);

		for (auto it = m_vNested.begin(); m_vNested.end() != it; ++it)
			(*it)->AddStats(s);
	}

	int TxKernel::cmp(const TxKernel& v) const
	{
		bool b1 = HasNonStd();
		bool b2 = v.HasNonStd();
		CMP_SIMPLE(b1, b2)

		if (b1)
			return 0; // no sort for non-std kernels (always keep their order)

		Subtype::Enum t0 = get_Subtype();
		Subtype::Enum t1 = v.get_Subtype();
		CMP_SIMPLE(t0, t1)

		return cmp_Subtype(v);
	}

	int TxKernel::cmp_Subtype(const TxKernel&) const
	{
		return 0; // currentl unreachable: all kernels besides TxKernelStd are defined as 'non-standard', and should not be compared
	}

	int TxKernelStd::cmp_Subtype(const TxKernel& v_) const
	{
		const TxKernelStd& v = Cast::Up<TxKernelStd>(v_);

		CMP_MEMBER_EX(m_Commitment)
		CMP_MEMBER_EX(m_Signature)
		CMP_MEMBER(m_Fee)
		CMP_MEMBER(m_Height.m_Min)
		CMP_MEMBER(m_Height.m_Max)

		auto it0 = m_vNested.begin();
		auto it1 = v.m_vNested.begin();

		for ( ; m_vNested.end() != it0; ++it0, ++it1)
		{
			if (v.m_vNested.end() == it1)
				return 1;

			int n = (*it0)->cmp(*(*it1));
			if (n)
				return n;
		}

		if (v.m_vNested.end() != it1)
			return -1;

		CMP_MEMBER_PTR(m_pHashLock)
		CMP_MEMBER_PTR(m_pRelativeLock)

		return 0;
	}

	int TxKernelStd::HashLock::cmp(const HashLock& v) const
	{
		CMP_MEMBER_EX(m_Value)
		return 0;
	}

	int TxKernelStd::RelativeLock::cmp(const RelativeLock& v) const
	{
		CMP_MEMBER_EX(m_ID)
		CMP_MEMBER(m_LockHeight)
		return 0;
	}

	void TxKernelStd::Sign(const ECC::Scalar::Native& sk)
	{
		m_Commitment = ECC::Context::get().G * sk;
		CalculateID();

		m_Signature.Sign(m_Lazy_ID.get(), sk);
	}

	void TxKernel::CopyFrom(const TxKernel& v)
	{
		m_Lazy_ID = v.m_Lazy_ID;
		m_Fee = v.m_Fee;
		m_Height = v.m_Height;
		m_CanEmbed = v.m_CanEmbed;

		m_vNested.resize(v.m_vNested.size());

		for (size_t i = 0; i < v.m_vNested.size(); i++)
			v.m_vNested[i]->Clone(m_vNested[i]);
	}

	void TxKernelStd::Clone(TxKernel::Ptr& p) const
	{
		p.reset(new TxKernelStd);
		TxKernelStd& v = Cast::Up<TxKernelStd>(*p);

		v.CopyFrom(*this);
		v.m_Commitment = m_Commitment;
		v.m_Signature = m_Signature;
		ClonePtr(v.m_pHashLock, m_pHashLock);
		ClonePtr(v.m_pRelativeLock, m_pRelativeLock);
	}

	bool TxKernel::IWalker::Process(const std::vector<TxKernel::Ptr>& v)
	{
		for (size_t i = 0; i < v.size(); i++)
			if (!Process(*v[i]))
				return false;
		return true;
	}

	bool TxKernel::IWalker::Process(const TxKernel& krn)
	{
		bool bRet = 
			Process(krn.m_vNested) &&
			OnKrn(krn);

		if (bRet)
			m_nKrnIdx++;

		return bRet;
	}

	bool TxKernelNonStd::HasNonStd() const
	{
		return true;
	}

	void TxKernelNonStd::CalculateID() const
	{
		ECC::Hash::Processor hp;
		hp << get_Msg();
		HashSelfForID(hp);

		hp >> m_Lazy_ID.m_Value;
		m_Lazy_ID.m_Valid = true;
	}

	void TxKernelNonStd::CalculateMsg() const
	{
		ECC::Hash::Processor hp;

		HashBase(hp);

		ECC::Point comm(Zero); // invalid point, avoid collision with Std kernel, which provides the commitment here
		bool bFlag = m_CanEmbed && Rules::get().IsPastFork_<3>(m_Height.m_Min);
		comm.m_Y = bFlag ? 2 : 1;

		hp
			<< comm
			<< static_cast<uint32_t>(get_Subtype());

		HashNested(hp);
		HashSelfForMsg(hp);

		hp >> m_Lazy_Msg.m_Value;
		m_Lazy_Msg.m_Valid = true;

		m_Lazy_ID.Invalidate();
	}

	void TxKernelNonStd::CopyFrom(const TxKernelNonStd& v)
	{
		TxKernel::CopyFrom(v);
		m_Lazy_Msg = v.m_Lazy_Msg;
	}

	void TxKernelNonStd::AddStats(TxStats& s) const
	{
		TxKernel::AddStats(s);
		s.m_KernelsNonStd++;
	}

	/////////////
	// TxKernelAssetControl
	void TxKernelAssetControl::HashSelfForMsg(ECC::Hash::Processor& hp) const
	{
		hp
			<< m_Commitment
			<< m_Owner;
	}

	void TxKernelAssetControl::HashSelfForID(ECC::Hash::Processor& hp) const
	{
		hp.Serialize(m_Signature);
	}

	void TxKernelAssetControl::TestValidAssetCtl(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent) const
	{
		TestValidBase(hScheme, exc, pParent);

		const Rules& r = Rules::get(); // alias
		r.TestForkAtLeast_<2>(hScheme);
		r.TestEnabledCA();

		ECC::Point::Native pPt[2];
		pPt[0].ImportNnzStrict(m_Commitment);

		exc += pPt[0];

		if (!m_Owner.ExportNnz(pPt[1]))
			ECC::Point::Native::Fail();

		assert(m_Owner != Zero); // the above ensures this

		// prover must prove knowledge of excess AND m_Owner sk
		if (!m_Signature.IsValid(ECC::Context::get().m_Sig.m_CfgG2, get_Msg(), m_Signature.m_pK, pPt))
			TxBase::Fail_Signature();
	}

	void TxKernelAssetControl::CopyFrom(const TxKernelAssetControl& v)
	{
		TxKernelNonStd::CopyFrom(v);
		m_Commitment = v.m_Commitment;
		m_Signature = v.m_Signature;
		m_Owner = v.m_Owner;
	}

	void TxKernelAssetControl::Sign_(const ECC::Scalar::Native& sk, const ECC::Scalar::Native& skAsset)
	{
		m_Commitment = ECC::Context::get().G * sk;
		CalculateMsg();

		ECC::Scalar::Native pSk[2] = { sk, skAsset };
		ECC::Scalar::Native res;
		m_Signature.Sign(ECC::Context::get().m_Sig.m_CfgG2, m_Lazy_Msg.get(), m_Signature.m_pK, pSk, &res);
	}

	void TxKernelAssetControl::Sign(const ECC::Scalar::Native& sk, Key::IKdf& kdf, const Asset::Metadata& md)
	{
		ECC::Scalar::Native skAsset;
		kdf.DeriveKey(skAsset, md.m_Hash);

		m_Owner.FromSk(skAsset);

		Sign_(sk, skAsset);
	}

	void TxKernelAssetControl::get_Sk(ECC::Scalar::Native& sk, Key::IKdf& kdf)
	{
		m_Commitment = Zero;
		CalculateMsg();

		Merkle::Hash hv;

		ECC::Hash::Processor()
			<< "ac.sk"
			<< m_Lazy_Msg.get()
			>> hv;

		kdf.DeriveKey(sk, hv);
	}

	/////////////
	// TxKernelAssetEmit
	void TxKernelAssetEmit::HashSelfForMsg(ECC::Hash::Processor& hp) const
	{
		TxKernelAssetControl::HashSelfForMsg(hp);
		hp
			<< m_AssetID
			<< Amount(m_Value);
	}

	void TxKernelAssetEmit::TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent /* = nullptr */) const
	{
		TestValidAssetCtl(hScheme, exc, pParent);

		if (!m_Value)
			Exc::Fail();

		bool isPositive;
		auto val = SplitAmountSigned(m_Value, isPositive);

		if (!m_AssetID)
		{
			// this is allowed for bridged networks
			ECC::Mode::Scope scope(ECC::Mode::Fast);

			ECC::Point::Native pt = ECC::Context::get().H * val;
			if (isPositive)
				pt = -pt;

			exc += pt;
		}
		else
		{
			CoinID::Generator g(m_AssetID);

			// In case of block validation with multiple asset instructions it's better to calculate this via MultiMac than multiplying each point separately
			if (isPositive)
				g.m_hGen = -g.m_hGen;

			g.AddValue(exc, val);
		}
	}

	void TxKernelAssetEmit::Clone(TxKernel::Ptr& p) const
	{
		p.reset(new TxKernelAssetEmit);
		TxKernelAssetEmit& v = Cast::Up<TxKernelAssetEmit>(*p);

		v.CopyFrom(*this);
		v.m_AssetID = m_AssetID;
		v.m_Value = m_Value;
	}

	/////////////
	// TxKernelAssetCreate
	void TxKernelAssetCreate::TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent /* = nullptr */) const
	{
		TestValidAssetCtl(hScheme, exc, pParent);

		if (m_MetaData.m_Value.size() > Asset::Info::s_MetadataMaxSize)
			Exc::Fail();

		ECC::Point::Native pt = ECC::Context::get().H * Rules::get().get_DepositForCA(hScheme);
		exc += pt;
	}

	void TxKernelAssetCreate::Clone(TxKernel::Ptr& p) const
	{
		p.reset(new TxKernelAssetCreate);
		TxKernelAssetCreate& v = Cast::Up<TxKernelAssetCreate>(*p);

		v.CopyFrom(*this);
		v.m_MetaData = m_MetaData;
	}

	void TxKernelAssetCreate::HashSelfForMsg(ECC::Hash::Processor& hp) const
	{
		TxKernelAssetControl::HashSelfForMsg(hp);
		hp << m_MetaData.m_Hash;
	}

	void TxKernelAssetCreate::Sign(const ECC::Scalar::Native& sk, Key::IKdf& kdf)
	{
		TxKernelAssetControl::Sign(sk, kdf, m_MetaData);
	}

	/////////////
	// TxKernelAssetDestroy
	void TxKernelAssetDestroy::HashSelfForMsg(ECC::Hash::Processor& hp) const
	{
		TxKernelAssetControl::HashSelfForMsg(hp);
		hp << m_AssetID;

		if (IsCustomDeposit())
			hp << m_Deposit;
	}

	bool TxKernelAssetDestroy::IsCustomDeposit() const
	{
		return Rules::get().IsPastFork_<5>(m_Height.m_Min);
	}

	Amount TxKernelAssetDestroy::get_Deposit() const
	{
		return IsCustomDeposit() ? m_Deposit : Rules::get().CA.DepositForList2;
	}

	void TxKernelAssetDestroy::TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent /* = nullptr */) const
	{
		TestValidAssetCtl(hScheme, exc, pParent);

		ECC::Point::Native pt = ECC::Context::get().H * get_Deposit();

		pt = -pt;
		exc += pt;
	}

	void TxKernelAssetDestroy::Clone(TxKernel::Ptr& p) const
	{
		p.reset(new TxKernelAssetDestroy);
		TxKernelAssetDestroy& v = Cast::Up<TxKernelAssetDestroy>(*p);

		v.CopyFrom(*this);
		v.m_AssetID = m_AssetID;
		v.m_Deposit = m_Deposit;
	}

	/////////////
	// TxKernelShieldedOutput
	void TxKernelShieldedOutput::TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent /* = nullptr */) const
	{
		TestValidBase(hScheme, exc, pParent);

		const Rules& r = Rules::get(); // alias
		r.TestForkAtLeast_<2>(hScheme);
		r.TestEnabledShielded();

		if (m_Txo.m_pAsset)
			r.TestEnabledCA();

		ECC::Oracle oracle;
		oracle << get_Msg();

		ECC::Point::Native comm, ser;
		if (!m_Txo.IsValid(oracle, hScheme, comm, ser))
			TxBase::Fail_Signature();

		exc += comm;
	}

	void TxKernelShieldedOutput::HashSelfForMsg(ECC::Hash::Processor& hp) const
	{
	}

	void TxKernelShieldedOutput::HashSelfForID(ECC::Hash::Processor& hp) const
	{
		hp.Serialize(m_Txo.m_RangeProof);
	}

	void TxKernelShieldedOutput::Clone(TxKernel::Ptr& p) const
	{
		p.reset(new TxKernelShieldedOutput);
		TxKernelShieldedOutput& v = Cast::Up<TxKernelShieldedOutput>(*p);

		v.CopyFrom(*this);
		v.m_Txo = m_Txo;
	}

	void TxKernelShieldedOutput::AddStats(TxStats& s) const
	{
		TxKernelNonStd::AddStats(s);
		s.m_OutputsShielded++;
	}

	/////////////
	// TxKernelShieldedInput
	void TxKernelShieldedInput::TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent /* = nullptr */) const
	{
		TestValidBase(hScheme, exc, pParent);

		const Rules& r = Rules::get(); // alias
		r.TestForkAtLeast_<2>(hScheme);
		r.TestEnabledShielded();

		ECC::Point ptNeg = m_SpendProof.m_Commitment;
		ptNeg.m_Y = !ptNeg.m_Y; // probably faster than negating the result

		ECC::Point::Native comm;
		if (m_pAsset)
		{
			r.TestEnabledCA();

			if (!m_pAsset->IsValid(hScheme, comm))
				TxBase::Fail_Signature();
		}

		comm.ImportNnzStrict(ptNeg);
		exc += comm;
		// Spend proof verification is not done here
	}

	void TxKernelShieldedInput::HashSelfForMsg(ECC::Hash::Processor& hp) const
	{
		hp << m_WindowEnd;
	}

	void TxKernelShieldedInput::HashSelfForID(ECC::Hash::Processor& hp) const
	{
		hp.Serialize(m_SpendProof);
	}

	void TxKernelShieldedInput::Clone(TxKernel::Ptr& p) const
	{
		p.reset(new TxKernelShieldedInput);
		TxKernelShieldedInput& v = Cast::Up<TxKernelShieldedInput>(*p);

		v.CopyFrom(*this);
		v.m_WindowEnd = m_WindowEnd;
		v.m_SpendProof = m_SpendProof;
		v.m_NotSerialized = m_NotSerialized;

		if (m_pAsset)
			m_pAsset->Clone(v.m_pAsset);
		else
			v.m_pAsset.reset();
	}

	void TxKernelShieldedInput::AddStats(TxStats& s) const
	{
		TxKernelNonStd::AddStats(s);
		s.m_InputsShielded++;
	}

	void TxKernelShieldedInput::Sign(Lelantus::Prover& p, Asset::ID aid)
	{
		ECC::Oracle oracle;
		oracle << get_Msg();

		if (Rules::get().IsPastFork_<3>(m_Height.m_Min))
			oracle << m_NotSerialized.m_hvShieldedState;

		// auto-generate seed for sigma proof and m_R_Output
		ECC::NoLeak<ECC::uintBig> hvSeed;
		ECC::GenRandom(hvSeed.V); // use both deterministic and random

		Lelantus::Prover::Witness& w = p.m_Witness; // alias

		ECC::Oracle(oracle) // copy
			<< hvSeed.V
			<< w.m_L
			<< w.m_V
			<< w.m_R
			<< w.m_R_Output
			<< w.m_SpendSk
			>> hvSeed.V;

		ECC::NonceGenerator("krn.sh.i")
			<< hvSeed.V
			>> hvSeed.V;

		ECC::Point::Native hGen;
		if (aid)
			Asset::Base(aid).get_Generator(hGen);

		if (Asset::Proof::Params::IsNeeded(aid, m_Height.m_Min))
		{
			m_pAsset = std::make_unique<Asset::Proof>();
			w.m_R_Adj = w.m_R_Output;
			m_pAsset->Create(m_Height.m_Min, hGen, w.m_R_Adj, w.m_V, aid, hGen, &hvSeed.V);
		}

		Asset::Proof::Expose(oracle, m_Height.m_Min, m_pAsset);

		p.Generate(hvSeed.V, oracle, &hGen);

		m_Lazy_ID.Invalidate();
	}

	/////////////
	// TxKernelContractControl
	void TxKernelContractControl::TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent /* = nullptr */) const
	{
		TestValidBase(hScheme, exc, pParent);

		const Rules& r = Rules::get(); // alias
		r.TestForkAtLeast_<3>(hScheme);

		ECC::Point::Native comm;
		comm.ImportNnzStrict(m_Commitment);
		exc += comm;
		// the rest is deferred till the context-bound validation
	}

	void TxKernelContractControl::CopyFrom(const TxKernelContractControl& v)
	{
		TxKernelNonStd::CopyFrom(v);
		m_Commitment = v.m_Commitment;
		m_Signature = v.m_Signature;
		m_Args = v.m_Args;
		m_Dependent = v.m_Dependent;
	}

	void TxKernelContractControl::HashSelfForMsg(ECC::Hash::Processor& hp) const
	{
		hp
			<< m_Commitment
			<< m_Args.size()
			<< Blob(m_Args);
	}

	void TxKernelContractControl::HashSelfForID(ECC::Hash::Processor& hp) const
	{
		hp.Serialize(m_Signature);
	}

	void TxKernelContractControl::Prepare(ECC::Hash::Processor& hp, const Merkle::Hash* pParentCtx) const
	{
		hp << get_Msg();
		if (m_Dependent)
		{
			assert(pParentCtx);
			if (Rules::get().IsPastFork_<4>(m_Height.m_Min))
				hp << *pParentCtx;
		}
	}

	void TxKernelContractControl::Sign(const ECC::Scalar::Native* pK, uint32_t nKeys, const ECC::Point::Native& ptFunds, const Merkle::Hash* pParentCtx)
	{
		assert(nKeys);
		ECC::Point::Native pt = ECC::Context::get().G * pK[nKeys - 1];
		pt += ptFunds;
		m_Commitment = pt;

		CalculateMsg();

		ECC::Hash::Processor hp;
		Prepare(hp, pParentCtx);

		for (uint32_t i = 0; i + 1 < nKeys; i++)
		{
			pt = ECC::Context::get().G * pK[i];
			hp << pt;
		}

		ECC::Hash::Value hv;
		hp
			<< m_Commitment
			>> hv;

		ECC::SignatureBase::Config cfg = ECC::Context::get().m_Sig.m_CfgG1; // copy
		cfg.m_nKeys = nKeys;

		ECC::Scalar::Native res;
		ECC::SignatureBase& sig = m_Signature;
		sig.Sign(cfg, hv, &m_Signature.m_k, pK, &res);
	}

	void TxKernelContractControl::AddStats(TxStats& s) const
	{
		TxKernelNonStd::AddStats(s);
		s.m_Contract++;
		s.m_ContractSizeExtra += static_cast<uint32_t>(m_Args.size());
	}

	/////////////
	// TxKernelContractCreate
	void TxKernelContractCreate::Clone(TxKernel::Ptr& p) const
	{
		p.reset(new TxKernelContractCreate);
		TxKernelContractCreate& v = Cast::Up<TxKernelContractCreate>(*p);

		v.CopyFrom(*this);
		v.m_Data = m_Data;
	}

	void TxKernelContractCreate::HashSelfForMsg(ECC::Hash::Processor& hp) const
	{
		TxKernelContractControl::HashSelfForMsg(hp);
		hp
			<< m_Data.size()
			<< Blob(m_Data);
	}

	void TxKernelContractCreate::AddStats(TxStats& s) const
	{
		TxKernelContractControl::AddStats(s);
		s.m_ContractSizeExtra += static_cast<uint32_t>(m_Data.size());
	}

	/////////////
	// TxKernelContractInvoke
	void TxKernelContractInvoke::Clone(TxKernel::Ptr& p) const
	{
		p.reset(new TxKernelContractInvoke);
		TxKernelContractInvoke& v = Cast::Up<TxKernelContractInvoke>(*p);

		v.CopyFrom(*this);
		v.m_Cid = m_Cid;
		v.m_iMethod = m_iMethod;
	}

	void TxKernelContractInvoke::HashSelfForMsg(ECC::Hash::Processor& hp) const
	{
		TxKernelContractControl::HashSelfForMsg(hp);
		hp
			<< m_Cid
			<< m_iMethod;
	}

	/////////////
	// TxKernelEvmInvoke
	void TxKernelEvmInvoke::Clone(TxKernel::Ptr& p) const
	{
		p.reset(new TxKernelEvmInvoke);
		TxKernelEvmInvoke& v = Cast::Up<TxKernelEvmInvoke>(*p);

		v.CopyFrom(*this);
		v.m_From = m_From;
		v.m_To = m_To;
		v.m_Nonce = m_Nonce;
		v.m_CallValue = m_CallValue;
		v.m_Subsidy = m_Subsidy;
	}

	void TxKernelEvmInvoke::HashSelfForMsg(ECC::Hash::Processor& hp) const
	{
		TxKernelContractControl::HashSelfForMsg(hp);
		hp
			<< Cast::Down<Evm::Address::Base>(m_From)
			<< Cast::Down<Evm::Address::Base>(m_To)
			<< m_Nonce
			<< m_CallValue
			<< static_cast<Amount>(m_Subsidy);
	}

	void TxKernelEvmInvoke::get_SubsidyCorrection(ECC::Point::Native& pt, bool isVerifying) const
	{
		if (m_Subsidy)
		{
			bool isPositive;
			Amount val = SplitAmountSigned(m_Subsidy, isPositive);

			ECC::Mode::Scope scope(ECC::Mode::Fast);

			ECC::Point::Native ptFunds = ECC::Context::get().H * val;
			if (isPositive == isVerifying)
				ptFunds = -ptFunds;

			pt += ptFunds;
		}
	}

	void TxKernelEvmInvoke::TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent /* = nullptr */) const
	{
		TestValidBase(hScheme, exc, pParent);

		const Rules& r = Rules::get(); // alias
		r.TestForkAtLeast_<6>(hScheme);

		ECC::Point::Native pt;
		pt.ImportNnzStrict(m_Commitment);
		exc += pt;

		// Compute pubkey from the signature, and compare to the caller address
		get_SubsidyCorrection(pt, true);

		ECC::Point::Native ptPk;
		if (!m_Signature.RecoverPubKey(ECC::Context::get().m_Sig.m_CfgG2, get_Msg(), &m_Signature.m_k, &pt, 0, ptPk))
			TxBase::Fail_Signature();

		Evm::Address addr;
		addr.FromPubKey(ptPk);

		if (addr != m_From)
			TxBase::Fail_Signature();
	}

	void TxKernelEvmInvoke::Sign(const ECC::Scalar::Native& skFrom, const ECC::Scalar::Native& skBlind)
	{
		ECC::Point::Native pt = ECC::Context::get().G * skFrom;
		m_From.FromPubKey(pt);

		pt = ECC::Context::get().G * skBlind;
		get_SubsidyCorrection(pt, false);
		m_Commitment = pt;

		CalculateMsg();

		ECC::Scalar::Native pSk[] = { skFrom, skBlind };
		ECC::Scalar::Native nnc;

		Cast::Down<ECC::SignatureBase>(m_Signature).Sign(ECC::Context::get().m_Sig.m_CfgG2, m_Lazy_Msg.get(), &m_Signature.m_k, pSk, &nnc);
	}

} // namespace beam
