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

#include "core/consensus/block.h"
#include "core/consensus/impl_helpers.h"

namespace beam
{
	void Block::NumberRange::Reset()
	{
		m_Min.v = 0;
		m_Max.v = MaxHeight;
	}

	bool Block::NumberRange::IsEmpty() const
	{
		return m_Min.v > m_Max.v;
	}

	/////////////
	// Pbft
	bool Block::Pbft::ValidatorSet::EnumValidators(ITarget& x) const
	{
		for (const auto& kv : m_map)
			if (!x.OnValidator(kv.first, kv.second))
				return false;

		return true;
	}

	void Block::Pbft::IValidatorSet::get_Hash(Merkle::Hash& hv) const
	{
		struct Target
			:public ITarget
		{
			ECC::Hash::Processor m_hp;

			bool OnValidator(const Address& addr, uint64_t weight) override
			{
				m_hp
					<< static_cast<uint8_t>(1)
					<< addr
					<< weight;
				return true;
			}
		} x;

		x.m_hp << "vs.state";
		EnumValidators(x);

		x.m_hp
			<< static_cast<uint8_t>(0)
			>> hv;
	}

	bool Block::Pbft::IValidatorSet::IsMajorityReached(uint64_t wVoted, uint64_t wTotal, uint32_t nWhite)
	{
		if (nWhite < Rules::get().m_Pbft.m_Whitelist.m_NumRequired)
			return false;

		assert(wVoted <= wTotal);
		return (wVoted * 3 > wTotal * 2); // TODO: overflow test
	}

	bool Block::Pbft::IValidatorSet::CheckQuorum(const Merkle::Hash& msg, const QC& qc) const
	{
		struct Target
			:public ITarget
		{
			uint64_t m_wTotal = 0;
			uint64_t m_wVoted = 0;
			uint32_t m_nWhite = 0;
			uint32_t m_iIdx = 0;
			uint32_t m_iWhitePos = 0;

			const Merkle::Hash& m_msg;
			const QC& m_qc;

			std::vector<ECC::Point::Native> m_vPks;

			Target(const Merkle::Hash& msg, const QC& qc)
				:m_msg(msg)
				,m_qc(qc)
			{}

			bool OnValidator(const Address& addr, uint64_t weight) override
			{
				m_wTotal += weight;

				if (m_qc.m_Mask.get_safe(m_iIdx++))
				{
					if (!addr.ExportNnz(m_vPks.emplace_back()))
						return false;

					m_wVoted += weight;

					if (Rules::get().m_Pbft.m_Whitelist.IsWhite(addr, m_iWhitePos))
						m_nWhite++;
				}

				return true;
			}
		};

		Target x(msg, qc);

		if (!EnumValidators(x))
			return false;

		ECC::Signature::Config cfg = ECC::Context::get().m_Sig.m_CfgG1; // copy
		cfg.m_nKeys = (uint32_t) x.m_vPks.size();

		if (!Cast::Down<ECC::SignatureBase>(qc.m_Signature).IsValid(cfg, msg, &qc.m_Signature.m_k, x.m_vPks.empty() ? nullptr : &x.m_vPks.front()))
			return false;

		// is quorum reached?
		return IsMajorityReached(x.m_wVoted, x.m_wTotal, x.m_nWhite);
	}

	void Block::Pbft::DeriveValidatorAddress(Key::IKdf& kdf, Block::Pbft::Address& addr, ECC::Scalar::Native& sk)
	{
		kdf.DeriveKey(sk, Key::ID(0, Key::Type::Coinbase));
		addr.FromSk(sk);
	}

	/////////////
	// Block

	int Block::SystemState::Full::cmp(const Full& v) const
	{
		CMP_MEMBER(m_Number.v)
		CMP_MEMBER_EX(m_Kernels)
		CMP_MEMBER_EX(m_Definition)
		CMP_MEMBER_EX(m_Prev)
		CMP_MEMBER_EX(m_ChainWork)
		CMP_MEMBER(m_TimeStamp)
		CMP_MEMBER(m_PoW.m_Difficulty.m_Packed)
		CMP_MEMBER_EX(m_PoW.m_Nonce)
		CMP_MEMBER(m_PoW.m_Indices)
		return 0;
	}

	bool Block::SystemState::Full::IsNext(const Full& sNext) const
	{
		if (m_Number.v + 1 != sNext.m_Number.v)
			return false;

		if (m_ChainWork + sNext.m_PoW.m_Difficulty != sNext.m_ChainWork) // for PoS this is equivalent to verifying height+span relation
			return false;

		Merkle::Hash hv;
		get_Hash(hv);
		return sNext.m_Prev == hv;
	}

	void Block::SystemState::Full::SetFirst(const Sequence::Prefix& prfx, const Element& x)
	{
		Cast::Down<Sequence::Prefix>(*this) = prfx;
		Cast::Down<Sequence::Element>(*this) = x;
	}

	void Block::SystemState::Full::SetNext(const Full& s, const Element& x)
	{
		s.get_Hash(m_Prev);
		m_Number.v = s.m_Number.v + 1;
		m_ChainWork = s.m_ChainWork + x.m_PoW.m_Difficulty;

		Cast::Down<Sequence::Element>(*this) = x;
	}

	void Block::SystemState::Full::get_HashInternal(Merkle::Hash& out, bool bTotal) const
	{
		// Our formula:
		ECC::Hash::Processor hp;
		hp
			<< m_Number.v
			<< m_Prev
			<< m_ChainWork
			<< m_Kernels
			<< m_Definition
			<< m_TimeStamp
			<< m_PoW.m_Difficulty.m_Packed;

		// Starting from Fork2: add Rules cfg. Make it harder to tamper using headers mined on different cfg
		const Rules& r = Rules::get();
		auto iFork = r.FindFork(get_Height());
		if (iFork >= 2)
			hp << r.pForks[iFork].m_Hash;

		if (Rules::Consensus::Pbft == r.m_Consensus)
		{
			// add everything except the QC
			auto& d = Cast::Reinterpret<Block::Pbft::HdrData>(m_PoW);
			hp
				<< d.m_hvVsBoth
				<< d.m_Time_ms
				<< d.m_Flags1;

			// Note: don't add QC-related fields to hash evaluation, not even for the 'final' hash. There may (theoretically) be differents variants of QC, 
			// but there must be only a single hash of the agreed block
		}
		else
			if (bTotal)
				hp
					<< Blob(&m_PoW.m_Indices.at(0), sizeof(m_PoW.m_Indices))
					<< m_PoW.m_Nonce;


		hp >> out;
	}

	void Block::SystemState::Full::get_HashForPoW(Merkle::Hash& hv) const
	{
		get_HashInternal(hv, false);
	}

	void Block::SystemState::Full::get_Hash(Merkle::Hash& hv) const
	{
		if (m_Number.v)
			get_HashInternal(hv, true);
		else
			hv = Rules::get().Prehistoric;
	}

	bool Block::SystemState::Full::IsSane() const
	{
		if (!m_Number.v)
			return false;

		if (m_Number.v == 1u)
		{
			if (m_Prev != Rules::get().Prehistoric)
				return false;

			if (m_ChainWork - m_PoW.m_Difficulty != Zero)
				return false;
		}

		return true;
	}

	void Block::SystemState::Full::get_ID(ID& out) const
	{
		out.m_Number = m_Number;
		get_Hash(out.m_Hash);
	}

	void Block::SystemState::Full::get_ID(HeightHash& out) const
	{
		out.m_Height = get_Height();
		get_Hash(out.m_Hash);
	}

	Height Block::SystemState::Full::get_Height() const
	{
		const Rules& r = Rules::get();
		if (r.IsConstantSpan())
			return m_Number.v;

		if (!m_Number.v)
			return 0;

		// extract height from chainwork
		uintBigFor<Height>::Type val;
		m_ChainWork.ShiftRight(Difficulty::s_MantissaBits, val);

		Height ret;
		val.Export(ret);

		return ret;
	}

	bool Block::SystemState::Full::IsValidPoW() const
	{
		const auto& r = Rules::get();
		switch (r.m_Consensus)
		{
		case Rules::Consensus::PoW:
			{
				Merkle::Hash hv;
				get_HashForPoW(hv);
				return m_PoW.IsValid(hv.m_pData, hv.nBytes, get_Height());
			}

		default:
			assert(false);
			// no break

		case Rules::Consensus::FakePoW:
		case Rules::Consensus::Pbft:
			return true;
		}
	}

	bool Block::SystemState::Full::GeneratePoW(const PoW::Cancel& fnCancel)
	{
		Merkle::Hash hv;
		get_HashForPoW(hv);

		return m_PoW.Solve(hv.m_pData, hv.nBytes, get_Height(), fnCancel);
	}

	uint64_t Block::SystemState::Sequence::Element::get_Timestamp_ms() const
	{
		const auto& d = Cast::Reinterpret<Block::Pbft::HdrData>(m_PoW);

		uint16_t frac_ms;
		d.m_Time_ms.Export(frac_ms);

		if (frac_ms >= 1000)
			return 0;

		// max value for which converting to ms won't overflow.
		// Realistic time is much much lower
		const uint64_t nMaxTime_s = std::numeric_limits<uint64_t>::max() / 1000;
		if (m_TimeStamp >= nMaxTime_s)
			return 0;

		return m_TimeStamp * 1000 + frac_ms; // guaranteed to not overflow
	}

	bool Block::SystemState::Evaluator::get_Definition(Merkle::Hash& hv)
	{
		Merkle::Hash hvHist;
		return Interpret(hv, hvHist, get_History(hvHist), hv, get_Live(hv));
	}

	void Block::SystemState::Evaluator::GenerateProof()
	{
		Merkle::Hash hvDummy;
		get_Definition(hvDummy);
	}

	bool Block::SystemState::Evaluator::get_History(Merkle::Hash& hv)
	{
		return OnNotImpl();
	}

	bool Block::SystemState::Evaluator::get_Live(Merkle::Hash& hv)
	{
		const Rules& r = Rules::get();

		if (r.IsPastFork_<3>(m_Height))
		{
			Merkle::Hash hvCSA;
			return Interpret(hv, hv, get_KL(hv), hvCSA, get_CSA(hvCSA));
		}

		bool bUtxo = get_Utxos(hv);
		if (!r.IsPastFork_<2>(m_Height))
			return bUtxo;

		Merkle::Hash hvSA;
		return Interpret(hv, hv, bUtxo, hvSA, get_SA(hvSA));
	}

	bool Block::SystemState::Evaluator::get_SA(Merkle::Hash& hv)
	{
		Merkle::Hash hvAssets;
		return Interpret(hv, hv, get_Shielded(hv), hvAssets, get_Assets(hvAssets));
	}

	bool Block::SystemState::Evaluator::get_KL(Merkle::Hash& hv)
	{
		Merkle::Hash hvL;
		return Interpret(hv, hv, get_Kernels(hv), hvL, get_Logs(hvL));
	}

	bool Block::SystemState::Evaluator::get_CSA(Merkle::Hash& hv)
	{
		Merkle::Hash hvSA;
		return Interpret(hv, hv, get_Contracts(hv), hvSA, get_SA(hvSA));
	}

	bool Block::SystemState::Evaluator::get_Utxos(Merkle::Hash&)
	{
		return OnNotImpl();
	}

	bool Block::SystemState::Evaluator::get_Kernels(Merkle::Hash&)
	{
		return OnNotImpl();
	}

	bool Block::SystemState::Evaluator::get_Logs(Merkle::Hash&)
	{
		return OnNotImpl();
	}

	bool Block::SystemState::Evaluator::get_Shielded(Merkle::Hash&)
	{
		return OnNotImpl();
	}

	bool Block::SystemState::Evaluator::get_Assets(Merkle::Hash&)
	{
		return OnNotImpl();
	}

	bool Block::SystemState::Evaluator::get_Contracts(Merkle::Hash&)
	{
		return OnNotImpl();
	}

	struct Block::SystemState::Full::ProofVerifierHard
		:public Merkle::HardVerifier
		,public Evaluator
	{
		using HardVerifier::HardVerifier;

		bool Verify(const Full& s, uint64_t iIdx, uint64_t nCount)
		{
			if (!InterpretMmr(iIdx, nCount))
				return false;

			m_Height = s.get_Height();
			m_Verifier = true;

			GenerateProof();

			return
				IsEnd() &&
				(m_hv == s.m_Definition);
		}

		void OnProof(Merkle::Hash&, bool bOnRight) override
		{
			InterpretOnce(!bOnRight);
		}
	};

	struct Block::SystemState::Full::ProofVerifier
		:public Evaluator
	{
		size_t m_Hashes = 0;
		const Merkle::Node* m_pNode = nullptr;

		bool VerifyKnownPartOrder(const Full& s, const Merkle::Proof& proof)
		{
			// deduce and verify the hashing direction of the last part
			m_Height = s.get_Height();
			m_Verifier = true;

			// Phase 1: count known elements
			GenerateProof();

			if (m_Hashes > proof.size())
				return false;
			if (!m_Hashes)
				return true; // nothing to check

			// Phase 2: verify order
			m_pNode = &proof.back() - (m_Hashes - 1);

			GenerateProof();
			return !m_Failed;
		}

		bool Verify(const Full& s, Merkle::Hash& hv, const Merkle::Proof& proof)
		{
			if (!VerifyKnownPartOrder(s, proof))
				return false;

			Merkle::Interpret(hv, proof);
			return hv == s.m_Definition;
		}

		void OnProof(Merkle::Hash&, bool bOnRight) override
		{
			if (m_pNode)
			{
				if (m_pNode->first == bOnRight)
					m_Failed = true;

				m_pNode++;
			}
			else
				m_Hashes++;
		}
	};

	void Block::get_HashContractVar(Merkle::Hash& hv, const Blob& key, const Blob& val)
	{
		ECC::Hash::Processor()
			<< "beam.contract.val"
			<< key.n
			<< key
			<< val.n
			<< val
			>> hv;
	}

	void Block::get_HashContractLog(Merkle::Hash& hv, const Blob& key, const Blob& val, uint32_t nPos)
	{
		ECC::Hash::Processor()
			<< "beam.contract.log"
			<< nPos
			<< key.n
			<< key
			<< val.n
			<< val
			>> hv;
	}

	bool Block::SystemState::Full::IsValidProofUtxo(const ECC::Point& comm, const Input::Proof& p) const
	{
		Merkle::Hash hv;
		p.m_State.get_ID(hv, comm);

		if (!Rules::get().IsPastFork_<3>(get_Height()))
		{
			struct MyVerifier
				:public ProofVerifier
			{
				bool get_Utxos(Merkle::Hash&) override {
					return true;
				}
			} v;

			return v.Verify(*this, hv, p.m_Proof);
		}

		Merkle::Interpret(hv, p.m_Proof);
		return (hv == m_Kernels);
	}

	bool Block::SystemState::Full::IsValidProofShieldedOutp(const ShieldedTxo::DescriptionOutp& d, const Merkle::Proof& p) const
	{
		Merkle::Hash hv;
		d.get_Hash(hv);
		return IsValidProofShielded(hv, p);
	}

	bool Block::SystemState::Full::IsValidProofShieldedInp(const ShieldedTxo::DescriptionInp& d, const Merkle::Proof& p) const
	{
		Merkle::Hash hv;
		d.get_Hash(hv);
		return IsValidProofShielded(hv, p);
	}

	bool Block::SystemState::Full::IsValidProofShielded(Merkle::Hash& hv, const Merkle::Proof& p) const
	{
		struct MyVerifier
			:public ProofVerifier
		{
			bool get_Shielded(Merkle::Hash&) override {
				return true;
			}
		} v;

		return v.Verify(*this, hv, p);
	}

	bool Block::SystemState::Full::IsValidProofAsset(const Asset::Full& ai, const Merkle::Proof& p) const
	{
		struct MyVerifier
			:public ProofVerifier
		{
			bool get_Assets(Merkle::Hash&) override {
				return true;
			}
		} v;

		Merkle::Hash hv;
		ai.get_Hash(hv);
		return v.Verify(*this, hv, p);
	}

	bool Block::SystemState::Full::IsValidProofContract(const Blob& key, const Blob& val, const Merkle::Proof& p) const
	{
		struct MyVerifier
			:public ProofVerifier
		{
			bool get_Contracts(Merkle::Hash&) override {
				return true;
			}
		} v;

		Merkle::Hash hv;
		get_HashContractVar(hv, key, val);
		return v.Verify(*this, hv, p);
	}

	bool Block::SystemState::Full::IsValidProofKernel(const TxKernel& krn, const TxKernel::LongProof& proof) const
	{
		return IsValidProofKernel(krn.get_ID(), proof);
	}

	bool Block::SystemState::Full::IsValidProofKernel(const Merkle::Hash& hvID, const TxKernel::LongProof& proof) const
	{
		if (!proof.m_State.IsValid())
			return false;

		if (!proof.m_State.IsValidProofKernel(hvID, proof.m_Inner))
			return false;

		if (proof.m_State == *this)
			return true;

		ID id;
		proof.m_State.get_ID(id);
		return IsValidProofState(id, proof.m_Outer);
	}

	bool Block::SystemState::Full::IsValidProofKernel(const Merkle::Hash& hvID, const Merkle::Proof& proof) const
	{
		Merkle::Hash hv = hvID;
		if (!Rules::get().IsPastFork_<3>(get_Height()))
		{
			Merkle::Interpret(hv, proof);
			return (hv == m_Kernels);
		}
		struct MyVerifier
			:public ProofVerifier
		{
			bool get_Kernels(Merkle::Hash&) override {
				return true;
			}
		} v;

		return v.Verify(*this, hv, proof);
	}

	bool Block::SystemState::Full::IsValidProofLog(const Merkle::Hash& hvLog, const Merkle::Proof& proof) const
	{
		Merkle::Hash hv = hvLog;

		struct MyVerifier
			:public ProofVerifier
		{
			bool get_Logs(Merkle::Hash&) override {
				return true;
			}
		} v;

		return v.Verify(*this, hv, proof);
	}

	bool Block::SystemState::Full::IsValidProofState(const ID& id, const Merkle::HardProof& proof) const
	{
		if (!id.m_Number.v || (id.m_Number.v >= m_Number.v))
			return false;

		struct MyVerifier
			:public ProofVerifierHard
		{
			using ProofVerifierHard::ProofVerifierHard;

			bool get_History(Merkle::Hash& hv) override {
				hv = m_hv;
				return true;
			}
		};

		MyVerifier hver(proof);
		hver.m_hv = id.m_Hash;
		return hver.Verify(*this, id.m_Number.v - 1, m_Number.v - 1);
	}

	void Block::BodyBase::ZeroInit()
	{
		ZeroObject(m_Offset);
	}

	void Block::BodyBase::Merge(const BodyBase& next)
	{
		ECC::Scalar::Native offs(m_Offset);
		offs += next.m_Offset;
		m_Offset = offs;
	}

	/////////////
	// SystemState::IHistory
	bool Block::SystemState::IHistory::get_Tip(Full& s)
	{
		struct Walker :public IWalker
		{
			Full& m_Res;
			Walker(Full& s) :m_Res(s) {}

			bool OnState(const Full& s) override {
				m_Res = s;
				return false;
			}
		} w(s);

		if (!Enum(w, NULL))
			return true;

		ZeroObject(s);
		return false;
	}

	bool Block::SystemState::HistoryMap::Enum(IWalker& w, const Height* pBelow)
	{
		for (auto it = (pBelow ? m_Map.lower_bound(*pBelow) : m_Map.end()); m_Map.begin() != it; )
			if (!w.OnState((--it)->second))
				return false;

		return true;
	}

	bool Block::SystemState::HistoryMap::get_At(Full& s, Height h)
	{
		auto it = m_Map.find(h);
		if (m_Map.end() == it)
			return false;

		s = it->second;
		return true;
	}

	void Block::SystemState::HistoryMap::AddStates(const Full* pS, size_t nCount)
	{
		for (size_t i = 0; i < nCount; i++)
		{
			const Full& s = pS[i];
			m_Map[s.get_Height()] = s;
		}
	}

	void Block::SystemState::HistoryMap::DeleteFrom(Height h)
	{
		while (!m_Map.empty())
		{
			auto it = m_Map.end();
			if ((--it)->first < h)
				break;

			m_Map.erase(it);
		}
	}

	void Block::SystemState::HistoryMap::ShrinkToWindow(Height dh)
	{
		if (m_Map.empty())
			return;

		Height h = m_Map.rbegin()->first;
		if (h <= dh)
			return;

		Height h0 = h - dh;

		while (true)
		{
			auto it = m_Map.begin();
			assert(m_Map.end() != it);

			if (it->first > h0)
				break;

			m_Map.erase(it);
		}
	}

	/////////////
	// Builder
	Block::Builder::Builder(Key::Index subIdx, Key::IKdf& coin, Key::IPKdf& tag, Height h)
		:m_SubIdx(subIdx)
		,m_Coin(coin)
		,m_Tag(tag)
		,m_Height(h)
	{
		m_Offset = Zero;
	}

	void Block::Builder::AddCoinbaseAndKrn(Amount fees, Output::Ptr& pSubsidy, Output::Ptr& pFees, TxKernel::Ptr& pKrn)
	{
		const auto& r = Rules::get();
		assert(Rules::Consensus::Pbft != r.m_Consensus);

		Amount subsidy = r.get_Emission(m_Height);
		if (r.IsPastFork_<6>(m_Height))
		{
			subsidy += fees; // don't care about overflow, block assembler should ensure this
			fees = 0;
		}

		if (!(subsidy || fees))
			return; // nothing to add

		ECC::Scalar::Native sk;

		if (subsidy)
		{
			pSubsidy.reset(new Output);
			pSubsidy->m_Coinbase = true;
			pSubsidy->Create(m_Height, sk, m_Coin, CoinID(subsidy, m_Height, Key::Type::Coinbase, m_SubIdx), m_Tag);
			m_Offset += sk;
		}

		if (fees)
		{
			pFees.reset(new Output);
			pFees->Create(m_Height, sk, m_Coin, CoinID(fees, m_Height, Key::Type::Comission, m_SubIdx), m_Tag);
			m_Offset += sk;
		}

		pKrn.reset(new TxKernelStd);
		pKrn->m_Height.m_Min = m_Height; // make it similar to others

		m_Coin.DeriveKey(sk, Key::ID(m_Height, Key::Type::Kernel2, m_SubIdx));
		Cast::Up<TxKernelStd>(*pKrn).Sign(sk);
		m_Offset += sk;
	}

	void Block::Builder::AddCoinbaseAndKrn(Amount fees)
	{
		Output::Ptr pSubsidy, pFees;
		TxKernel::Ptr pKrn;
		AddCoinbaseAndKrn(fees, pSubsidy, pFees, pKrn);

		if (pSubsidy)
			m_Txv.m_vOutputs.push_back(std::move(pSubsidy));
		if (pFees)
			m_Txv.m_vOutputs.push_back(std::move(pFees));
		if (pKrn)
			m_Txv.m_vKernels.push_back(std::move(pKrn));
	}

	std::ostream& operator << (std::ostream& s, const HeightHash& id)
	{
		s << id.m_Height << "-" << id.m_Hash;
		return s;
	}

	std::ostream& operator << (std::ostream& s, const Block::SystemState::ID& id)
	{
		s << "Num-" << id.m_Number.v << "-" << id.m_Hash;
		return s;
	}

	int Block::SystemState::ID::cmp(const Block::SystemState::ID& v) const
	{
		CMP_MEMBER(m_Number.v)
		CMP_MEMBER_EX(m_Hash)
		return 0;
	}

} // namespace beam
