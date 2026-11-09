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

#include "node_pbft.h"

namespace beam {

void Node::Validator::UpdateAssessment(const ValidatorPlus& v, uint8_t& nStatus)
{
	assert(nStatus == v.m_Status);
	typedef Shaders::I_PBFT::State::Validator::Status Status;

	if ((v.m_Assessment.m_hShouldSlashAt > v.m_hSuspend) &&
		(v.m_Assessment.m_hShouldSlashAt + Assessment::Settings::SlashMaxLatency <= get_ParentObj().m_Processor.m_Cursor.m_hh.m_Height))
	{
		nStatus = (uint8_t) Status::Slash;
		return;
	}

	if (v.m_Status == (uint8_t) Status::Tombed)
		return; // never change

	if (v.m_NumSlashed >= Assessment::Settings::MaxSlash)
	{
		nStatus = (uint8_t) Status::Suspended; // tombed
		return;
	}

	if (nStatus == (uint8_t) Status::Suspended)
	{
		if (v.m_hSuspend + Assessment::Settings::SuspendDh > get_ParentObj().m_Processor.m_Cursor.m_hh.m_Height)
			return;

		nStatus = (uint8_t) Status::Jailed;
	}

	if (v.get_EffectiveWeight())
		switch (nStatus)
		{
		case (uint8_t) Status::Jailed:
			if (v.m_Assessment.m_Score < Assessment::Settings::Watermark::Unjail)
				nStatus = (uint8_t) Status::Active;
			break;

		case (uint8_t) Status::Active:
			if (v.m_Assessment.m_Score >= Assessment::Settings::Watermark::Jail)
				nStatus = (uint8_t) Status::Jailed;
			break;

		default:
			break; // suppress warning
		}

}

void Node::Validator::OnContractStoreReset()
{
	if (!m_ValidatorSet.m_mapValidators.empty())
	{
		m_ValidatorSet.m_mapValidators.clear();
		m_ValidatorSet.m_hv.reset();
	}
}

bool Node::Validator::OnContractInvoke(const ContractID& cid, uint32_t iMethod, const Blob& args, bool bTemporary)
{
	if (!m_cid && !iMethod)
	{
		// we assume the very first deployed contract is the PBFT
		m_cid = cid;
		BEAM_LOG_INFO() << "PBFT contract is " << cid;

		Blob blob = cid;
		get_ParentObj().m_Processor.get_DB().ParamSet(NodeDB::ParamID::PbftCid, nullptr, &blob);
	}

	if (*m_cid != cid)
		return true;

	switch (iMethod)
	{
	case Shaders::I_PBFT::Method::ValidatorStatusUpdate::s_iMethod:
		if (bTemporary && m_pMe)
		{
			// I'm a validator, and the block isn't finalized yet.
			// check if my assessment is consistent with this action
			if (args.n < sizeof(Shaders::I_PBFT::Method::ValidatorStatusUpdate))
				return false;
			const auto& m = *(Shaders::I_PBFT::Method::ValidatorStatusUpdate*) args.p;

			const auto& msg = m_pMe->m_Assessment.m_Last;
			auto it = msg.m_Reputation.find(Cast::Up<Block::Pbft::Address>(m.m_Address));
			if (msg.m_Reputation.end() != it)
			{
				auto fMy = it->second;
				auto fStatus = ByteOrder::from_le((uint8_t) m.m_Status);

				if (fMy != fStatus)
					return false; // currently demand exact match
			}
		}
		break;
	}

	return true;
}

const Block::Pbft::IValidatorSet& Node::Validator::get_Validators()
{
	return m_ValidatorSet;
}

uint64_t Node::Validator::ValidatorPlus::get_EffectiveWeight() const
{
	if (m_Status >= (uint8_t)Shaders::I_PBFT::State::Validator::Status::Suspended)
		return 0;

	return m_Weight;
}

void Node::Validator::RefreshValidatorSet()
{
	assert(m_cid);

	typedef Shaders::Env::Key_T< Shaders::I_PBFT::State::Validator::Key> VKeyType;

	VKeyType k0;
	k0.m_Prefix.m_Cid = *m_cid;
	k0.m_Prefix.m_Tag = Shaders::KeyTag::Internal;
	k0.m_KeyInContract.m_Tag = Shaders::I_PBFT::State::Tag::s_Validator;
	Cast::Down<ECC::uintBig>(k0.m_KeyInContract.m_Address) = Zero;
	VKeyType k1 = k0;
	memset(k1.m_KeyInContract.m_Address.m_pData, -1, ECC::uintBig::nBytes);

	NodeDB::WalkerContractData wlk;
	get_ParentObj().m_Processor.get_DB().ContractDataEnum(wlk, Blob(&k0, sizeof(k0)), Blob(&k1, sizeof(k1)));
	while (wlk.MoveNext())
		OnContractVarChange(wlk.m_Key, wlk.m_Val, false);
}

void Node::Validator::OnContractVarChange(const Blob& key, const Blob& val, bool bTemporary)
{
	if (!m_cid)
		return;

	// track validator changes
	typedef Shaders::Env::Key_T< Shaders::I_PBFT::State::Validator::Key> VKeyType;

	// check sizes first (cheap) before checking cid
	if (key.n != sizeof(VKeyType))
		return;

	if (val.n && (val.n < sizeof(Shaders::I_PBFT::State::Validator)))
		return;

	const auto& vk = *(const VKeyType*) key.p;

	if ((vk.m_Prefix.m_Cid != *m_cid) ||
		(vk.m_Prefix.m_Tag != Shaders::KeyTag::Internal) ||
		(vk.m_KeyInContract.m_Tag != Shaders::I_PBFT::State::Tag::s_Validator))
		return;

	// It's indeed a validator update action
	const auto& addr = Cast::Up<Block::Pbft::Address>(vk.m_KeyInContract.m_Address);
	auto* pV = m_ValidatorSet.Find(addr);
	PBFT_LOG(DEBUG, "\tV " << addr << " update");

	if (!val.n)
	{
		// delete it
		if (pV)
		{
			if (bTemporary)
				pV->m_Deleted = true;
			else
			{
				m_ValidatorSet.m_hv.reset();
				m_ValidatorSet.m_mapValidators.Delete(*pV);
			}
		}

		return;
	}

	const auto& vd = *(const Shaders::I_PBFT::State::Validator*) val.p;

	if (!pV)
	{
		pV = m_ValidatorSet.m_mapValidators.Create(addr);
		pV->m_Whitelisted = Rules::get().m_Pbft.m_Whitelist.IsWhite(addr);
		pV->m_hvCommitted = Zero;
		pV->m_Status = 0;
		pV->m_Weight = 0;
	}

	auto w0 = pV->get_EffectiveWeight();

	pV->m_Deleted = false;
	pV->m_Weight = ByteOrder::from_le(vd.m_Weight);
	pV->m_Status = ByteOrder::from_le((uint8_t) vd.m_Status);
	pV->m_NumSlashed = ByteOrder::from_le(vd.m_NumSlashed);
	pV->m_hSuspend = ByteOrder::from_le(vd.m_hSuspend);

	if (pV->get_EffectiveWeight() != w0)
		m_ValidatorSet.m_hv.reset();
}

bool Node::Validator::AddReward(AmountBig::Number fees, bvm2::Storage::IBase& s)
{
	// emulate what AddReward is supposed to do

	if (!m_cid)
		return false;
	if (fees.get_Element<Amount, 1>())
		return false; // fee overflow?

	Shaders::Env::Key_T<uint8_t> kg;
	kg.m_Prefix.m_Cid = *m_cid;
	kg.m_KeyInContract = Shaders::I_PBFT::State::Tag::s_Global;

	Blob blob;
	s.LoadVar(Blob(&kg, sizeof(kg)), blob);

	typedef Shaders::I_PBFT::State::Global Global;

	if (blob.n < sizeof(Global))
		return false;

	ByteBuffer buf;
	blob.Export(buf);
	auto& g = *(Global*) &buf.front();

	Amount rews = ByteOrder::from_le(g.m_RewardPending);

	auto feesLo = fees.get_Element<Amount, 0>();
	rews += feesLo;
	if (rews < feesLo)
		return false; // overflow

	g.m_RewardPending = ByteOrder::to_le(rews);

	Shaders::Env::Key_T<Asset::ID> kl;
	kl.m_Prefix.m_Cid = *m_cid;
	kl.m_Prefix.m_Tag = Shaders::KeyTag::LockedAmount;
	kl.m_KeyInContract = 0;

	AmountBig::Type valLocked;
	s.LoadVar(Blob(&kl, sizeof(kl)), blob);
	if (valLocked.nBytes == blob.n)
		valLocked = *(AmountBig::Type*) blob.p;
	else
		valLocked = Zero;

	AmountBig::Type valLockedNew = valLocked;
	valLockedNew  += uintBigFrom(feesLo);
	if (valLockedNew < valLocked)
		return false; // overflow

	// all good, modify state
	s.SaveVar(Blob(&kg, sizeof(kg)), buf);
	s.SaveVar(Blob(&kl, sizeof(kl)), valLockedNew);

	return true;
}

bool Node::Validator::ValidatorSet::EnumValidators(ITarget& x) const
{
	for (const auto& v : m_mapValidators)
	{
		if (v.m_Deleted)
			continue;

		auto w = v.get_EffectiveWeight();
		if (w && !x.OnValidator(v.m_Key, w))
			return false;
	}

	return true;
}

void Node::Validator::ValidatorSet::get_Hash(Merkle::Hash& hv) const
{
	if (m_hv)
		hv = *m_hv;
	else
	{
		Block::Pbft::IValidatorSet::get_Hash(hv);
		m_hv = hv;
	}
}

Node::Validator::ValidatorPlus* Node::Validator::ValidatorSet::Find(const Block::Pbft::Address& addr) const
{
	auto it = m_mapValidators.find(addr, ValidatorPlus::Comparator());
	return (m_mapValidators.end() == it) ? nullptr : &Cast::NotConst(*it);
}

Node::Validator::ValidatorPlus* Node::Validator::ValidatorSet::FindActive(const Block::Pbft::Address& addr) const
{
	auto* pV = Find(addr);
	return (pV && pV->get_EffectiveWeight()) ? pV : nullptr;
}

Node::Validator::ValidatorPlus& Node::Validator::ValidatorSet::FindActiveStrict(const Block::Pbft::Address& addr) const
{
	auto* pV = FindActive(addr);
	if (!pV)
		proto::NodeConnection::ThrowUnexpected("not an active validator");
	return *pV;
}

Node::Validator::ValidatorPlus* Node::Validator::ValidatorSet::SelectLeader(const Merkle::Hash& hvInp, uint32_t iRound) const
{
	typedef Shaders::I_PBFT::State::Validator::Status Status;

	uint64_t wUnjailed = 0;
	for (const auto& v : m_mapValidators)
	{
		if (v.m_Status == (uint8_t) Status::Active)
			wUnjailed += v.m_Weight;
	}

	if (!wUnjailed)
		return nullptr;

	// Select in a pseudo-random way. Probability according to the validator weight
	ECC::Oracle oracle;
	oracle
		<< "vs.select"
		<< hvInp
		<< iRound;

	uint64_t w = get_Random(oracle, wUnjailed);
	assert(w < wUnjailed);

	for (auto it = m_mapValidators.begin(); ; it++)
	{
		assert(m_mapValidators.end() != it);
		auto& v = *it;
		if (v.m_Status != (uint8_t) Status::Active)
			continue;

		if (w < v.m_Weight)
			return Cast::NotConst(&v);

		w -= v.m_Weight;
	}

	// unreachable
}

uint64_t Node::Validator::ValidatorSet::get_Random(ECC::Oracle& oracle, uint64_t nBound)
{
	assert(nBound);

	uint64_t nResid = ((static_cast<uint64_t>(-1) % nBound) + 1) % nBound;

	while (true)
	{
		Merkle::Hash hv;
		oracle >> hv;

		uint64_t w;
		hv.ExportWord<0>(w);

		if (w <= static_cast<uint64_t>(-1) - nResid)
			return w % nBound;
	}

	// unreachable
}

void Node::Validator::get_AssessmentMsg(Merkle::Hash& hv, const proto::PbftPeerAssessment& msg)
{
	ECC::Hash::Processor hp;
	hp
		<< "dredd.1"
		<< msg.m_From
		<< msg.m_Height
		<< msg.m_Reputation.size();

	for (const auto& x : msg.m_Reputation)
	{
		hp
			<< x.first
			<< x.second;
	}

	hp >> hv;
}

void Node::Validator::get_RoundStartMsg(Merkle::Hash& hv, const proto::PbftRoundStart& msg)
{
	ECC::Hash::Processor()
		<< "pbft.2.rs"
		<< m_hAnchor
		<< msg.m_Address
		<< msg.m_iRound
		<< msg.m_IsCommitted
		>> hv;
}

bool Node::Validator::IsAssessmentRelevant(const proto::PbftPeerAssessment& msg) const
{
	if (!msg.m_Height)
		return false;

	Height hMy = get_ParentObj().m_Processor.m_Cursor.m_hh.m_Height;
	Height val = hMy - msg.m_Height + 5;
	return val <= 10;
}

void Node::Validator::OnMsg(proto::PbftPeerAssessment&& msg, const Peer& src)
{
	Merkle::Hash hv;
	get_AssessmentMsg(hv, msg);

	if (!IsAssessmentRelevant(msg))
		return; // not interested

	//auto& p = get_ParentObj().m_Processor; // alias
	auto* pV = m_ValidatorSet.Find(msg.m_From);
	if (!pV)
		return; // not a validator

	auto& vp = *pV;
	if (vp.m_Assessment.m_Last.m_Height >= msg.m_Height)
		return;

	if (!msg.m_From.CheckSignature(hv, msg.m_Signature))
		src.ThrowUnexpected();

	// ok
	vp.m_Assessment.m_Last = std::move(msg);
	Broadcast(vp.m_Assessment.m_Last, &src);
}

void Node::Validator::FinalyzeRoundAssessment()
{
	const uint32_t iR = 0; // round data to finalyze
	auto& rd = m_pR[iR];
	if (!rd.m_pLeader)
		return;

	PBFT_LOG(DEBUG, "Finalizing assessments");

	for (auto& vp : m_ValidatorSet.m_mapValidators)
	{
		if (!vp.m_Weight)
			continue;

		Assessment::Settings::Regress(vp.m_Assessment.m_Score);

		if (&vp == m_pMe)
		{
			PBFT_LOG(DEBUG, "\t" << vp.m_Key << " self");
			continue; // don't assess self
		}

		vp.m_Assessment.m_Score += GetAssessment(vp);

		PBFT_LOG(DEBUG, "\tV " << vp.m_Key << " score=" << vp.m_Assessment.m_Score << ", status=" << static_cast<uint32_t>(vp.m_Status));

	}

}

uint32_t Node::Validator::GetAssessment(const ValidatorPlus& vp)
{
	const uint32_t iR = 0; // round data to finalyze
	const auto& rd = m_pR[iR];
	auto& pr = vp.m_pR[iR];

	if (!pr.m_MsgRoundStart)
	{
		PBFT_LOG(DEBUG, "\t\tno round-start");
		return Assessment::Settings::NotRoundStart; // didn't send round-start msg
	}

	bool bWasLeader = (rd.m_pLeader == &vp);
	if (bWasLeader)
	{
		// did the leader propose?
		if ((Proposal::State::None == rd.m_Proposal.m_State) && ShouldAcceptProposal())
		{
			PBFT_LOG(DEBUG, "\t\tnot proposed");
			return Assessment::Settings::NotProposed;
		}

		if (rd.m_pPwr[VoteKind::Commit].IsMajorityReached(m_wTotal) && !rd.m_Multisig)
		{
			PBFT_LOG(DEBUG, "\t\tnot sent sig-request");
			return Assessment::Settings::NotProposed;
		}
	}

	if (Proposal::State::Accepted != rd.m_Proposal.m_State)
		return 0;

	if (vp.m_hvCommitted == Zero)
	{
		// did it pre-vote?
		bool bHadToPreVote = bWasLeader || !pr.m_MsgRoundStart->m_IsCommitted;

		if (bHadToPreVote && !pr.m_pVote[VoteKind::PreVote])
		{
			PBFT_LOG(DEBUG, "\t\tnot voted pre");
			return Assessment::Settings::NotVotedPre;
		}

		if (rd.m_pPwr[VoteKind::PreVote].IsMajorityReached(m_wTotal))
		{
			PBFT_LOG(DEBUG, "\t\tnot committed");
			return Assessment::Settings::NotVotedCommit;
		}
	}

	if (pr.m_Sig && (pr.m_Sig->m_E != Zero))
	{
		PBFT_LOG(DEBUG, "\t\tnot msig-signed");
		return Assessment::Settings::NotVotedCommit;
	}

	return 0; // all good
}

void Node::Validator::RoundTimes::FromRound(const Rules& r, const Block::SystemState::Full& s0)
{
	if (s0.m_Number.v)
		FromRoundProgressive(r, s0.get_Timestamp_ms());
	else
		FromRoundInitial(r);
}

void Node::Validator::RoundTimes::FromRoundProgressive(const Rules& r, const uint64_t tPrev_ms)
{
	auto extend_ms = (m_iRound > 1) ? (r.m_Pbft.m_RoundUp_ms * (m_iRound - 1)) : 0;
	m_t0 = tPrev_ms + r.DA.Target_ms * m_iRound + extend_ms * (m_iRound - 2) / 2;
	m_ts = m_t0 + extend_ms;
	m_t1 = m_ts + r.DA.Target_ms;
}

void Node::Validator::RoundTimes::FromRoundInitial(const Rules& r)
{
	// no progression
	m_t0 = r.DA.Target_ms * m_iRound;
	m_ts = m_t0;
	m_t1 = m_t0 + r.DA.Target_ms;
}

void Node::Validator::RoundTimes::FromT0(const Rules& r, const Block::SystemState::Full& s0)
{
	if (!s0.m_Number.v)
	{
		m_iRound = r.DA.Target_ms ? (m_t0 / r.DA.Target_ms) : 0;
		FromRoundInitial(r);
		return;
	}

	auto tPrev_ms = s0.get_Timestamp_ms();

	// need to solve a quadratic equation to find the round number.
	// Solve this iteratively, optimize for the case where t_ms is small (i.e. we're talking about the consequent round
	auto t0 = m_t0;
	if (!r.DA.Target_ms || (t0 < tPrev_ms))
	{
		// didn't reach 0th round
		m_iRound = 0;
		FromRoundProgressive(r, tPrev_ms);
		return;
	}

	// init guess. The actual result is lower if round-up if nnz
	uint64_t iMax = (t0 - tPrev_ms) / r.DA.Target_ms;
	uint64_t iMin = 0;
	m_iRound = iMax;

	while (true)
	{
		FromRoundProgressive(r, tPrev_ms);

		if (m_t0 > t0)
		{
			if (m_iRound == iMin)
				break;

			iMax = m_iRound - 1;
		}
		else
		{
			if (m_t1 > t0)
				break;

			if (m_iRound == iMax)
				break;

			iMin = m_iRound + 1;
		}


		m_iRound = (iMin + iMax) / 2;
	}
}

//void Node::Validator::RoundTimes::Test()
//{
//	Rules r;
//	r.DA.Target_ms = 1000;
//	r.m_Pbft.m_RoundUp_ms = 165;
//
//	RoundTimes rt;
//	uint64_t t0 = 0;
//
//	Block::SystemState::Full s0;
//	ZeroObject(s0);
//	s0.m_Number.v = 1;
//	s0.m_TimeStamp = 0;
//
//	for (uint32_t iRound = 0; iRound < 25; iRound++)
//	{
//		rt.m_iRound = iRound;
//		rt.FromRound(r, s0);
//
//		uint32_t nIncr = (iRound > 1) ? (iRound - 1) : 0;
//		
//		assert(rt.m_t0 == t0);
//		assert(rt.m_t0 + r.DA.Target_ms + r.m_Pbft.m_RoundUp_ms * nIncr == rt.m_t1);
//		assert(rt.m_ts + r.DA.Target_ms == rt.m_t1);
//
//		auto tNext = rt.m_t1;
//
//		if (iRound)
//		{
//			rt.m_t0 = t0 - 1;
//			rt.FromT0(r, s0);
//
//			assert(iRound - 1 == rt.m_iRound);
//			assert(rt.m_t1 == t0);
//		}
//
//		rt.m_t0 = tNext - 1;
//		rt.FromT0(r, s0);
//		assert(rt.m_iRound == iRound);
//
//		rt.m_t0 = tNext;
//		rt.FromT0(r, s0);
//		assert(rt.m_iRound == iRound + 1);
//
//		t0 = tNext;
//	}
//}

} // namespace beam
