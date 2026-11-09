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

Node::Validator::Validator()
{
	m_hAnchor = MaxHeight;
	m_iRound = 0;
}

void Node::Validator::Initialize()
{
	auto& keys = get_ParentObj().m_Keys; // alias
	if (keys.m_pMiner)
	{
		Block::Pbft::DeriveValidatorAddress(*keys.m_pMiner, keys.m_Validator.m_Addr, keys.m_Validator.m_sk);
		BEAM_LOG_INFO() << "Validator ID=" << keys.m_Validator.m_Addr;
	}
	else
		keys.m_Validator.m_Addr = Zero;

	auto& p = get_ParentObj().m_Processor;

	// it could be set already, in case the Processor started in rubuild-nonstd mode. Then we can skip the next section
	if (!m_cid)
	{
		ContractID cid;
		Blob blob = cid;
		if (p.get_DB().ParamGet(NodeDB::ParamID::PbftCid, nullptr, &blob))
		{
			m_cid = cid;
			RefreshValidatorSet();
		}
	}

	ByteBuffer buf;
	p.get_DB().ParamGet(NodeDB::ParamID::PbftStamp, nullptr, nullptr, &buf);
	if (!buf.empty())
	{
		Deserializer der;
		der.reset(buf);
		der
			& m_Stamp.m_ID
			& m_Stamp.m_Msg;
	}
	else
		ZeroObject(m_Stamp.m_ID);
}

void Node::Validator::OnRolledBack()
{
	m_hAnchor = MaxHeight;
	m_iRound = 0;
	KillTimer();
	// wait till the next state
}

void Node::Validator::OnNewState()
{
	if (Rules::Consensus::Pbft != Rules::get().m_Consensus)
		return;

	auto& p = get_ParentObj().m_Processor; // alias
	if (!p.IsTreasuryHandled() || p.IsFastSync())
	{
		OnRolledBack(); // i.e. reset self
		return;
	}

	m_iRound = 0;

	KillTimer();
	OnNewRound();
}

void Node::Validator::ShuffleRoundData(bool bConsequent)
{
	auto& rdcurr = m_pR[0];
	auto& rdnext = m_pR[1];

	if (bConsequent)
		rdcurr = std::move(rdnext);
	else
		rdcurr.Reset();

	rdnext.Reset();

	for (auto& v : m_ValidatorSet.m_mapValidators)
	{
		auto& pr0 = v.m_pR[0];
		auto& pr1 = v.m_pR[1];
		for (uint32_t iV = 0; iV < _countof(pr0.m_pVote); iV++)
		{
			auto& vc = pr0.m_pVote[iV];
			auto& vn = pr1.m_pVote[iV];

			if (bConsequent)
				vc = std::move(vn);
			else
				vc.reset();

			vn.reset();
		}

		if (bConsequent)
		{
			pr0.m_MsgRoundStart = std::move(pr1.m_MsgRoundStart);
			pr0.m_Sig = std::move(pr1.m_Sig);
		}
		else
		{
			pr0.m_MsgRoundStart.reset();
			pr0.m_Sig.reset();
		}

		pr1.m_MsgRoundStart.reset();
		pr1.m_Sig.reset();
	}
}


void Node::Validator::OnNewRound()
{
	assert(!m_bTimerPending);

	auto tNow_ms = get_RefTime_ms();
	auto& p = get_ParentObj().m_Processor; // alias
	const Rules& r = Rules::get();

	RoundTimes rt;
	rt.m_t0 = tNow_ms;
	rt.FromT0(r, p.m_Cursor.m_Full);

	bool bNewState = (m_hAnchor != p.m_Cursor.m_hh.m_Height);
	if (bNewState)
	{
		OnNewStateInternal();
		ShuffleRoundData(false);
		PBFT_LOG(DEBUG, "tip=" << m_hAnchor);
	}

	if (rt.m_iRound <= m_iRound)
	{
		// set timer till the next relevant round start
		SetTimer(tNow_ms, rt.m_t1);
		return;
	}

	uint64_t iExpectedRound = m_iRound + 1;
	bool bConsequentRound = (rt.m_iRound == iExpectedRound);

	if (m_iRound && bConsequentRound)
		FinalyzeRoundAssessment();

	SetTimer(tNow_ms, rt.m_t1);

	m_iRound = rt.m_iRound;

	PBFT_LOG(INFO, "round start dt=" << (rt.m_t1 - rt.m_t0));

	auto& rdcurr = m_pR[0];

	// save last proposal, if it's superior so far
	bool bSelectLast =
		((Proposal::State::Accepted == rdcurr.m_Proposal.m_State) ||
		((Proposal::State::Received == rdcurr.m_Proposal.m_State) && (Proposal::State::Accepted != m_FutureCandidate.m_State)));

	if (bSelectLast)
		m_FutureCandidate = std::move(rdcurr.m_Proposal);

	if (!bNewState)
		ShuffleRoundData(bConsequentRound);

	if (!rdcurr.m_pLeader)
		rdcurr.m_pLeader = m_ValidatorSet.SelectLeader(p.m_Cursor.m_hh.m_Hash, static_cast<uint32_t>(m_iRound));

	if (m_pMe)
	{
		if (m_pMe->m_hvCommitted != Zero)
			PBFT_LOG(INFO, "committed to " << m_pMe->m_hvCommitted);

		m_skNonce.GenRandomNnz();

		auto& msg = m_pMe->m_pR[0].m_MsgRoundStart.emplace();
		msg.m_Address = m_pMe->m_Key;
		msg.m_iRound = static_cast<uint32_t>(m_iRound);
		msg.m_IsCommitted = (State::None != m_State);

		msg.m_NoncePub = ECC::Context::get().G * m_skNonce;

		ECC::Hash::Value hv;
		get_RoundStartMsg(hv, msg);
		Sign(msg.m_Signature, hv);

		OnRoundStartReceived(0, *m_pMe, nullptr);

		CheckState(0);
	}
}

void Node::Validator::OnNewStateInternal()
{
	auto& p = get_ParentObj().m_Processor; // alias
	m_hAnchor = p.m_Cursor.m_hh.m_Height;

	m_FutureCandidate.m_State = Proposal::State::None;
	m_State = State::None;

	m_wTotal = 0;
	for (auto& v : m_ValidatorSet.m_mapValidators)
	{
		m_wTotal += v.get_EffectiveWeight();
		v.m_hvCommitted = Zero;
	}

	m_pMe = m_ValidatorSet.FindActive(get_ParentObj().m_Keys.m_Validator.m_Addr);
	if (!m_pMe)
		return;

	auto& msg = m_pMe->m_Assessment.m_Last;
	msg.m_Reputation.clear();
	msg.m_Height = 0;

	// update our assessment
	for (auto& v : m_ValidatorSet.m_mapValidators)
	{
		if (&v == m_pMe)
			continue; // I don't assess myself

		uint8_t nStatus = v.m_Status;
		UpdateAssessment(v, nStatus);

		if (v.m_Status != nStatus)
			PBFT_LOG(DEBUG, v.m_Key << " assessment " << static_cast<uint32_t>(v.m_Status) << " -> " << static_cast<uint32_t>(nStatus));

		msg.m_Reputation[v.m_Key] = nStatus;
	}

	msg.m_From = m_pMe->m_Key;
	msg.m_Height = p.m_Cursor.m_hh.m_Height;

	// sign
	Merkle::Hash hv;
	get_AssessmentMsg(hv, msg);
	Sign(msg.m_Signature, hv);

	Broadcast(msg, nullptr);
}

void Node::Validator::TestBlock(const Block::SystemState::Full& s, const HeightHash& id, const Block::Body& block, bool bTestOnly)
{
	const auto& d = Cast::Reinterpret<Block::Pbft::HdrData>(s.m_PoW);

	// Skip timestamp testing. During proposal testing it's checked anyway

	if (!bTestOnly) // during block test the QC isn't ready yet
	{
		if (!m_ValidatorSet.CheckQuorum(id.m_Hash, d.m_QC))
			Exc::Fail("QC");
	}

	uint8_t nFlags = 0;
	if (block.IsEmpty() && (s.m_Number.v > 1))
		nFlags = Block::Pbft::HdrData::Flags::Empty;

	if (d.m_Flags1 != nFlags)
		Exc::Fail("flags mismatch");
}

void Node::Validator::RoundData::Reset()
{
	m_Proposal.m_State = Proposal::State::None;

	for (uint32_t iK = 0; iK < _countof(m_pPwr); iK++)
		m_pPwr[iK].Reset();

	m_pwrNotCommitted.Reset();
	m_Multisig.reset();
	m_pLeader = nullptr;
}

void Node::Validator::GenerateProposal()
{
	auto& rd = m_pR[0];
	assert(Proposal::State::None == rd.m_Proposal.m_State);
	assert(m_pMe == rd.m_pLeader);

	if (Proposal::State::None != m_FutureCandidate.m_State)
	{
		PBFT_LOG(DEBUG, "reusing past proposal");

		rd.m_Proposal = std::move(m_FutureCandidate);
		m_FutureCandidate.m_State = Proposal::State::None;

		rd.SetHashes();
	}
	else
	{
		// theoretically can generate it before receiving enough NotCommitted msgs, but no reason to do it
		if (!ShouldAcceptProposal())
		{
			PBFT_LOG(DEBUG, "not ready to propose");
			return;
		}

		if (!CreateProposal())
		{
			PBFT_LOG(WARNING, "Block generation failed, can't make a proposal!");
			return;
		}
	}

	rd.m_Proposal.m_State = Proposal::State::Received;

	// sign the proposal
	rd.m_Proposal.m_Msg.m_iRound = static_cast<uint32_t>(m_iRound);

	Merkle::Hash hv;
	get_ProposalMsg(hv, rd);
	Sign(rd.m_Proposal.m_Msg.m_Signature, hv);
}

bool Node::Validator::ShouldAcceptProposal() const
{
	if (m_iRound <= 1)
		return true; // 1st round, no need to wait for not-committed vote

	const uint32_t iR = 0;
	auto& rdcurr = m_pR[iR];
	Power pwr = rdcurr.m_pwrNotCommitted;
	pwr.Add(rdcurr.m_pPwr[VoteKind::Commit]);

	return pwr.IsMajorityReached(m_wTotal);
}


void Node::Validator::Sign(ECC::Signature& sig, const Merkle::Hash& hv)
{
	sig.Sign(hv, get_ParentObj().m_Keys.m_Validator.m_sk);
}

bool Node::Validator::ShouldSendTo(const Peer& peer) const
{
	if (!(proto::LoginFlags::SpreadingTransactions & peer.m_LoginFlags))
		return false;

	if (get_ParentObj().m_Processor.m_Cursor.m_hh != peer.m_Tip.m_hh)
		return false; // optionally we can tolerate 1 block behind, for the broadcast to be more rapid

	return true;
}

void Node::Validator::SendState(Peer& peer) const
{
	auto& p = get_ParentObj().m_Processor;
	if (p.m_Cursor.m_hh.m_Height != m_hAnchor)
		return;

	if (!ShouldSendTo(peer))
		return;

	for (uint32_t iR = 0; iR < _countof(m_pR); iR++)
	{
		const auto& rd = m_pR[iR];

		// order is important. First - the proposal.
		if (Proposal::State::None != rd.m_Proposal.m_State)
			peer.Send(rd.m_Proposal.m_Msg);

		// now round-start and vote msgs
		proto::PbftVote msgVote;
		msgVote.m_iRound = static_cast<uint32_t>(m_iRound + iR);

		for (const auto& vp : m_ValidatorSet.m_mapValidators)
		{
			if (!vp.get_EffectiveWeight())
				continue;
			auto& pr = vp.m_pR[iR];

			if (pr.m_MsgRoundStart)
				peer.Send(*pr.m_MsgRoundStart);

			msgVote.m_Address = vp.m_Key;
			for (msgVote.m_iKind = 0; msgVote.m_iKind < _countof(pr.m_pVote); msgVote.m_iKind++)
			{
				const auto& sig = pr.m_pVote[msgVote.m_iKind];
				if (sig)
				{
					msgVote.m_Signature = *sig;
					peer.Send(msgVote);
				}
			}

			// assessment
			if (IsAssessmentRelevant(vp.m_Assessment.m_Last))
				peer.Send(vp.m_Assessment.m_Last);
		}

		if (rd.m_Multisig)
		{
			auto& ms = *rd.m_Multisig;
			peer.Send(ms.m_Msg);

			// send partial sigs
			proto::PbftSig msgSig;
			msgSig.m_iRound = msgVote.m_iRound;

			for (const auto& vp : m_ValidatorSet.m_mapValidators)
			{
				if (!vp.get_EffectiveWeight())
					continue;
				auto& pr = vp.m_pR[iR];
				if (pr.m_Sig)
				{
					auto& sig = *pr.m_Sig;
					if (sig.m_E == Zero)
					{
						msgSig.m_Address = msgVote.m_Address;
						msgSig.m_Signature = sig.m_k;
						peer.Send(msgSig);
					}
				}
			}
		}
	}
}

void Node::Validator::Power::Add(const ValidatorPlus& v)
{
	assert(v.m_Weight && (v.get_EffectiveWeight() == v.m_Weight)); 
	m_wVoted += v.m_Weight;
	if (v.m_Whitelisted)
		m_nWhite++;
}

void Node::Validator::Power::Add(const Power& pwr)
{
	m_wVoted += pwr.m_wVoted;
	m_nWhite += pwr.m_nWhite;
}

bool Node::Validator::Power::IsMajorityReached(uint64_t wTotal) const
{
	assert(m_wVoted <= wTotal);
	return Block::Pbft::IValidatorSet::IsMajorityReached(m_wVoted, wTotal, m_nWhite);
}

void Node::Validator::Vote(uint8_t iKind)
{
	const uint32_t iR = 0;
	auto& rd = m_pR[iR];

	if (!m_pMe)
		return; // I'm not a validator

	if (m_pMe->m_pR[iR].m_pVote[iKind])
		return; // already voted

	auto& v = get_ParentObj().m_Keys.m_Validator; // alias

	PBFT_LOG(DEBUG, "voted " << static_cast<uint32_t>(iKind));

	proto::PbftVote msg;
	msg.m_iRound = static_cast<uint32_t>(m_iRound);
	msg.m_iKind = iKind;
	msg.m_Address = v.m_Addr;
	Sign(msg.m_Signature, rd.m_pPwr[iKind].m_hv);

	m_pMe->m_pR[iR].m_pVote[iKind] = msg.m_Signature;
	rd.m_pPwr[iKind].Add(*m_pMe);

	Broadcast(msg, nullptr);
}

uint32_t Node::Validator::get_PeerRound(const Peer& src, uint32_t iRoundMsg)
{
	auto& p = get_ParentObj().m_Processor; // alias
	if (p.m_Cursor.m_hh.m_Height != m_hAnchor)
		return _countof(m_pR); // I'm inactive

	if (src.m_Tip.m_hh != p.m_Cursor.m_hh)
		return _countof(m_pR); // peer state is different

	auto iR = iRoundMsg - static_cast<uint32_t>(m_iRound);
	if (iR < _countof(m_pR))
	{
		auto& rd = m_pR[iR];
		if (!rd.m_pLeader)
			rd.m_pLeader = m_ValidatorSet.SelectLeader(p.m_Cursor.m_hh.m_Hash, static_cast<uint32_t>(m_iRound + iR));
	}


	return iR;

}

void Node::Validator::OnMsg(proto::PbftVote&& msg, const Peer& src)
{
	auto iR = get_PeerRound(src, msg.m_iRound);
	if (iR >= _countof(m_pR))
		return;

	auto& rd = m_pR[iR];
	if (msg.m_iKind >= _countof(rd.m_pPwr))
		src.ThrowUnexpected("invalid vote kind");

	auto& vp = m_ValidatorSet.FindActiveStrict(msg.m_Address);

	auto& vsig = vp.m_pR[iR].m_pVote[msg.m_iKind];
	if (vsig)
		return; // already accounted

	PBFT_LOG(DEBUG, "vote " << static_cast<uint32_t>(msg.m_iKind) << " from " << msg.m_Address << " iR=" << iR);

	// there must be a proposal
	if (Proposal::State::None == rd.m_Proposal.m_State)
		return;

	// Note: we collect votes even BEFORE the proposal is accepted.

	auto& pwr = rd.m_pPwr[msg.m_iKind];

	if (!msg.m_Address.CheckSignature(pwr.m_hv, msg.m_Signature))
		src.ThrowUnexpected("invalid validator sig");

	if ((VoteKind::Commit == msg.m_iKind) && (vp.m_hvCommitted != rd.m_Proposal.m_hv))
	{
		if (vp.m_hvCommitted != Zero)
		{
			PBFT_LOG(DEBUG, "\tV " << vp.m_Key << "double commit");
			vp.m_Assessment.m_hShouldSlashAt = get_ParentObj().m_Processor.m_Cursor.m_hh.m_Height;
		}

		vp.m_hvCommitted = pwr.m_hv;
	}

	// ok
	vsig = msg.m_Signature;
	pwr.Add(vp);

	PBFT_LOG(DEBUG, "vote accepted");
	Broadcast(msg, &src);
	CheckState(iR);
}

void Node::Validator::OnMsg(proto::PbftRoundStart&& msg, const Peer& src)
{
	auto iR = get_PeerRound(src, msg.m_iRound);
	if (iR >= _countof(m_pR))
		return;

	if (msg.m_IsCommitted && !(m_iRound + iR))
		src.ThrowUnexpected("commit on r0");

	auto& vp = m_ValidatorSet.FindActiveStrict(msg.m_Address);
	auto& pr = vp.m_pR[iR];
	if (pr.m_MsgRoundStart)
		return; // already received

	ECC::Hash::Value hv;
	get_RoundStartMsg(hv, msg);
	if (!msg.m_Address.CheckSignature(hv, msg.m_Signature))
		src.ThrowUnexpected("invalid validator sig");

	pr.m_MsgRoundStart = std::move(msg);

	OnRoundStartReceived(iR, vp, &src);
	CheckState(iR);
}

void Node::Validator::OnRoundStartReceived(uint32_t iR, ValidatorPlus& vp, const Peer*)
{
	assert(vp.m_pR[iR].m_MsgRoundStart);
	auto& msg = *vp.m_pR[iR].m_MsgRoundStart;

	if (!msg.m_IsCommitted)
		m_pR[iR].m_pwrNotCommitted.Add(vp);

	PBFT_LOG(DEBUG, "round-start from " << msg.m_Address << " iR=" << iR << ", Committed=" << msg.m_IsCommitted);
	Broadcast(msg, nullptr);
}


void Node::Validator::OnMsg(proto::PbftSigRequest&& msg, const Peer& src)
{
	auto iR = get_PeerRound(src, msg.m_iRound);
	if (iR >= _countof(m_pR))
		return;

	auto& rd = m_pR[iR];
	if (rd.m_Multisig)
		return; // already received

	if (!rd.m_pLeader)
		return;

	if (Proposal::State::None == rd.m_Proposal.m_State)
		src.ThrowUnexpected("no proposal");

	ECC::Hash::Value hv;
	get_SigRequestMsg(hv, rd, msg);
	if (!rd.m_pLeader->m_Key.CheckSignature(hv, msg.m_Signature))
		src.ThrowUnexpected("invalid validator sig");

	// check the mask is ok, and the selected voting power is enough
	Power pwr;
	uint32_t iIdx = 0;
	for (const auto& vp : m_ValidatorSet.m_mapValidators)
	{
		auto w = vp.get_EffectiveWeight();
		if (!w)
			continue;
		if (!msg.m_Mask.get_safe(iIdx++))
			continue;

		if (!vp.m_pR[iR].m_MsgRoundStart)
			src.ThrowUnexpected("no rs");

		pwr.Add(vp);
	}

	if (!pwr.IsMajorityReached(m_wTotal))
		src.ThrowUnexpected("sig-request invalid");

	auto& ms = rd.m_Multisig.emplace();
	ms.m_Msg = std::move(msg);

	OnSigRequestReceived(iR, &src);
	CheckState(iR);
}

void Node::Validator::OnSigRequestReceived(uint32_t iR, const Peer* pSrc)
{
	auto& rd = m_pR[iR];
	assert(rd.m_Multisig);

	auto& ms = *rd.m_Multisig;
	ms.m_SigsRemaining = 0;

	// 1st phase: calculate the nonce
	ECC::Point::Native ptNoncePub;
	uint32_t iIdx = 0;
	for (auto& vp : m_ValidatorSet.m_mapValidators)
	{
		if (!vp.get_EffectiveWeight())
			continue;
		if (!ms.m_Msg.m_Mask.get_safe(iIdx++))
			continue;

		auto& pr = vp.m_pR[iR];
		assert(pr.m_MsgRoundStart);

		ECC::Point::Native pt;
		pt.Import(pr.m_MsgRoundStart->m_NoncePub); // don't care if failed
		ptNoncePub += pt;
	}

	ms.m_SigAggregate.m_NoncePub = ptNoncePub;
	ms.m_SigAggregate.m_k = Zero;

	// 2nd phase: derive the challenges
	ECC::Oracle oracle;
	ms.m_SigAggregate.Expose(oracle, rd.m_Proposal.m_hv);

	iIdx = 0;
	for (auto& vp : m_ValidatorSet.m_mapValidators)
	{
		if (!vp.get_EffectiveWeight())
			continue;
		if (!ms.m_Msg.m_Mask.get_safe(iIdx++))
			continue;

		ms.m_SigsRemaining++;

		auto& pr = vp.m_pR[iR];
		assert(!pr.m_Sig);

		auto& sig = pr.m_Sig.emplace();
		oracle >> sig.m_E;
		sig.m_k = Zero;
	}

	PBFT_LOG(INFO, "sig-request, iR=" << iR << ", num=" << ms.m_SigsRemaining);
	Broadcast(rd.m_Multisig->m_Msg, pSrc);
}

void Node::Validator::OnMsg(proto::PbftSig&& msg, const Peer& src)
{
	auto iR = get_PeerRound(src, msg.m_iRound);
	if (iR >= _countof(m_pR))
		return;

	auto& vp = m_ValidatorSet.FindActiveStrict(msg.m_Address);
	auto& pr = vp.m_pR[iR];

	if (!pr.m_Sig)
		src.ThrowUnexpected("not included");
		
	auto& sig = *pr.m_Sig;
	if (sig.m_E == Zero)
		return; // already signed

	assert(pr.m_MsgRoundStart);

	// check the partial signature: noncepub + G*sig + Pubkey*e == Zero
	{
		ECC::Mode::Scope scope(ECC::Mode::Fast);

		ECC::Point::Native pt, pt2;
		pt.Import(pr.m_MsgRoundStart->m_NoncePub);

		pt2 = ECC::Context::get().G * msg.m_Signature;
		pt += pt2;

		pt2 = Zero;
		vp.m_Key.ExportNnz(pt2);
		pt2 = pt2 * sig.m_E;
		pt += pt2;

		if (pt != Zero)
			src.ThrowUnexpected("invalid sig");
	}
	
	sig.m_k = msg.m_Signature;
	OnSigReceived(iR, vp, &src);

	CheckState(iR);
}

void Node::Validator::OnSigReceived(uint32_t iR, ValidatorPlus& vp, const Peer* pPeer)
{
	auto& pr = vp.m_pR[iR];
	assert(pr.m_Sig);
	auto& sig = *pr.m_Sig;
	assert(sig.m_E != Zero);

	auto& rd = m_pR[iR];
	assert(rd.m_Multisig);
	auto& ms = *rd.m_Multisig;
	assert(ms.m_SigsRemaining);

	ECC::Scalar::Native sk = sig.m_k;
	sk += ms.m_SigAggregate.m_k;
	
	ms.m_SigAggregate.m_k = sk;
	ms.m_SigsRemaining--;
	sig.m_E = Zero;

	PBFT_LOG(DEBUG, "p-sig from " << vp.m_Key << " iR=" << iR);

	proto::PbftSig msg;
	msg.m_iRound = static_cast<uint32_t>(m_iRound + iR);
	msg.m_Address = vp.m_Key;
	msg.m_Signature = sig.m_k;

	Broadcast(msg, pPeer);
}

void Node::Validator::OnMsg(proto::PbftProposal&& msg, const Peer& src)
{
	auto iR = get_PeerRound(src, msg.m_iRound);
	if (iR >= _countof(m_pR))
		return;

	auto& rd = m_pR[iR];
	PBFT_LOG(DEBUG, "proposal iR=" << iR);

	if (Proposal::State::None != rd.m_Proposal.m_State)
		return; // already received

	auto& p = get_ParentObj().m_Processor; // alias

	if (!rd.m_pLeader)
		return;

	rd.m_Proposal.m_Msg = std::move(msg);

	Block::SystemState::Full s;
	MakeFullHdr(s, rd.m_Proposal.m_Msg.m_Hdr);
	s.get_Hash(rd.m_Proposal.m_hv);
	rd.SetHashes();

	Merkle::Hash hv;
	get_ProposalMsg(hv, rd);
	if (!rd.m_pLeader->m_Key.CheckSignature(hv, rd.m_Proposal.m_Msg.m_Signature))
		src.ThrowUnexpected("invalid proposal sig");

	auto tm_ms = rd.m_Proposal.m_Msg.m_Hdr.get_Timestamp_ms();
	RoundTimes rt;
	rt.m_t0 = tm_ms;
	rt.FromT0(Rules::get(), p.m_Cursor.m_Full);

	if ((rt.m_ts != tm_ms) || (rt.m_iRound > msg.m_iRound))
	{
		PBFT_LOG(DEBUG, "invalid proposal ts");
		return;
	}

	HeightHash id;
	id.m_Hash = rd.m_Proposal.m_hv; // not the final hash
	id.m_Height = s.get_Height();

	if (!p.TestBlock(id, s, rd.m_Proposal.m_Msg.m_Body))
	{
		// TODO: don't disconnect this peer. Save and broadcast this info, punish leader for invalid proposal
		// current design: ignore invalid proposals
		PBFT_LOG(DEBUG, "invalid proposal");
		return;
	}

	// all good
	rd.m_Proposal.m_State = Proposal::State::Received;
	OnProposalReceived(iR, &src);
	CheckState(iR);
}

void Node::Validator::OnProposalReceived(uint32_t iR, const Peer* pSrc)
{
	auto& rd = m_pR[iR];
	assert(Proposal::State::Received == rd.m_Proposal.m_State);

	PBFT_LOG(INFO, "proposal received " << rd.m_Proposal.m_hv << ", iR=" << iR);
	Broadcast(rd.m_Proposal.m_Msg, pSrc);
}

void Node::Validator::get_ProposalMsg(Merkle::Hash& hv, const RoundData& rd) const
{
	ECC::Hash::Processor()
		<< m_hAnchor
		<< "pbft.propose.2"
		<< rd.m_Proposal.m_hv
		<< rd.m_Proposal.m_Msg.m_iRound
		>> hv;
}

void Node::Validator::get_SigRequestMsg(Merkle::Hash& hv, const RoundData& rd, const proto::PbftSigRequest& msg) const
{
	ECC::Hash::Processor()
		<< m_hAnchor
		<< "pbft.sigreq.2"
		<< rd.m_Proposal.m_hv
		<< msg.m_iRound
		<< Blob(msg.m_Mask.m_p, msg.m_Mask.nBytes)
		>> hv;
}

void Node::Validator::MakeFullHdr(Block::SystemState::Full& s, const Block::SystemState::Sequence::Element& el) const
{
	auto& p = get_ParentObj().m_Processor; // alias

	s.m_ChainWork = p.m_Cursor.m_Full.m_ChainWork + el.m_PoW.m_Difficulty;

	const auto& d0 = Cast::Reinterpret<Block::Pbft::HdrData>(p.m_Cursor.m_Full.m_PoW);
	if (Block::Pbft::HdrData::Flags::Empty & d0.m_Flags1)
	{
		// cannibalize prev
		s.m_ChainWork -= p.m_Cursor.m_Full.m_PoW.m_Difficulty;
		s.m_Prev = p.m_Cursor.m_Full.m_Prev;
		s.m_Number = p.m_Cursor.m_Full.m_Number;
	}
	else
	{
		s.m_Prev = p.m_Cursor.m_hh.m_Hash;
		s.m_Number.v = p.m_Cursor.m_Full.m_Number.v + 1;
	}

	Cast::Down<Block::SystemState::Sequence::Element>(s) = el;
}

void Node::Validator::RoundData::SetHashes()
{
	for (uint32_t iKind = 0; iKind < _countof(m_pPwr); iKind++)
		ECC::Hash::Processor()
			<< "pbft.vote.2.kind"
			<< m_Proposal.m_hv
			<< iKind
			>> m_pPwr[iKind].m_hv;
}

void Node::Validator::CheckState(uint32_t iR)
{
	if (State::QuorumReached == m_State)
		return;

	if (m_pMe && !iR)
		CheckStateCurrent();

	auto& rd = m_pR[iR];
	if (!rd.m_Multisig)
		return;
	auto& ms = *rd.m_Multisig;
	if (ms.m_SigsRemaining)
		return;

	// We have the quorum
	m_State = State::QuorumReached;
	PBFT_LOG(INFO, "quorum reached");

	auto& d = Cast::Reinterpret<Block::Pbft::HdrData>(rd.m_Proposal.m_Msg.m_Hdr.m_PoW);
	d.m_QC.m_Signature = ms.m_SigAggregate;
	d.m_QC.m_Mask = ms.m_Msg.m_Mask;

	Block::SystemState::Full s;
	MakeFullHdr(s, rd.m_Proposal.m_Msg.m_Hdr);

	Block::SystemState::ID id;
	id.m_Number = s.m_Number;
	id.m_Hash = rd.m_Proposal.m_hv;

	PBFT_LOG(INFO, "quorum reached " << id.m_Hash);

	const Rules& r = Rules::get();

	if (r.m_Pbft.m_Whitelist.m_NumRequired)
	{
		Height h = s.get_Height();
		if (h > m_Stamp.m_ID.m_Height)
		{
			m_ValidatorSet.get_Hash(m_Stamp.m_Msg.m_hvVsNext);

			// export our validator set it in a form compatible with basic set
			m_Stamp.m_Msg.m_ValidatorSet.m_map.clear();
			for (const auto& vp : m_ValidatorSet.m_mapValidators)
			{
				auto w = vp.get_EffectiveWeight();
				if (w)
					m_Stamp.m_Msg.m_ValidatorSet.m_map[vp.m_Key] = w;
			}

			m_Stamp.m_ID.m_Height = h;
			m_Stamp.m_ID.m_Hash = id.m_Hash;

			SaveStamp();
		}
	}

	auto& p = get_ParentObj().m_Processor;
	auto eVal = p.OnState(s, Zero);

	switch (eVal)
	{
	case NodeProcessor::DataStatus::Accepted:
	case NodeProcessor::DataStatus::Rejected: // unlikely, but nevermind
		break; // ok

	default:
		return; // invalid or unreachable header?!
	}

	eVal = p.OnBlock(id, rd.m_Proposal.m_Msg.m_Body.m_Perishable, rd.m_Proposal.m_Msg.m_Body.m_Eternal, Zero);

	switch (eVal)
	{
	case NodeProcessor::DataStatus::Accepted:
	case NodeProcessor::DataStatus::Rejected: // unlikely, but nevermind
		break; // ok

	default:
		return; // block with no header, or etc?!
	}

	get_ParentObj().m_Processor.TryGoUpAsync();
}

void Node::Validator::CheckStateCurrent()
{
	assert(m_pMe);

	const uint32_t iR = 0;
	auto& rd = m_pR[iR];
	if (!rd.m_pLeader)
		return;

	if (Proposal::State::Accepted != rd.m_Proposal.m_State)
	{
		if (Proposal::State::Received != rd.m_Proposal.m_State)
		{
			if (rd.m_pLeader != m_pMe)
				return;

			GenerateProposal();
			if (Proposal::State::Received != rd.m_Proposal.m_State)
				return;

			OnProposalReceived(iR, nullptr);
		}

		bool bAlreadyCommitted = (m_pMe->m_hvCommitted == rd.m_Proposal.m_hv);
		if (!bAlreadyCommitted && !ShouldAcceptProposal())
			return;

		rd.m_Proposal.m_State = Proposal::State::Accepted;

		PBFT_LOG(INFO, "proposal accepted");

		if (State::None == m_State)
			Vote(VoteKind::PreVote);
		else
			if (bAlreadyCommitted)
			{
				Vote(VoteKind::PreVote);
				Vote(VoteKind::Commit);
			}
	}

	if ((State::None == m_State) && m_pMe->m_pR[iR].m_pVote[VoteKind::PreVote] && rd.m_pPwr[VoteKind::PreVote].IsMajorityReached(m_wTotal))
	{
		Vote(VoteKind::Commit);

		PBFT_LOG(INFO, "committed");
		m_State = State::Committed;
		m_pMe->m_hvCommitted = rd.m_Proposal.m_hv;
	}

	if (!rd.m_pPwr[VoteKind::Commit].IsMajorityReached(m_wTotal))
		return;

	if (!rd.m_Multisig)
	{
		if (m_pMe != rd.m_pLeader)
			return;

		// try to select the peers for the multisig
		Bitmask<Block::Pbft::s_MaxValidators> msk;
		if (!SelectMultisigValidators(msk))
			return;

		auto& ms = rd.m_Multisig.emplace();
		ms.m_Msg.m_iRound = static_cast<uint32_t>(m_iRound);
		ms.m_Msg.m_Mask = std::move(msk);

		ECC::Hash::Value hv;
		get_SigRequestMsg(hv, rd, ms.m_Msg);
		Sign(ms.m_Msg.m_Signature, hv);

		OnSigRequestReceived(iR, nullptr);
	}

	if (m_pMe->m_pR[iR].m_Sig)
	{
		auto& sig = *m_pMe->m_pR[iR].m_Sig;
		if (sig.m_E != Zero)
		{
			// submit my signature
			assert(rd.m_Multisig->m_SigsRemaining);

			// check the partial signature: skNonce + sig + skValidator*e == Zero

			ECC::Scalar::Native k = get_ParentObj().m_Keys.m_Validator.m_sk * sig.m_E;
			k += m_skNonce;
			k = -k;

			sig.m_k = k;
			OnSigReceived(iR, *m_pMe, nullptr);
		}
	}
}

bool Node::Validator::SelectMultisigValidators(Bitmask<Block::Pbft::s_MaxValidators>& mask)
{
	// select the validators for the multisig ceremony
	// their current commitment is irrelevant: once they observe the quorum (supermajority) - each would participate
	//
	// we need to reach >2/3 (standard supermajority), but can select more. Attempt to select those with higher assessment (rating)

	ZeroObject(mask.m_p);

	const uint32_t iR = 0;
	std::pair<uint32_t, const ValidatorPlus*> pVec[Block::Pbft::s_MaxValidators];
	uint32_t nCount = 0;

	uint32_t iIdx = 0;
	for (const auto& vp : m_ValidatorSet.m_mapValidators)
	{
		if (!vp.get_EffectiveWeight())
			continue;

		auto& pr = vp.m_pR[iR];
		if (pr.m_MsgRoundStart)
			pVec[nCount++] = std::make_pair(iIdx, &vp);

		if (++iIdx == _countof(pVec))
			break;
	}

	std::stable_sort(pVec, pVec + nCount,
		[](const auto& a, const auto& b)
		{
			// higher score - worse validator
			return a.second->m_Assessment.m_Score < b.second->m_Assessment.m_Score;
		}
	);

	Power pwr;
	for (uint32_t i = 0; i < nCount; i++)
	{
		auto& x = pVec[i];
		const auto& vp = *x.second;

		if ((vp.m_Assessment.m_Score > Assessment::Settings::Watermark::AcceptForMultisig) && pwr.IsMajorityReached(m_wTotal))
			break;

		mask.set(x.first);
		pwr.Add(vp);
	}

	return pwr.IsMajorityReached(m_wTotal);
}

void Node::Validator::SaveStamp()
{
	Serializer ser;
	ser
		& m_Stamp.m_ID
		& m_Stamp.m_Msg;

	Blob blob(ser.buffer().first, (uint32_t) ser.buffer().second);
	get_ParentObj().m_Processor.get_DB().ParamSet(NodeDB::ParamID::PbftStamp, nullptr, &blob);
}

bool Node::Validator::ShouldSendStamp()
{
	const Rules& r = Rules::get();
	return
		r.IsPbftWhitelistMode() &&
		(get_ParentObj().m_Processor.m_Cursor.m_hh == m_Stamp.m_ID);
}

void Node::Validator::CreateProposalMd(NodeProcessor::BlockContext& bc)
{
	if (!m_cid)
		return; // for more safety, shouldn't happen

	auto& p = get_ParentObj().m_Processor; // alias

	assert(m_pMe && m_wTotal);
	auto& my_map = m_pMe->m_Assessment.m_Last.m_Reputation;

	for (const auto& x : my_map)
	{
		const auto* pV = m_ValidatorSet.Find(x.first);
		if (!pV)
			continue;

		auto fMy = x.second;
		if (pV->m_Status == fMy)
			continue; // no change
		
		// check if there's a consensus here
		Power pwr;

		for (const auto& vp : m_ValidatorSet.m_mapValidators)
			if (IsAssessmentRelevant(vp.m_Assessment.m_Last))
			{
				auto it = vp.m_Assessment.m_Last.m_Reputation.find(x.first);
				if (vp.m_Assessment.m_Last.m_Reputation.end() != it)
				{
					auto fThis = it->second;
					if (fThis == fMy)
						pwr.Add(vp); // supports this
				}
			}

		if (!pwr.IsMajorityReached(m_wTotal))
			continue;

		using namespace Shaders::I_PBFT;


		auto pKrn = std::make_unique<TxKernelContractInvoke>();
		pKrn->m_Cid = *m_cid;
		pKrn->m_iMethod = Method::ValidatorStatusUpdate::s_iMethod;

		pKrn->m_Args.resize(sizeof(Method::ValidatorStatusUpdate));
		auto& r = *(Method::ValidatorStatusUpdate*) &pKrn->m_Args.front();
		r.m_Address = Cast::Down<Address>(x.first);
		r.m_Status = (Shaders::I_PBFT::State::Validator::Status) ByteOrder::to_le(fMy);


		pKrn->m_Height.m_Min = p.m_Cursor.m_hh.m_Height + 1;

		ECC::Point::Native ptFunds(Zero);
		pKrn->Sign(&get_ParentObj().m_Keys.m_Validator.m_sk, 1, ptFunds, nullptr);

		bc.m_Block.m_vKernels.push_back(std::move(pKrn));

		ECC::Scalar::Native s = -get_ParentObj().m_Keys.m_Validator.m_sk;
		s += bc.m_Block.m_Offset;
		bc.m_Block.m_Offset = s;
	}
}

bool Node::Validator::CreateProposal()
{
	// In PBFT mode miner/owner keys are not used, since the block contains neither subsidy nor UTXO collecting fees
	auto& kdfDummy = *get_ParentObj().m_Keys.m_pGeneric;

	NodeProcessor::BlockContext bc(get_ParentObj().m_TxPool, 0, kdfDummy, kdfDummy);
	bc.m_pParent = get_ParentObj().m_TxDependent.m_pBest;

	auto& p = get_ParentObj().m_Processor;

	RoundTimes rt;
	rt.m_iRound = m_iRound;
	rt.FromRound(Rules::get(), p.m_Cursor.m_Full);

	bc.m_Hdr.m_TimeStamp = static_cast<Timestamp>(rt.m_ts / 1000);

	auto& d = Cast::Reinterpret<Block::Pbft::HdrData>(bc.m_Hdr.m_PoW);
	d.m_Time_ms = static_cast<uint16_t>(rt.m_ts % 1000);

	CreateProposalMd(bc);

	bool bRes = p.GenerateNewBlock(bc);

	if (bRes)
	{
		auto& rdcurr = m_pR[0];
		bc.m_Hdr.get_Hash(rdcurr.m_Proposal.m_hv);
		rdcurr.m_Proposal.m_Msg.m_Hdr = std::move(bc.m_Hdr);
		rdcurr.m_Proposal.m_Msg.m_Body = std::move(bc.m_Body);
		rdcurr.SetHashes();
	}

	return bRes;
}

uint64_t Node::Validator::get_RefTime_ms() const
{
	using namespace std::chrono;
	auto tNow = system_clock::now().time_since_epoch();
	return duration_cast<milliseconds>(tNow).count() + get_ParentObj().m_Cfg.m_TestMode.m_TimeDrift_ms;
}

void Node::Validator::OnTimer()
{
	m_bTimerPending = false;
	OnNewRound();
}

void Node::Validator::KillTimer()
{
	if (m_bTimerPending)
	{
		m_bTimerPending = false;
		m_pTimer->cancel();
	}
}

void Node::Validator::SetTimer(uint64_t tNow_ms, uint64_t tTrg_ms)
{
	auto dt_ms = (tTrg_ms > tNow_ms) ? static_cast<uint32_t>(tTrg_ms - tNow_ms) : 0;

	if (!m_pTimer)
		m_pTimer = io::Timer::create(io::Reactor::get_Current());

	m_pTimer->start(dt_ms, false, [this]() { OnTimer(); });
	m_bTimerPending = true;
}

} // namespace beam
