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

#include "node_common.h"

namespace beam {

void Node::Peer::OnMsg(proto::GetHdr&& msg)
{
	uint64_t rowid = m_This.m_Processor.get_DB().StateFindSafe(msg.m_ID);
	if (rowid)
	{
		proto::Hdr msgHdr;
		m_This.m_Processor.get_DB().get_State(rowid, msgHdr.m_Description);
		Send(msgHdr);
	} else
		Send(proto::DataMissing());
}

void Node::Peer::OnMsg(proto::GetHdrPack&& msg)
{
	NodeDB::StateID sid;
	sid.m_Number = msg.m_Top.m_Number;
	sid.m_Row = m_This.m_Processor.get_DB().StateFindSafe(msg.m_Top);
	SendHdrs(sid, msg.m_Count);
}

void Node::Peer::OnMsg(proto::EnumHdrs&& msg)
{
	NodeDB::StateID sid;
	uint32_t nCount;

	HeightRange hr(1, m_This.m_Processor.m_Cursor.m_hh.m_Height);
	hr.Intersect(msg.m_Height);
	if (!hr.IsEmpty())
	{
		m_This.m_Processor.FindAtivePastHeight(sid, hr.m_Min);
		auto num1 = m_This.m_Processor.FindAtivePastHeight(hr.m_Max);

		uint64_t dn = num1.v - sid.m_Number.v + 1;
		nCount = (dn > proto::g_HdrPackMaxSize) ? proto::g_HdrPackMaxSize : static_cast<uint32_t>(dn);
	}
	else
	{
		nCount = 0;
		sid.m_Row = 0;
	}

	SendHdrs(sid, nCount);
}

void Node::Peer::SendHdrs(NodeDB::StateID& sid, uint32_t nCount)
{
	if (nCount && sid.m_Row)
	{
		// don't throw unexpected if pack size is bigger than max. In case it'll be increased in future versions - just truncate it.
		beam::setmin(nCount, proto::g_HdrPackMaxSize);

		proto::HdrPack msgOut;
		msgOut.m_vElements.reserve(nCount);

		NodeDB& db = m_This.m_Processor.get_DB();

		NodeDB::WalkerSystemState wlk;
		for (db.EnumSystemStatesBkwd(wlk, sid); wlk.MoveNext(); )
		{
			msgOut.m_vElements.push_back(wlk.m_State);

			if (msgOut.m_vElements.size() == nCount)
				break;
		}

		if (!msgOut.m_vElements.empty())
		{
			msgOut.m_Prefix = wlk.m_State;
			Send(msgOut);
			return;
		}
	}

	Send(proto::DataMissing());
}

bool Node::DecodeAndCheckHdrs(std::vector<Block::SystemState::Full>& v, const proto::HdrPack& msg)
{
	if (msg.m_vElements.empty() || (msg.m_vElements.size() > proto::g_HdrPackMaxSize))
		return false;

	// PoW verification is heavy for big packs. Do it in parallel
	Executor::Scope scope(m_Processor.m_ExecutorMT);

	proto::FlyClient::Data::DecodedHdrPack ex;
	if (!ex.DecodeAndCheck(msg))
		return false;

	v = std::move(ex.m_vStates);
	return true;
}

void Node::Peer::OnMsg(proto::HdrPack&& msg)
{
	Task& t = get_FirstTask();

	if (t.m_bSkeleton)
	{
		std::vector<Block::SystemState::Full> v;
		if ((msg.m_vElements.size() != 1) || !m_This.DecodeAndCheckHdrs(v, msg) || (v.front().m_Number.v != t.m_Key.first.m_Number.v))
			ThrowUnexpected();

		Block::SystemState::ID id;
		if (NodeProcessor::DataStatus::Invalid == m_This.m_Processor.OnStateSilent(v.front(), m_pInfo->m_ID.m_Key, id, true))
			ThrowUnexpected();

		BEAM_LOG_DEBUG() << "Hdr skeleton point received " << id;
		t.m_bSkeletonDone = true;

		uint64_t row = m_This.m_Processor.get_DB().StateFindSafe(id);
		if (row)
			m_This.m_HdrSkeleton.m_setPoints.insert(std::make_pair(id.m_Number.v, row));

		// Don't modify the rating. It's the effective bandwidth, whereas the point is tiny, and the time since it was assigned
		// includes the replies to the previous tasks in the peer's queue. The peer would look very slow.
		// A timeout is still accounted for (see OnRequestTimeout).

		ReleaseTask(t);
		SetTimerWrtFirstTask();

		// The pack below this point is requested by the deferred RefreshCongestions() (see OnGoUpTimer). Unlike OnFirstTaskDone(),
		// don't refresh right away: the points come in a row, and a single refresh for all of them is enough.
		m_This.m_Processor.TryGoUpAsync();
		return;
	}

	if (t.m_Key.second || !t.m_nCount) {
		// stupid compiler insists on parentheses here!
		ThrowUnexpected();
	}

	std::vector<Block::SystemState::Full> v;
	if (!m_This.DecodeAndCheckHdrs(v, msg))
		ThrowUnexpected();

	// just to be pedantic, AND prevent non-stamped hdrs in pbft-w mode
	Block::SystemState::ID idLast;
	v.back().get_ID(idLast);
	if (!(idLast == t.m_Key.first))
		ThrowUnexpected();

	for (size_t i = 0; i < v.size(); i++)
	{
		NodeProcessor::DataStatus::Enum eStatus = m_This.m_Processor.OnStateSilent(v[i], m_pInfo->m_ID.m_Key, idLast, true);
		switch (eStatus)
		{
		case NodeProcessor::DataStatus::Invalid:
			// though PoW was already tested, header can still be invalid. For instance, due to improper Timestamp
			ThrowUnexpected();
			break;

		case NodeProcessor::DataStatus::Accepted:
			// no break;

		default:
			break; // suppress warning
		}
	}

	BEAM_LOG_INFO() << "Hdr pack received " << msg.m_Prefix.m_Number.v << "-" << idLast;

	ModifyRatingWrtData(sizeof(msg.m_Prefix) + msg.m_vElements.size() * sizeof(msg.m_vElements.front()));

	OnFirstTaskDone(NodeProcessor::DataStatus::Accepted);
	m_This.UpdateSyncStatus();
}

void Node::Peer::OnMsg(proto::GetBody&& msg)
{
	proto::GetBodyPack msg2;
	msg2.m_Top = msg.m_ID;
	OnMsg(std::move(msg2));
}

void Node::Peer::OnMsg(proto::GetBodyPack&& msg)
{
	Processor& p = m_This.m_Processor; // alias

	if (msg.m_Top.m_Number.v)
	{
		NodeDB::StateID sid;
		sid.m_Row = p.get_DB().StateFindSafe(msg.m_Top);
		if (sid.m_Row)
		{
			sid.m_Number = msg.m_Top.m_Number;

			if (msg.m_CountExtra.v)
			{
				if (sid.m_Number.v - 1 < msg.m_CountExtra.v)
					ThrowUnexpected();

				if (NodeDB::StateFlags::Active & p.get_DB().GetStateFlags(sid.m_Row))
				{
					// functionality only supported for active states
					proto::BodyPack msgBody;
					size_t nSize = 0;

					sid.m_Number.v -= msg.m_CountExtra.v;
					auto nMax = std::min(msg.m_Top.m_Number.v, sid.m_Number.v + m_This.m_Cfg.m_BandwidthCtl.m_MaxBodyPackCount);

					for (; sid.m_Number.v <= nMax; sid.m_Number.v++)
					{
						sid.m_Row = p.FindActiveAtStrict(sid.m_Number);

						proto::BodyBuffers bb;
						if (!GetBlock(bb, sid, msg, true))
							break;

						nSize += bb.m_Eternal.size() + bb.m_Perishable.size();
						msgBody.m_Bodies.push_back(std::move(bb));

						if (nSize >= m_This.m_Cfg.m_BandwidthCtl.m_MaxBodyPackSize)
							break;
					}

					if (msgBody.m_Bodies.size())
					{
						Send(msgBody);
						return;
					}
				}
			}
			else
			{
				proto::Body msgBody;
				if (GetBlock(msgBody.m_Body, sid, msg, false))
				{
					Send(msgBody);
					return;
				}
			}
		}
	}
	else
	{
		if ((msg.m_Top.m_Hash == Zero) && p.IsTreasuryHandled())
		{
			proto::Body msgBody;
			if (p.get_DB().ParamGet(NodeDB::ParamID::Treasury, NULL, NULL, &msgBody.m_Body.m_Eternal))
			{
				Send(msgBody);
				return;
			}
		}
	}

	Send(proto::DataMissing());
}

bool Node::Peer::GetBlock(proto::BodyBuffers& out, const NodeDB::StateID& sid, const proto::GetBodyPack& msg, bool bActive)
{
	ByteBuffer* pP = nullptr;
	ByteBuffer* pE = nullptr;

	switch (msg.m_FlagE)
	{
	case proto::BodyBuffers::Full:
		pE = &out.m_Eternal;
		// no break;
	case proto::BodyBuffers::None:
		break;
	default:
		ThrowUnexpected();
	}

	switch (msg.m_FlagP)
	{
	case proto::BodyBuffers::Recovery1:
	case proto::BodyBuffers::Full:
		pP = &out.m_Perishable;
		// no break;
	case proto::BodyBuffers::None:
		break;
	default:
		ThrowUnexpected();
	}

	// A diluted block depends on where its outputs are spent up to m_HorizonHi1 (the requester's sync target), in our active chain.
	// Above our cursor it's unknown: such outputs would be sent as unspent, and the requester's fast sync would fail at its TxoLo.
	if ((msg.m_HorizonHi1.v > sid.m_Number.v) && (msg.m_HorizonHi1.v > m_This.m_Processor.m_Cursor.m_Full.m_Number.v))
		return false;

	if (!m_This.m_Processor.GetBlock(sid, pE, pP, msg.m_Block0, msg.m_HorizonLo1, msg.m_HorizonHi1, bActive))
		return false;

	if (proto::BodyBuffers::Recovery1 == msg.m_FlagP)
	{
		Block::Body block;

		Deserializer der;
		der.reset(out.m_Perishable);
		der & Cast::Down<Block::BodyBase>(block);
		der & Cast::Down<TxVectors::Perishable>(block);

		Serializer ser;
		ser & block.m_vInputs;
		ser & block.m_vOutputs.size();

		auto h = m_This.m_Processor.Num2Height(sid);

		for (size_t i = 0; i < block.m_vOutputs.size(); i++)
		{
			const auto& outp = *block.m_vOutputs[i];
			yas::detail::saveRecovery(ser, outp, h);
		}

		ser & Cast::Down<Block::BodyBase>(block);
		ser & Cast::Down<TxVectors::Perishable>(block);

		ser.swap_buf(out.m_Perishable);
	}

	return true;
}

bool Node::Peer::ShouldAcceptBodyPack()
{
	Task& t = get_FirstTask();
	const NodeProcessor::SyncData& d = m_This.m_Processor.m_SyncData; // alias
	return (t.m_n0.v == d.m_n0.v) && (t.m_nTxoLo.v == d.m_TxoLo.v);
}

void Node::Peer::OnMsg(proto::Body&& msg)
{
	Task& t = get_FirstTask();

	if (!t.m_Key.second)
		ThrowUnexpected();

	ModifyRatingWrtData(msg.m_Body.m_Eternal.size() + msg.m_Body.m_Perishable.size());

	const Block::SystemState::ID& id = t.m_Key.first;
	auto num = id.m_Number;

	Processor& p = m_This.m_Processor; // alias

	NodeProcessor::DataStatus::Enum eStatus = num.v ?
		ShouldAcceptBodyPack() ?
			p.OnBlock(id, msg.m_Body.m_Perishable, msg.m_Body.m_Eternal, m_pInfo->m_ID.m_Key) :
			NodeProcessor::DataStatus::Rejected :
		p.OnTreasury(msg.m_Body.m_Eternal);

	p.TryGoUpAsync();
	OnFirstTaskDone(eStatus);
}

void Node::Peer::OnMsg(proto::BodyPack&& msg)
{
	Task& t = get_FirstTask();

	if (!t.m_Key.second || !t.m_nCount)
		ThrowUnexpected();

	const Block::SystemState::ID& id = t.m_Key.first;
	Processor& p = m_This.m_Processor;

	assert(t.m_sidTrgRequested.m_Number.v >= id.m_Number.v);
	uint64_t hCountExtra = t.m_sidTrgRequested.m_Number.v - id.m_Number.v;

	if (msg.m_Bodies.size() > hCountExtra + 1)
		ThrowUnexpected();

	size_t nSize = 0;
	for (size_t i = 0; i < msg.m_Bodies.size(); i++)
	{
		nSize +=
			msg.m_Bodies[i].m_Eternal.size() +
			msg.m_Bodies[i].m_Perishable.size();
	}
	ModifyRatingWrtData(nSize);

	NodeProcessor::DataStatus::Enum eStatus = NodeProcessor::DataStatus::Rejected;
	if (!msg.m_Bodies.empty() && ShouldAcceptBodyPack())
	{
		std::deque<uint64_t>::const_iterator itRows;
		if (p.get_CachedRows(t.m_sidTrgRequested, hCountExtra, itRows))
		{
			BEAM_LOG_INFO() << id << " Block pack received " << id.m_Number.v << "-" << (id.m_Number.v + msg.m_Bodies.size() - 1);

			eStatus = NodeProcessor::DataStatus::Accepted;

			for (uint64_t n = 0; n < msg.m_Bodies.size(); n++)
			{
				NodeDB::StateID sid;
				sid.m_Row = itRows[hCountExtra - n];
				sid.m_Number.v = id.m_Number.v + n;

				const proto::BodyBuffers& bb = msg.m_Bodies[n];

				NodeProcessor::DataStatus::Enum es2 = p.OnBlock(sid, bb.m_Perishable, bb.m_Eternal, m_pInfo->m_ID.m_Key);
				if (NodeProcessor::DataStatus::Invalid == es2)
				{
					p.OnPeerInsane(m_pInfo->m_ID.m_Key);
					break;
				}
			}
		}
	}

	p.TryGoUpAsync();
	OnFirstTaskDone(eStatus);
}

void Node::Peer::OnFirstTaskDone(NodeProcessor::DataStatus::Enum eStatus)
{
	if (NodeProcessor::DataStatus::Invalid == eStatus)
		ThrowUnexpected();

	get_FirstTask().m_bNeeded = false;
	OnFirstTaskDone();
}

} // namespace beam
