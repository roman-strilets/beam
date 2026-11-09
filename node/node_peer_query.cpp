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

void Node::Peer::OnMsg(proto::GetCommonState&& msg)
{
	proto::ProofCommonState msgOut;

	Processor& p = m_This.m_Processor; // alias

	for (size_t i = 0; i < msg.m_IDs.size(); i++)
	{
		const Block::SystemState::ID& id = msg.m_IDs[i];
		if (!id.m_Number.v)
			ThrowUnexpected();

		if ((id.m_Number.v < p.m_Cursor.m_Full.m_Number.v) && !p.IsFastSync())
		{
			Merkle::Hash hv;
			p.get_DB().get_StateHash(p.FindActiveAtStrict(id.m_Number), hv);

			if ((hv == id.m_Hash) || (i + 1 == msg.m_IDs.size()))
			{
				msgOut.m_ID.m_Number = id.m_Number;
				msgOut.m_ID.m_Hash = hv;
				p.GenerateProofStateStrict(msgOut.m_Proof, id.m_Number);
				break;
			}
		}
	}

	Send(msgOut);
}

void Node::Peer::OnMsg(proto::GetProofState&& msg)
{
	if (!msg.m_Number.v)
		ThrowUnexpected();

	proto::ProofState msgOut;

	Processor& p = m_This.m_Processor;
	if ((msg.m_Number.v < p.m_Cursor.m_Full.m_Number.v) && !p.IsFastSync())
		p.GenerateProofStateStrict(msgOut.m_Proof, msg.m_Number);

	Send(msgOut);
}

void Node::Peer::OnMsg(proto::GetProofKernel&& msg)
{
	proto::ProofKernel msgOut;

	Processor& p = m_This.m_Processor;
	if (!p.IsFastSync())
	{
		NodeDB::StateID sid;
		Height h = p.get_ProofKernel(&msgOut.m_Proof.m_Inner, nullptr, sid, msg.m_ID, nullptr);
		if (h)
		{
			p.get_DB().get_State(sid.m_Row, msgOut.m_Proof.m_State);

			if (sid.m_Number.v < p.m_Cursor.m_Full.m_Number.v)
				p.GenerateProofStateStrict(msgOut.m_Proof.m_Outer, msgOut.m_Proof.m_State.m_Number);
		}
	}
	Send(msgOut);
}

void Node::Peer::OnMsg(proto::GetProofKernel2&& msg)
{
	proto::ProofKernel2 msgOut;

	Processor& p = m_This.m_Processor;
	if (!p.IsFastSync())
	{
		NodeDB::StateID sid;
		msgOut.m_Height = p.get_ProofKernel(&msgOut.m_Proof, msg.m_Fetch ? &msgOut.m_Kernel : nullptr, sid, msg.m_ID, nullptr);
	}
	Send(msgOut);
}

void Node::Peer::OnMsg(proto::GetProofKernel3&& msg)
{
	proto::ProofKernel2 msgOut;

	Processor& p = m_This.m_Processor;
	if (!p.IsFastSync())
	{
		Merkle::Hash idKrn(Zero); // dummy
		NodeDB::StateID sid;
		msgOut.m_Height = p.get_ProofKernel(msg.m_WithProof ? &msgOut.m_Proof : nullptr, &msgOut.m_Kernel, sid, idKrn, &msg.m_Pos);
	}
	Send(msgOut);
}

void Node::Peer::OnMsg(proto::GetProofUtxo&& msg)
{
	struct Traveler :public UtxoTree::ITraveler
	{
		proto::ProofUtxo m_Msg;
		NodeProcessor& m_Proc;

		bool OnLeaf(const RadixTree::Leaf& x) override {

			const UtxoTree::MyLeaf& v = Cast::Up<UtxoTree::MyLeaf>(x);
			UtxoTree::Key::Data d;
			d = v.m_Key;

			Input::Proof& ret = m_Msg.m_Proofs.emplace_back();

			ret.m_State.m_Count = v.get_Count();
			ret.m_State.m_Maturity = d.m_Maturity;
			m_Proc.get_Utxos().get_Proof(ret.m_Proof, *m_pCu);

			struct MyProofBuilder
				:public NodeProcessor::ProofBuilder
			{
				using ProofBuilder::ProofBuilder;
				bool get_Utxos(Merkle::Hash&) override { return false; }
			};

			MyProofBuilder pb(m_Proc, ret.m_Proof);
			pb.GenerateProof();

			return m_Msg.m_Proofs.size() < Input::Proof::s_EntriesMax;
		}

		Traveler(NodeProcessor& np) :m_Proc(np) {}
	};

	Processor& p = m_This.m_Processor;
	Traveler t(p);

	if (!p.IsFastSync())
	{
		UtxoTree::Cursor cu;
		t.m_pCu = &cu;

		// bounds
		UtxoTree::Key kMin, kMax;

		UtxoTree::Key::Data d;
		d.m_Commitment = msg.m_Utxo;
		d.m_Maturity = msg.m_MaturityMin;
		kMin = d;
		d.m_Maturity = Height(-1);
		kMax = d;

		t.m_pBound[0] = kMin.V.m_pData;
		t.m_pBound[1] = kMax.V.m_pData;

		p.get_Utxos().Traverse(t);
	}

	Send(t.m_Msg);
}

void Node::Peer::OnMsg(proto::GetProofShieldedOutp&& msg)
{
	if (msg.m_SerialPub.m_Y > 1)
		ThrowUnexpected(); // would not be necessary if/when our serialization will take care of this

	proto::ProofShieldedOutp msgOut;

	Processor& p = m_This.m_Processor;
	if (!p.IsFastSync())
	{
		NodeDB::Recordset rs;
		Blob blob(&msg.m_SerialPub, sizeof(msg.m_SerialPub));
		if (p.get_DB().UniqueFind(blob, rs))
		{
			const NodeProcessor::ShieldedOutpPacked& sop = rs.get_As<NodeProcessor::ShieldedOutpPacked>(0); // Note: will throw CorruptionException if of wrong size

			sop.m_Height.Export(msgOut.m_Height);
			sop.m_TxoID.Export(msgOut.m_ID);
			msgOut.m_Commitment = sop.m_Commitment;

			p.GenerateProofShielded(msgOut.m_Proof, sop.m_MmrIndex);
		}
	}

	Send(msgOut);
}

void Node::Peer::OnMsg(proto::GetProofShieldedInp&& msg)
{
	if (msg.m_SpendPk.m_Y > 1)
		ThrowUnexpected(); // would not be necessary if/when our serialization will take care of this

	proto::ProofShieldedInp msgOut;

	Processor& p = m_This.m_Processor;
	if (!p.IsFastSync())
	{
		NodeDB::Recordset rs;

		msg.m_SpendPk.m_Y |= 2;
		Blob blob(&msg.m_SpendPk, sizeof(msg.m_SpendPk));
		if (p.get_DB().UniqueFind(blob, rs))
		{
			const NodeProcessor::ShieldedInpPacked& sip = rs.get_As<NodeProcessor::ShieldedInpPacked>(0); // Note: will throw CorruptionException if of wrong size

			sip.m_Height.Export(msgOut.m_Height);

			p.GenerateProofShielded(msgOut.m_Proof, sip.m_MmrIndex);
		}
	}

	Send(msgOut);
}

void Node::Peer::OnMsg(proto::GetProofAsset&& msg)
{
	proto::ProofAsset msgOut;
	msgOut.m_Info.m_Deposit = Rules::get().CA.DepositForList2; // for backward compatibility, if asset not found - older deserialization should work

	Processor& p = m_This.m_Processor;
	if (!p.IsFastSync())
	{
		Asset::Full ai;
		ai.m_ID = msg.m_AssetID ?
			msg.m_AssetID :
			p.get_DB().AssetFindByOwner(msg.m_Owner);

		if  (ai.m_ID && p.get_DB().AssetGetSafe(ai))
		{
			msgOut.m_Info = std::move(ai);

			if (get_Ext() < 10)
				msgOut.m_Info.SetCid(nullptr);

			p.m_Mmr.m_Assets.get_Proof(msgOut.m_Proof, msgOut.m_Info.m_ID - (Rules::get().CA.ForeignEnd + 1));

			struct MyProofBuilder
				:public NodeProcessor::ProofBuilder
			{
				using ProofBuilder::ProofBuilder;
				bool get_Assets(Merkle::Hash&) override { return false; }
			};

			MyProofBuilder pb(p, msgOut.m_Proof);
			pb.GenerateProof();
		}
	}

	Send(msgOut);
}

void Node::Peer::OnMsg(proto::GetShieldedList&& msg)
{
	proto::ShieldedList msgOut;

	Processor& p = m_This.m_Processor;
	if ((msg.m_Id0 < p.m_Extra.m_ShieldedOutputs) && msg.m_Count)
	{
		beam::setmin(msg.m_Count, Rules::get().Shielded.m_ProofMax.get_N() * 2); // no reason to ask for more

		TxoID n = p.m_Extra.m_ShieldedOutputs - msg.m_Id0;

		if (msg.m_Count > n)
			msg.m_Count = static_cast<uint32_t>(n);

		msgOut.m_Items.resize(msg.m_Count);
		p.get_DB().ShieldedRead(msg.m_Id0, &msgOut.m_Items.front(), msg.m_Count);
		p.get_DB().ShieldedStateRead(msg.m_Id0 + msg.m_Count - 1, &msgOut.m_State1, 1);
	}

	Send(msgOut);
}

void Node::Peer::OnMsg(proto::GetProofChainWork&& msg)
{
	proto::ProofChainWork msgOut;

	Processor& p = m_This.m_Processor;
	if (!p.IsFastSync() && p.BuildCwp())
	{
		msgOut.m_Proof.m_LowerBound = msg.m_LowerBound;
		BEAM_VERIFY(msgOut.m_Proof.Crop(p.m_Cwp));
	}

	Send(msgOut);
}

void Node::Peer::OnMsg(proto::PeerInfoSelf&& msg)
{
	m_Port = msg.m_Port;
}

void Node::Peer::OnMsg(proto::PeerInfo&& msg)
{
	if (msg.m_ID != m_This.m_MyPublicID)
		m_This.m_PeerMan.OnPeer(msg.m_ID, msg.m_LastAddr, false);
}

void Node::Peer::OnMsg(proto::GetExternalAddr&& msg)
{
	proto::ExternalAddr msgOut;
	msgOut.m_Value = m_RemoteAddr.ip();
	Send(msgOut);
}

void Node::Peer::OnMsg(proto::GetEvents&& msg)
{
	proto::Events msgOut;

	if (Flags::Viewer & m_Flags)
	{
		assert(m_pAccount);
		Processor& p = m_This.m_Processor;
		NodeDB& db = p.get_DB();
		NodeDB::WalkerEvent wlk;

		Height hLast = 0;
		uint32_t nCount = 0;

		Serializer ser, serCvt;

		Height hMax = MaxHeight;
		if (p.IsFastSync())
			hMax = p.Num2Height(p.m_SyncData.m_n0); // don't report further during fast-sync

		for (db.EnumEvents(wlk, m_pAccount->m_iAccount, msg.m_HeightMin); wlk.MoveNext(); hLast = wlk.m_Pos.m_Height)
		{
			if ((nCount >= proto::Event::s_Max) && (wlk.m_Pos.m_Height != hLast))
				break;

			if (wlk.m_Pos.m_Height > hMax)
				break;

			ser & wlk.m_Pos.m_Height;
			ser.WriteRaw(wlk.m_Body.p, wlk.m_Body.n);

			nCount++;   
		}

		ser.swap_buf(msgOut.m_Events);
	}
	else
		BEAM_LOG_WARNING() << "Peer " << m_RemoteAddr << " Unauthorized Utxo events request.";

	Send(msgOut);
}

void Node::Peer::OnMsg(proto::BlockFinalization&& msg)
{
	if (!(Flags::Owner & m_Flags) ||
		!(Flags::Finalizing & m_Flags))
		ThrowUnexpected();

	m_Flags &= ~Flags::Finalizing;

	if (!msg.m_Value)
		ThrowUnexpected();
	Transaction& tx = *msg.m_Value;

	if (this != m_This.m_Miner.m_pFinalizer)
		return; // outdated

	if (!m_This.m_Miner.m_pTaskToFinalize)
	{
		m_This.m_Miner.Restart(); // time to restart
		return;
	}

	Miner::Task::Ptr pTask;
	pTask.swap(m_This.m_Miner.m_pTaskToFinalize);
	NodeProcessor::GeneratedBlock& x = *pTask;

	{
		ECC::Mode::Scope scope(ECC::Mode::Fast);

		// and do the overall validation
		TxBase::Context ctx;
		ctx.m_Height = m_This.m_Processor.m_Cursor.m_hh.m_Height + 1;
		ctx.m_Params.m_Kind = TxBase::Kind::Block; // the following also ensures the coinbase value corresponds to the subsidy

		bool bAddAuxKrn = !!x.m_Fees;
		if (bAddAuxKrn)
		{
			struct TxKernelDummy
				:public TxKernel
			{
				~TxKernelDummy() override {}
				Subtype::Enum get_Subtype() const override { return TxKernel::Subtype::count;  }
				void CalculateID() const override {}
				bool HasNonStd() const override { return true; }
				void TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParentW) const override
				{
					ECC::Point::Native pt;
					AmountBig::AddTo(pt, AmountBig::Number(m_Fee));
					pt = -pt;
					exc += pt;
				}
				void Clone(Ptr&) const override {}
			};

			auto pKrn = std::make_unique<TxKernelDummy>();
			pKrn->m_Fee = x.m_Fees;
			pKrn->m_Height = ctx.m_Height;
			tx.m_vKernels.push_back(std::move(pKrn));
		}

		std::string sErr;
		if (!m_This.m_Processor.ValidateAndSummarize(ctx, *msg.m_Value, msg.m_Value->get_Reader(), sErr))
			ThrowUnexpected(sErr.c_str());
		ctx.TestSigma();

		if (!tx.m_vInputs.empty())
			ThrowUnexpected();

		if (bAddAuxKrn)
			tx.m_vKernels.pop_back();

		// verify that all the outputs correspond to our viewer's Kdf (in case our comm was hacked this'd prevent mining for someone else)
		for (size_t i = 0; i < tx.m_vOutputs.size(); i++)
		{
			CoinID cid;
			if (!tx.m_vOutputs[i]->Recover(m_This.m_Processor.m_Cursor.m_hh.m_Height + 1, *m_This.m_Keys.m_pOwner, cid))
				ThrowUnexpected();
		}

		tx.MoveInto(x.m_Block);

		ECC::Scalar::Native offs = x.m_Block.m_Offset;
		offs += ECC::Scalar::Native(tx.m_Offset);
		x.m_Block.m_Offset = offs;

	}

	TxPool::Fluff txpEmpty;

	NodeProcessor::BlockContext bc(txpEmpty, 0, *m_This.m_Keys.m_pGeneric, *m_This.m_Keys.m_pGeneric); // the key isn't used anyway
	bc.m_Mode = NodeProcessor::BlockContext::Mode::Finalize;
	Cast::Down<NodeProcessor::GeneratedBlock>(bc) = std::move(x);

	bool bRes = m_This.m_Processor.GenerateNewBlock(bc);

	if (!bRes)
	{
		BEAM_LOG_WARNING() << "Block finalization failed";
		return;
	}

	Cast::Down<NodeProcessor::GeneratedBlock>(x) = std::move(bc);

	BEAM_LOG_INFO() << "Block Finalized by owner";

	m_This.m_Miner.StartMining(std::move(pTask));
}

void Node::Peer::OnMsg(proto::GetStateSummary&& msg)
{
	Processor& p = m_This.m_Processor;

	proto::StateSummary msgOut;
	msgOut.m_TxoLo = p.m_Extra.m_TxoLo;
	msgOut.m_Txos = p.m_Extra.m_Txos - p.m_Cursor.m_Full.m_Number.v; // by convention after each block there's an artificial gap in Txo counting
	msgOut.m_ShieldedOuts = p.m_Extra.m_ShieldedOutputs;
	msgOut.m_ShieldedIns = p.m_Mmr.m_Shielded.m_Count - p.m_Extra.m_ShieldedOutputs;
	msgOut.m_AssetsMax = p.get_AidMax();
	msgOut.m_AssetsActive = static_cast<Asset::ID>(p.get_DB().ParamIntGetDef(NodeDB::ParamID::AssetsActive));

	Send(msgOut);
}

void Node::Peer::OnMsg(proto::GetShieldedOutputsAt&& msg)
{
	Processor& p = m_This.m_Processor;
	Height h = std::min(msg.m_Height, p.m_Cursor.m_hh.m_Height); // don't throw exc if the height is too high, theoretically height can go down due to reorg (of course the chainwork must be higher anyway)

	proto::ShieldedOutputsAt msgOut;
	msgOut.m_ShieldedOuts = p.get_DB().ShieldedOutpGet(h);
	Send(msgOut);
}

void Node::Peer::OnMsg(proto::GetAssetsListAt&& msg)
{
	proto::AssetsListAt msgOut;

	Asset::Full ai;
	ai.m_ID = msg.m_Aid0 ? (msg.m_Aid0 - 1) : 0;

	bool bCurrent = (msg.m_Height >= m_This.m_Processor.m_Cursor.m_hh.m_Height);
	bool bLegacy = (get_Ext() < 10);

	while (ai.m_ID < Asset::s_MaxCount)
	{
		if (bCurrent)
		{
			if (!m_This.m_Processor.get_DB().AssetGetNext(ai))
				break;
		}
		else
		{
			++ai.m_ID;
			int ret = m_This.m_Processor.get_AssetAt(ai, msg.m_Height, true);
			if (!ret)
				break;

			if (ret < 0)
				continue;
		}

		if (msgOut.m_Assets.size() >= 3000)
		{
			msgOut.m_bMore = true;
			break;
		}
		
		if (bLegacy)
			ai.SetCid(nullptr);

		msgOut.m_Assets.push_back(std::move(ai));
	}

	Send(msgOut);
}

void Node::Peer::OnMsg(proto::ContractVarsEnum&& msg)
{
	struct Wrk
		:public NodeProcessor::IWorker
	{
		Peer& m_This;
		proto::ContractVarsEnum& m_In;
		proto::ContractVars m_Out;

		Wrk(Peer& x, proto::ContractVarsEnum& msgIn)
			:m_This(x)
			,m_In(msgIn)
		{}

		void Do() override
		{
			NodeDB::WalkerContractData wlk;
			m_This.m_This.m_Processor.get_DB().ContractDataEnum(wlk, m_In.m_KeyMin, m_In.m_KeyMax);

			Serializer ser;

			while (true)
			{
				if (!wlk.MoveNext())
					break;

				if (m_In.m_bSkipMin && (wlk.m_Key == m_In.m_KeyMin))
					continue; // skip

				ser
					& wlk.m_Key.n
					& wlk.m_Val.n;

				ser.WriteRaw(wlk.m_Key.p, wlk.m_Key.n);
				ser.WriteRaw(wlk.m_Val.p, wlk.m_Val.n);

				if (m_This.IsChocking(ser.buffer().second))
				{
					m_Out.m_bMore = true;
					break;
				}
			}

			ser.swap_buf(m_Out.m_Result);
		}
	};

	Wrk wrk(*this, msg);

	m_This.m_Processor.ExecInDependentContext(wrk, m_Dependent.m_pQuery.get(), m_This.m_TxDependent);

	Send(wrk.m_Out);
}

void Node::Peer::OnMsg(proto::ContractLogsEnum&& msg)
{
	struct Wrk
		:public NodeProcessor::IWorker
	{
		Peer& m_This;
		proto::ContractLogsEnum& m_In;
		proto::ContractLogs m_Out;

		Wrk(Peer& x, proto::ContractLogsEnum& msgIn)
			:m_This(x)
			, m_In(msgIn)
		{}

		void Do() override
		{
			auto& db = m_This.m_This.m_Processor.get_DB();

			NodeDB::ContractLog::Walker wlk;
			if (m_In.m_KeyMin.empty() && m_In.m_KeyMax.empty())
				db.ContractLogEnum(wlk, m_In.m_PosMin, m_In.m_PosMax);
			else
				db.ContractLogEnum(wlk, m_In.m_KeyMin, m_In.m_KeyMax, m_In.m_PosMin, m_In.m_PosMax);

			Serializer ser;

			while (true)
			{
				if (!wlk.MoveNext())
					break;

				HeightPos dp;
				dp.m_Height = wlk.m_Entry.m_Pos.m_Height - m_In.m_PosMin.m_Height;
				if (dp.m_Height)
				{
					m_In.m_PosMin.m_Height = wlk.m_Entry.m_Pos.m_Height;
					m_In.m_PosMin.m_Pos = 0;
				}

				dp.m_Pos = wlk.m_Entry.m_Pos.m_Pos - m_In.m_PosMin.m_Pos;
				m_In.m_PosMin.m_Pos = wlk.m_Entry.m_Pos.m_Pos;

				ser
					& dp
					& wlk.m_Entry.m_Key.n
					& wlk.m_Entry.m_Val.n;

				ser.WriteRaw(wlk.m_Entry.m_Key.p, wlk.m_Entry.m_Key.n);
				ser.WriteRaw(wlk.m_Entry.m_Val.p, wlk.m_Entry.m_Val.n);

				if (m_This.IsChocking(ser.buffer().second))
				{
					m_Out.m_bMore = true;
					break;
				}
			}

			ser.swap_buf(m_Out.m_Result);
		}
	};

	Wrk wrk(*this, msg);

	m_This.m_Processor.ExecInDependentContext(wrk, m_Dependent.m_pQuery.get(), m_This.m_TxDependent);

	Send(wrk.m_Out);
}

void Node::Peer::OnMsg(proto::GetContractVar&& msg)
{
	proto::ContractVar msgOut;

	Blob val;
	NodeDB::Recordset rs;

	NodeProcessor& p = m_This.m_Processor;
	if (p.get_DB().ContractDataFind(msg.m_Key, val, rs))
	{
		val.Export(msgOut.m_Value);

		if (p.IsContractVarStoredInMmr(msg.m_Key))
		{
			Merkle::Hash hv;
			Block::get_HashContractVar(hv, msg.m_Key, val);

			RadixHashOnlyTree& t = p.get_Contracts();
			RadixHashOnlyTree::Cursor cu;
			bool bCreate = false;
			if (!t.Find(cu, hv, bCreate))
				NodeProcessor::OnCorrupted();

			t.get_Proof(msgOut.m_Proof, cu);

			struct MyProofBuilder
				:public NodeProcessor::ProofBuilder
			{
				using ProofBuilder::ProofBuilder;
				bool get_Contracts(Merkle::Hash&) override { return false; }
			};

			MyProofBuilder pb(p, msgOut.m_Proof);
			pb.GenerateProof();
		}
	}

	Send(msgOut);
}

void Node::Peer::OnMsg(proto::GetContractLogProof&& msg)
{
	proto::ContractLogProof msgOut;
	m_This.m_Processor.get_ProofContractLog(msgOut.m_Proof, msg.m_Pos);
	Send(msgOut);
}

void Node::Peer::OnMsg(proto::SetDependentContext&& msg)
{
	m_Dependent.m_pQuery = std::move(msg.m_Context);
}

#define THE_MACRO(msg) \
void Node::Peer::OnMsg(proto::msg&& x) \
{ \
	if (Rules::Consensus::Pbft != Rules::get().m_Consensus) \
		ThrowUnexpected("not pbft"); \
	m_This.m_Validator.OnMsg(std::move(x), *this); \
}

BeamNodeMsg_PbftHandleAll(THE_MACRO)
#undef THE_MACRO

void Node::Peer::OnMsg(proto::PbftStamp&& msg)
{
	auto& v = m_This.m_Validator;
	if (m_Tip.m_hh.m_Height <= v.m_Stamp.m_ID.m_Height)
		return;

	NodeConnection::Test(msg, m_Tip.m_Full);

	// all good
	v.m_Stamp.m_ID = m_Tip.m_hh;
	v.m_Stamp.m_Msg = std::move(msg);

	v.SaveStamp();

	OnNewTip2();
	TakeTasks();
}

} // namespace beam
