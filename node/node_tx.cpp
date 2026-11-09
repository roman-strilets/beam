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

void Node::DeleteOutdated()
{
	auto dh = m_Cfg.m_RollbackLimit.m_Max;
	beam::setmin(dh, Rules::get().MaxRollback);

	auto hTop = m_Processor.m_Cursor.m_hh.m_Height;

	if (hTop > dh)
	{
		auto h0 = hTop - dh;

		while (!m_TxPool.m_lstOutdated.empty())
		{
			TxPool::Fluff::Element& x = m_TxPool.m_lstOutdated.front().get_ParentObj();
			if (x.m_Hist.m_Height > h0)
				break;

			m_TxPool.Delete(x);
		}
	}

	for (TxPool::Fluff::ProfitSet::iterator it = m_TxPool.m_setProfit.begin(); m_TxPool.m_setProfit.end() != it; )
	{
		TxPool::Fluff::Element& x = (it++)->get_ParentObj();
		Transaction& tx = *x.m_pValue;

		uint32_t nBvmCharge = 0;
		if (proto::TxStatus::Ok != m_Processor.ValidateTxContextEx(tx, x.m_Profit.m_Stats.m_Hr, true, nBvmCharge, nullptr, nullptr, nullptr))
		{
			x.m_Hist.m_Height = hTop;
			m_TxPool.SetState(x, TxPool::Fluff::State::Outdated);
		}
	}

	for (TxPool::Stem::TimeSet::iterator it = m_Dandelion.m_setTime.begin(); m_Dandelion.m_setTime.end() != it; )
	{
		TxPool::Stem::Element& x = (it++)->get_ParentObj();

		uint32_t nBvmCharge = 0;
		uint8_t nStatus = m_Processor.ValidateTxContextEx(*x.m_pValue, x.m_Profit.m_Stats.m_Hr, true, nBvmCharge, nullptr, nullptr, nullptr);
		if (proto::TxStatus::Ok != nStatus)
		{
			LogTxStem(*x.m_pValue, "out-1");
			m_Dandelion.Delete(x);
		}
	}

	if (hTop >= m_Cfg.m_Dandelion.m_dhStemConfirm)
	{
		auto h0 = hTop - m_Cfg.m_Dandelion.m_dhStemConfirm;

		while (!m_TxPool.m_lstWaitFluff.empty())
		{
			auto& c = m_TxPool.m_lstWaitFluff.front();
			if (c.m_Height >= h0)
				break;

			auto& x = c.get_ParentObj();

			uint32_t nBvmCharge = 0;
			uint8_t nStatus = m_Processor.ValidateTxContextEx(*x.m_pValue, x.m_Profit.m_Stats.m_Hr, true, nBvmCharge, nullptr, nullptr, nullptr);
			if (proto::TxStatus::Ok == nStatus)
			{
				LogTxStem(*x.m_pValue, "auto-fluffing");

				m_TxPool.SetState(x, TxPool::Fluff::State::Fluffed);
				OnTransactionFluff(x, nullptr);
			}
			else
			{
				LogTxStem(*x.m_pValue, "out-2");
				m_TxPool.Delete(x);
			}
		}
	}
}

void Node::Peer::OnMsg(proto::NewTransaction&& msg)
{
	if (!msg.m_Transaction)
		ThrowUnexpected(); // our deserialization permits NULL Ptrs.
	// However the transaction body must have already been checked for NULLs

	// Change in protocol!
	// Historically the node replied with proto::Status message for transactions in stem phase only, for fluff phase no reply was necessary.
	// Now we reply regardless to phase, but ONLY to non-node client (means - FlyClient).
	// For other cases the tx verification is deferred, until the node is idle.
	bool bReply = !(Flags::PiRcvd & m_Flags);

	const PeerID* pSender = m_pInfo ? &m_pInfo->m_ID.m_Key : nullptr;

	if (bReply)
	{
		std::ostringstream errInfo;

		proto::Status msgOut;
		msgOut.m_Value = m_This.OnTransaction(std::move(msg.m_Transaction), std::move(msg.m_Context), pSender, msg.m_Fluff, &errInfo);

		msgOut.m_ExtraInfo = errInfo.str();
		Send(msgOut);
	}
	else
	{
		m_This.OnTransactionDeferred(std::move(msg.m_Transaction), std::move(msg.m_Context), pSender, msg.m_Fluff);
	}
}

void Node::OnTransactionDeferred(Transaction::Ptr&& pTx, std::unique_ptr<Merkle::Hash>&& pCtx, const PeerID* pSender, bool bFluff)
{
	TxDeferred::Element txd;
	txd.m_pTx = std::move(pTx);
	txd.m_pCtx = std::move(pCtx);
	txd.m_Fluff = bFluff;

	if (pSender)
		txd.m_Sender = *pSender;
	else
		txd.m_Sender = Zero;

	//if (m_Cfg.m_LogTxFluff)
	//{
	//    Transaction::KeyType key;
	//    txd.m_pTx->get_Key(key);

	//    BEAM_LOG_INFO() << "Tx " << key << " deferred";
	//}

	if (m_TxDeferred.m_lst.empty())
		m_TxDeferred.start();
	else
	{
		while (m_TxDeferred.m_lst.size() > m_Cfg.m_MaxDeferredTransactions)
			m_TxDeferred.m_lst.pop_front();
	}

	m_TxDeferred.m_lst.push_back(std::move(txd));
}

void Node::TxDeferred::OnSchedule()
{
	if (!m_lst.empty())
	{
		TxDeferred::Element& x = m_lst.front();
		get_ParentObj().OnTransaction(std::move(x.m_pTx), std::move(x.m_pCtx), &x.m_Sender, x.m_Fluff, nullptr);
		m_lst.pop_front();
	}

	if (m_lst.empty())
		cancel();

}

uint8_t Node::OnTransaction(Transaction::Ptr&& pTx, std::unique_ptr<Merkle::Hash>&& pCtx, const PeerID* pSender, bool bFluff, std::ostream* pExtraInfo)
{
	return 
		pCtx ?
			OnTransactionDependent(std::move(pTx), *pCtx, pSender, bFluff, pExtraInfo) :
			bFluff ?
				OnTransactionFluff(std::move(pTx), pExtraInfo, pSender, nullptr) :
				OnTransactionStem(std::move(pTx), pExtraInfo);
}

uint8_t Node::ValidateTx(TxPool::Stats& stats, const Transaction& tx, const Transaction::KeyType& keyTx, std::ostream* pExtraInfo, bool& bAlreadyRejected)
{
	auto it = m_TxReject.find(keyTx);
	if (m_TxReject.end() != it)
	{
		bAlreadyRejected = true;
		return it->second;
	}

	Transaction::Context ctx;
	uint32_t nBvmCharge = 0;
	Amount feeReserve = 0;

	uint8_t nRet = ValidateTx2(ctx, tx, nBvmCharge, feeReserve, nullptr, pExtraInfo, nullptr);
	if (proto::TxStatus::Ok == nRet)
	{
		if (AmountBig::get_Hi(ctx.m_Stats.m_Fee))
			nRet = proto::TxStatus::LowFee; // actually it's ridiculously-high fee
		else
		{
			auto nSizeCorrection = (uint32_t)(((uint64_t)nBvmCharge) * Rules::get().MaxBodySize / bvm2::Limits::BlockCharge);
			stats.From(tx, ctx, feeReserve, nSizeCorrection);
		}
	}

	if (proto::TxStatus::Ok != nRet)
	{
		m_TxReject[keyTx] = nRet;
		m_Wtx.Delete(keyTx);
	}

	return nRet;
}

uint8_t Node::ValidateTx2(Transaction::Context& ctx, const Transaction& tx, uint32_t& nBvmCharge, Amount& feeReserve, TxPool::Dependent::Element* pParent, std::ostream* pExtraInfo, Merkle::Hash* pNewCtx)
{
	ctx.m_Height.m_Min = m_Processor.m_Cursor.m_hh.m_Height + 1;

	std::string sErr;
	bool bValid = m_Processor.ValidateAndSummarize(ctx, tx, tx.get_Reader(), sErr);
	if (bValid)
	{
		try {
			ctx.TestSigma();
		} catch (const std::exception& e) {
			bValid = false;
			sErr = e.what();
		}
	}

	if (!bValid)
	{
		if (pExtraInfo)
			*pExtraInfo << "Context-free validation failed: " << sErr;
		return proto::TxStatus::Invalid;
	}

	uint8_t nCode = m_Processor.ValidateTxContextEx(tx, ctx.m_Height, false, nBvmCharge, pParent, pExtraInfo, pNewCtx);
	if (proto::TxStatus::Ok != nCode)
		return nCode;


	if (!CalculateFeeReserve(ctx.m_Stats, ctx.m_Height, ctx.m_Stats.m_Fee, nBvmCharge, feeReserve))
	{
		if (pExtraInfo)
		{
			*pExtraInfo << "Low fee. Min = " << feeReserve << ", provided = " << AmountBig::get_Lo(ctx.m_Stats.m_Fee);
			if (nBvmCharge)
				*pExtraInfo << ", Bvm charge = " << nBvmCharge;
		}
		return proto::TxStatus::LowFee;
	}

	return proto::TxStatus::Ok;
}

bool Node::CalculateFeeReserve(const TxStats& s, const HeightRange& hr, const AmountBig::Number& fees, uint32_t nBvmCharge, Amount& feeReserve)
{
	feeReserve = static_cast<Amount>(-1);

	if (Rules::get().IsPastFork_<1>(hr.m_Min))
	{
		auto& fs = Transaction::FeeSettings::get(hr.m_Min);
		Amount feesMin = fs.Calculate(s);

		if (nBvmCharge)
			feesMin += fs.CalculateForBvm(s, nBvmCharge);

		AmountBig::Number val(feesMin);

		if (fees < val)
		{
			feeReserve = feesMin;
			return false;
		}

		val = fees - val;

		if (!AmountBig::get_Hi(val))
			feeReserve = AmountBig::get_Lo(val);
	}

	return true;
}

void Node::LogTx(const Transaction& tx, uint8_t nStatus, const Transaction::KeyType& key)
{
	if (!m_Cfg.m_LogTxFluff)
		return;

	std::ostringstream os;

	os << "Tx " << key;

	for (size_t i = 0; i < tx.m_vInputs.size(); i++)
		os << "\n\tI: " << tx.m_vInputs[i]->m_Commitment;

	for (size_t i = 0; i < tx.m_vOutputs.size(); i++)
	{
		const Output& outp = *tx.m_vOutputs[i];
		os << "\n\tO: " << outp.m_Commitment;

		if (outp.m_Incubation)
			os << ", Incubation +" << outp.m_Incubation;

		if (outp.m_pPublic)
			os << ", Sum=" << outp.m_pPublic->m_Value;
	}

	for (size_t i = 0; i < tx.m_vKernels.size(); i++)
	{
		const TxKernel& krn = *tx.m_vKernels[i];

		char sz[Merkle::Hash::nTxtLen + 1];
		krn.get_ID().Print(sz);

		os << "\n\tK: " << sz << " Fee=" << krn.m_Fee;
	}

	os << "\n\tStatus: " << static_cast<uint32_t>(nStatus);
	BEAM_LOG_INFO() << os.str();
}

void Node::LogTxStem(const Transaction& tx, const char* szTxt)
{
	if (!m_Cfg.m_LogTxStem)
		return;

	std::ostringstream os;
	os << "Stem-Tx " << szTxt;

	for (size_t i = 0; i < tx.m_vKernels.size(); i++)
	{
		char sz[Merkle::Hash::nTxtLen + 1];
		tx.m_vKernels[i]->get_ID().Print(sz);

		os << "\n\tK: " << sz;
	}

	BEAM_LOG_INFO() << os.str();
}

const ECC::uintBig& Node::NextNonce()
{
	ECC::Scalar::Native sk;
	NextNonce(sk);
	return m_NonceLast.V;
}

void Node::NextNonce(ECC::Scalar::Native& sk)
{
	m_Keys.m_pGeneric->DeriveKey(sk, m_NonceLast.V);
	ECC::Hash::Processor() << sk >> m_NonceLast.V;
}

uint32_t Node::RandomUInt32(uint32_t threshold)
{
	if (threshold)
	{
		typedef uintBigFor<uint32_t>::Type Type;

		Type thr(threshold), val;
		Type::Threshold thrSel(thr);

		do
		{
			val = NextNonce();
		} while (!thrSel.Accept(val));

		val.Export(threshold);
	}
	return threshold;
}

uint8_t Node::OnTransactionStem(Transaction::Ptr&& ptx, std::ostream* pExtraInfo)
{
	TxStats s;
	ptx->get_Reader().AddStats(s);
	if (!s.m_Kernels) {
		// stupid compiler insists on parentheses here!
		return proto::TxStatus::TooSmall;
	}

	const auto& r = Rules::get();

	if ((s.m_InputsShielded > r.Shielded.MaxIns) || (s.m_OutputsShielded > r.Shielded.MaxOuts)) {
		return proto::TxStatus::LimitExceeded;
	}

	Transaction::KeyType keyTx;
	ptx->get_Key(keyTx);

	auto itF = m_TxPool.m_setTxs.find(keyTx, TxPool::Fluff::Element::Tx::Comparator());
	TxPool::Fluff::Element* pF = (m_TxPool.m_setTxs.end() == itF) ? nullptr : &itF->get_ParentObj();

	TxPool::Stats stats;
	bool bTested = false;

	if (pF)
	{
		if (TxPool::Fluff::State::Fluffed == pF->m_State)
		{
			LogTxStem(*ptx, "Already fluffed");
			return proto::TxStatus::Ok;
		}

		if (pF->m_Hist.m_Height == m_Processor.m_Cursor.m_hh.m_Height)
		{
			stats = pF->m_Profit.m_Stats;
			bTested = true;
		}

		ptx = pF->m_pValue; // prefer it, to avoid ambiguity
	}

	LogTxStem(*ptx, pF ? "Already received" : "New");

	if (!bTested)
	{
		bool bAlreadyRejected = false;
		uint8_t nCode = ValidateTx(stats, *ptx, keyTx, pExtraInfo, bAlreadyRejected);
		if (proto::TxStatus::Ok != nCode)
		{
			if (!bAlreadyRejected)
				LogTxStem(*ptx, "invalid");
			return nCode;
		}
	}

	bool bDontAggregate = true;
	if (!pF)
	{
		bDontAggregate =
			(ptx->m_vOutputs.size() >= m_Cfg.m_Dandelion.m_OutputsMax) || // already big enough
			s.m_KernelsNonStd || // contains non-std elements
			!m_Keys.m_pMiner || // can't manage decoys
			(Rules::Consensus::Pbft == r.m_Consensus);  // PBFT is built for speed

		// add it to wait-fluff list BEFORE we modify it
		Transaction::Ptr pTxOrig;
		if (bDontAggregate)
			pTxOrig = ptx;
		else
		{
			// clone the tx, since we may modify it in the fure
			pTxOrig = std::make_shared<Transaction>();
			TxVectors::Writer wtx(*pTxOrig, *pTxOrig);

			wtx.Dump(ptx->get_Reader());
			pTxOrig->m_Offset = ptx->m_Offset;
		}

		pF = m_TxPool.AddValidTx(std::move(pTxOrig), stats, keyTx, TxPool::Fluff::State::PreFluffed, m_Processor.m_Cursor.m_hh.m_Height);
		m_Wtx.Delete(keyTx);
	}

	if (bDontAggregate)
		OnTransactionAggregated(std::move(ptx), stats);
	else
	{
		AddDummyInputs(*ptx, stats);

		auto pGuard = std::make_unique<TxPool::Stem::Element>();
		pGuard->m_Time.m_Value = 0;
		pGuard->m_Profit.m_Stats = stats;
		pGuard->m_pValue.swap(ptx);

		m_Dandelion.InsertAggr(*pGuard);
		auto* pElem = pGuard.release();

		PerformAggregation(*pElem);
	}

	return proto::TxStatus::Ok;
}

Node::Peer* Node::SelectRandomPeer(Peer::ISelector& sel)
{
	// must have at least 1 peer to continue the stem phase
	uint32_t nCount = 0;

	for (PeerList::iterator it = m_lstPeers.begin(); m_lstPeers.end() != it; ++it)
		if (sel.IsValid(*it))
			nCount++;

	if (nCount)
	{
		auto thr = uintBigFrom(m_Cfg.m_Dandelion.m_FluffProbability);

		// Compare two bytes of threshold with random nonce 
		if (memcmp(thr.m_pData, NextNonce().m_pData, thr.nBytes) < 0)
		{
			// Choose random peer index between 0 and nStemPeers - 1 
			uint32_t nRandomPeerIdx = RandomUInt32(nCount);

			for (PeerList::iterator it = m_lstPeers.begin(); ; ++it)
				if (sel.IsValid(*it) && !nRandomPeerIdx--)
					return & *it;
		}
	}

	return nullptr;
}

void Node::OnTransactionAggregated(Transaction::Ptr&& pTx, const TxPool::Stats& stats)
{
	LogTxStem(*pTx, "Aggregation finished");

	Peer::Selector_Stem sel;
	Peer* pNext = SelectRandomPeer(sel);
	if (pNext)
	{
		if (m_Cfg.m_LogTxStem)
		{
			BEAM_LOG_INFO() << "Stem continues to " << pNext->m_RemoteAddr;
		}

		pNext->SendTx(pTx, false);
	}
	else
	{
		LogTxStem(*pTx, "Going to fluff");
		OnTransactionFluff(std::move(pTx), nullptr, nullptr, &stats);
	}
}

void Node::PerformAggregation(TxPool::Stem::Element& x)
{
	bool bModified = false;
	// Aggregation policiy: first select those with worse profit, than those with better
	TxPool::Stem::ProfitSet::iterator it = TxPool::Stem::ProfitSet::s_iterator_to(x.m_Profit);
	++it;

	while (x.m_pValue->m_vOutputs.size() <= m_Cfg.m_Dandelion.m_OutputsMax)
	{
		if (m_Dandelion.m_setProfit.end() == it)
			break;

		TxPool::Stem::Element& src = it->get_ParentObj();
		++it;

		if (m_Dandelion.TryMerge(x, src))
			bModified = true;
	}

	it = TxPool::Stem::ProfitSet::s_iterator_to(x.m_Profit);
	if (m_Dandelion.m_setProfit.begin() != it)
	{
		--it;
		while (x.m_pValue->m_vOutputs.size() <= m_Cfg.m_Dandelion.m_OutputsMax)
		{
			TxPool::Stem::Element& src = it->get_ParentObj();

			bool bEnd = (m_Dandelion.m_setProfit.begin() == it);
			if (!bEnd)
				--it;

			if (m_Dandelion.TryMerge(x, src))
				bModified = true;

			if (bEnd)
				break;
		}
	}

	if (bModified)
	{
		m_Dandelion.DeleteAggr(x);
		m_Dandelion.InsertAggr(x);
	}

	LogTxStem(*x.m_pValue, "Aggregated so far");

	if (x.m_pValue->m_vOutputs.size() >= m_Cfg.m_Dandelion.m_OutputsMin)
	{
		OnTransactionAggregated(std::move(x.m_pValue), x.m_Profit.m_Stats);
		m_Dandelion.Delete(x);
	}
	else
	{
		LogTxStem(*x.m_pValue, "Aggregation pending");
		m_Dandelion.SetTimer(m_Cfg.m_Dandelion.m_AggregationTime_ms, x);
	}
}

void Node::AddDummyInputs(Transaction& tx, TxPool::Stats& stats)
{
	if (!m_Keys.m_pMiner)
		return;

	bool bModified = false;

	while (tx.m_vInputs.size() < m_Cfg.m_Dandelion.m_OutputsMax)
	{
		CoinID cid(Zero);
		Height h = m_Processor.get_DB().GetLowestDummy(cid);
		if (h > m_Processor.m_Cursor.m_hh.m_Height)
			break;

		bModified = true;
		cid.m_Value = 0;

		if (AddDummyInputEx(tx, cid))
		{
			/// in the (unlikely) case the tx will be lost - we'll retry spending this UTXO after the following num of blocks
			m_Processor.get_DB().SetDummyHeight(cid, m_Processor.m_Cursor.m_hh.m_Height + m_Cfg.m_Dandelion.m_DummyLifetimeLo + 1);
		}
		else
		{
			// spent
			m_Processor.get_DB().DeleteDummy(cid);
		}
	}

	if (bModified)
	{
		stats.SetSize(tx);
		m_Processor.FlushDB(); // make sure they're not lost
		tx.Normalize();
	}
}

bool Node::AddDummyInputEx(Transaction& tx, const CoinID& cid)
{
	if (AddDummyInputRaw(tx, cid))
		return true;

	// try workaround
	if (!cid.IsBb21Possible())
		return false;

	CoinID cid2 = cid;
	cid2.set_WorkaroundBb21();
	return AddDummyInputRaw(tx, cid2);

}

bool Node::AddDummyInputRaw(Transaction& tx, const CoinID& cid)
{
	assert(m_Keys.m_pMiner);

	Key::IKdf::Ptr pChild;
	Key::IKdf* pKdf = nullptr;

	if (cid.get_Subkey() == m_Keys.m_nMinerSubIndex)
		pKdf = m_Keys.m_pMiner.get();
	else
	{
		// was created by other miner. If we have the root key - we can recreate its key
		if (m_Keys.m_nMinerSubIndex)
			return false;

		pChild = cid.get_ChildKdf(m_Keys.m_pMiner);
		pKdf = pChild.get();
	}

	ECC::Scalar::Native sk;

	// bounds
	ECC::Point comm;
	CoinID::Worker(cid).Create(sk, comm, *pKdf);

	if (!m_Processor.ValidateInputs(comm))
		return false;

	// unspent
	auto pInp = std::make_unique<Input>();
	pInp->m_Commitment = comm;

	tx.m_vInputs.push_back(std::move(pInp));
	tx.m_Offset = ECC::Scalar::Native(tx.m_Offset) + ECC::Scalar::Native(sk);

	return true;
}

void Node::AddDummyOutputs(Transaction& tx, TxPool::Stats& stats)
{
	if (!m_Cfg.m_Dandelion.m_DummyLifetimeHi || !m_Keys.m_pMiner)
		return;

	// add dummy outputs
	bool bModified = false;
	auto& fs = Transaction::FeeSettings::get(m_Processor.m_Cursor.m_hh.m_Height + 1);
	NodeDB& db = m_Processor.get_DB();

	while (tx.m_vOutputs.size() < m_Cfg.m_Dandelion.m_OutputsMin)
	{
		if (stats.m_FeeReserve < fs.m_Output)
			break;

		CoinID cid(Zero);
		cid.m_Type = Key::Type::Decoy;
		cid.set_Subkey(m_Keys.m_nMinerSubIndex);

		while (true)
		{
			NextNonce().ExportWord<0>(cid.m_Idx);
			if (MaxHeight == db.GetDummyHeight(cid))
				break;
		}

		bModified = true;

		Asset::Proof::Params::Override po(m_Processor.get_AidMax());

		auto pOutput = std::make_unique<Output>();
		ECC::Scalar::Native sk;
		pOutput->Create(m_Processor.m_Cursor.m_hh.m_Height + 1, sk, *m_Keys.m_pMiner, cid, *m_Keys.m_pOwner);

		Height h = SampleDummySpentHeight();
		db.InsertDummy(h, cid);

		tx.m_vOutputs.push_back(std::move(pOutput));

		sk = -sk;
		tx.m_Offset = ECC::Scalar::Native(tx.m_Offset) + sk;

		stats.m_FeeReserve -= fs.m_Output;
	}

	if (bModified)
	{
		stats.SetSize(tx);
		m_Processor.FlushDB();
		tx.Normalize();
	}
}

Height Node::SampleDummySpentHeight()
{
	const Config::Dandelion& d = m_Cfg.m_Dandelion; // alias

	Height h = m_Processor.m_Cursor.m_hh.m_Height + d.m_DummyLifetimeLo + 1;

	if (d.m_DummyLifetimeHi > d.m_DummyLifetimeLo)
		h += RandomUInt32(d.m_DummyLifetimeHi - d.m_DummyLifetimeLo);

	return h;
}

uint8_t Node::OnTransactionFluff(Transaction::Ptr&& ptxArg, std::ostream* pExtraInfo, const PeerID* pSender, const TxPool::Stats* pStats)
{
	Transaction::Ptr ptx;
	ptx.swap(ptxArg);

	Transaction::KeyType keyTx;
	ptx->get_Key(keyTx);

	auto itF = m_TxPool.m_setTxs.find(keyTx, TxPool::Fluff::Element::Tx::Comparator());
	TxPool::Fluff::Element* pF = (m_TxPool.m_setTxs.end() == itF) ? nullptr : &itF->get_ParentObj();

	if (pF && (TxPool::Fluff::State::Fluffed == pF->m_State))
		return proto::TxStatus::Ok; // already fluffed

	TxPool::Stats stats;
	if (!pStats)
	{
		bool bTested = pF && (pF->m_Hist.m_Height == m_Processor.m_Cursor.m_hh.m_Height);
		if (!bTested)
		{
			const auto& pTxToTest = pF ? pF->m_pValue : ptx; // avoid ambiguity

			bool bAlreadyRejected = false;
			uint8_t nCode = ValidateTx(stats, *pTxToTest, keyTx, pExtraInfo, bAlreadyRejected);

			if (!bAlreadyRejected)
				LogTx(*pTxToTest, nCode, keyTx);

			if (proto::TxStatus::Ok != nCode)
				return nCode;
		}
	}

	if (!pF)
	{
		pF = m_TxPool.AddValidTx(std::move(ptx), (pStats ? *pStats : stats), keyTx, TxPool::Fluff::State::Fluffed);
		m_Wtx.Delete(keyTx);
	}

	while (m_TxPool.m_setProfit.size() + m_TxPool.m_lstOutdated.size() > m_Cfg.m_MaxPoolTransactions)
	{
		TxPool::Fluff::Element& txDel = m_TxPool.m_lstOutdated.empty() ?
			m_TxPool.m_setProfit.rbegin()->get_ParentObj() :
			m_TxPool.m_lstOutdated.front().get_ParentObj();

		if (&txDel == pF)
			pF = nullptr; // Anti-spam protection: in case the maximum pool capacity is reached - ensure this tx is any better BEFORE broadcasting ti

		m_TxPool.Delete(txDel);
	}

	if (pF)
		OnTransactionFluff(*pF, pSender);

	return proto::TxStatus::Ok;
}

void Node::OnTransactionFluff(TxPool::Fluff::Element& x, const PeerID* pSender)
{
	m_TxPool.SetState(x, TxPool::Fluff::State::Fluffed);

	proto::HaveTransaction msgOut;
	msgOut.m_ID = x.m_Tx.m_Key;

	for (PeerList::iterator it2 = m_lstPeers.begin(); m_lstPeers.end() != it2; ++it2)
	{
		Peer& peer = *it2;
		if (pSender && peer.m_pInfo && (peer.m_pInfo->m_ID.m_Key == *pSender))
			continue;
		if (!(peer.m_LoginFlags & proto::LoginFlags::SpreadingTransactions) || peer.IsChocking())
			continue;

		peer.Send(msgOut);
		peer.SetTxCursor(x.m_pSend);
	}

	m_Miner.SoftRestart();
}

uint8_t Node::OnTransactionDependent(Transaction::Ptr&& pTx, const Merkle::Hash& hvCtx, const PeerID* pSender, bool bFluff, std::ostream* pExtraInfo)
{
	Transaction::KeyType keyTx;
	pTx->get_Key(keyTx);

	TxPool::Dependent::Element* pElem;
	auto itTx = m_TxDependent.m_setTxs.find(keyTx, TxPool::Dependent::Element::Tx::Comparator());
	if (m_TxDependent.m_setTxs.end() != itTx)
		pElem = &itTx->get_ParentObj();
	else
	{
		TxPool::Dependent::Element* pParent;
		if (m_Processor.m_Cursor.m_Full.m_Prev == hvCtx)
			pParent = nullptr;
		else
		{
			auto itCtx = m_TxDependent.m_setContexts.find(hvCtx, TxPool::Dependent::Element::Context::Comparator());
			if (m_TxDependent.m_setContexts.end() == itCtx)
				return proto::TxStatus::DependentNoParent;

			pParent = &itCtx->get_ParentObj();
		}

		Transaction::Context ctx;
		uint32_t nBvmCharge = 0;
		Amount feeReserve = 0;
		Merkle::Hash hvCtxNew;
		uint8_t nRes = ValidateTx2(ctx, *pTx, nBvmCharge, feeReserve, pParent, pExtraInfo, &hvCtxNew);

		if (proto::TxStatus::Ok != nRes)
			return nRes;

		if (m_TxDependent.m_setContexts.find(hvCtxNew, TxPool::Dependent::Element::Context::Comparator()) != m_TxDependent.m_setContexts.end())
			return proto::TxStatus::DependentNoNewCtx;
		
		pElem = m_TxDependent.AddValidTx(std::move(pTx), ctx, keyTx, hvCtxNew, pParent);
		pElem->m_BvmCharge = nBvmCharge;
		if (pParent)
			pElem->m_BvmCharge += pParent->m_BvmCharge;
	}

	struct MySelector :public Peer::ISelector {
		static bool IsValid_(Peer& p) {
			return Peer::Selector_Stem::IsValid_(p) && (p.get_Ext() >= 9);
		}
		bool IsValid(Peer& p) override {
			return IsValid_(p);
		}
	};

	if (!bFluff)
	{
		MySelector sel;

		Peer* pNext = SelectRandomPeer(sel);
		if (pNext)
			pNext->SendTx(pElem->m_pValue, false, &hvCtx);
		else
			bFluff = true;
	}

	bool bNewBest = (m_TxDependent.m_pBest == pElem);

	if (bFluff && !pElem->m_Fluff)
	{
		pElem->m_Fluff = true;

		for (PeerList::iterator it2 = m_lstPeers.begin(); m_lstPeers.end() != it2; ++it2)
		{
			Peer& peer = *it2;
			if (bNewBest)
				peer.MaybeSendDependent();

			if (pSender && peer.m_pInfo && (peer.m_pInfo->m_ID.m_Key == *pSender))
				continue;
			if (!MySelector::IsValid_(peer)/* || peer.IsChocking()*/)
				continue;

			// save time, send the full tx
			peer.SendTx(pElem->m_pValue, true, &hvCtx);
		}

	}

	if (bNewBest)
		m_Miner.SoftRestart();

	return bNewBest ? proto::TxStatus::Ok : proto::TxStatus::DependentNotBest;
}

void Node::Dandelion::OnTimedOut(Element& x)
{
	get_ParentObj().AddDummyOutputs(*x.m_pValue, x.m_Profit.m_Stats);
	get_ParentObj().LogTxStem(*x.m_pValue, "Aggregation timed-out, dummies added");
	get_ParentObj().OnTransactionAggregated(std::move(x.m_pValue), x.m_Profit.m_Stats);

	get_ParentObj().m_Dandelion.Delete(x);
}

bool Node::Dandelion::ValidateTxContext(const Transaction& tx, const HeightRange& hr, const AmountBig::Number& fees, Amount& feeReserve)
{
	uint32_t nBvmCharge = 0;
	if (proto::TxStatus::Ok != get_ParentObj().m_Processor.ValidateTxContextEx(tx, hr, true, nBvmCharge, nullptr, nullptr, nullptr))
		return false;

	TxStats s;
	tx.get_Reader().AddStats(s);
	CalculateFeeReserve(s, hr, fees, nBvmCharge, feeReserve);

	return true;
}

void Node::Peer::OnMsg(proto::HaveTransaction&& msg)
{
	TxPool::Fluff::Element::Tx key;
	key.m_Key = msg.m_ID;

	TxPool::Fluff::TxSet::iterator it = m_This.m_TxPool.m_setTxs.find(key);
	if (m_This.m_TxPool.m_setTxs.end() != it)
	{
		// already have it
		TxPool::Fluff::Element& x = it->get_ParentObj();
		if (TxPool::Fluff::State::Fluffed != x.m_State)
		{
			const PeerID* pSender = m_pInfo ? &m_pInfo->m_ID.m_Key : nullptr;
			m_This.OnTransactionFluff(x, pSender);
		}

		return;
	}

	if (m_This.m_TxReject.end() != m_This.m_TxReject.find(msg.m_ID))
		return;

	if (!m_This.m_Wtx.Add(key.m_Key))
		return; // already waiting for it

	proto::GetTransaction msgOut;
	msgOut.m_ID = msg.m_ID;
	Send(msgOut);
}

void Node::Peer::OnMsg(proto::GetTransaction&& msg)
{
	TxPool::Fluff::Element::Tx key;
	key.m_Key = msg.m_ID;

	TxPool::Fluff::TxSet::iterator it = m_This.m_TxPool.m_setTxs.find(key);
	if (m_This.m_TxPool.m_setTxs.end() == it)
		return; // don't have it

	SendTx(it->get_ParentObj().m_pValue, true);
}

void Node::Peer::SendTx(Transaction::Ptr& ptx, bool bFluff, const Merkle::Hash* pCtx /* = nullptr */)
{
	struct MyMsg :public proto::NewTransaction {
		~MyMsg() {
			m_Context.release();
		}
	} msg;
	msg.m_Fluff = bFluff;
	msg.m_Context.reset(Cast::NotConst(pCtx)); // fictive
	TemporarySwap scope(msg.m_Transaction, ptx);

	Send(msg);
}

} // namespace beam
