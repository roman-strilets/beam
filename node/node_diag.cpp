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

bool Node::GenerateRecoveryInfo(const char* szPath)
{
	if (!m_Processor.BuildCwp())
		return false; // no info yet

	typedef yas::binary_oarchive<beam::FStream, SERIALIZE_OPTIONS> MySerializer;

	struct MyTraveler
		:public RadixTree::ITraveler
	{
		RecoveryInfo::Writer m_Writer;
		NodeDB* m_pDB;

		bool OnLeaf(const RadixTree::Leaf& x) override
		{
			const UtxoTree::MyLeaf& n = Cast::Up<UtxoTree::MyLeaf>(x);
			UtxoTree::Key::Data d;
			d = n.m_Key;

			if (n.IsExt())
			{
				for (auto p = n.m_pIDs.get_Strict()->m_pTop.get_Strict(); p; p = p->m_pNext.get())
					OnUtxo(d, p->m_ID);
			}
			else
				OnUtxo(d, n.m_ID);

			return true;
		}

		void OnUtxo(const UtxoTree::Key::Data& d, TxoID id)
		{
			NodeDB::WalkerTxo wlk;
			m_pDB->TxoGetValue(wlk, id);

			Deserializer der;
			der.reset(wlk.m_Value.p, wlk.m_Value.n);

			Output outp;
			der & outp;

			assert(outp.m_Commitment == d.m_Commitment);

			// 2 ways to discover the UTXO create height: either directly by looking its TxoID in States table, or reverse-engineer it from Maturity
			// Since currently maturity delta is independent of current height (not a function of height, not changed in current forks) - we prefer the 2nd method, which is faster.

			//m_Processor.FindHeightByTxoID(val.m_CreateHeight, id);
			//assert(val.m_Output.get_MinMaturity(val.m_CreateHeight) == d.m_Maturity);

			Height hCreateHeight = d.m_Maturity - outp.get_MinMaturity(0);

			MySerializer ser(m_Writer.m_Stream);
			ser & hCreateHeight;

			yas::detail::saveRecovery(ser, outp, hCreateHeight);
		}
	};

	MyTraveler ctx;
	ctx.m_pDB = &m_Processor.get_DB();

	try
	{
		ctx.m_Writer.Open(szPath, m_Processor.m_Cwp, m_Processor.m_Cursor.m_hh.m_Height);
		m_Processor.get_Utxos().Traverse(ctx);

		const Rules& r = Rules::get();

		if (r.IsPastFork_<2>(m_Processor.m_Cursor.m_hh.m_Height))
		{
			MySerializer ser(ctx.m_Writer.m_Stream);
			ser & MaxHeight; // terminator

			// shielded in/outs
			struct MyKrnWalker
				:public NodeProcessor::KrnWalkerShielded
			{
				MySerializer& m_Ser;
				MyKrnWalker(MySerializer& ser) :m_Ser(ser) {}

				bool OnKrnEx(const TxKernelShieldedInput& krn) override
				{
					uint8_t nFlags = 0;

					m_Ser & m_Height;
					m_Ser & nFlags;
					m_Ser & krn.m_SpendProof.m_SpendPk;
					return true;
				}

				bool OnKrnEx(const TxKernelShieldedOutput& krn) override
				{
					Asset::Proof::Ptr pAsset;

					uint8_t nFlags = RecoveryInfo::Flags::Output;
					if (krn.m_Txo.m_pAsset)
					{
						pAsset.swap(Cast::NotConst(krn).m_Txo.m_pAsset);
						nFlags |= RecoveryInfo::Flags::HadAsset;
					}

					m_Ser & m_Height;
					m_Ser & nFlags;
					m_Ser & krn.m_Txo;
					m_Ser & krn.get_Msg();

					if (pAsset && Rules::get().IsPastFork_<3>(m_Height))
						m_Ser & pAsset->m_hGen;

					return true;
				}

			} wlk(ser);

			Block::NumberRange nr;
			nr.m_Min = m_Processor.FindAtivePastHeight(r.pForks[2].m_Height);
			nr.m_Max = m_Processor.m_Cursor.m_Full.m_Number;

			m_Processor.EnumKernels(wlk, nr);

			ser & MaxHeight; // terminator

			// assets
			Asset::Full ai;
			ai.m_ID = 0;

			while (m_Processor.get_DB().AssetGetNext(ai))
			{
				if (!r.IsPastFork_<6>(m_Processor.m_Cursor.m_hh.m_Height))
					ai.SetCid(nullptr);

				ser & ai;
			}

			ser & (Asset::s_MaxCount + 1); // terminator

			if (r.IsPastFork_<3>(m_Processor.m_Cursor.m_hh.m_Height))
			{
				m_Processor.EnsureCursorKernels();

				Merkle::Hash hv;
				NodeProcessor::Evaluator ev(m_Processor);

				BEAM_VERIFY(ev.get_Contracts(hv));
				ser & hv;

				BEAM_VERIFY(ev.get_KL(hv));
				ser & hv;
			}
		}

	}
	catch (const std::exception& ex)
	{
		BEAM_LOG_ERROR() << ex.what();
		return false;
	}

	return true;
}

void Node::PrintTxos()
{
	for (const auto& acc : m_Processor.m_vAccounts)
	{
		struct PerAsset {
			Amount m_Avail = 0;
			Amount m_Locked = 0;
		};
		typedef std::map<Asset::ID, PerAsset> AssetMap;

		std::ostringstream os;
		os << "Printing Events for Endpoint=" << acc.get_Endpoint() << std::endl;

		if (m_Processor.IsFastSync())
			os << "Note: Fast-sync is in progress. Data is preliminary and not fully verified yet." << std::endl;

		if (acc.m_hTxoHi)
			os << "Note: Cut-through up to Height=" << acc.m_hTxoHi << ", Txos spent earlier may be missing. To recover them too please make full sync." << std::endl;

		struct MyParser :public proto::Event::IParser
		{
			std::ostringstream& m_os;
			AssetMap m_Map;
			Height m_Height = 0;

			MyParser(std::ostringstream& os) :m_os(os) {}

			void OnEventBase(proto::Event::Base& x) override
			{
				x.Dump(m_os);
			}

			void OnValue(Amount v, Asset::ID aid, uint8_t nFlags, bool bMature)
			{
				PerAsset& pa = m_Map[aid];

				if (proto::Event::Flags::Add & nFlags)
					(bMature ? pa.m_Avail : pa.m_Locked) += v;
				else
					pa.m_Avail -= v;
			}

			void OnEventType(proto::Event::Utxo& x) override
			{
				OnEventBase(x);
				OnValue(x.m_Cid.m_Value, x.m_Cid.m_AssetID, x.m_Flags, x.m_Maturity <= m_Height);
			}

			void OnEventType(proto::Event::Shielded& x) override
			{
				OnEventBase(x);
				OnValue(x.m_CoinID.m_Value, x.m_CoinID.m_AssetID, x.m_Flags, true);
			}

		} p(os);
		p.m_Height = m_Processor.m_Cursor.m_hh.m_Height;

		NodeDB::WalkerEvent wlk;
		for (m_Processor.get_DB().EnumEvents(wlk, acc.m_iAccount, 0); wlk.MoveNext(); )
		{
			os << "\tHeight=" << wlk.m_Pos.m_Height << ", ";
			p.ProceedOnce(wlk.m_Body);
			os << std::endl;
		}

		os << "Totals:" << std::endl;

		for (AssetMap::iterator it = p.m_Map.begin(); p.m_Map.end() != it; ++it)
		{
			const PerAsset& pa = it->second;
			if (pa.m_Avail || pa.m_Locked)
			{
				os << "\tAssetID=" << it->first << ", Avail=" << AmountBig::Printable(pa.m_Avail) << ", Locked=" << AmountBig::Printable(pa.m_Locked) << std::endl;
			}
		}

		BEAM_LOG_INFO() << os.str();
	}
}

void Node::PrintRollbackStats()
{
	std::ostringstream os;
	os << "Printing recent rollback stats" << std::endl;

	NodeDB& db = m_Processor.get_DB(); // alias

	ByteBuffer bufP, bufE;
	std::vector<NodeDB::StateInput> vIns;

	NodeDB::WalkerState wlk;
	for (db.EnumFunctionalTips(wlk); wlk.MoveNext(); )
	{
		Block::SystemState::Full sTip1;
		db.get_State(wlk.m_Sid.m_Row, sTip1);
		assert(sTip1.m_Number.v == wlk.m_Sid.m_Number.v);

		typedef std::map<ECC::Point, Block::Number> InputMap;
		InputMap inpMap;

		typedef std::map<ECC::Hash::Value, Block::Number> KrnMap;
		KrnMap krnMap;

		NodeDB::StateID sidSplit = wlk.m_Sid;

		for (; sidSplit.m_Row; db.get_Prev(sidSplit))
		{
			if (NodeDB::StateFlags::Active & db.GetStateFlags(sidSplit.m_Row))
				break;

			bufP.clear();
			bufE.clear();
			db.GetStateBlock(sidSplit.m_Row, &bufP, &bufE, nullptr);

			Block::Body block;

			Deserializer der;
			der.reset(bufP);
			der & Cast::Down<Block::BodyBase>(block);
			der & Cast::Down<TxVectors::Perishable>(block);

			der.reset(bufE);
			der & Cast::Down<TxVectors::Eternal>(block);

			for (size_t i = 0; i < block.m_vInputs.size(); i++)
				inpMap[block.m_vInputs[i]->m_Commitment] = sidSplit.m_Number;

			for (size_t i = 0; i < block.m_vKernels.size(); i++)
				krnMap[block.m_vKernels[i]->get_ID()] = sidSplit.m_Number;
		}

		if (inpMap.empty())
			continue;

		os << "Rollback " << sTip1.m_Number.v << " -> " << sidSplit.m_Number.v << std::endl;

		auto h = sidSplit.m_Number;
		while (h.v < m_Processor.m_Cursor.m_Full.m_Number.v)
		{
			++h.v;
			uint64_t row = db.FindActiveStateStrict(h);
			Block::SystemState::Full s2;
			db.get_State(row, s2);

			if (s2.m_TimeStamp > sTip1.m_TimeStamp)
			{
				os << "	H=" << h.v << ", Stopping" << std::endl;
				break;
			}

			vIns.clear();
			db.get_StateInputs(row, vIns);

			if (vIns.empty())
				continue;

			os << "	H=" << h.v << ", Inputs=" << vIns.size() << std::endl;

			bufE.clear();
			db.GetStateBlock(row, nullptr, &bufE, nullptr);

			TxVectors::Eternal krns;

			Deserializer der;
			der.reset(bufE);
			der & krns;

			typedef std::map<uint64_t, uint32_t> DoubleSpendMap;
			DoubleSpendMap dsm;

			for (size_t i = 0; i < vIns.size(); i++)
			{
				ECC::Point comm;
				vIns[i].Get(comm);

				InputMap::iterator it = inpMap.find(comm);
				if (inpMap.end() != it)
				{
					Block::Number hOld = it->second;
					inpMap.erase(it);

					DoubleSpendMap::iterator it2 = dsm.find(hOld.v);
					if (dsm.end() == it2)
					{
						// check if there're corresponding kernels
						uint32_t nCommonKrns = 0;
						for (size_t j = 0; j < krns.m_vKernels.size(); j++)
						{
							if (krnMap.end() != krnMap.find(krns.m_vKernels[j]->get_ID()))
								nCommonKrns++;
						}

						it2 = dsm.insert(DoubleSpendMap::value_type(hOld.v, nCommonKrns ? uint32_t(-1) : 0)).first;
					}

					if (it2->second != uint32_t(-1))
						it2->second++;
				}
			}

			for (DoubleSpendMap::iterator it = dsm.begin(); dsm.end() != it; ++it)
			{
				uint32_t n = it->second;
				if (uint32_t(-1) == n)
					continue;

				os << "		Double-spend. Old Height=" << it->first << ", Count=" << n << std::endl;
			}

		}

	}

	BEAM_LOG_INFO() << os.str();
}

void Node::GenerateFakeBlocks(uint32_t n)
{
	assert(Rules::Consensus::FakePoW == Rules::get().m_Consensus);
	m_Miner.m_FakeBlocksToGenerate = n;
	m_Miner.SetTimer(0, true);
}

} // namespace beam
