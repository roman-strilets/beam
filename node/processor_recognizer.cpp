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

#include "processor_bic.h"

namespace beam {

void NodeProcessor::Account::InitFromOwner()
{
	m_vSh.resize(1); // Change this if/when we decide to use multiple keys

	for (Key::Index nIdx = 0; nIdx < m_vSh.size(); nIdx++)
		m_vSh[nIdx].FromOwner(*m_pOwner, nIdx);
}

std::string NodeProcessor::Account::get_Endpoint() const
{
	Key::ID kid(Zero);
	kid.m_Type = ECC::Key::Type::EndPoint;

	PeerID pid;
	kid.get_Hash(pid);

	ECC::Point::Native ptN;
	m_pOwner->DerivePKeyG(ptN, pid);
	pid.Import(ptN);

	return Base58::to_string(pid);
}

template <typename TKey, typename TEvt>
bool NodeProcessor::Recognizer::FindEvent(const TKey& key, TEvt& evt, std::vector<TEvt>* pvDups /* = nullptr */)
{
	struct MyHandler
		:public Recognizer::IEventHandler
	{
		TEvt& m_Evt;
		std::vector<TEvt>* m_pvDups;
		bool m_Found = false;

		MyHandler(TEvt& x) :m_Evt(x) {}

		bool OnEvent(Height, const Blob& body) override
		{
			Deserializer der;

			proto::Event::Type::Enum eType;
			der.reset(body.p, body.n);
			eType = proto::Event::Type::Load(der);

			if (TEvt::s_Type != eType)
				return false;

			if (m_Found)
			{
				assert(m_pvDups);
				der & m_pvDups->emplace_back();
			}
			else
			{
				der & m_Evt;
				m_Found = true;
			}

			return !m_pvDups; // stop if not interested in dups
		}

	} h(evt);
	h.m_pvDups = pvDups;

	m_Handler.FindEvents(Blob(&key, sizeof(key)), h);

	return h.m_Found;
}

template <typename TEvt>
void NodeProcessor::Recognizer::AddEventInternal(const TEvt& evt, const Blob& key)
{
	Serializer ser;
	ser & TEvt::s_Type;
	ser & evt;

	m_Handler.InsertEvent(m_Pos, Blob(ser.buffer().first, static_cast<uint32_t>(ser.buffer().second)), key);
	m_Pos.m_Pos++;

	m_Handler.OnEvent(m_Pos.m_Height, evt);
}

template <typename TEvt, typename TKey>
void NodeProcessor::Recognizer::AddEvent(const TEvt& evt, const TKey& key)
{
	AddEventInternal(evt, Blob(&key, sizeof(key)));
}

template <typename TEvt>
void NodeProcessor::Recognizer::AddEvent(const TEvt& evt)
{
	AddEventInternal(evt, Blob(nullptr, 0));
}

NodeProcessor::Recognizer::Recognizer(IHandler& h, Extra& extra)
	: m_Handler(h)
	, m_Extra(extra)
{

}

void NodeProcessor::Recognizer::RecognizeBlock(const TxVectors::Full& block, uint32_t shieldedOuts, bool validateShieldedOuts)
{
	assert(m_Handler.m_pAccount);
	const auto& acc = *m_Handler.m_pAccount;

	// recognize all
	for (size_t i = 0; i < block.m_vInputs.size(); i++)
		Recognize(*block.m_vInputs[i]);

	for (size_t i = 0; i < block.m_vOutputs.size(); i++)
		Recognize(*block.m_vOutputs[i], *acc.m_pOwner);

	if (!acc.m_vSh.empty())
	{
		KrnWalkerRecognize wlkKrn(*this);
		wlkKrn.m_Height = m_Pos.m_Height;

		TxoID nOuts = m_Extra.m_ShieldedOutputs;
		m_Extra.m_ShieldedOutputs -= shieldedOuts;

		wlkKrn.Process(block.m_vKernels);
		if (validateShieldedOuts)
		{
			assert(m_Extra.m_ShieldedOutputs == nOuts);
			nOuts; // supporess unused var warning in release
		}
	}
}

void NodeProcessor::Recognizer::Recognize(const Input& x)
{
	const EventKey::Utxo& key = x.m_Commitment;
	proto::Event::Utxo evt;

	std::vector<proto::Event::Utxo> vDups;
	if (!FindEvent(key, evt, &vDups))
		return;

	auto* pEvt = &evt;

	if (!vDups.empty())
	{
		// there're dups.
		assert(proto::Event::Flags::Add & evt.m_Flags); // the very 1st evt must be input

		for (uint32_t iOutp = 0; iOutp < vDups.size(); iOutp++)
		{
			const auto& evtOutp = vDups[iOutp];
			if (proto::Event::Flags::Add & evtOutp.m_Flags)
				continue;

			// this is output event. Pair it with the appropriate input event
			if ((proto::Event::Flags::Add & evt.m_Flags) && (evt.m_Maturity == evtOutp.m_Maturity))
				evt.m_Maturity = MaxHeight;
			else
			{
				for (uint32_t iInp = 0; ; iInp++)
				{
					assert(iInp < iOutp);
					auto& evtInp = vDups[iInp];

					if ((proto::Event::Flags::Add & evtInp.m_Flags) && (evtInp.m_Maturity == evtOutp.m_Maturity))
					{
						evtInp.m_Maturity = MaxHeight;
						break;
					}
				}
			}
		}

		// make sure we select the remaining input with minimum maturity
		for (uint32_t iInp = 0; iInp < vDups.size(); iInp++)
		{
			auto& evtInp = vDups[iInp];

			if ((proto::Event::Flags::Add & evtInp.m_Flags) && (evtInp.m_Maturity < pEvt->m_Maturity))
			{
				pEvt = &evtInp;
				break;
			}
		}

	}

	pEvt->m_Flags &= ~proto::Event::Flags::Add;
	AddEvent(*pEvt);
}

void NodeProcessor::Recognizer::Recognize(const TxKernelShieldedInput& x, uint32_t)
{
	EventKey::Shielded key = x.m_SpendProof.m_SpendPk;
	key.m_Y |= EventKey::s_FlagShielded;

	proto::Event::Shielded evt;
	if (!FindEvent(key, evt))
		return;

	evt.m_Flags &= ~proto::Event::Flags::Add;

	AddEvent(evt);
}

bool NodeProcessor::KrnWalkerShielded::OnKrn(const TxKernel& krn)
{
	switch (krn.get_Subtype())
	{
	case TxKernel::Subtype::ShieldedInput:
		return OnKrnEx(Cast::Up<TxKernelShieldedInput>(krn));
	case TxKernel::Subtype::ShieldedOutput:
		return OnKrnEx(Cast::Up<TxKernelShieldedOutput>(krn));

	default:
		break; // suppress warning
	}

	return true;
}

bool NodeProcessor::KrnWalkerRecognize::OnKrn(const TxKernel& krn)
{
	switch (krn.get_Subtype())
	{
#define THE_MACRO(name) \
	case TxKernel::Subtype::name: \
		m_Rec.Recognize(Cast::Up<TxKernel##name>(krn), m_nKrnIdx); \
		break;

	BeamKernelsRecongizableAll(THE_MACRO)
#undef THE_MACRO

	default:
		break; // suppress warning
	}

	return true;
}

void NodeProcessor::Recognizer::Recognize(const TxKernelShieldedOutput& v, uint32_t)
{
	TxoID nID = m_Extra.m_ShieldedOutputs++;

	assert(m_Handler.m_pAccount);
	const auto& acc = *m_Handler.m_pAccount;

	for (Key::Index nIdx = 0; nIdx < acc.m_vSh.size(); nIdx++)
	{
		const ShieldedTxo& txo = v.m_Txo;

		ShieldedTxo::Data::Params pars;
		if (!pars.m_Ticket.Recover(txo.m_Ticket, acc.m_vSh[nIdx]))
			continue;

		ECC::Oracle oracle;
		oracle << v.get_Msg();

		if (!pars.m_Output.Recover(txo, pars.m_Ticket.m_SharedSecret, m_Pos.m_Height, oracle))
			continue;

		proto::Event::Shielded evt;
		evt.m_TxoID = nID;
		pars.ToID(evt.m_CoinID);
		evt.m_CoinID.m_Key.m_nIdx = nIdx;
		evt.m_Flags = proto::Event::Flags::Add;

		EventKey::Shielded key = pars.m_Ticket.m_SpendPk;
		key.m_Y |= EventKey::s_FlagShielded;

		AddEvent(evt, key);
		break;
	}
}

void NodeProcessor::Recognizer::Recognize(const Output& x, Key::IPKdf& keyViewer)
{
	CoinID cid;
	Output::User user;
	if (!x.Recover(m_Pos.m_Height, keyViewer, cid, &user))
		return;

	// filter-out dummies
	if (cid.IsDummy())
	{
		m_Handler.OnDummy(cid, m_Pos.m_Height);
		return;
	}

	// bingo!
	proto::Event::Utxo evt;
	evt.m_Flags = proto::Event::Flags::Add;
	evt.m_Cid = cid;
	evt.m_Commitment = x.m_Commitment;
	evt.m_Maturity = x.get_MinMaturity(m_Pos.m_Height);
	evt.m_User = user;

	const EventKey::Utxo& key = x.m_Commitment;
	AddEvent(evt, key);
}

void NodeProcessor::Recognizer::Recognize(const TxKernelAssetCreate& v, uint32_t nKrnIdx)
{
	assert(m_Handler.m_pAccount);
	const auto& acc = *m_Handler.m_pAccount;

	EventKey::AssetCtl key;
	v.m_MetaData.get_Owner(key, *acc.m_pOwner);
	if (key != v.m_Owner)
		return;

	// recognized!
	proto::Event::AssetCtl evt;

	evt.m_Flags = proto::Event::Flags::Add;
	evt.m_EmissionChange = 0; // no change upon creation

	NodeDB::AssetEvt wlk;
	m_Handler.AssetEvtsGetStrict(wlk, m_Pos.m_Height, nKrnIdx);
	assert(wlk.m_ID > Asset::s_MaxCount);

	evt.m_Info.m_ID = wlk.m_ID - Asset::s_MaxCount;
	evt.m_Info.m_LockHeight = m_Pos.m_Height;
	TemporarySwap<ByteBuffer> ts(Cast::NotConst(v).m_MetaData.m_Value, evt.m_Info.m_Metadata.m_Value);
	evt.m_Info.m_Owner = v.m_Owner;
	evt.m_Info.m_Value = Zero;
	evt.m_Info.m_Deposit = Rules::get().get_DepositForCA(m_Pos.m_Height);
	evt.m_Info.SetCid(nullptr);

	AddEvent(evt, key);
}

void NodeProcessor::Recognizer::Recognize(const TxKernelAssetEmit& v, uint32_t nKrnIdx)
{
	proto::Event::AssetCtl evt;
	if (!FindEvent(v.m_Owner, evt))
		return;

	evt.m_Flags = 0;
	evt.m_EmissionChange = v.m_Value;

	NodeDB::AssetEvt wlk;
	m_Handler.AssetEvtsGetStrict(wlk, m_Pos.m_Height, nKrnIdx);
	assert(wlk.m_ID == evt.m_Info.m_ID);

	AssetDataPacked adp;
	adp.set_Strict(wlk.m_Body);

	evt.m_Info.m_Value = adp.m_Amount;
	adp.m_LockHeight.Export(evt.m_Info.m_LockHeight);
	evt.m_Info.m_Deposit = Rules::get().CA.DepositForList2; // not used anyway

	AddEvent(evt);
}

void NodeProcessor::Recognizer::Recognize(const TxKernelAssetDestroy& v, uint32_t nKrnIdx)
{
	proto::Event::AssetCtl evt;
	if (!FindEvent(v.m_Owner, evt))
		return;

	evt.m_Flags = proto::Event::Flags::Delete;
	evt.m_EmissionChange = 0;

	evt.m_Info.m_Value = Zero;
	evt.m_Info.m_Deposit = v.get_Deposit();

	AddEvent(evt);
}

void NodeProcessor::RescanAccounts(uint32_t nRecent)
{
	if (!nRecent)
		return;
	assert(nRecent <= m_vAccounts.size());

	MyRecognizer rec(*this);

	struct TxoRecover
		:public ITxoRecover
	{
		MyRecognizer& m_Rec;
		uint32_t m_Total = 0;
		uint32_t m_Unspent = 0;

		const Account* m_pAcc;
		uint32_t m_nAcc;

		TxoRecover(MyRecognizer& rec)
			:m_Rec(rec)
		{
		}

		bool OnTxo(const NodeDB::WalkerTxo& wlk, Height hCreate, Output& outp) override
		{
			for (uint32_t iAcc = 0; iAcc < m_nAcc; iAcc++)
			{
				m_Rec.m_Handler.m_pAccount = m_pAcc + iAcc;
				m_pKey = m_Rec.m_Handler.m_pAccount->m_pOwner.get();

				if (!ITxoRecover::OnTxo(wlk, hCreate, outp))
					return false;
			}

			return true;
		}

		bool OnTxo(const NodeDB::WalkerTxo& wlk, Height hCreate, Output& outp, const CoinID& cid, const Output::User& user) override
		{
			if (cid.IsDummy())
			{
				m_Rec.m_Handler.m_Proc.OnDummy(cid, hCreate);
				return true;
			}

			proto::Event::Utxo evt;
			evt.m_Flags = proto::Event::Flags::Add;
			evt.m_Cid = cid;
			evt.m_Commitment = outp.m_Commitment;
			evt.m_Maturity = outp.get_MinMaturity(hCreate);
			evt.m_User = user;

			m_Rec.m_Recognizer.m_Pos.m_Height = hCreate;
			// don't reset the Pos.Index. Its value is not important, it only should be monotonic
			const EventKey::Utxo& key = outp.m_Commitment;
			m_Rec.m_Recognizer.AddEvent(evt, key);

			m_Total++;

			if (MaxHeight == wlk.m_SpendHeight)
				m_Unspent++;
			else
			{
				evt.m_Flags = 0;
				m_Rec.m_Recognizer.m_Pos.m_Height = wlk.m_SpendHeight;
				m_Rec.m_Recognizer.AddEvent(evt);
			}

			return true;
		}
	};

	{
		LongAction la("Rescanning owned Txos...", 0, m_pExternalHandler);

		TxoRecover wlk(rec);
		wlk.m_pLa = &la;
		wlk.m_pAcc = &m_vAccounts.front() + m_vAccounts.size() - nRecent;
		wlk.m_nAcc = nRecent;

		EnumTxos(wlk);

		BEAM_LOG_INFO() << "Recovered " << wlk.m_Unspent << "/" << wlk.m_Total << " unspent/total Txos";
	}

	// shielded items
	Height h0 = Rules::get().pForks[2].m_Height;
	if (m_Cursor.m_hh.m_Height >= h0)
	{
		TxoID nOuts = m_Extra.m_ShieldedOutputs;
		m_Extra.m_ShieldedOutputs = 0;

		LongAction la("Rescanning shielded Txos...", 0, m_pExternalHandler);

		struct MyKrnWalker
			:public KrnWalkerRecognize
		{
			using KrnWalkerRecognize::KrnWalkerRecognize;

			const Account* m_pAcc;
			uint32_t m_nAcc;

			bool ProcessBlock(const NodeDB::StateID& sid, const std::vector<TxKernel::Ptr>& v) override
			{
				TxoID nOuts = m_Rec.m_Extra.m_ShieldedOutputs;
				m_Rec.m_Pos.m_Height = m_Height;

				for (uint32_t iAcc = 0; iAcc < m_nAcc; iAcc++)
				{
					m_Rec.m_Extra.m_ShieldedOutputs = nOuts;

					m_Rec.m_Handler.m_pAccount = m_pAcc + iAcc;

					if (!KrnWalkerRecognize::ProcessBlock(sid, v))
						return false;
				}

				return true;
			}

		};

		MyKrnWalker wlkKrn(rec.m_Recognizer);
		wlkKrn.m_pAcc = &m_vAccounts.front() + m_vAccounts.size() - nRecent;
		wlkKrn.m_nAcc = nRecent;

		Block::NumberRange nr;
		nr.m_Min = FindAtivePastHeight(h0);
		nr.m_Max = m_Cursor.m_Full.m_Number;

		wlkKrn.m_pLa = &la;
		EnumKernels(wlkKrn, nr);

		assert(m_Extra.m_ShieldedOutputs == nOuts);
		nOuts; // suppress unused var warning in release
	}
}

} // namespace beam
