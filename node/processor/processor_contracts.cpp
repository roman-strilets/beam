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

bool NodeProcessor::BlockInterpretCtx::HandleKernelType(const TxKernelContractCreate& krn)
{
	if (m_Fwd)
	{
		bvm2::ShaderID sid;
		bvm2::get_ShaderID(sid, krn.m_Data);
		bvm2::ContractID cid;
		bvm2::get_CidViaSid(cid, sid, krn.m_Args);

		auto& e = m_Storage.get_Var(cid);
		if (!e.m_Data.empty())
		{
			m_TxStatus = proto::TxStatus::ContractFailNode;

			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << "Contract " << cid << " already exists";

			return false; // contract already exists
		}

		BlockInterpretCtx::BvmProcessor proc(*this);

		Blob blob = krn.m_Data;
		proc.AddRemoveShader(cid, &blob);

		if (!proc.Invoke(cid, 0, krn))
			return false;

	}
	else
		m_Storage.UndoVars();

	return true;
}

bool NodeProcessor::BlockInterpretCtx::HandleKernelType(const TxKernelContractInvoke& krn)
{
	if (m_Fwd)
	{
		if (!krn.m_iMethod)
		{
			m_TxStatus = proto::TxStatus::ContractFailNode;

			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << "Contract " << krn.m_Cid << " c'tor call attempt";

			return false; // c'tor call attempt
		}

		BlockInterpretCtx::BvmProcessor proc(*this);
		if (!proc.Invoke(krn.m_Cid, krn.m_iMethod, krn))
			return false;

		if (1 == krn.m_iMethod)
		{
			// d'tor called. Make sure no variables are left except for the contract data
			proc.AddRemoveShader(krn.m_Cid, nullptr);

			if (!proc.EnsureNoVars(krn.m_Cid))
			{
				m_TxStatus = proto::TxStatus::ContractFailNode;

				if (m_pTxErrorInfo)
					*m_pTxErrorInfo << "Contract " << krn.m_Cid << " d'tor not fully clean";

				m_Storage.UndoVars();
				return false;
			}
		}
	}
	else
		m_Storage.UndoVars();

	return true;
}

bool NodeProcessor::BlockInterpretCtx::BvmProcessor::EnsureNoVars(const bvm2::ContractID& cid)
{
	Blob key(cid);

	auto* pE = m_Bic.m_Storage.FindVarEx(key, true, true);
	return !(pE && IsOwnedVar(cid, pE->ToBlob()));
}

bool NodeProcessor::BlockInterpretCtx::BvmProcessor::IsOwnedVar(const bvm2::ContractID& cid, const Blob& key)
{
	return
		(key.n >= cid.nBytes) &&
		!memcmp(cid.m_pData, key.p, cid.nBytes);
}

NodeProcessor::BlockInterpretCtx::VmProcessorBase::VmProcessorBase(BlockInterpretCtx& bic)
	:m_Bic(bic)
{
	assert(bic.m_Fwd);
	m_Bic.m_Storage.PutVarsTerminator();
}

bool NodeProcessor::BlockInterpretCtx::BvmProcessor::Invoke(const bvm2::ContractID& cid, uint32_t iMethod, const TxKernelContractControl& krn)
{
	if (Rules::Consensus::Pbft == Rules::get().m_Consensus)
	{
		auto* pPbft = m_Bic.m_Proc.get_PbftHandler();
		if (pPbft && !pPbft->OnContractInvoke(cid, iMethod, krn.m_Args, m_Bic.m_Temporary))
			return  false;
	}

	bool bRes = false;
	try
	{
		m_Charge = m_Bic.m_ChargePerBlock;

		if (m_Bic.m_pTxErrorInfo)
			m_FarCalls.m_SaveLocal = true;

		InitStackPlus(m_Stack.AlignUp(static_cast<uint32_t>(krn.m_Args.size())));
		m_Stack.PushAlias(krn.m_Args);

		m_Instruction.m_Mode = 
			IsPastFork_<4>() ?
			Wasm::Reader::Mode::Standard :
			m_Bic.m_TxValidation ?
				Wasm::Reader::Mode::Restrict :
				Wasm::Reader::Mode::Emulate_x86;

		ECC::Hash::Processor hp;
		FundsChangeMap fundsIO;

		if (!m_Bic.m_AlreadyValidated)
		{
			const auto& hvCtx = m_Bic.m_DependentCtxSet ? m_Bic.m_hvDependentCtx : m_Bic.m_Proc.m_Cursor.m_Full.m_Prev;
			krn.Prepare(hp, &hvCtx);

			m_pSigValidate = &hp;
			m_pFundsIO = &fundsIO;
		}

		if (m_Bic.m_pvC)
			m_pFundsIO = &fundsIO;

		CallFar(cid, iMethod, m_Stack.get_AlasSp(), (uint32_t)krn.m_Args.size(), 0);

		while (!IsDone())
		{
			DischargeUnits(bvm2::Limits::Cost::Cycle);
			RunOnce();
		}

		if (!m_Bic.m_AlreadyValidated)
			CheckSigs(krn.m_Commitment, krn.m_Signature);

		bRes = true;

		UpdChargeWithRecovery(m_Charge);

	}
	catch (const Exc& e)
	{
		uint32_t n = e.m_Type + proto::TxStatus::ContractFailFirst;
		m_Bic.m_TxStatus = (n < proto::TxStatus::ContractFailLast) ? static_cast<uint8_t>(n) : proto::TxStatus::ContractFailFirst;

		if (e.m_Type == bvm2::ErrorSubType::NoCharge)
			m_Bic.m_LimitExceeded = true;

		if (m_Bic.m_pTxErrorInfo)
			*m_Bic.m_pTxErrorInfo << e.what();
	}
	catch (const std::exception& e)
	{
		m_Bic.m_TxStatus = proto::TxStatus::ContractFailFirst;

		if (m_Bic.m_pTxErrorInfo)
			*m_Bic.m_pTxErrorInfo << e.what();
	}

	if (!bRes)
	{
		if (m_Bic.m_pTxErrorInfo)
		{
			DumpCallstack(*m_Bic.m_pTxErrorInfo);
			*m_Bic.m_pTxErrorInfo << " <- cid=" << cid << " method=" << iMethod;
		}

		m_Bic.m_Storage.UndoVars();
	}

	if (m_Instruction.m_ModeTriggered) {
		BEAM_LOG_WARNING() << " Potential wasm conflict";
	}

	return bRes;
}

struct NodeProcessor::ProcessorInfoParser
	:public bvm2::ProcessorManager
{
	NodeProcessor& m_Proc;

	Height m_Height;
	ByteBuffer m_bufParser;
	std::ostringstream m_os;

	void SelectContext(bool /* bDependent */, uint32_t /* nChargeNeeded */) override {
		m_Context.m_Height = m_Height;
	}
	bool get_HdrAt(Block::SystemState::Full& s, Height h) override
	{
		if (h > m_Height)
			return false;
		return m_Proc.get_HdrAt(s, h);
	}

	void VarsEnum(const Blob& kMin, const Blob& kMax, IReadVars::Ptr& pOut) override
	{
		struct Context
			:public IReadVars
		{
			NodeDB::WalkerContractData m_Wlk;
			ByteBuffer m_Buf1, m_Buf2;

			bool MoveNext() override
			{
				if (!m_Wlk.MoveNext())
					return false;

				m_LastKey = m_Wlk.m_Key;
				m_LastVal = m_Wlk.m_Val;
				return true;
			}
		};

		pOut = std::make_unique<Context>();
		auto& x = Cast::Up<Context>(*pOut);

		kMin.Export(x.m_Buf1);
		kMax.Export(x.m_Buf2);

		m_Proc.m_DB.ContractDataEnum(x.m_Wlk, x.m_Buf1, x.m_Buf2);
	}

	void LogsEnum(const Blob& kMin, const Blob& kMax, const HeightPos* pPosMin, const HeightPos* pPosMax, IReadLogs::Ptr& pOut) override
	{
		struct Context
			:public IReadLogs
		{
			NodeDB::ContractLog::Walker m_Wlk;
			ByteBuffer m_Buf1, m_Buf2;

			bool MoveNext() override
			{
				if (!m_Wlk.MoveNext())
					return false;

				m_LastKey = m_Wlk.m_Entry.m_Key;
				m_LastVal = m_Wlk.m_Entry.m_Val;
				m_LastPos = m_Wlk.m_Entry.m_Pos;
				return true;
			}
		};

		pOut = std::make_unique<Context>();
		auto& x = Cast::Up<Context>(*pOut);

		HeightPos hpMin, hpMax;
		if (!pPosMin)
		{
			pPosMin = &hpMin;
			hpMin = HeightPos(0);
		}

		if (!pPosMax)
		{
			pPosMax = &hpMax;
			hpMax = HeightPos(MaxHeight);
		}

		if (kMin.n && kMax.n)
		{
			kMin.Export(x.m_Buf1);
			kMax.Export(x.m_Buf2);
			m_Proc.m_DB.ContractLogEnum(x.m_Wlk, x.m_Buf1, x.m_Buf2, *pPosMin, *pPosMax);
		}
		else
			m_Proc.m_DB.ContractLogEnum(x.m_Wlk, *pPosMin, *pPosMax);
	}
/*
	bool VarGetProof(Blob& key, ByteBuffer& val, beam::Merkle::Proof&) override {
		return false;
	}

	bool LogGetProof(const HeightPos&, beam::Merkle::Proof&) override {
		return false;
	}

	bool get_SpecialParam(const char*, Blob&) override {
		return false;
	}
*/

	bool get_AssetInfo(Asset::Full& ai) override
	{
		return m_Proc.get_DB().AssetGetSafe(ai);
	}

	ProcessorInfoParser(NodeProcessor& p)
		:m_Proc(p)
	{
		m_Height = p.m_Cursor.m_hh.m_Height;
	}

	bool Init(uint32_t nStackBytesExtra)
	{
		m_Proc.m_DB.ParamGet(NodeDB::ParamID::RichContractParser, nullptr, nullptr, &m_bufParser);
		if (m_bufParser.empty())
			return false;

		InitMem(nStackBytesExtra);
		m_Code = m_bufParser;

		m_pOut = &m_os;
		return true;
	}

	std::string Execute()
	{
		while (!IsDone())
			RunOnce();

		auto ret = m_os.str();
		if (2 == ret.size())
			ret.clear(); // remove empty group
		return ret;
	}

	template <typename T>
	Wasm::Word PushArgAlias(const T& arg)
	{
		m_Stack.AliasAlloc(sizeof(T));
		*(T*) m_Stack.get_AliasPtr() = arg;
		return m_Stack.get_AlasSp();
	}

	template <typename T>
	void PushArgBoth(const T& arg)
	{
		Wasm::Word val = PushArgAlias(arg);
		m_Stack.Push(val);
	}
};

void NodeProcessor::BlockInterpretCtx::BvmProcessor::ParseExtraInfo(ContractInvokeExtraInfo& x, const bvm2::ShaderID& sid, uint32_t iMethod, const Blob& args)
{
	try
	{
		ProcessorInfoParser proc(m_Bic.m_Proc);
		proc.m_Height = m_Bic.m_Height - 1;

		if (!proc.Init(m_Stack.AlignUp(args.n)))
			return;

		proc.m_Stack.PushAlias(args);
		Wasm::Word pArgs_ = proc.m_Stack.get_AlasSp();

		proc.PushArgBoth(sid);
		proc.PushArgBoth(x.m_Cid);
		proc.m_Stack.Push(iMethod);
		proc.m_Stack.Push(pArgs_);
		proc.m_Stack.Push(args.n);

		proc.CallMethod(0);

		x.m_sParsed = proc.Execute();
	}
	catch (const std::exception& e)
	{
		BEAM_LOG_WARNING() << "contract parser error: " << e.what();
	}
}

void NodeProcessor::get_ContractDescr(const ECC::uintBig& sid, const ECC::uintBig& cid, std::string& res, bool bFullState)
{
	try
	{
		ProcessorInfoParser proc(*this);
		if (!proc.Init(0))
			return;

		proc.PushArgBoth(sid);
		proc.PushArgBoth(cid);

		proc.CallMethod(bFullState ? 2 : 1);

		res = proc.Execute();
	}
	catch (const std::exception& e)
	{
		BEAM_LOG_WARNING() << "contract parser error: " << e.what();
	}
}

BlobMap::Entry& NodeProcessor::BlockInterpretCtx::Storage::get_Var(const Blob& key)
{
	auto* pE = m_Vars.Find(key);
	if (!pE)
	{
		pE = m_Vars.Create(key);

		Blob data;
		NodeDB::Recordset rs;
		if (get_ParentObj().m_Proc.m_DB.ContractDataFind(key, data, rs))
			data.Export(pE->m_Data);
	}
	return *pE;
}

void NodeProcessor::BlockInterpretCtx::Storage::LoadVar(const Blob& key, Blob& res)
{
	auto& e = get_Var(key);
	res = e.m_Data;
}

BlobMap::Entry* NodeProcessor::BlockInterpretCtx::Storage::FindVarEx(const Blob& key, bool bExact, bool bBigger)
{
	auto* pE = &get_Var(key);
	if (pE->m_Data.empty() || !bExact)
	{
		while (true)
		{
			NodeDB::Recordset rs;
			Blob keyDB = pE->ToBlob();
			bool bNextDB = bBigger ?
				get_ParentObj().m_Proc.m_DB.ContractDataFindNext(keyDB, rs) :
				get_ParentObj().m_Proc.m_DB.ContractDataFindPrev(keyDB, rs);

			if (bNextDB)
				get_Var(keyDB);

			auto it = BlobMap::Set::s_iterator_to(*pE);
			if (bBigger)
			{
				++it;
				if (m_Vars.end() == it)
					return nullptr;
			}
			else
			{
				if (m_Vars.begin() == it)
					return nullptr;
				--it;
			}

			pE = &(*it);
			if (!pE->m_Data.empty())
				break;
		}
	}
	return pE;
}

void NodeProcessor::BlockInterpretCtx::Storage::LoadVarEx(Blob& key, Blob& res, bool bExact, bool bBigger)
{
	auto* pE = FindVarEx(key, bExact, bBigger);
	if (pE)
	{
		key = pE->ToBlob();
		res = pE->m_Data;
	}
	else
	{
		key.n = 0;
		res.n = 0;
	}
}

uint32_t NodeProcessor::BlockInterpretCtx::Storage::SaveVar(const Blob& key, const Blob& data)
{
	auto& e = get_Var(key);
	auto nOldSize = static_cast<uint32_t>(e.m_Data.size());

	if (Rules::Consensus::Pbft == Rules::get().m_Consensus)
	{
		auto* pPbft = get_ParentObj().m_Proc.get_PbftHandler();
		if (pPbft)
			pPbft->OnContractVarChange(key, data, get_ParentObj().m_Temporary);
	}

	DataSaveWithRecovery(e, data);

	return nOldSize;
}

void NodeProcessor::BlockInterpretCtx::Storage::DataSaveWithRecovery(BlobMap::Entry& e, const Blob& data)
{
	if (Blob(e.m_Data) != data)
	{
		auto key = e.ToBlob();

		typedef VmProcessorBase::RecoveryTag RecoveryTag;
		RecoveryTag::Type nTag = RecoveryTag::Insert;

		if (data.n)
		{
			if (e.m_Data.size())
			{
				nTag = RecoveryTag::Update;
				DataUpdate(key, data, e.m_Data);
			}
			else
			{
				nTag = RecoveryTag::Delete;
				DataInsert(key, data);
			}
		}
		else
		{
			assert(e.m_Data.size());
			DataDel(key, e.m_Data);
		}

		Ser ser(get_ParentObj());
		ser & nTag;
		ser & key.n;
		ser.WriteRaw(key.p, key.n);
		if (e.m_Data.size())
			ser & e.m_Data;

		data.Export(e.m_Data);
	}
}

void NodeProcessor::BlockInterpretCtx::VmProcessorBase::UpdChargeWithRecovery(uint32_t val)
{
	if (m_Bic.m_Temporary)
	{
		Ser ser(m_Bic);
		RecoveryTag::Type nTag = RecoveryTag::Recharge;
		ser & nTag;
		ser & m_Bic.m_ChargePerBlock;
	}

	m_Bic.m_ChargePerBlock = val;

}

void NodeProcessor::BlockInterpretCtx::Storage::DataInsert(const Blob& key, const Blob& data)
{
	DataToggleTree(key, data, true);
	if (!get_ParentObj().m_Temporary)
		get_ParentObj().m_Proc.m_DB.ContractDataInsert(key, data);
}

void NodeProcessor::BlockInterpretCtx::Storage::DataUpdate(const Blob& key, const Blob& val, const Blob& valOld)
{
	DataToggleTree(key, val, true);
	DataToggleTree(key, valOld, false);
	if (!get_ParentObj().m_Temporary)
		get_ParentObj().m_Proc.m_DB.ContractDataUpdate(key, val);
}

void NodeProcessor::BlockInterpretCtx::Storage::DataDel(const Blob& key, const Blob& valOld)
{
	DataToggleTree(key, valOld, false);
	if (!get_ParentObj().m_Temporary)
		get_ParentObj().m_Proc.m_DB.ContractDataDel(key);
}

void NodeProcessor::BlockInterpretCtx::Storage::DataToggleTree(const Blob& key, const Blob& data, bool bAdd)
{
	if (!get_ParentObj().m_SkipDefinition)
		get_ParentObj().m_Proc.m_Mapped.m_Contract.Toggle(key, data, bAdd);
}

uint32_t NodeProcessor::BlockInterpretCtx::Storage::OnLog(const Blob& key, const Blob& val)
{
	typedef VmProcessorBase::RecoveryTag RecoveryTag;

	auto& bic = get_ParentObj(); // alias
	assert(bic.m_Fwd);
	if (!bic.m_Temporary)
	{

		NodeDB::ContractLog::Entry x;
		x.m_Pos.m_Height = bic.m_Height;
		x.m_Pos.m_Pos = bic.m_ContractLogs;
		x.m_Key = key;
		x.m_Val = val;
		bic.m_Proc.m_DB.ContractLogInsert(x);
	}

	Ser ser(bic);
	RecoveryTag::Type nTag = RecoveryTag::Log;
	ser & nTag;
	ser & bic.m_ContractLogs;

	if (!bic.m_SkipDefinition)
	{
		bool bMmr = IsContractVarStoredInMmr(key);
		ser & bMmr;

		if (bMmr)
			Block::get_HashContractLog(bic.m_vLogs.emplace_back(), key, val, bic.m_ContractLogs);
	}

	return bic.m_ContractLogs++;
}

bool NodeProcessor::BlockInterpretCtx::BvmProcessor::get_AssetInfo(Asset::Full& ai)
{
	return m_Bic.m_Proc.get_DB().AssetGetSafe(ai);
}

Height NodeProcessor::BlockInterpretCtx::BvmProcessor::get_Height()
{
	return m_Bic.m_Height - 1;
}

bool NodeProcessor::BlockInterpretCtx::BvmProcessor::get_HdrAt(Block::SystemState::Full& s, Height h)
{
	if (h > m_Bic.m_Height - 1)
		return false;

	return m_Bic.m_Proc.get_HdrAt(s, h);
}

Asset::ID NodeProcessor::BlockInterpretCtx::BvmProcessor::AssetCreate(const Asset::Metadata& md, const PeerID& pidOwner, Amount& valDeposit)
{
	Asset::ID aid = 0;
	if (!m_Bic.HandleAssetCreate(pidOwner, &m_FarCalls.m_Stack.back().m_Cid, md, aid, valDeposit, m_AssetEvtSubIdx))
		return 0;

	Ser ser(m_Bic);
	RecoveryTag::Type nTag = RecoveryTag::AssetCreate;
	ser & nTag;

	m_AssetEvtSubIdx++;

	assert(aid);
	return aid;
}

bool NodeProcessor::BlockInterpretCtx::BvmProcessor::AssetEmit(Asset::ID aid, const PeerID& pidOwner, AmountSigned val)
{
	if (!m_Bic.HandleAssetEmit(pidOwner, aid, val, m_AssetEvtSubIdx))
		return false;

	if (m_Bic.m_pvC)
	{
		auto& vec = *m_Bic.m_pvC; // alias

		assert(m_iCurrentInvokeExtraInfo <= vec.size());
		auto& x = vec[m_iCurrentInvokeExtraInfo - 1];

		bool bAdd;
		auto valUns = SplitAmountSigned(val, bAdd);
		x.m_Emission.Add(valUns, aid, bAdd);
	}

	Ser ser(m_Bic);
	RecoveryTag::Type nTag = RecoveryTag::AssetEmit;
	ser & nTag;
	ser & aid;
	ser & val;

	if (Rules::get().CA.IsForeign(aid))
		ser & pidOwner;

	m_AssetEvtSubIdx++;
	return true;
}

bool NodeProcessor::BlockInterpretCtx::BvmProcessor::AssetDestroy(Asset::ID aid, const PeerID& pidOwner, Amount& valDeposit)
{
	const ContractID& cid = m_FarCalls.m_Stack.back().m_Cid;

	if (!m_Bic.HandleAssetDestroy(pidOwner, &cid, aid, valDeposit, false, m_AssetEvtSubIdx))
		return false;

	Ser ser(m_Bic);
	RecoveryTag::Type nTag = RecoveryTag::AssetDestroy;
	ser & nTag;
	ser & aid;
	ser & cid;

	m_AssetEvtSubIdx++;
	return true;
}

void NodeProcessor::BlockInterpretCtx::Storage::PutVarsTerminator()
{
	Ser ser(get_ParentObj());

	auto n = VmProcessorBase::RecoveryTag::Terminator;
	ser & n;
}

void NodeProcessor::BlockInterpretCtx::Storage::UndoVars()
{
	typedef VmProcessorBase::RecoveryTag RecoveryTag;
	auto& bic = get_ParentObj(); // alias

	ByteBuffer key;
	for (RecoveryTag::Type nTag = 0; ; )
	{
		Der der(bic);
		der & nTag;

		switch (nTag)
		{
		case RecoveryTag::Terminator:
			return;

		case RecoveryTag::AssetCreate:
			{
				bool bFwd = false;
				TemporarySwap swp(bFwd, bic.m_Fwd);

				Asset::ID aid = 0;
				PeerID pidOwner;
				Asset::Metadata md;
				Amount valDeposit;

				if (!bic.HandleAssetCreate(pidOwner, nullptr, md, aid, valDeposit))
					return OnCorrupted();
			}
			break;

		case RecoveryTag::AssetEmit:
		{
			bool bFwd = false;
			TemporarySwap swp(bFwd, bic.m_Fwd);

			Asset::ID aid = 0;
			PeerID pidOwner;
			AmountSigned val;
			der & aid;
			der & val;

			if (Rules::get().CA.IsForeign(aid))
				der & pidOwner;

			if (!bic.HandleAssetEmit(pidOwner, aid, val))
				return OnCorrupted();
		}
		break;

		case RecoveryTag::AssetDestroy:
		{
			bool bFwd = false;
			TemporarySwap swp(bFwd, bic.m_Fwd);

			Asset::ID aid = 0;
			PeerID pidOwner;
			ContractID cid;
			der & aid;
			der & cid;

			Amount valDeposit;
			if (!bic.HandleAssetDestroy(pidOwner, &cid, aid, valDeposit, false))
				return OnCorrupted();
		}
		break;

		case RecoveryTag::Log:
		{
			der & bic.m_ContractLogs;

			if (!bic.m_Temporary)
			{
				HeightPos pos(bic.m_Height, bic.m_ContractLogs);
				bic.m_Proc.m_DB.ContractLogDel(pos, pos);
				bic.m_Proc.m_DB.TestChanged1Row();
			}

			if (!bic.m_SkipDefinition)
			{
				bool bMmr = false;
				der & bMmr;

				// Note: during reorg (i.e. proper rollback, not just tx undo) the logs array will be empty. Ignore this.
				if (bMmr && !bic.m_vLogs.empty())
					bic.m_vLogs.pop_back();
			}
		}
		break;

		case RecoveryTag::Recharge:
		{
			assert(bic.m_Temporary);
			der & bic.m_ChargePerBlock;
		}
		break;

		default:
			{
				der & key;
				auto& e = bic.m_Storage.get_Var(key);

				IPbftHandler* pPbft = (Rules::Consensus::Pbft == Rules::get().m_Consensus) ? bic.m_Proc.get_PbftHandler() : nullptr;

				if (RecoveryTag::Delete == nTag)
				{
					if (pPbft)
						pPbft->OnContractVarChange(key, Blob(), false);

					bic.m_Storage.DataDel(key, e.m_Data);
					e.m_Data.clear();
				}
				else
				{
					ByteBuffer data;
					der & data;

					if (pPbft)
						pPbft->OnContractVarChange(key, data, false);

					if (RecoveryTag::Insert == nTag)
						bic.m_Storage.DataInsert(key, data);
					else
					{
						if (RecoveryTag::Update != nTag)
							OnCorrupted();
						bic.m_Storage.DataUpdate(key, data, e.m_Data);
					}

					e.m_Data.swap(data);
				}

			}
		}
	}
}

void NodeProcessor::BlockInterpretCtx::BvmProcessor::CopyExtraInfoSigs()
{
	assert(m_Bic.m_pvC);
	auto& vec = *m_Bic.m_pvC; // alias

	assert(m_iCurrentInvokeExtraInfo <= vec.size());
	ContractInvokeExtraInfo& x = vec[m_iCurrentInvokeExtraInfo - 1];

	assert(m_iSig0 <= m_vSigs.size());
	if (m_iSig0 < m_vSigs.size())
	{
		x.m_vSigs.reserve(x.m_vSigs.size() + m_vSigs.size() - m_iSig0);
		while (m_iSig0 < m_vSigs.size())
			x.m_vSigs.push_back(m_vSigs[m_iSig0++]);
	}
}

void NodeProcessor::BlockInterpretCtx::BvmProcessor::CallFar(const bvm2::ContractID& cid, uint32_t iMethod, Wasm::Word pArgs, uint32_t nArgs, uint32_t nFlags)
{
	bvm2::ProcessorContract::CallFar(cid, iMethod, pArgs, nArgs, nFlags);

	if (m_Bic.m_pvC)
	{
		auto& vec = *m_Bic.m_pvC; // alias

		uint32_t iParent = 0;
		assert(!m_FarCalls.m_Stack.empty());
		if (m_FarCalls.m_Stack.size() > 1)
		{
			iParent = m_iCurrentInvokeExtraInfo;
			CopyExtraInfoSigs();
		}

		ContractInvokeExtraInfo& x = vec.emplace_back();
		x.m_iParent = iParent;
		x.m_NumNested = 0;
		m_iCurrentInvokeExtraInfo = static_cast<uint32_t>(vec.size());

		assert(m_pFundsIO);
		m_pFundsIO->m_Map.swap(x.m_FundsIO.m_Map);

		x.m_Cid = m_FarCalls.m_Stack.back().m_Cid; // may be different from passed cid, if inheriting context

		// estimate the args size (roughly). Currently support only stack memory type (for in-context calls theoretically this may be any memory type)
		Wasm::Word nArgsOffs = pArgs ^ Wasm::MemoryType::Stack;
		
		Blob args;
		if (nArgsOffs < m_Stack.m_BytesMax)
		{
			args.n = (bvm2::CallFarFlags::InheritContext & nFlags) ?  (m_Stack.m_BytesMax - nArgsOffs) : nArgs;
			args.p = reinterpret_cast<uint8_t*>(m_Stack.m_pPtr) + nArgsOffs;
		}
		else
			ZeroObject(args);
	
		bvm2::ShaderID sid;
		bvm2::get_ShaderID(sid, m_Code); // code should be intact, contract didn't get control yet

		ParseExtraInfo(x, sid, iMethod, args);

		// skip args for inherited-context calls, we don't know the size, only the high bound. No need to save all this.
		if (bvm2::CallFarFlags::InheritContext & nFlags)
			args.n = 0;

		x.SetUnk(iMethod, args, &sid);
	}
}

void NodeProcessor::ContractInvokeExtraInfoBase::SetUnk(uint32_t iMethod, const Blob& args, const ECC::uintBig* pSid)
{
	m_iMethod = iMethod;
	if (m_sParsed.empty())
	{
		args.Export(m_Args);
		if (pSid)
			m_Sid.reset(*pSid);
	}
}

void NodeProcessor::BlockInterpretCtx::BvmProcessor::OnRet(Wasm::Word nRetAddr)
{
	bvm2::ProcessorContract::OnRet(nRetAddr);

	if (m_Bic.m_pvC && !nRetAddr)
	{
		auto& vec = *m_Bic.m_pvC; // alias
		CopyExtraInfoSigs();

		assert(m_iCurrentInvokeExtraInfo <= vec.size());
		auto& x = vec[m_iCurrentInvokeExtraInfo - 1];

		m_iCurrentInvokeExtraInfo = x.m_iParent;
		ContractInvokeExtraInfo* pParent = x.m_iParent ? &vec[x.m_iParent - 1] : nullptr;
		if (pParent)
			pParent->m_NumNested += x.m_NumNested + 1;

		if (x.m_FundsIO.m_Map.empty())
			x.m_FundsIO = *m_pFundsIO; // save it
		else
		{
			m_pFundsIO->m_Map.swap(x.m_FundsIO.m_Map); // our + nested

			// merge
			for (auto it = x.m_FundsIO.m_Map.begin(); x.m_FundsIO.m_Map.end() != it; it++)
				m_pFundsIO->Add(it->second, it->first);
		}
	}
}

} // namespace beam
