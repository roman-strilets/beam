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

#pragma once

#include "processor_common.h"

namespace beam {

struct NodeProcessor::BlockInterpretCtx
{
	NodeProcessor& m_Proc;
	Height m_Height;
	uint32_t m_nKrnIdx = 0;
	bool m_Fwd;

	bool m_AlreadyValidated = false; // Block/tx already validated, i.e. this is not the 1st invocation (reorgs, block generation multi-pass, etc.)
	bool m_Temporary = false; // Interpretation will be followed by 'undo', try to avoid heavy state changes (use mem vars whenever applicable)
	bool m_SkipDefinition = false; // no need to calculate the full definition (i.e. not generating/interpreting a block), MMR updates and etc. can be omitted
	bool m_LimitExceeded = false;
	bool m_TxValidation = false; // tx or block
	bool m_DependentCtxSet = false;
	bool m_SkipInOuts = false;
	uint8_t m_TxStatus = proto::TxStatus::Unspecified;
	std::ostream* m_pTxErrorInfo = nullptr;

	std::vector<InputAux> m_vInpAux;

	uint32_t m_ShieldedIns = 0;
	uint32_t m_ShieldedOuts = 0;
	Asset::ID m_AidMax = static_cast<Asset::ID>(-1); // last valid Asset ID

	uint32_t m_ContractLogs = 0;
	std::vector<Merkle::Hash> m_vLogs;

	ByteBuffer m_Rollback;

	Merkle::Hash m_hvDependentCtx;

	// Contract signature check, saved to run later in the verification threads
	struct ContractSig
	{
		ECC::Hash::Processor m_Hp;
		std::vector<ECC::Point> m_vSigs;
		FundsChangeMap m_FundsIO;
		ECC::Point m_Commitment;
		ECC::Signature m_Signature;
		bvm2::ContractID m_Cid;
		uint32_t m_iMethod;

		void Check(); // throws on failure
	};

	// If set, the contract signatures are collected here instead of being checked during the interpretation.
	// The caller must check them before the block is considered valid.
	std::vector<ContractSig>* m_pvContractSigs = nullptr;

	struct Ser
		:public Serializer
	{
		typedef uintBigFor<uint32_t>::Type Marker;

		BlockInterpretCtx& m_This;
		size_t m_Pos;

		Ser(BlockInterpretCtx&);
		~Ser();
	};

	struct Der
		:public Deserializer
	{
		Der(BlockInterpretCtx&);
	private:
		void SetBwd(ByteBuffer&, uint32_t nPortion);
	};

	BlobMap::Set m_Dups; // mirrors 'unique' DB table in temporary mode

	typedef std::multiset<Blob> BlobPtrSet; // like BlobMap, but buffers are not allocated/copied
	BlobPtrSet m_KrnIDs; // mirrors kernel ID DB table in temporary mode

	struct VmProcessorBase
	{
		BlockInterpretCtx& m_Bic;

		VmProcessorBase(BlockInterpretCtx&);

		struct RecoveryTag {
			typedef uint8_t Type;
			static const Type Terminator = 0;
			static const Type Insert = 1;
			static const Type Update = 2;
			static const Type Delete = 3;
			static const Type AssetCreate = 4;
			static const Type AssetEmit = 5;
			static const Type AssetDestroy = 6;
			static const Type Log = 7;
			static const Type Recharge = 8;
		};

		void UpdChargeWithRecovery(uint32_t);
	};

	struct BvmProcessor
		:public VmProcessorBase
		,public bvm2::ProcessorContract
	{

		uint32_t m_AssetEvtSubIdx = 0;

		using VmProcessorBase::VmProcessorBase;

		bvm2::Storage::IBase& get_Storage() override {
			return m_Bic.m_Storage;
		}

		bool get_AssetInfo(Asset::Full&) override;

		Height get_Height() override;
		bool get_HdrAt(Block::SystemState::Full&, Height) override;

		Asset::ID AssetCreate(const Asset::Metadata&, const PeerID&, Amount& valDeposit) override;
		bool AssetEmit(Asset::ID, const PeerID&, AmountSigned) override;
		bool AssetDestroy(Asset::ID, const PeerID&, Amount& valDeposit) override;

		bool EnsureNoVars(const bvm2::ContractID&);
		static bool IsOwnedVar(const bvm2::ContractID&, const Blob& key);

		bool Invoke(const bvm2::ContractID&, uint32_t iMethod, const TxKernelContractControl&);

		void ParseExtraInfo(ContractInvokeExtraInfo&, const bvm2::ShaderID&, uint32_t iMethod, const Blob& args);

		void CallFar(const bvm2::ContractID&, uint32_t iMethod, Wasm::Word pArgs, uint32_t nArgs, uint32_t nFlags) override;
		void OnRet(Wasm::Word nRetAddr) override;

		uint32_t m_iCurrentInvokeExtraInfo = 0;
		uint32_t m_iSig0 = 0;

		void CopyExtraInfoSigs();
	};


	struct MyEvmProcessor
		:public VmProcessorBase
		,public Evm::Processor
	{
		Height get_Height() override;
		bool get_BlockHeader(BlockHeader&, Height) override;

		Evm::Processor::Account& LoadAccount(const Evm::Address& addr) override;
		Evm::Processor::Account::Slot& LoadSlot(Evm::Processor::Account&, const Evm::Word&) override;

		struct AccountWrap :public Evm::Processor::Account
		{
			BlobMap::Entry* m_pEntry;
			BlobMap::Entry* m_pContractCode;
			uint64_t m_Nonce;
		};

		struct SlotWrap :public Evm::Processor::Account::Slot {
			BlobMap::Entry* m_pEntry;
		};

		struct AccountKey {
			Evm::Address m_Addr;
			Evm::Word m_Var;
		};

		AccountWrap& LoadAccountInternal(const Evm::Address& addr);

		using VmProcessorBase::VmProcessorBase;
		void InvokeGuarded(const TxKernelEvmInvoke&);
		void CommitChanges();
	};

	uint32_t m_ChargePerBlock = bvm2::Limits::BlockCharge;

	struct Storage
		:public bvm2::Storage::IBase
	{
		BlobMap::Set m_Vars;
		BlobMap::Entry& get_Var(const Blob& key);

		void LoadVar(const Blob&, Blob& res) override;
		void LoadVarEx(Blob& key, Blob& res, bool bExact, bool bBigger) override;
		uint32_t SaveVar(const Blob&, const Blob& val) override;
		uint32_t OnLog(const Blob&, const Blob& val) override;

		BlobMap::Entry* FindVarEx(const Blob& key, bool bExact, bool bBigger);

		void DataInsert(const Blob& key, const Blob&);
		void DataUpdate(const Blob& key, const Blob& val, const Blob& valOld);
		void DataDel(const Blob& key, const Blob& valOld);

		void DataToggleTree(const Blob& key, const Blob&, bool bAdd);
		void DataSaveWithRecovery(BlobMap::Entry&, const Blob&);

		void PutVarsTerminator();
		void UndoVars();

		IMPLEMENT_GET_PARENT_OBJ(BlockInterpretCtx, m_Storage)
	} m_Storage;


	std::vector<ContractInvokeExtraInfo>* m_pvC = nullptr;

	BlockInterpretCtx(NodeProcessor& proc, Height h, bool bFwd)
		:m_Proc(proc)
		,m_Height(h)
		,m_Fwd(bFwd)
	{
		m_AidMax = m_Proc.get_AidMax();
	}

	bool ValidateAssetRange(const Asset::Proof::Ptr& p) const
	{
		if (!p || (p->m_Begin <= m_AidMax))
			return true;

		if (m_pTxErrorInfo)
			*m_pTxErrorInfo << "asset range oob " << p->m_Begin << ", limit=" << m_AidMax;

		return false;

	}

	void AddKrnInfo(Serializer&);

	static uint64_t get_AssetEvtIdx(uint32_t nKrnIdx, uint32_t nSubIdx) {
		return (static_cast<uint64_t>(nKrnIdx) << 32) | nSubIdx;
	}

	void AssetEvtInsert(NodeDB::AssetEvt&, uint32_t nSubIdx);

	struct ChangesFlushGlobal
	{
		TxoID m_ShieldedInputs;
		ChangesFlushGlobal(NodeProcessor& p)
		{
			m_ShieldedInputs = p.get_ShieldedInputs();
		}

		void Do(NodeProcessor& p)
		{
			TxoID val = p.get_ShieldedInputs();
			if (val != m_ShieldedInputs)
				p.m_DB.ParamIntSet(NodeDB::ParamID::ShieldedInputs, val);
		}
	};

	struct ChangesFlush
		:public ChangesFlushGlobal
	{
		TxoID m_ShieldedOutps;
		ChangesFlush(NodeProcessor& p)
			:ChangesFlushGlobal(p)
		{
			m_ShieldedOutps = p.m_Extra.m_ShieldedOutputs;
		}

		void Do(NodeProcessor& p, Height h)
		{
			if (p.m_Extra.m_ShieldedOutputs != m_ShieldedOutps)
				p.m_DB.ShieldedOutpSet(h, p.m_Extra.m_ShieldedOutputs);

			ChangesFlushGlobal::Do(p);
		}
	};

	template <typename T>
	bool HandleElementVecFwd(const T& vec, size_t& n)
	{
		assert(m_Fwd);

		for (; n < vec.size(); n++)
			if (!HandleBlockElement(*vec[n]))
				return false;

		return true;
	}

	template <typename T>
	void HandleElementVecBwd(const T& vec, size_t n)
	{
		assert(!m_Fwd);

		while (n--)
			if (!HandleBlockElement(*vec[n]))
				OnCorrupted();
	}

	bool HandleBlockElement(const Input&);
	bool HandleBlockElement(const Output&);
	bool HandleBlockElement(const TxKernel&);

	Height FindVisibleKernel(const Merkle::Hash& id);
	void ManageKrnID(const TxKernel&);

	bool HandleKernel(const TxKernel&);
	bool HandleKernelTypeAny(const TxKernel&);

#define THE_MACRO(id, name) bool HandleKernelType(const TxKernel##name&);
	BeamKernelsAll(THE_MACRO)
#undef THE_MACRO

	bool ValidateUniqueNoDup(const Blob& key, const Blob* pVal);

	bool HandleAssetCreate(const PeerID&, const ContractID*, const Asset::Metadata&, Asset::ID&, Amount& valDeposit, uint32_t nSubIdx = 0);
	bool HandleAssetEmit(const PeerID&, Asset::ID, AmountSigned, uint32_t nSubIdx = 0);
	bool HandleAssetEmitLocal(const PeerID&, Asset::ID, AmountSigned, uint32_t nSubIdx);
	bool HandleAssetEmitForeign(const PeerID&, Asset::ID, AmountSigned, uint32_t nSubIdx);
	bool HandleAssetDestroy(const PeerID&, const ContractID*, Asset::ID, Amount& valDeposit, bool bDepositCheck, uint32_t nSubIdx = 0);
	bool HandleAssetDestroy2(const PeerID&, const ContractID*, Asset::ID, Amount& valDeposit, bool bDepositCheck, uint32_t nSubIdx);

	bool HandleValidatedTx(const TxVectors::Full&);
	bool HandleValidatedBlock(const Block::Body&, IPbftHandler*);
	bool HandlePbftReward(const std::vector<TxKernel::Ptr>& v, IPbftHandler*);

	size_t GenerateNewBlockInternal(BlockContext&);
	void GenerateNewHdr(BlockContext&);
};

struct NodeProcessor::MyRecognizer
{
	struct Handler
		:public Recognizer::IHandler
	{
		NodeProcessor& m_Proc;
		Handler(NodeProcessor& proc) :m_Proc(proc) {}


		void OnDummy(const CoinID& cid, Height h) override
		{
			m_Proc.OnDummy(cid, h);
		}

		void OnEvent(Height h, const proto::Event::Base& evt) override
		{
			m_Proc.OnEvent(h, evt);
		}

		void AssetEvtsGetStrict(NodeDB::AssetEvt& event, Height h, uint32_t nKrnIdx) override
		{
			NodeDB::WalkerAssetEvt wlk;
			m_Proc.m_DB.AssetEvtsGetStrict(wlk, h, BlockInterpretCtx::get_AssetEvtIdx(nKrnIdx, 0));
			event = Cast::Down<NodeDB::AssetEvt>(wlk);
		}

		void InsertEvent(const HeightPos& pos, const Blob& b, const Blob& key) override
		{
			m_Proc.m_DB.InsertEvent(m_pAccount->m_iAccount, pos, b, key);
		}

		bool FindEvents(const Blob& key, Recognizer::IEventHandler& h) override
		{
			NodeDB::WalkerEvent wlk;
			for (m_Proc.m_DB.FindEvents(wlk, m_pAccount->m_iAccount, key); wlk.MoveNext(); )
			{
				if (h.OnEvent(wlk.m_Pos.m_Height, wlk.m_Body))
					return true;
			}

			return false;
		}

	} m_Handler;

	Recognizer m_Recognizer;

	MyRecognizer(NodeProcessor& x)
		:m_Handler(x)
		,m_Recognizer(m_Handler, x.m_Extra)
	{
	}
};

struct NodeProcessor::DependentContextSwitch
{
	typedef std::vector<const TxPool::Dependent::Element*> Vec;

	BlockInterpretCtx& m_Bic;
	Vec m_vec;
	uint32_t m_Applied;

	DependentContextSwitch(BlockInterpretCtx& bic)
		:m_Bic(bic)
		,m_Applied(0)
	{
	}

	~DependentContextSwitch()
	{
		m_Bic.m_Fwd = false;
		while (m_Applied)
			m_Bic.HandleValidatedTx(*m_vec[--m_Applied]->m_pValue);
	}

	static void Convert(Vec& vec, const TxPool::Dependent::Element* pTop)
	{
		uint32_t n = 0;
		for (auto p = pTop; p; p = p->m_pParent)
			n++;

		vec.resize(n);
		for (auto p = pTop; p; p = p->m_pParent)
			vec[--n] = p;
	}

	bool Apply(const TxPool::Dependent::Element* pTop) 
	{
		Convert(m_vec, pTop);

		assert(m_Bic.m_Fwd && !m_Applied);

		for (; m_Applied < m_vec.size(); m_Applied++)
			if (!m_Bic.HandleValidatedTx(*m_vec[m_Applied]->m_pValue))
				return false;

		return true;
	}
};

} // namespace beam
