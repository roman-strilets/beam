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
#include "core/consensus/txo.h"

namespace beam
{
#define BeamKernelsAll(macro) \
	macro(1, Std) \
	macro(2, AssetEmit) \
	macro(3, ShieldedOutput) \
	macro(4, ShieldedInput) \
	macro(5, AssetCreate) \
	macro(6, AssetDestroy) \
	macro(7, ContractCreate) \
	macro(8, ContractInvoke) \
	macro(9, EvmInvoke)

#define THE_MACRO(id, name) struct TxKernel##name;
	BeamKernelsAll(THE_MACRO)
#undef THE_MACRO

	template <typename T>
	struct Lazy
	{
		T m_Value;
		bool m_Valid = false;

		const T& get() const
		{
			assert(m_Valid);
			return m_Value;
		}

		void Invalidate()
		{
			m_Valid = false;
		}
	};

	struct TxKernel
	{
		typedef std::unique_ptr<TxKernel> Ptr;

		struct Subtype
		{
			enum Enum {
#define THE_MACRO(id, name) name = id,
				BeamKernelsAll(THE_MACRO)
#undef THE_MACRO
				count
			};
		};

		Amount			m_Fee;			// can be 0 (for instance for coinbase transactions)
		HeightRange		m_Height;
		bool			m_CanEmbed;

		TxKernel()
			:m_Fee(0)
			,m_CanEmbed(false)
		{}

		mutable Lazy<Merkle::Hash> m_Lazy_ID; // not serialized

		std::vector<Ptr> m_vNested; // nested kernels, included in the signature.

		static const uint32_t s_MaxRecursionDepth = 2;

		static void TestRecursion(uint32_t n)
		{
			if (n > s_MaxRecursionDepth)
				throw std::runtime_error("recursion too deep");
		}

		virtual ~TxKernel() {}
		virtual Subtype::Enum get_Subtype() const = 0;
		virtual void CalculateID() const = 0;
		virtual bool HasNonStd() const = 0;
		virtual void TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent = nullptr) const = 0;
		virtual void AddStats(TxStats&) const; // including self and nested
		virtual int cmp_Subtype(const TxKernel&) const;
		virtual void Clone(Ptr&) const = 0;

		void EnsureID() const
		{
			if (!m_Lazy_ID.m_Valid)
				CalculateID();
		}

		const Merkle::Hash& get_ID() const
		{
			EnsureID();
			return m_Lazy_ID.get();
		}

		bool IsValid(Height hScheme, std::string* psErr = nullptr) const;

		HeightRange get_EffectiveHeightRange() const;

		// simplified version AddStats()
		void AddFees(AmountBig::Number&) const;
		static void AddFees(AmountBig::Number&, const std::vector<Ptr>&);

		struct LongProof; // legacy

		int cmp(const TxKernel&) const;
		COMPARISON_VIA_CMP


		struct IWalker
		{
			uint32_t m_nKrnIdx = 0;

			virtual bool OnKrn(const TxKernel&) = 0;

			bool Process(const std::vector<TxKernel::Ptr>&);
			bool Process(const TxKernel&);
		};

		struct Checkpoint :public Exc::Checkpoint {

			const TxKernel& m_Krn;
			Checkpoint(const TxKernel& krn) :m_Krn(krn) {}

			void Dump(std::ostream& os) override {
				os << "Kernel ID=" << m_Krn.get_ID() << ", type=" << (uint32_t) m_Krn.get_Subtype();
			}
		};

#define THE_MACRO(id, name) \
		TxKernel##name & CastTo_##name() { \
			assert(get_Subtype() == Subtype::name); \
			return Cast::Up<TxKernel##name>(*this); \
		} \
		const TxKernel##name & CastTo_##name() const { \
			return Cast::NotConst(*this).CastTo_##name(); \
		}

		BeamKernelsAll(THE_MACRO)
#undef THE_MACRO

	protected:
		void HashBase(ECC::Hash::Processor&) const;
		void HashNested(ECC::Hash::Processor&) const;
		void CopyFrom(const TxKernel&);
		void TestValidBase(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent, ECC::Point::Native* pComm = nullptr) const;
	private:
		void operator = (const TxKernel&);
	};

	struct TxKernelStd
		:public TxKernel
	{
		typedef std::unique_ptr<TxKernelStd> Ptr;

		ECC::Point		m_Commitment;	// aggregated, including nested kernels
		ECC::Signature	m_Signature;	// For the whole body, including nested kernels

		struct HashLock
		{
			ECC::Hash::Value m_Value;
			bool m_IsImage = false; // not serialized. Used only internally to get the kernel ID after it'd be substituted

			const ECC::Hash::Value& get_Image(ECC::Hash::Value& hv) const;

			int cmp(const HashLock&) const;
			COMPARISON_VIA_CMP
		};

		std::unique_ptr<HashLock> m_pHashLock;

		struct RelativeLock
		{
			Merkle::Hash m_ID;
			Height m_LockHeight;

			int cmp(const RelativeLock&) const;
			COMPARISON_VIA_CMP
		};

		std::unique_ptr<RelativeLock> m_pRelativeLock;

		virtual ~TxKernelStd() {}
		Subtype::Enum get_Subtype() const override;
		void CalculateID() const override;
		bool HasNonStd() const override;
		void TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent = nullptr) const override;
		int cmp_Subtype(const TxKernel&) const override;
		void Clone(TxKernel::Ptr&) const override;

		void Sign(const ECC::Scalar::Native&); // suitable for aux kernels, created by single party
	};

	struct TxKernelNonStd
		:public TxKernel
	{
		mutable Lazy<Merkle::Hash> m_Lazy_Msg; // message to sign, diffetent from ID

		void CalculateMsg() const;

		void EnsureMsg() const
		{
			if (!m_Lazy_Msg.m_Valid)
				CalculateMsg();
		}

		const Merkle::Hash& get_Msg() const
		{
			EnsureMsg();
			return m_Lazy_Msg.get();
		}

		void CalculateID() const override;
		bool HasNonStd() const override;

	protected:
		virtual void HashSelfForMsg(ECC::Hash::Processor&) const = 0;
		virtual void HashSelfForID(ECC::Hash::Processor&) const = 0;
		void CopyFrom(const TxKernelNonStd&);
		void AddStats(TxStats&) const override;
	};

	struct TxKernelAssetControl
		:public TxKernelNonStd
	{
		PeerID m_Owner;

		ECC::Point m_Commitment;	// aggregated, including nested kernels
		ECC::SignatureGeneralized<1> m_Signature;

		void Sign_(const ECC::Scalar::Native& sk, const ECC::Scalar::Native& skAsset);
		void Sign(const ECC::Scalar::Native& sk, Key::IKdf&, const Asset::Metadata&);

		void get_Sk(ECC::Scalar::Native&, Key::IKdf&); // pseudo-random sk for this kernel

	protected:
		void CopyFrom(const TxKernelAssetControl&);
		void TestValidAssetCtl(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent) const;
		void HashSelfForMsg(ECC::Hash::Processor&) const override;
		void HashSelfForID(ECC::Hash::Processor&) const override;
	};

	struct TxKernelAssetEmit
		:public TxKernelAssetControl
	{
		typedef std::unique_ptr<TxKernelAssetEmit> Ptr;

		Asset::ID m_AssetID;
		AmountSigned m_Value;
		TxKernelAssetEmit() : m_AssetID(Asset::s_InvalidID), m_Value(0) {}

		virtual ~TxKernelAssetEmit() {}
		Subtype::Enum get_Subtype() const override;
		void TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent = nullptr) const override;
		void Clone(TxKernel::Ptr&) const override;
	protected:
		void HashSelfForMsg(ECC::Hash::Processor&) const override;
	};

	struct TxKernelAssetCreate
		:public TxKernelAssetControl
	{
		typedef std::unique_ptr<TxKernelAssetCreate> Ptr;

		Asset::Metadata m_MetaData;

		void Sign(const ECC::Scalar::Native& sk, Key::IKdf&);

		virtual ~TxKernelAssetCreate() {}
		Subtype::Enum get_Subtype() const override;
		void TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent = nullptr) const override;
		void Clone(TxKernel::Ptr&) const override;
	protected:
		void HashSelfForMsg(ECC::Hash::Processor&) const override;
	};

	struct TxKernelAssetDestroy
		:public TxKernelAssetControl
	{
		typedef std::unique_ptr<TxKernelAssetDestroy> Ptr;

		Asset::ID m_AssetID;
		Amount m_Deposit = 0;
        TxKernelAssetDestroy(): m_AssetID(Asset::s_InvalidID) {}

		bool IsCustomDeposit() const;
		Amount get_Deposit() const;

		virtual ~TxKernelAssetDestroy() {}
		Subtype::Enum get_Subtype() const override;
		void TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent = nullptr) const override;
		void Clone(TxKernel::Ptr&) const override;
	protected:
		void HashSelfForMsg(ECC::Hash::Processor&) const override;
	};

	struct TxKernelShieldedOutput
		:public TxKernelNonStd
	{
		typedef std::unique_ptr<TxKernelShieldedOutput> Ptr;

		ShieldedTxo m_Txo;

		virtual ~TxKernelShieldedOutput() {}
		Subtype::Enum get_Subtype() const override;
		void TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent = nullptr) const override;
		void AddStats(TxStats&) const override;
		void Clone(TxKernel::Ptr&) const override;
	protected:
		void HashSelfForMsg(ECC::Hash::Processor&) const override;
		void HashSelfForID(ECC::Hash::Processor&) const override;
	};

	struct TxKernelShieldedInput
		:public TxKernelNonStd
	{
		typedef std::unique_ptr<TxKernelShieldedInput> Ptr;

		TxoID m_WindowEnd; // ID of the 1st element outside the window
		Lelantus::Proof m_SpendProof;
		Asset::Proof::Ptr m_pAsset;

		struct NotSerialized {
			ECC::Hash::Value m_hvShieldedState;
		} m_NotSerialized;

		void Sign(Lelantus::Prover&, Asset::ID aids);

		virtual ~TxKernelShieldedInput() {}
		Subtype::Enum get_Subtype() const override;
		void TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent = nullptr) const override;
		void AddStats(TxStats&) const override;
		void Clone(TxKernel::Ptr&) const override;
	protected:
		void HashSelfForMsg(ECC::Hash::Processor&) const override;
		void HashSelfForID(ECC::Hash::Processor&) const override;
	};

	struct TxKernelContractControl
		:public TxKernelNonStd
	{
		ECC::Point m_Commitment; // arbitrary blinding factor + all the values consumed/emitted by the contract
		ECC::Signature m_Signature; // aggreagtedmulti-signature of the blinding factor + all the keys required by the contract

		ByteBuffer m_Args;
		bool m_Dependent = false;

		void TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent = nullptr) const override;
		void AddStats(TxStats&) const override;

		void Prepare(ECC::Hash::Processor&, const Merkle::Hash* pParentCtx) const;
		void Sign(const ECC::Scalar::Native*, uint32_t nKeys, const ECC::Point::Native& ptFunds, const Merkle::Hash* pParentCtx);

	protected:
		void CopyFrom(const TxKernelContractControl&);
		void HashSelfForMsg(ECC::Hash::Processor&) const override;
		void HashSelfForID(ECC::Hash::Processor&) const override;
	};

	struct TxKernelContractCreate
		:public TxKernelContractControl
	{
		ByteBuffer m_Data;

		typedef std::unique_ptr<TxKernelContractCreate> Ptr;

		virtual ~TxKernelContractCreate() {}
		Subtype::Enum get_Subtype() const override;
		void Clone(TxKernel::Ptr&) const override;
		void AddStats(TxStats&) const override;
	protected:
		void HashSelfForMsg(ECC::Hash::Processor&) const override;
	};

	struct TxKernelContractInvoke
		:public TxKernelContractControl
	{
		ContractID m_Cid;
		uint32_t m_iMethod;

		typedef std::unique_ptr<TxKernelContractInvoke> Ptr;

		virtual ~TxKernelContractInvoke() {}
		Subtype::Enum get_Subtype() const override;
		void Clone(TxKernel::Ptr&) const override;
	protected:
		void HashSelfForMsg(ECC::Hash::Processor&) const override;
	};

	struct TxKernelEvmInvoke
		:public TxKernelContractControl
	{
		Evm::Address m_From;
		Evm::Address m_To;
		uint64_t m_Nonce; // not necessary
		Evm::Word m_CallValue; // in wei
		AmountSigned m_Subsidy;

		typedef std::unique_ptr<TxKernelEvmInvoke> Ptr;

		void Sign(const ECC::Scalar::Native& skFrom, const ECC::Scalar::Native& skBlind);

		virtual ~TxKernelEvmInvoke() {}
		Subtype::Enum get_Subtype() const override;
		void Clone(TxKernel::Ptr&) const override;
		void TestValid(Height hScheme, ECC::Point::Native& exc, const TxKernel* pParent = nullptr) const override;
	protected:
		void HashSelfForMsg(ECC::Hash::Processor&) const override;
		void get_SubsidyCorrection(ECC::Point::Native& dst, bool isVerifying) const;
	};

	inline bool operator < (const TxKernel::Ptr& a, const TxKernel::Ptr& b) { return *a < *b; }
}
