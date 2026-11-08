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
#include "core/consensus/tx_kernel.h"

namespace beam
{
	struct DependentContext
	{
		static void get_Ancestor(Merkle::Hash& hvRes, const Merkle::Hash& hvParent, const Merkle::Hash& hvTx)
		{
			ECC::Hash::Processor()
				<< "dep.tx"
				<< hvParent
				<< hvTx
				>> hvRes;
		}
	};

	struct TxBase
	{
		class Context;
		static int CmpInOut(const Input&, const Output&);

		enum class Kind {
			Tx,
			Block,
			SparseBlock,
		};

		struct IReader
		{
			typedef std::unique_ptr<IReader> Ptr;

			// during iterations those pointers are guaranteed to be valid during at least 1 consequent iteration
			const Input* m_pUtxoIn;
			const Output* m_pUtxoOut;
			const TxKernel* m_pKernel;

			virtual ~IReader() {}
			virtual void Clone(Ptr&) = 0;
			virtual void Reset() = 0;
			// For all the following methods: the returned pointer should be valid during at least 2 consequent calls!
			virtual void NextUtxoIn() = 0;
			virtual void NextUtxoOut() = 0;
			virtual void NextKernel() = 0;

			void Compare(IReader&& rOther, bool& bICover, bool& bOtherCovers);
			size_t get_SizeNetto(); // account only for elements. Ignore offset and array sizes
			void AddStats(TxStats&);
		};

		struct IWriter
		{
			virtual ~IWriter() {}
			virtual void Write(const Input&) = 0;
			virtual void Write(const Output&) = 0;
			virtual void Write(const TxKernel&) = 0;

			void Dump(IReader&&);
			bool Combine(IReader** ppR, int nR, const volatile bool& bStop); // combine consequent blocks, merge-sort and delete consumed outputs
			// returns false if aborted
			bool Combine(IReader&& r0, IReader&& r1, const volatile bool& bStop);
		};

		static void Fail_Order();
		static void Fail_Signature();

		ECC::Scalar m_Offset;
	};

	struct TxVectors
	{
		struct Perishable
		{
			std::vector<Input::Ptr> m_vInputs;
			std::vector<Output::Ptr> m_vOutputs;
			size_t NormalizeP(); // w.r.t. the standard, delete spent outputs. Returns the num deleted
		};

		struct Eternal
		{
			std::vector<TxKernel::Ptr> m_vKernels;
			void NormalizeE();
		};

		class Reader :public TxBase::IReader {
			size_t m_pIdx[3];
		public:
			const Perishable& m_P;
			const Eternal& m_E;
			Reader(const Perishable& p, const Eternal& e) :m_P(p) ,m_E(e) {}
			// IReader
			void Clone(Ptr&) override;
			void Reset() override;
			void NextUtxoIn() override;
			void NextUtxoOut() override;
			void NextKernel() override;
		};

		struct Writer :public TxBase::IWriter
		{
			Perishable& m_P;
			Eternal& m_E;
			Writer(Perishable& p, Eternal& e) :m_P(p), m_E(e) {}

			void Write(const Input&) override;
			void Write(const Output&) override;
			void Write(const TxKernel&) override;
		};

		struct Full
			:public TxVectors::Perishable
			,public TxVectors::Eternal
		{
			Reader get_Reader() const {
				return Reader(*this, *this);
			}

			size_t Normalize();
			bool IsEmpty() const;

			void MoveInto(Full& trg);
		};
	};

	struct Transaction
		:public TxBase
		,public TxVectors::Full
	{
		typedef std::shared_ptr<Transaction> Ptr;

		void TestValid(Context&) const; // Explicit fees are considered "lost" in the transactions (i.e. would be collected by the miner)
		bool IsValid(Context&, std::string* psErr = nullptr) const;

		typedef uintBig_t<ECC::nBytes> KeyType; // key len for map of transactions. Can actually be less than 256 bits.

		void get_Key(KeyType&) const;

		struct FeeSettings
		{
			Amount m_Output;
			Amount m_Kernel; // nested kernels are accounted too
			Amount m_ShieldedInputTotal; // including 1 kernel price
			Amount m_ShieldedOutputTotal; // including 1 kernel price
			Amount m_Default; // for std tx

			struct Bvm {
				Amount m_ChargeUnitPrice;
				Amount m_Minimum;
				Amount m_ExtraBytePrice;
				uint32_t m_ExtraSizeFree;
			} m_Bvm;

			static const FeeSettings& get(Height);

			Amount Calculate(const Transaction&) const;
			Amount Calculate(const TxStats&) const;
			Amount CalculateForBvm(const TxStats&, uint32_t nBvmCharge) const;

			Amount get_DefaultStd() const;
			Amount get_DefaultShieldedOut(uint32_t nNumShieldedOutputs = 1) const;
		};
	};

	class TxBase::Context
	{
		bool ShouldVerify(uint32_t& iV) const;
		void TestAbort() const;
		void TestHeightNotEmpty() const;
		void HandleElementHeightStrict(const HeightRange&);

	public:
		// Tests the validity of all the components, overall arithmetics, and the lexicographical order of the components.
		// Determines the min/max block height that the transaction can fit, wrt component heights and maturity policies
		// Does *not* check the existence of the input UTXOs
		//
		// Validation formula
		//
		// Sum(Input UTXOs) = Sum(Output UTXOs) + Sum(Output Kernels.Excess) + m_Offset*G [ + Sum(Fee)*H ]
		//
		// For transaction validation fees are considered as implicit outputs (i.e. Sum(Fee)*H should be added for the right equation side)
		//
		// For a block validation Fees are not accounted for, since they are consumed by new outputs injected by the miner.
		// However Each block contains extra outputs (coinbase) for block closure, which should be subtracted from the outputs for sum validation.
		//
		// Define: Sigma = Sum(Output UTXOs) - Sum(Input UTXOs) + Sum(Output Kernels.Excess) + m_Offset*G
		// In other words Sigma = <all outputs> - <all inputs>
		// Sigma is either zero or -Sum(Fee)*H, depending on what we validate

		struct Params
		{
			TxBase::Kind m_Kind;

			// for multi-tasking, parallel verification
			uint32_t m_nVerifiers;
			volatile bool* m_pAbort;

			Params(); // defaults
		};

		Params m_Params;

		ECC::Point::Native m_Sigma;
		TxStats m_Stats;
		HeightRange m_Height;

		uint32_t m_iVerifier;

		Context()
		{
			Reset();
		}

		void Reset();

		void ValidateAndSummarizeStrict(const TxBase&, IReader&&);
		bool ValidateAndSummarize(const TxBase&, IReader&&, std::string* psErr = nullptr);
		void MergeStrict(const Context&);

		// hi-level functions, should be used after all parts were validated and merged
		void TestSigma() const;
	};
}
