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
#include "core/consensus/transaction.h"
#include "core/consensus/rules.h"
#include "core/consensus/difficulty.h"

namespace beam
{
	struct Block
	{
		// Different parts of the block are split into different structs, so that they can be manipulated (transferred, processed, saved and etc.) independently
		// For instance, there is no need to keep PoW (at least in SPV client) once it has been validated.

		struct Number {
			uint64_t v;
			Number() {}
			explicit Number(uint64_t val) :v(val) {}

			template <typename Archive>
			void serialize(Archive& ar) { ar & v; }

			bool operator < (const Number& x) const { return v < x.v; }
		};

		struct NumberRange
		{
			Number m_Min;
			Number m_Max;

			NumberRange() { Reset(); }

			void Reset();
			bool IsEmpty() const;
		};

		struct PoW
		{
			// equihash parameters. 
			// Parameters recommended by BTG are 144/5, to make it asic-resistant (~1GB average, spikes about 1.5GB). On CPU solve time about 1 minutes
			// The following are the parameters for testnet, to make it of similar size, and much faster solve time, to test concurrency and difficulty adjustment
			static const uint32_t N = 150;
			static const uint32_t K = 5;

			static const uint32_t nNumIndices		= 1 << K; // 32
			static const uint32_t nBitsPerIndex		= N / (K + 1) + 1; // 26

			static const uint32_t nSolutionBits		= nNumIndices * nBitsPerIndex; // 832 bits

			static_assert(!(nSolutionBits & 7), "PoW solution should be byte-aligned");
			static const uint32_t nSolutionBytes	= nSolutionBits >> 3; // 104 bytes

			std::array<uint8_t, nSolutionBytes>	m_Indices;

			typedef uintBig_t<8> NonceType;
			NonceType m_Nonce; // 8 bytes. The overall solution size is 96 bytes.
			Difficulty m_Difficulty;

			bool IsValid(const void* pInput, uint32_t nSizeInput, Height) const;

			using Cancel = std::function<bool(bool bRetrying)>;
			// Difficulty and Nonce must be initialized. During the solution it's incremented each time by 1.
			// returns false only if cancelled
			bool Solve(const void* pInput, uint32_t nSizeInput, Height, const Cancel& = [](bool) { return false; });

		private:
			struct Helper;
		};

		struct Pbft
		{
#pragma pack (push, 1)

			static const uint32_t s_MaxValidators = 96;

			struct QC
			{
				ECC::Signature m_Signature;
				Bitmask<s_MaxValidators> m_Mask;
			};

			struct HdrData {
				ECC::Hash::Value m_hvVsBoth; // hash(prev|next)
				uintBigFor<uint16_t>::Type m_Time_ms;
				QC m_QC;
				uint8_t m_Flags1;
				Difficulty m_Difficulty;

				struct Flags {
					static const uint8_t Empty = 1; // empty block, hence not final, likely to be reorged
				};
			};
#pragma pack (pop)
			static_assert(sizeof(HdrData) == sizeof(PoW), "");

			typedef PeerID Address;

			static void DeriveValidatorAddress(Key::IKdf&, Address&, ECC::Scalar::Native& sk);

			struct IValidatorSet
			{
				struct ITarget {
					virtual bool OnValidator(const Address&, uint64_t weight) = 0;
				};

				virtual bool EnumValidators(ITarget&) const = 0;
				virtual void get_Hash(Merkle::Hash&) const; // def implementation recalculates

				static bool IsMajorityReached(uint64_t wVoted, uint64_t wTotal, uint32_t nWhite);
				bool CheckQuorum(const Merkle::Hash&, const QC&) const;
			};

			struct ValidatorSet
				:public IValidatorSet
			{
				std::map<Address, uint64_t> m_map;

				bool EnumValidators(ITarget&) const override;
			};
		};


		static void get_HashContractVar(Merkle::Hash&, const Blob& key, const Blob& val);
		static void get_HashContractLog(Merkle::Hash&, const Blob& key, const Blob& val, uint32_t nPos);

		struct SystemState
		{
			struct ID
			{
				Merkle::Hash m_Hash;
				Number m_Number;

				template <typename Archive>
				void serialize(Archive& ar)
				{
					ar
						& m_Number
						& m_Hash;
				}

				int cmp(const ID&) const;
				COMPARISON_VIA_CMP

			};

			struct Evaluator
				:public Merkle::IEvaluator
			{
				Height m_Height;

				// The state Definition is defined as Hash[ History | Live ]
				// Before Fork2: Live = Utxos
				// Before Fork3: Live = Hash[ Utxos | Hash[Shielded | Assets] ]
				// Past Fork3:
				//		CSA = Hash[ Contracts | Hash[Shielded | Assets] ]
				//		KL = Hash[ Kernels | Logs ]
				//		Live = Hash[ KL | CSA ]

				bool get_Definition(Merkle::Hash&);
				void GenerateProof(); // same as above, except it's used for proof generation, and the resulting hash is not evaluated

				virtual bool get_History(Merkle::Hash&);
				virtual bool get_Live(Merkle::Hash&);
				virtual bool get_CSA(Merkle::Hash&);
				virtual bool get_KL(Merkle::Hash&);
				virtual bool get_Utxos(Merkle::Hash&);
				virtual bool get_Kernels(Merkle::Hash&);
				virtual bool get_Logs(Merkle::Hash&);
				virtual bool get_Shielded(Merkle::Hash&);
				virtual bool get_Assets(Merkle::Hash&);
				virtual bool get_Contracts(Merkle::Hash&);

				bool get_SA(Merkle::Hash&);
			};

			struct Sequence
			{
				struct Prefix {
					Number				m_Number;
					Merkle::Hash		m_Prev;			// explicit referebce to prev
					Difficulty::Raw		m_ChainWork;
				};

				struct Element
				{
					Merkle::Hash	m_Kernels; // Before Fork3: kernels (of this block only), after Fork3: Utxos
					Merkle::Hash	m_Definition;
					Timestamp		m_TimeStamp;
					PoW				m_PoW;

					uint64_t get_Timestamp_ms() const; // PBFT only
				};
			};

			struct Full
				:public Sequence::Prefix
				,public Sequence::Element
			{
				void SetFirst(const Sequence::Prefix&, const Element&);
				void SetNext(const Full&, const Element&);
				void MoveNext(const Element& x) { SetNext(*this, x); }

				void get_HashForPoW(Merkle::Hash&) const; // all except PoW
				void get_Hash(Merkle::Hash&) const; // all

				void get_ID(ID&) const; // with Block number
				void get_ID(HeightHash&) const; // with Height

				Height get_Height() const; // PoW: equals to Number, PoS can be heigher

				bool IsSane() const;
				bool IsValidPoW() const;
				bool IsValid() const {
					return IsSane() && IsValidPoW(); 
				}
                bool GeneratePoW(const PoW::Cancel& = [](bool) { return false; });

				// the most robust proof verification - verifies the whole proof structure
				bool IsValidProofState(const ID&, const Merkle::HardProof&) const;

				bool IsValidProofKernel(const TxKernel&, const TxKernel::LongProof&) const;
				bool IsValidProofKernel(const Merkle::Hash& hvID, const TxKernel::LongProof&) const;
				bool IsValidProofKernel(const Merkle::Hash& hvID, const Merkle::Proof&) const;

				bool IsValidProofLog(const Merkle::Hash& hvLog, const Merkle::Proof&) const;

				bool IsValidProofUtxo(const ECC::Point&, const Input::Proof&) const;
				bool IsValidProofShieldedOutp(const ShieldedTxo::DescriptionOutp&, const Merkle::Proof&) const;
				bool IsValidProofShieldedInp(const ShieldedTxo::DescriptionInp&, const Merkle::Proof&) const;
				bool IsValidProofAsset(const Asset::Full&, const Merkle::Proof&) const;
				bool IsValidProofContract(const Blob& key, const Blob& val, const Merkle::Proof&) const;

				int cmp(const Full&) const;
				COMPARISON_VIA_CMP

				bool IsNext(const Full& sNext) const;

			private:
				void get_HashInternal(Merkle::Hash&, bool bTotal) const;
				bool IsValidProofShielded(Merkle::Hash&, const Merkle::Proof&) const;

				struct ProofVerifier;
				struct ProofVerifierHard;
			};

			struct IHistory
			{
				// should provide access to some recent states of the active branch

				struct IWalker {
					virtual bool OnState(const Block::SystemState::Full&) = 0;
				};

				virtual bool Enum(IWalker&, const Height* pBelow) = 0;
				virtual bool get_At(Full&, Height) = 0;
				virtual void AddStates(const Full*, size_t nCount) = 0;
				virtual void DeleteFrom(Height) = 0;

				bool get_Tip(Full&); // zero-inits if no tip
			};

			struct HistoryMap
				:public IHistory
			{
				// simple impl
				std::map<Height, Full> m_Map;
				void ShrinkToWindow(Height dh);

				bool Enum(IWalker&, const Height* pBelow) override;
				bool get_At(Full&, Height) override;
				void AddStates(const Full*, size_t nCount) override;
				void DeleteFrom(Height) override;
			};

		};

		struct BodyBase
			:public TxBase
		{
			void ZeroInit();

			struct IMacroReader
				:public IReader
			{
				virtual void get_Start(BodyBase&, SystemState::Sequence::Prefix&) = 0;
				virtual bool get_NextHdr(SystemState::Sequence::Element&) = 0;
			};

			struct IMacroWriter
				:public IWriter
			{
				virtual void put_Start(const BodyBase&, const SystemState::Sequence::Prefix&) = 0;
				virtual void put_NextHdr(const SystemState::Sequence::Element&) = 0;

				bool CombineHdr(IMacroReader&& r0, IMacroReader&& r1, const volatile bool& bStop);
			};

			void Merge(const BodyBase& next);
		};

		struct Body
			:public BodyBase
			,public TxVectors::Full
		{
		};

		struct ChainWorkProof;

		struct Builder
		{
			ECC::Scalar::Native m_Offset; // the sign is opposite
			TxVectors::Full m_Txv;

			Key::Index m_SubIdx;
			Key::IKdf& m_Coin;
			Key::IPKdf& m_Tag;
			Height m_Height;

			Builder(Key::Index, Key::IKdf& coin, Key::IPKdf& tag, Height);

			void AddCoinbaseAndKrn(Amount fees);
			void AddCoinbaseAndKrn(Amount fees, Output::Ptr&, Output::Ptr&, TxKernel::Ptr&);
		};
	};

	std::ostream& operator << (std::ostream&, const Block::SystemState::ID&);

	struct TxKernel::LongProof
	{
		Merkle::Proof m_Inner;
		Block::SystemState::Full m_State;
		Merkle::HardProof m_Outer;

		bool empty() const { return !m_State.m_Number.v; }

		template <typename Archive>
		void serialize(Archive& ar)
		{
			ar
				& m_Inner
				& m_State
				& m_Outer;
		}
	};

	struct Block::ChainWorkProof
	{
		// Compressed consecutive states (likely to appear at the end)
		struct Heading {
			SystemState::Sequence::Prefix m_Prefix;
			std::vector<SystemState::Sequence::Element> m_vElements;
		} m_Heading;
		// other states
		std::vector<SystemState::Full> m_vArbitraryStates;
		// compressed proof
		Merkle::MultiProof m_Proof;
		// last node to go from History to Definition.
		Merkle::Hash m_hvRootLive;
		// crop thereshold. Off by default
		Difficulty::Raw m_LowerBound;

		struct ISource
		{
			virtual void get_StateAt(SystemState::Full&, const Difficulty::Raw&) = 0;
			virtual void get_Proof(Merkle::IProofBuilder&, Number) = 0;
		};

		ChainWorkProof()
		{
			ZeroInit();
		}

		void Reset();
		void Create(ISource&, const SystemState::Full& sRoot);
		bool IsValid(SystemState::Full* pTip = NULL) const;
		bool Crop(); // according to current bound
		bool Crop(const ChainWorkProof& src);
		bool IsEmpty() const { return m_Heading.m_vElements.empty(); }

		template <typename Archive>
		void serialize(Archive& ar)
		{
			ar
				& m_Heading.m_Prefix
				& m_Heading.m_vElements
				& m_vArbitraryStates
				& m_Proof
				& m_hvRootLive
				& m_LowerBound;
		}

		struct IStateWalker
		{
			virtual bool OnState(const SystemState::Full&, bool bIsTip) = 0;
		};

		// enumerates all the embedded states in standard order (from lo to hi)
		bool EnumStates(IStateWalker&) const;
		void UnpackStates(std::vector<SystemState::Full>&) const;

	private:
		struct Sampler;
		bool IsValidInternal(size_t& iState, size_t& iHash, const Difficulty::Raw& lowerBound, SystemState::Full* pTip) const;
		void ZeroInit();
		bool EnumStatesHeadingOnly(IStateWalker&) const; // skip arbitrary
	};
}

inline void ZeroObject(beam::Block::SystemState::Full& x)
{
	ZeroObjectUnchecked(x);
}
