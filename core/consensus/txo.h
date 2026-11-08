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
#include "core/consensus/asset.h"

namespace beam
{
	struct CoinID
		:public Key::ID
	{
		struct Scheme
		{
			static const uint8_t V0 = 0;
			static const uint8_t V1 = 1;
			static const uint8_t BB21 = 2; // worakround for BB.2.1
			static const uint8_t V3 = 3;
			static const uint8_t V_Miner0 = 4;

			static const uint32_t s_SubKeyBits = 24;
			static const Key::Index s_SubKeyMask = (static_cast<Key::Index>(1) << s_SubKeyBits) - 1;
		};

		Amount m_Value;

		Asset::ID m_AssetID = 0;

		CoinID() = default;
		CoinID(Zero_)
			:ID(Zero)
			,m_Value(0)
		{
			set_Subkey(0);
		}

		CoinID(Amount v, uint64_t nIdx, Key::Type type, Key::Index nSubIdx = 0)
			:ID(nIdx, type)
			,m_Value(v)
		{
			set_Subkey(nSubIdx, Scheme::V3);
		}

		Key::Index get_Scheme() const
		{
			return m_SubIdx >> Scheme::s_SubKeyBits;
		}

		Key::Index get_Subkey() const
		{
			return m_SubIdx & Scheme::s_SubKeyMask;
		}

		void set_Subkey(Key::Index nSubIdx, Key::Index nScheme = Scheme::V3)
		{
			m_SubIdx = (nSubIdx & Scheme::s_SubKeyMask) | (nScheme << Scheme::s_SubKeyBits);
		}

		bool IsBb21Possible() const
		{
			return m_SubIdx && (Scheme::V0 == get_Scheme());
		}

		void set_WorkaroundBb21()
		{
			set_Subkey(get_Subkey(), Scheme::BB21);
		}

		bool IsWorkaroundMiner0Possible() const
		{
			return (Scheme::V3 == get_Scheme()) && !get_Subkey();
		}

		void set_WorkaroundMiner0()
		{
			set_Subkey(0, Scheme::V_Miner0);
		}

		void get_Hash(ECC::Hash::Value&) const;

		bool get_ChildKdfIndex(Key::Index&) const; // returns false if chils is not needed
		Key::IKdf::Ptr get_ChildKdf(const Key::IKdf::Ptr& pMasterKdf) const;

		bool IsDummy() const
		{
			return
				!m_Value &&
				!m_AssetID &&
				(Key::Type::Decoy == m_Type);
		}

		struct Generator
		{
			ECC::Point::Native m_hGen;
			Generator(Asset::ID);
			void AddValue(ECC::Point::Native& comm, Amount) const;
		};

		class Worker
			:public Generator
		{
			static void get_sk1(ECC::Scalar::Native& res, const ECC::Point::Native& comm0, const ECC::Point::Native& sk0_J);
			void CreateInternal(ECC::Scalar::Native&, ECC::Point::Native&, bool bComm, Key::IKdf& kdf) const;

		public:
			const CoinID& m_Cid;

			Worker(const CoinID&);

			void AddValue(ECC::Point::Native& comm) const;

			void Create(ECC::Scalar::Native& sk, Key::IKdf&) const;
			void Create(ECC::Scalar::Native& sk, ECC::Point::Native& comm, Key::IKdf&) const;
			void Create(ECC::Scalar::Native& sk, ECC::Point& comm, Key::IKdf&) const;

			void Recover(ECC::Point::Native& comm, Key::IPKdf&) const;
			void Recover(ECC::Point::Native& pkG_in_res_out, const ECC::Point::Native& pkJ) const;
		};
	};

	std::ostream& operator << (std::ostream&, const CoinID&);

	struct TxStats
	{
		AmountBig::Number m_Fee;

		uint32_t m_Kernels;
		uint32_t m_KernelsNonStd;
		uint32_t m_Inputs; // MW only
		uint32_t m_Outputs; // MW only
		uint32_t m_InputsShielded;
		uint32_t m_OutputsShielded;
		uint32_t m_Contract;
		uint32_t m_ContractSizeExtra;

		TxStats() { Reset(); }

		void Reset();
		void operator += (const TxStats&);
	};

	struct TxElement
	{
		ECC::Point m_Commitment;
		int cmp(const TxElement&) const;
	};

	struct Input
		:public TxElement
	{
		typedef std::unique_ptr<Input> Ptr;
		typedef uint32_t Count; // the type for count of duplicate UTXOs in the system

		struct State
		{
			Height m_Maturity;
			Input::Count m_Count;

			void get_ID(Merkle::Hash&, const ECC::Point&) const;

			template <typename Archive>
			void serialize(Archive& ar)
			{
				ar
					& m_Maturity
					& m_Count;
			}
		};

		struct Proof
		{
			State m_State;
			Merkle::Proof m_Proof;

			template <typename Archive>
			void serialize(Archive& ar)
			{
				ar
					& m_State
					& m_Proof;
			}

			static const uint32_t s_EntriesMax = 20; // if this is the size of the vector - the result is probably trunacted
		};

		Input() = default;
		Input(const Input& v)
			:TxElement(v)
		{
		}
		Input(Input&& v) noexcept
			:TxElement(std::move(v))
		{
		}

		void AddStats(TxStats&) const;

		Input& operator = (const Input&);
		Input& operator = (Input&&) noexcept;
		COMPARISON_VIA_CMP
	};

	inline bool operator < (const Input::Ptr& a, const Input::Ptr& b) { return *a < *b; }

	struct Output
		:public TxElement
	{
		typedef std::unique_ptr<Output> Ptr;

		bool		m_Coinbase;
		Height		m_Incubation; // # of blocks before it's mature

		Output()
			:m_Coinbase(false)
			,m_Incubation(0)
		{
		}

		Output(Output&& o)
			:TxElement(o)
			,m_Coinbase(o.m_Coinbase)
			,m_Incubation(o.m_Incubation)
			,m_pConfidential(std::move(o.m_pConfidential))
			,m_pPublic(std::move(o.m_pPublic))
			,m_pAsset(std::move(o.m_pAsset))
		{
		}

		struct User
		{
			ECC::Scalar m_pExtra[2];
			User()
			{
				ZeroObject(m_pExtra);
			}
#pragma pack (push, 1)
			struct Packed
			{
				typedef uintBig_t<16> TxID;
				Amount m_Fee;
				Amount m_Amount;
				TxID m_TxID;
				PeerID m_Peer;
			};
#pragma pack (pop)
			static Packed* ToPacked(User& user)
			{
				return reinterpret_cast<Packed*>(user.m_pExtra);
			}

			static const Packed* ToPacked(const User& user)
			{
				return reinterpret_cast<const Packed*>(user.m_pExtra);
			}

			template <typename Archive>
			void serialize(Archive& ar)
			{
				ar & m_pExtra;
			}
		};

		static const Amount s_MinimumValue = 1;

		// one of the following *must* be specified
		std::unique_ptr<ECC::RangeProof::Confidential>	m_pConfidential;
		std::unique_ptr<ECC::RangeProof::Public>		m_pPublic;
		Asset::Proof::Ptr								m_pAsset;

		struct OpCode {
			enum Enum {
				Standard,
				Public, // insist on public rangeproof, regardless to m_Coinbase. For tests only
				Mpc_1, // ignore coinKdf, generate rangeproof without sk, up to T1/T2
				Mpc_2, // Finish rangeproof after T1/T2 and TauX were updated by the peer
			};
		};

		void Create(Height hScheme, ECC::Scalar::Native&, Key::IKdf& coinKdf, const CoinID&, Key::IPKdf& tagKdf, OpCode::Enum = OpCode::Standard, const User* = nullptr);

		bool Recover(Height hScheme, Key::IPKdf& tagKdf, CoinID&, User* = nullptr) const;
		bool VerifyRecovered(Key::IPKdf& coinKdf, const CoinID&) const;

		bool IsValid(Height hScheme, ECC::Point::Native& comm) const;
		Height get_MinMaturity(Height h) const; // regardless to the explicitly-overridden

		void AddStats(TxStats&) const;

		Output& operator = (const Output&);
		int cmp(const Output&) const;
		COMPARISON_VIA_CMP

		static void GenerateSeedKid(ECC::uintBig&, const ECC::Point& comm, Key::IPKdf&);
		void Prepare(ECC::Oracle&, Height hScheme) const;

	private:
		struct PackedKA; // Key::ID + Asset::ID
		bool IsValid2(Height hScheme, ECC::Point::Native& comm, const ECC::Point::Native* pGen) const;
	};

	inline bool operator < (const Output::Ptr& a, const Output::Ptr& b) { return *a < *b; }

	struct ShieldedTxo
	{
		static void UpdateState(ECC::Hash::Value&, const ECC::Point::Storage&);

		struct Ticket
		{
			ECC::Point m_SerialPub; // blinded
			ECC::SignatureGeneralized<2> m_Signature;

			bool IsValid(ECC::Point::Native&) const;
			void get_Hash(ECC::Hash::Value&) const;
		};

		struct DescriptionBase
		{
			Height m_Height;
		};

		struct DescriptionOutp
			:public DescriptionBase
		{
			TxoID m_ID;
			ECC::Point m_SerialPub; // blinded
			ECC::Point m_Commitment;

			void get_Hash(Merkle::Hash&) const;
		};

		struct DescriptionInp
			:public DescriptionBase
		{
			ECC::Point m_SpendPk;

			void get_Hash(Merkle::Hash&) const;
		};

		ECC::Point m_Commitment;
		ECC::RangeProof::Confidential m_RangeProof;
		Asset::Proof::Ptr m_pAsset;
		Ticket m_Ticket;

		void Prepare(ECC::Oracle&, Height hScheme) const;
		bool IsValid(ECC::Oracle&, Height hScheme, ECC::Point::Native& comm, ECC::Point::Native& ser) const;

		ShieldedTxo& operator = (const ShieldedTxo&); // clone

		struct PublicGen;
		struct Viewer;
		struct Data;
		struct DataParams; // just a fwd-declaration of Data::Params

		struct BaseKey
		{
			Key::Index m_nIdx;
			bool m_IsCreatedByViewer;
			ECC::Scalar m_kSerG;

			bool operator ==(const BaseKey& other) const
			{
				return
					m_kSerG == other.m_kSerG &&
					m_nIdx == other.m_nIdx &&
					m_IsCreatedByViewer == other.m_IsCreatedByViewer;
			}
			
			bool operator!=(const BaseKey& other) const
			{
				return !operator==(other);
			}

			template <typename Archive>
			void serialize(Archive& ar)
			{
				ar
					& m_nIdx
					& m_IsCreatedByViewer
					& m_kSerG;
			}
		};

		struct User
		{
			PeerID m_Sender;
			ECC::uintBig m_pMessage[2];
			bool operator == (const User& other) const
			{
				return
					m_Sender == other.m_Sender &&
					m_pMessage[0] == other.m_pMessage[0] &&
					m_pMessage[1] == other.m_pMessage[1];
			}

			bool operator != (const User& other) const
			{
				return !operator == (other);
			}

#pragma pack (push, 1)
			struct PackedMessage
			{
				typedef uintBig_t<16> TxID;
				TxID m_TxID;
				// value from 1 to 64 to set privacy level for transaction.
				// if value less than 64 - allow extract coin from pool before anonymity set was reached
				uint8_t m_MaxPrivacyMinAnonymitySet;
				uint64_t m_ReceiverOwnID;
				uint8_t m_Flags;
				uint8_t m_Padding[sizeof(m_pMessage)
									- sizeof(TxID)
									- sizeof(uint8_t)
									- sizeof(uint64_t)
									- sizeof(uint8_t)];
			};
#pragma pack (pop)
			static PackedMessage* ToPackedMessage(User& user)
			{
				return reinterpret_cast<PackedMessage*>(user.m_pMessage);
			}

			static const PackedMessage* ToPackedMessage(const User& user)
			{
				return reinterpret_cast<const PackedMessage*>(user.m_pMessage);
			}

			template <typename Archive>
			void serialize(Archive& ar)
			{
				ar
					& m_Sender
					& m_pMessage;
			}
		};

		struct ID
		{
			BaseKey m_Key;
			User m_User;
			Amount m_Value;
			Asset::ID m_AssetID = 0;
			bool operator == (const ID&) const;
			bool operator != (const ID& other) const
			{
				return !operator==(other);
			}

			template <typename Archive>
			void serialize(Archive& ar)
			{
				ar
					& m_Value
					& m_AssetID
					& m_Key
					& m_User;
			}

			void get_SkOut(ECC::Scalar::Native&, Amount fee, Key::IKdf& kdf) const;
			void get_SkOutPreimage(ECC::Hash::Value&, Amount fee) const;

			static const Key::Index s_iChildOut = static_cast<uint32_t>(-2);
		};

		struct Voucher
		{
			// single-usage
			Ticket m_Ticket;
			ECC::Hash::Value m_SharedSecret;
			ECC::Signature m_Signature;

			void get_Hash(ECC::Hash::Value&) const;
			bool IsValid(const PeerID&) const;
			bool IsValid(const ECC::Point::Native&) const;
		};
	};
}

// TODO: review this types, they don't have standard layout
inline void ZeroObject(beam::CoinID& x)
{
	ZeroObjectUnchecked(x);
}
