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
#include "core/consensus/chain_types.h"
#include "core/crypto/lelantus.h"

namespace beam
{
	struct Asset
	{
		typedef uint32_t ID; // 1-based asset index. 0 is reserved for default asset (Beam)
		static constexpr ID s_MaxCount  = uint32_t(1) << 30; // 1 billion. Of course practically it'll be very much smaller
		static constexpr ID s_InvalidID = 0;
		static constexpr ID s_BeamID    = 0;
		static const PeerID s_InvalidOwnerID;

		struct Base
		{
			ID m_ID;

			explicit Base(ID id = s_InvalidID) :m_ID(id) {}

			void get_Generator(ECC::Point::Native&) const;
			void get_Generator(ECC::Point::Storage&) const;
			void get_Generator(ECC::Point::Native&, ECC::Point::Storage&) const;

			void get_GeneratorSafe(ECC::Point::Storage&) const; // works for aid=0
			static const ECC::Point::Compact& get_H();
		};

		struct Metadata
		{
			ByteBuffer m_Value;
			ECC::Hash::Value m_Hash = Zero; // not serialized

			void set_String(const std::string&, bool bLegacy);
			void get_String(std::string&) const;

			void Reset();
			void UpdateHash(); // called automatically during deserialization
			void get_Owner(PeerID&, Key::IPKdf&) const;
			void get_Owner(PeerID&, const ContractID&) const;
		};

		struct CreateInfo
		{
			PeerID m_Owner = Zero;
			ContractID m_Cid = Zero;
			Amount m_Deposit = 0;
			Metadata m_Metadata;
			static const uint32_t s_MetadataMaxSize = 1024 * 16; // 16K

			void SetCid(const ContractID*);
			bool Recognize(Key::IPKdf&) const;
			bool IsDefDeposit() const;
		};

		struct Info
			:public CreateInfo
		{
			AmountBig::Type m_Value = Zero;
			Height m_LockHeight = 0; // last emitted/burned change height. if emitted atm - when was latest 1st emission. If burned atm - what was last burn.

			void Reset();
			bool IsEmpty() const;
			bool IsValid() const;
		};

		struct Full
			:public Base
			,public Info
		{
			void get_Hash(ECC::Hash::Value&) const;
		};

		struct Proof
			:public Sigma::Proof
		{
			typedef std::unique_ptr<Proof> Ptr;

			struct Params
			{
				static thread_local Asset::ID s_AidMax_Override;

				static Asset::ID Make(Asset::ID aid, bool bHideAlways) {
					return aid ? aid : bHideAlways ? 1 : 0;
				}

				static bool IsNeeded(Asset::ID aid, Height hScheme);

				struct Override
				{
					Asset::ID m_Prev;

					Override(Asset::ID aid)
					{
						m_Prev = s_AidMax_Override;
						s_AidMax_Override = aid + 1;

					}
					~Override()
					{
						s_AidMax_Override = m_Prev;
					}
				};
			};

			Asset::ID m_Begin; // 1st element
			ECC::Point m_hGen;

			bool IsValid(Height, ECC::Point::Native& hGen) const; // for testing only, in real-world cases batch verification should be used!
			bool IsValidPrepare(ECC::Point::Native& hGen, ECC::InnerProduct::BatchContext& bc, ECC::Scalar::Native* pKs) const;
			void Create(Height, ECC::Point::Native& genBlinded, ECC::Scalar::Native& skInOut, Amount val, Asset::ID, const ECC::Point::Native& gen, const ECC::Hash::Value* phvSeed = nullptr);
			void Create(Height, ECC::Point::Native& genBlinded, ECC::Scalar::Native& skInOut, Amount val, Asset::ID, const ECC::Hash::Value* phvSeed = nullptr);
			void Create(Height, ECC::Point::Native& genBlinded, const ECC::Scalar::Native& skGen, Asset::ID, const ECC::Point::Native& gen);

			static void ModifySk(ECC::Scalar::Native& skInOut, const ECC::Scalar::Native& skGen, Amount val);

			static void Expose(ECC::Oracle&, Height hScheme, const Ptr&);

			void Clone(Ptr&) const;

			struct BatchContext
			{
				static thread_local BatchContext* s_pInstance;

				struct Scope
				{
					BatchContext* m_pPrev;

					Scope(BatchContext& bc) {
						m_pPrev = s_pInstance;
						s_pInstance = &bc;
					}
					~Scope() {
						s_pInstance = m_pPrev;
					}
				};

				virtual bool IsValid(Height, ECC::Point::Native& hGen, const Proof&) = 0;
			};

		private:
			struct CmList;
		};
	};

	struct FundsChangeMap
	{
		std::map<Asset::ID, AmountBig::Number> m_Map;

		void Add(Amount val, Asset::ID, bool bSpend);
		void Add(const AmountBig::Number&, Asset::ID);
		void ToCommitment(ECC::Point::Native&) const;
	};
}

inline void ZeroObject(beam::Asset::Full& x)
{
	ZeroObjectUnchecked(x);
}
