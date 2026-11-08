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
#include "core/consensus/difficulty.h"
#include "utility/executor.h"

namespace beam
{
	struct Rules
	{
		thread_local static const Rules* s_pInstance;
		static const Rules& get();

		Rules();

		struct Scope {
			const Rules* m_pPrev;
			Scope(const Rules&);
			~Scope();
		};

		static constexpr Amount Coin = AmountBig::Text::nCoin; // how many quantas in a single coin. Just cosmetic, has no meaning to the processing (which is in terms of quantas)

#define RulesNetworks(macro) \
		macro(mainnet) \
		macro(masternet) \
		macro(testnet) \
		macro(dappnet) \
		macro(dappnet2) \
		macro(warp_dev3) \

		enum struct Network
		{
#define THE_MACRO(name) name,
			RulesNetworks(THE_MACRO)
#undef THE_MACRO

		} m_Network;

		const char* get_NetworkName() const;

		struct {
			// emission parameters
			Amount Value0;
			Height Drop0;
			Height Drop1;
		} Emission;

		struct {
			Height Coinbase;
		} Maturity;

		struct {
			// timestamp & difficulty.
			uint32_t Target_ms; // changed recently to ms, to support more precise PBFT timeslots
			uint32_t WindowWork;
			uint32_t MaxAhead_s;
			uint32_t WindowMedian0;
			uint32_t WindowMedian1;
			Difficulty Difficulty0;

			uint32_t get_Target_s() const {
				return Target_ms / 1000;
			}

			struct {
				// damp factor. Adjustment of actual dt toward expected, effectively dampens
				uint32_t M;
				uint32_t N;
			} Damp;
		} DA;

		enum struct Consensus {
			PoW = 0,
			FakePoW = 1,
			Pbft = 2,
		};

		Consensus m_Consensus;

		struct {
			bool Enabled;
			Amount DepositForList2;
			Amount DepositForList5;
			Height LockPeriod;
			Sigma::Cfg m_ProofCfg;

			Asset::ID ForeignEnd = 0;

			bool IsForeign(Asset::ID aid) const {
				return aid < ForeignEnd;
			}

		} CA;

		struct Pbft {

			struct Whitelist
			{
				std::vector<PeerID> m_Addresses; // must be sorted
				uint32_t m_NumRequired;

				bool IsWhite(const PeerID&) const;
				bool IsWhite(const PeerID&, uint32_t& iPos) const;

			} m_Whitelist;

			uint32_t m_RoundUp_ms; // how much longer each new round is supposed to be

			IMPLEMENT_GET_PARENT_OBJ(Rules, m_Pbft)
		} m_Pbft;

		uint32_t MaxRollback;

		size_t MaxBodySize;

		bool AllowPublicUtxos;

		Height MaxKernelValidityDH; // past Fork2
		// if kernel has higher lifetime - its max height is implicitly decreased

		ECC::Hash::Value Prehistoric; // Prev hash of the 1st block
		ECC::Hash::Value TreasuryChecksum;

		struct {
			bool Enabled = true; // past Fork2

			Sigma::Cfg m_ProofMax;
			Sigma::Cfg m_ProofMin;

			// Max distance of the specified window from the tip where the prover is allowed to use m_ProofMax.
			// For proofs with bigger distance only m_ProofMin is supported
			uint32_t MaxWindowBacklog;
			// Hence "big" proofs won't need more than 128K most recent elements

			// max shielded ins/outs per block
			uint32_t MaxIns; // input processing is heavy
			uint32_t MaxOuts; // dust protection

		} Shielded;

		struct
		{
			uint32_t v0;
			uint32_t v2;
			bool IsTestnet;
		} Magic;

		struct
		{
			// So, here's our interpretation of the above.
			// - Gas is drawn from the BVM charge (which is 100 mln per block) at 1:1 ratio, i.e. bvm and evm would use this shared limit
			// - Tx gas limit is calculated from the kernel fee (w.r.t. groth/wei conversion ratio)
			// - All tx gas is always consumed. All goes to miner, nothing is burned
			// - Min tx gas price is alwas required (for basic account access).
			// - We assume always base fee (i.e. no dynamic raise of gas price when more than half of block limit is used).

			uint64_t Groth2Wei; // set to 0 to disable EVM
			uint64_t BaseGasPrice; // 100 gwei
			uint32_t MinTxGasUnits;
		} Evm;

		void SetNetworkParams();
		void UpdateChecksum();

		Amount get_Emission(Height) const;
		void get_Emission(AmountBig::Number&, const HeightRange&) const;
		void get_Emission(AmountBig::Number&, const HeightRange&, Amount base) const;

		HeightHash pForks[7];

		const HeightHash& get_LastFork() const;
		const HeightHash* FindFork(const Merkle::Hash&) const;
		uint32_t FindFork(Height) const;
		Height get_ForkMaxHeightSafe(uint32_t iFork) const;
		void DisableForksFrom(uint32_t);
		std::string get_SignatureStr() const;
		Amount get_DepositForCA(Height hScheme) const;
		bool IsEnabledCA(Height hScheme) const;

		static void Fail_Fork(uint32_t iFork);

		bool IsPastFork(Height h, uint32_t iFork) const
		{
			assert(iFork < _countof(pForks));
			return (h >= pForks[iFork].m_Height);
		}

		template <uint32_t iFork>
		bool IsPastFork_(Height h) const
		{
			static_assert(iFork < _countof(pForks), "");
			return IsPastFork(h, iFork);
		}

		void TestForkAtLeast(Height h, uint32_t iFork) const
		{
			if (!IsPastFork(h, iFork))
				Fail_Fork(iFork);
		}

		template <uint32_t iFork>
		void TestForkAtLeast_(Height h) const
		{
			static_assert(iFork < _countof(pForks), "");
			TestForkAtLeast(h, iFork);
		}

		void TestEnabledCA() const
		{
			if (!CA.Enabled)
				Exc::Fail("CA disabled");
		}

		uint32_t get_BpScheme(Height h) const
		{
			return IsPastFork_<6>(h) ? 1 : 0;
		}

		void TestEnabledShielded() const
		{
			if (!Shielded.Enabled)
				Exc::Fail("Shielded disabled");
		}

		bool IsConstantSpan() const
		{
			return Consensus::Pbft != m_Consensus;
		}

		void Height2Difficulty(Difficulty::Raw&, Height) const;
		Difficulty Span2Difficulty(uint32_t) const;
		uint32_t Difficulty2Span(Difficulty) const;

		void SetForksFrom(uint32_t iBegin, Height);
		void SetParamsPbft(uint32_t nTarget_ms);

		bool IsPbftWhitelistMode() const
		{
			return
				(Rules::Consensus::Pbft == m_Consensus) &&
				m_Pbft.m_Whitelist.m_NumRequired;
		}

	private:
		Amount get_EmissionEx(Height, Height& hEnd, Amount base) const;
		bool IsForkHeightsConsistent() const;
	};

	class ExecutorMT_R
		:public ExecutorMT
	{
		void StartThread(MyThread&, uint32_t iThread) override;
		void RunThreadInternal(uint32_t iThread, const Rules&);
		virtual void RunThread(uint32_t iThread);

	};
}
