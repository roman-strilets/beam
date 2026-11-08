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

#include <sstream>
#include "core/consensus/rules.h"
#include "core/consensus/block.h"
#include "core/consensus/impl_helpers.h"

namespace beam
{
	/////////////
	// Rules
	const Rules& Rules::get()
	{
		assert(s_pInstance);
		if (!s_pInstance)
			Exc::Fail("no rules");

		return *s_pInstance;
	}

	Rules::Scope::Scope(const Rules& r) {
		m_pPrev = s_pInstance;
		s_pInstance = &r;
	}

	Rules::Scope::~Scope() {
		s_pInstance = m_pPrev;
	}

	Rules::Rules()
	{
		Prehistoric = {
			// BTC Block #556833
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x25, 0x2d, 0x12, 0x33, 0xb4, 0x5d, 0xb2,
			0x39, 0x81, 0x47, 0x67, 0x6e, 0x16, 0x62, 0xf4,
			0x3c, 0x26, 0xa5, 0x26, 0xd2, 0xe2, 0x20, 0x63,
		};

		// emission parameters
		Emission.Value0 = Coin * 80; // Initial emission. Each drop it will be halved. In case of odd num it's rounded to the lower value.
		Emission.Drop0 = 1440 * 365; // 1 year roughly. This is the height of the last block that still has the initial emission, the drop is starting from the next block
		Emission.Drop1 = 1440 * 365 * 4; // 4 years roughly. Each such a cycle there's a new drop

		// maturity
		Maturity.Coinbase = 240; // 4 hours

		// timestamp & difficulty.
		DA.Target_ms = 60'000; // 1 minute
		DA.WindowWork = 120; // 2 hours roughly (under normal operation)
		DA.MaxAhead_s = 60 * 15; // 15 minutes. Timestamps ahead by more than 15 minutes won't be accepted
		DA.WindowMedian0 = 25; // Timestamp for a block must be (strictly) higher than the median of preceding window
		DA.WindowMedian1 = 7; // Num of blocks taken at both endings of WindowWork, to pick medians.
		// damp factor. Adjustment of actual dt toward expected, effectively dampens
		DA.Damp.M = 1; // Multiplier of the actual dt
		DA.Damp.N = 3; // Denominator. The goal is multiplied by (N-M)

		// CA
		CA.Enabled = true;
		CA.DepositForList5 = Coin * 10; // after HF5
		CA.LockPeriod = 1440; // how long it's locked (can't be destroyed) after it was completely burned
		CA.m_ProofCfg = { 4, 3 }; // 4^3 = 64
		CA.ForeignEnd = 0;

		// Shielded
		Shielded.Enabled = true; // past Fork2
		Shielded.m_ProofMax = { 4, 8 }; // 4^8 = 64K
		Shielded.m_ProofMin = { 4, 5 }; // 4^5 = 1K

		// Max distance of the specified window from the tip where the prover is allowed to use m_ProofMax.
		// For proofs with bigger distance only m_ProofMin is supported
		Shielded.MaxWindowBacklog = 0x10000; // 64K
		// Hence "big" proofs won't need more than 128K most recent elements

		// max shielded ins/outs per block
		Shielded.MaxIns = 20; // input processing is heavy
		Shielded.MaxOuts = 30; // dust protection

		// other
		MaxRollback = 1440; // 1 day roughly
		MaxBodySize = 0x100000; // 1MB
		MaxKernelValidityDH = 1440 * 30; // past Fork2
		AllowPublicUtxos = false;
		Magic.v2 = 2;

		m_Pbft.m_Whitelist.m_NumRequired = 0;
		m_Pbft.m_RoundUp_ms = 0;

		// 1 eth == 10^18 wei
		// 1 beam == 10^8 groth
		// for equivalency we define  1 groth == 10^0 wei
		Evm.Groth2Wei = 0; // EVM is currently disabled by default
		//Evm.Groth2Wei = 10'000'000'000ull;

		Evm.BaseGasPrice = 10ull * 1'000'000'000ull; // 10 gwei
		Evm.MinTxGasUnits = 21000;

		m_Network = Network::mainnet;
		SetNetworkParams();
	}

	void Rules::SetNetworkParams()
	{
		// treasury
		switch (m_Network)
		{
		case Network::masternet:
		case Network::dappnet:
			TreasuryChecksum = {
				0xcf, 0x9c, 0xc2, 0xdf, 0x67, 0xa2, 0x24, 0x19,
				0x2d, 0x2f, 0x88, 0xda, 0x20, 0x20, 0x00, 0xac,
				0x94, 0xb9, 0x11, 0x45, 0x26, 0x51, 0x3a, 0x8f,
				0xc0, 0x7c, 0xd2, 0x58, 0xcd, 0x7e, 0x50, 0x70,
			};

			break;

		case Network::dappnet2:
			TreasuryChecksum = {
				0x04, 0x12, 0xdd, 0xd4, 0x05, 0x42, 0x6c, 0x4c,
				0x7d, 0x63, 0x88, 0xf0, 0x78, 0xe8, 0x00, 0xe4,
				0xa1, 0x82, 0x2b, 0xcb, 0xb2, 0xf5, 0xe6, 0x14,
				0xb2, 0x01, 0x3f, 0x5d, 0xc4, 0xae, 0x83, 0x30, 
			};

			break;

		case Network::warp_dev3:
			TreasuryChecksum = {
				0x4b, 0x2e, 0xf2, 0x8c, 0xf7, 0x2b, 0x82, 0xc6,
				0x55, 0x74, 0xa3, 0xd5, 0x32, 0x86, 0x18, 0xf3,
				0xab, 0x46, 0xaa, 0x22, 0x68, 0x98, 0x62, 0xc2,
				0xa5, 0x4e, 0x5b, 0x73, 0xba, 0xb7, 0xb1, 0xcd
			};

			break;

		default: // testnet, mainnet
			TreasuryChecksum = {
				0x5d, 0x9b, 0x18, 0x78, 0x9c, 0x02, 0x1a, 0x1e,
				0xfb, 0x83, 0xd9, 0x06, 0xf4, 0xac, 0x7d, 0xce,
				0x99, 0x7d, 0x4a, 0xc5, 0xd4, 0x71, 0xd7, 0xb4,
				0x6f, 0x99, 0x77, 0x6e, 0x7a, 0xbd, 0x2e, 0xc9
			};
		}

		// forks and misc
		Magic.v0 = 15;
		Magic.IsTestnet = false;
		m_Consensus = Consensus::PoW;
		CA.DepositForList2 = Coin * 3000; // after HF2
		DA.Difficulty0 = Difficulty(8 << Difficulty::s_MantissaBits); // 2^8 = 256

		ZeroObject(pForks);
		DisableForksFrom(6); // future forks

		switch (m_Network)
		{
		case Network::masternet:
			pForks[1].m_Height = 30;
			pForks[2].m_Height = 30;
			pForks[3].m_Height = 1500;
			pForks[4].m_Height = 516700;
			pForks[5].m_Height = 676330;

			break;

		case Network::testnet:

			pForks[1].m_Height = 270910;
			pForks[2].m_Height = 690000;
			pForks[3].m_Height = 1135300;
			pForks[4].m_Height = 1670000;
			pForks[5].m_Height = 1780000;

			Magic.IsTestnet = true;
			CA.DepositForList2 = Coin * 1000;
			break;

		case Network::dappnet:
			pForks[1].m_Height = 30;
			pForks[2].m_Height = 30;
			pForks[3].m_Height = 100;
			pForks[4].m_Height = 100;
			pForks[5].m_Height = 599999;

			m_Consensus = Consensus::FakePoW;
			break;

		case Network::dappnet2:

			SetParamsPbft(15'000);
			SetForksFrom(0, 0);

			m_Pbft.m_Whitelist.m_NumRequired = 1;
			m_Pbft.m_Whitelist.m_Addresses.resize(1);

			Cast::Down<ECC::uintBig>(m_Pbft.m_Whitelist.m_Addresses[0]) = {
				0x5f, 0xa7, 0x39, 0x27, 0xfe, 0x4f, 0x7c, 0xd2,
				0xea, 0xb3, 0x81, 0xe2, 0xb6, 0x8a, 0x60, 0xa3,
				0xf7, 0xd6, 0x13, 0x45, 0xcf, 0x69, 0x9d, 0x5d,
				0xbf, 0xcf, 0x06, 0xc5, 0x5a, 0x74, 0x0d, 0x23
			};

			break;

		case Network::warp_dev3:

			SetParamsPbft(3'000);
			SetForksFrom(0, 0);

			CA.ForeignEnd = 1'000'000;

			m_Pbft.m_Whitelist.m_NumRequired = 2;

			m_Pbft.m_Whitelist.m_Addresses.resize(4);

			Cast::Down<ECC::uintBig>(m_Pbft.m_Whitelist.m_Addresses[0]) = {
				0x9b, 0x4c, 0xc0, 0xb1, 0xf4, 0x29, 0x26, 0xcc,
				0xbf, 0x51, 0xd6, 0x4e, 0xec, 0x99, 0xd1, 0x7f,
				0xde, 0x79, 0x42, 0x49, 0x04, 0xb2, 0x6b, 0x32,
				0xf0, 0x53, 0xfe, 0x13, 0xfe, 0xf5, 0xb0, 0x22
			};
			Cast::Down<ECC::uintBig>(m_Pbft.m_Whitelist.m_Addresses[1]) = {
				0xde, 0x90, 0xc5, 0xe5, 0x01, 0x53, 0xe6, 0x52,
				0x1b, 0xe0, 0x34, 0x6a, 0x81, 0x88, 0x9c, 0x95,
				0x2d, 0x96, 0x61, 0x47, 0xe8, 0xc5, 0x96, 0x9b,
				0x4b, 0x5a, 0xc4, 0x93, 0x5c, 0x91, 0x03, 0x76
			};
			Cast::Down<ECC::uintBig>(m_Pbft.m_Whitelist.m_Addresses[2]) = {
				0x9e, 0x88, 0x59, 0xdd, 0xf4, 0x52, 0x85, 0x8e,
				0x6c, 0xd0, 0x4e, 0x4c, 0xb1, 0x9c, 0x29, 0xa3,
				0x26, 0x10, 0x7a, 0x53, 0x3b, 0xf0, 0x72, 0xfa,
				0xd3, 0xe1, 0x7f, 0x1b, 0x41, 0xca, 0xcf, 0x4d
			};
			Cast::Down<ECC::uintBig>(m_Pbft.m_Whitelist.m_Addresses[3]) = {
				0xa6, 0xf0, 0x90, 0x63, 0x88, 0x51, 0x88, 0x40,
				0x75, 0xb9, 0xfe, 0xab, 0xcd, 0x66, 0xc9, 0xd4,
				0x7d, 0x55, 0x11, 0xd8, 0x21, 0xc8, 0x60, 0x2f,
				0x48, 0x6b, 0x0a, 0x86, 0xfe, 0xb5, 0x42, 0x50
			};

			break;

		default: // mainnet
			pForks[1].m_Height = 321321;
			pForks[2].m_Height = 777777;
			pForks[3].m_Height = 1280000;
			pForks[4].m_Height = 1820000;
			pForks[5].m_Height = 1920000;
			pForks[6].m_Height = 3928666;

			Magic.v0 = 14;
			DA.Difficulty0 = Difficulty(22 << Difficulty::s_MantissaBits); // 2^22 = 4,194,304. For GPUs producing 7 sol/sec this is roughly equivalent to 10K GPUs.
		}
	}

	bool Rules::IsEnabledCA(Height hScheme) const
	{
		return IsPastFork_<2>(hScheme) && CA.Enabled;
	}

	Amount Rules::get_EmissionEx(Height h, Height& hEnd, Amount base) const
	{
		if (!base)
		{
			hEnd = MaxHeight;
			return 0;
		}

		h--; // may overflow, but it's ok. If h is zero (which must not happen anyway) - then it'll give a huge height, for which the emission would be zero anyway.

		// Current emission strategy:
		// at Emission.Drop0 - 1/2
		// at Emission.Drop1 - 5/8
		// each Emission.Drop1 cycle - 1/2

		if (h < Emission.Drop0)
		{
			hEnd = Emission.Drop0 + 1;
			return base;
		}

		assert(Emission.Drop1);
		Height n = 1 + (h - Emission.Drop0) / Emission.Drop1;

		const uint32_t nBitsMax = sizeof(Amount) << 3;
		if (n >= nBitsMax)
		{
			hEnd = MaxHeight;
			return 0;
		}

		hEnd = Emission.Drop0 + n * Emission.Drop1 + 1;

		if (n >= 2)
			base += (base >> 2); // the unusual part - add 1/4

		return base >> n;
	}

	Amount Rules::get_Emission(Height h) const
	{
		return get_EmissionEx(h, h, Emission.Value0);
	}

	void Rules::get_Emission(AmountBig::Number& res, const HeightRange& hr) const
	{
		get_Emission(res, hr, Emission.Value0);
	}

	void Rules::get_Emission(AmountBig::Number& res, const HeightRange& hr, Amount base) const
	{
		res = Zero;

		if (hr.IsEmpty())
			return;

		for (Height hPos = hr.m_Min; ; )
		{
			Height hEnd;
			Amount nCurrent = get_EmissionEx(hPos, hEnd, base);
			if (!nCurrent)
				break;

			assert(hEnd > hPos);

			if (hr.m_Max < hEnd)
			{
				res += MultiWord::From(nCurrent) * MultiWord::From(hr.m_Max - hPos + 1);
				break;
			}

			res += MultiWord::From(nCurrent) * MultiWord::From(hEnd - hPos);
			hPos = hEnd;
		}
	}

	bool Rules::IsForkHeightsConsistent() const
	{
		if (pForks[0].m_Height)
			return false;

		for (size_t i = 1; i < _countof(pForks); i++)
			if (pForks[i].m_Height < pForks[i - 1].m_Height)
				return false;

		return true;
	}

	bool Rules::Pbft::Whitelist::IsWhite(const PeerID& addr) const
	{
		return std::binary_search(m_Addresses.begin(), m_Addresses.end(), addr);
		//uint32_t iPos = 0;
		//return IsWhite(addr, iPos);
	}

	bool Rules::Pbft::Whitelist::IsWhite(const PeerID& addr, uint32_t& iPos) const
	{
		// for sequential check it's better not to use binary search, but just scan sequentially
		while (iPos < m_Addresses.size())
		{
			const auto& val = m_Addresses[iPos];
			if (val > addr)
				break;

			++iPos;
			if (val == addr)
				return true;
		}

		return false;

		//auto it = std::lower_bound(m_Addresses.begin() + iPos, m_Addresses.end(), addr);
		//iPos = static_cast<uint32_t>(it - m_Addresses.begin());

		//return
		//	(iPos < m_Addresses.size()) &&
		//	(addr == m_Addresses[iPos]);
	}

	void Rules::UpdateChecksum()
	{
		Exc::CheckpointTxt cp("Rules");

		if (!IsForkHeightsConsistent())
			Exc::Fail("Inconsistent Forks");

		if (!CA.m_ProofCfg.get_N())
			Exc::Fail("Bad CA/Sigma cfg");

		uint32_t n1 = Shielded.m_ProofMin.get_N();
		uint32_t n2 = Shielded.m_ProofMax.get_N();

		if (!n1 || (n1 > n2))
			Exc::Fail("Bad Shielded/Sigma cfg");

		if (!IsConstantSpan())
		{
			if (DA.Difficulty0.m_Packed)
				Exc::Fail("Bad D0 for irregular height network");
		}

		// all parameters, including const (in case they'll be hardcoded to different values in later versions)
		ECC::Oracle oracle;
		oracle
			<< ECC::Context::get().m_hvChecksum
			<< Prehistoric
			<< TreasuryChecksum
			<< static_cast<Height>(1) // HeightGenesis
			<< Coin
			<< Emission.Value0
			<< Emission.Drop0
			<< Emission.Drop1
			<< Maturity.Coinbase
			<< static_cast<Height>(0) // Maturity.Std
			<< MaxBodySize
			<< (uint32_t) m_Consensus
			<< AllowPublicUtxos
			<< false // deprecated CA.Enabled
			<< true // deprecated CA.Deposit
			<< DA.get_Target_s()
			<< DA.MaxAhead_s
			<< DA.WindowWork
			<< DA.WindowMedian0
			<< DA.WindowMedian1
			<< DA.Difficulty0.m_Packed
			<< MaxRollback
			<< uint32_t(720) // deprecated parameter
			<< (uint32_t) Block::PoW::K
			<< (uint32_t) Block::PoW::N
			<< (uint32_t) Block::PoW::NonceType::nBits
			<< Magic.v0; // increment this whenever we change something in the protocol

		if (Rules::Consensus::Pbft == m_Consensus)
		{
			auto& x = m_Pbft; // alias
			Exc::CheckpointTxt cp2("pbft");

			if (!DA.Target_ms)
				Exc::Fail("slot duration not specified");

			auto& vec = x.m_Whitelist.m_Addresses;
			std::sort(vec.begin(), vec.end());

			oracle
				<< "pbft.3"
				<< DA.Target_ms
				<< x.m_RoundUp_ms
				<< x.m_Whitelist.m_NumRequired
				<< vec.size();

			for (const auto& addr : vec)
				oracle << addr;

			for (uint32_t i = 1; i < vec.size(); i++)
			{
				if (vec[i] <= vec[i-1])
					Exc::Fail("whitelist dups");
			}

			if (vec.size() < x.m_Whitelist.m_NumRequired)
				Exc::Fail("not enough white");
		}

		if (!Magic.IsTestnet)
			oracle << "masternet";

		oracle
			// out
			>> pForks[0].m_Hash;

		oracle
			<< "fork1"
			<< pForks[1].m_Height
			<< DA.Damp.M
			<< DA.Damp.N
			// out
			>> pForks[1].m_Hash;

		oracle
			<< "fork2"
			<< pForks[2].m_Height
			<< MaxKernelValidityDH
			<< Shielded.Enabled
			<< Magic.v2; // increment this whenever we change something in the protocol

		// for historical reasons we didn't include Shielded.MaxIns and Shielded.MaxOuts
		if ((Shielded.MaxIns != 20) || (Shielded.MaxOuts != 30))
		{
			oracle
				<< Shielded.MaxIns
				<< Shielded.MaxOuts;
		}

		uint8_t flagCA =
			(CA.Enabled ? 1 : 0) |
			(CA.ForeignEnd ? 2 : 0);

		oracle
			<< Shielded.m_ProofMax.n
			<< Shielded.m_ProofMax.M
			<< Shielded.m_ProofMin.n
			<< Shielded.m_ProofMin.M
			<< Shielded.MaxWindowBacklog
			<< flagCA
			<< CA.DepositForList2
			<< CA.LockPeriod
			<< CA.m_ProofCfg.n
			<< CA.m_ProofCfg.M
			<< Asset::ID(Asset::s_MaxCount);

		if (CA.ForeignEnd)
			oracle << CA.ForeignEnd;

		oracle
			// out
			>> pForks[2].m_Hash;

		oracle
			<< "fork3"
			<< pForks[3].m_Height
			<< (uint32_t) 5 // bvm version
			// TODO: bvm contraints
			>> pForks[3].m_Hash;

		oracle
			<< "fork4"
			<< pForks[4].m_Height
			// no more flexible parameters so far
			>> pForks[4].m_Hash;

		oracle
			<< "fork5"
			<< pForks[5].m_Height
			<< CA.DepositForList5
			>> pForks[5].m_Hash;

		oracle
			<< "fork6"
			<< pForks[6].m_Height
			<< (uint32_t) 1 // fees in coinbase
			<< Evm.Groth2Wei
			<< Evm.BaseGasPrice
			<< Evm.MinTxGasUnits
			>> pForks[6].m_Hash;
	}

	const HeightHash* Rules::FindFork(const Merkle::Hash& hv) const
	{
		for (size_t i = _countof(pForks); i--; )
		{
			const HeightHash& x = pForks[i];

			if ((MaxHeight!= x.m_Height) && (x.m_Hash == hv))
				return pForks + i;
		}

		return nullptr;
	}

	uint32_t Rules::FindFork(Height h) const
	{
		for (uint32_t i = (uint32_t) _countof(pForks); i--; )
		{
			if (h >= pForks[i].m_Height)
				return i;
		}

		return 0; // should not be reached
	}

	Height Rules::get_ForkMaxHeightSafe(uint32_t iFork) const
	{
		assert(iFork < _countof(pForks));
		if (iFork + 1 < _countof(pForks))
		{
			Height h = pForks[iFork + 1].m_Height;
			if (h < MaxHeight)
				return h - 1;
		}

		return MaxHeight;
	}

	const HeightHash& Rules::get_LastFork() const
	{
		size_t i = _countof(pForks);
		while (--i)
		{
			if (MaxHeight != pForks[i].m_Height)
				break;
		}

		return pForks[i];
	}

	void Rules::SetForksFrom(uint32_t iBegin, Height h)
	{
		for (; iBegin < _countof(pForks); iBegin++)
			pForks[iBegin].m_Height = h;
	}

	void Rules::DisableForksFrom(uint32_t i)
	{
		SetForksFrom(i, MaxHeight);
	}

	void Rules::SetParamsPbft(uint32_t nTarget_ms)
	{
		m_Consensus = Consensus::Pbft;
		DA.Target_ms = nTarget_ms;
		DA.Difficulty0.m_Packed = 0;
		m_Pbft.m_RoundUp_ms = nTarget_ms / 4;

		ZeroObject(Emission);
		Maturity.Coinbase = 0;
	}

	Amount Rules::get_DepositForCA(Height hScheme) const
	{
		return IsPastFork_<5>(hScheme) ? CA.DepositForList5 : CA.DepositForList2;
	}

	const char* Rules::get_NetworkName() const
	{
		switch (m_Network)
		{
#define THE_MACRO(name) case Network::name: return #name;
			RulesNetworks(THE_MACRO)
#undef THE_MACRO
		}

		return "unspecified";
	}

	std::string Rules::get_SignatureStr() const
	{
		std::ostringstream os;
		os << "network=" << get_NetworkName();

		for (size_t i = 0; i < _countof(pForks); i++)
		{
			const HeightHash& x = pForks[i];
			if (MaxHeight == x.m_Height)
				break; // skip those

			os << "\n\t" << x;
		}

		return os.str();
	}

	void Rules::Fail_Fork(uint32_t iFork)
	{
		std::ostringstream ss;
		ss << "Fork required: " << iFork;
		Exc::Fail(ss.str().c_str());
	}

	void Rules::Height2Difficulty(Difficulty::Raw& d, Height h) const
	{
		assert(!IsConstantSpan());
		d = Zero;
		d.AssignRange<Height, Difficulty::s_MantissaBits>(h);
	}

	Difficulty Rules::Span2Difficulty(uint32_t n) const
	{
		assert(!IsConstantSpan());
		Difficulty d;
		d.PackLo(n);
		return d;
	}

	uint32_t Rules::Difficulty2Span(Difficulty d) const
	{
		assert(!IsConstantSpan());

		uint32_t ret;
		d.UnpackLo(ret);
		return ret;
	}

	int HeightHash::cmp(const HeightHash& v) const
	{
		CMP_MEMBER(m_Height)
		CMP_MEMBER_EX(m_Hash)
		return 0;
	}

	int HeightPos::cmp(const HeightPos& v) const
	{
		CMP_MEMBER(m_Height)
		CMP_MEMBER(m_Pos)
		return 0;
	}

	void ExecutorMT_R::StartThread(MyThread& t, uint32_t iThread)
	{
		t = MyThread(&ExecutorMT_R::RunThreadInternal, this, iThread, Rules::get());
	}

	void ExecutorMT_R::RunThreadInternal(uint32_t iThread, const Rules& r)
	{
		Rules::Scope scopeRules(r);
		RunThread(iThread);
	}

	void ExecutorMT_R::RunThread(uint32_t iThread)
	{
		Context ctx;
		ctx.m_iThread = iThread;
		RunThreadCtx(ctx);
	}

} // namespace beam
