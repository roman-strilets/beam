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
#include <limits>
#include "core/crypto/ecc_native.h"
#include "core/consensus/merkle.h"

namespace beam
{
	class IExternalPOW;

	const Height MaxHeight = std::numeric_limits<Height>::max();
	const uint8_t MaxPrivacyAnonimitySetFractionsCount = 64;
	const uint8_t kIsSelfTxBit = 0b00000001;

	struct PeerID :public ECC::uintBig
	{
		using ECC::uintBig::uintBig;
		using ECC::uintBig::operator =;

		bool ExportNnz(ECC::Point::Native&) const;
		bool Import(const ECC::Point::Native&); // returns if the sign is preserved
		bool FromSk(ECC::Scalar::Native&); // will negate the scalar iff necessary. returns if the sign is preserved

		bool CheckSignature(const Merkle::Hash&, const ECC::Signature&) const;
	};

	typedef uint64_t BbsChannel;
	typedef ECC::Hash::Value BbsMsgID;
	typedef uint64_t TxoID;

	using ECC::Key;

	typedef ECC::uintBig ContractID;

	namespace MasterKey
	{
		Key::IKdf::Ptr get_Child(Key::IKdf&, Key::Index);
	}

	namespace Evm
	{
		typedef ECC::uintBig Word;

		struct Address
			:public uintBig_t<20>
		{
			typedef uintBig_t<20> Base;

			static void WPad(Word& w)
			{
				memset0(w.m_pData, Word::nBytes - nBytes);
			}
			static Address& W2A(Word& w)
			{
				static_assert(Word::nBytes >= nBytes);
				return *(Address*)(w.m_pData + Word::nBytes - nBytes);
			}

			void ToWord(Word& w) const
			{
				WPad(w);
				W2A(w) = *this;
			}

			Word ToWord() const
			{
				Word w;
				ToWord(w);
				return w;
			}

			void FromPubKey(const ECC::Point::Native&);
			void FromPubKey(const ECC::Point::Storage&);
			bool FromPubKey(const ECC::Point&);
			bool FromPubKey(const PeerID&);
		};
	}

	Timestamp getTimestamp();
	uint32_t GetTime_ms(); // platform-independent GetTickCount
	uint32_t GetTimeNnz_ms(); // guaranteed non-zero

	struct ILongAction
	{
		virtual void Reset(const char*, uint64_t nTotal) = 0;
		virtual void SetTotal(uint64_t nTotal) = 0;
		virtual bool OnProgress(uint64_t pos) = 0;
	};
	struct LongAction : ILongAction
	{
		uint32_t m_Last_ms = 0;
		uint64_t m_Total = 0;
		ILongAction *m_pExternal = nullptr;

		LongAction(const char* sz, uint64_t nTotal, ILongAction *pExternal = nullptr)
			: m_pExternal(pExternal)
		{
			Reset(sz, nTotal);
		}
		LongAction() = default;

		void Reset(const char*, uint64_t nTotal) final;
		void SetTotal(uint64_t nTotal) final;
		bool OnProgress(uint64_t pos) final;
	};

	void HeightAdd(Height& trg, Height val); // saturates if overflow

	struct HeightRange
	{
		// Convention: inclusive, i.e. both endings are part of the range.
		// m_Min == m_Max means the range includes a single height. Therefore (m_Min > m_Max) is NOT invalid, it just denotes an empty range.
		Height m_Min;
		Height m_Max;

		HeightRange() {
			Reset();
		}

		HeightRange(Height h0, Height h1) {
			m_Min = h0;
			m_Max = h1;
		}

		HeightRange(Height h) {
			m_Min = m_Max = h;
		}

		void Reset();
		void Intersect(const HeightRange&);

		bool IsEmpty() const;
		bool IsInRange(Height) const;
		bool IsInRangeRelative(Height) const; // assuming m_Min was already subtracted
	};

	struct Group3Base
	{
		static const uint32_t nGroup = 3;
		static uint32_t Expand(char* szDst, const char* szSrc, uint32_t nSrc);
	};

	template <uint32_t len>
	struct Group3
		:public Group3Base
	{
		static const uint32_t nNumSeparators = (len - 1) / nGroup;
		static const uint32_t nLenUndecorated = len;
		static const uint32_t nLenDecorated = nLenUndecorated + nNumSeparators;
	};

	namespace AmountBig
	{
		typedef uintBig_t<sizeof(Amount) + sizeof(Height)> Type; // 128 bits
		typedef Type::Number Number;
		Amount get_Lo(const Number&);
		Amount get_Hi(const Number&);

		void AddTo(ECC::Point::Native&, const Number&);

		// This one is not optimized (slow)
		void AddTo(ECC::Point::Native& res, const Number& x, const ECC::Point::Native& hGen);

		struct Text {

			// how many quantas in a single coin. Just cosmetic, has no meaning to the processing (which is in terms of quantas)
			static const uint32_t nGrothDigits = 8;
			static const uint32_t nCoin = (uint32_t) NumericUtils::PowerOf<10, nGrothDigits>::N;

			static const uint32_t nLenUndecorated = Number::nTxtLen10Max;

			// max decorated length, printed as x.xxx,xxx,xxx,xxx.yyyyyyyy
			static const uint32_t nLenMax = nGrothDigits + 1 + Group3<nLenUndecorated - nGrothDigits>::nLenDecorated;
			// includes 1 groth separator (dot), and multiple group separaters (commas)

			// buf assumed to be nLenMax + 1
			static uint32_t Print(char*, Amount, bool bTrimGroths = true); 
			static uint32_t Print(char*, const Number&, bool bTrimGroths = true);

			// if szDst and szSrc may overlap, szSrc must be aligned to end
			static uint32_t Expand(char* szDst, const char* szSrc, uint32_t nSrc, bool bTrimGroths = true);
		};

		void Print(std::ostream&, const Number&, bool bTrim = true);
		void Print(std::ostream&, Amount, bool bTrim = true);

		template <typename T>
		struct Printable {
			const T& m_Val;
			Printable(const T& x) :m_Val(x) {}

			friend std::ostream& operator << (std::ostream& os, const AmountBig::Printable<T>& x) {
				AmountBig::Print(os, x.m_Val);
				return os;
			}
		};
	};

	typedef int64_t AmountSigned;
	static_assert(sizeof(Amount) == sizeof(AmountSigned), "");

	Amount SplitAmountSigned(AmountSigned val, bool& isPositive);

	struct HeightHash
	{
		Merkle::Hash	m_Hash;
		Height			m_Height;

		int cmp(const HeightHash&) const;
		COMPARISON_VIA_CMP
	};

	std::ostream& operator << (std::ostream&, const HeightHash&);

    struct HeightPos
    {
        Height m_Height = 0;
        uint32_t m_Pos = 0;

		HeightPos() = default;
		HeightPos(Height h, uint32_t pos = 0)
			:m_Height(h)
			,m_Pos(pos)
		{}

        template <typename Archive>
        void serialize(Archive& ar)
        {
            ar
                & m_Height
                & m_Pos;
        }

		int cmp(const HeightPos&) const;
		COMPARISON_VIA_CMP
	};
}

inline ECC::Hash::Processor& operator << (ECC::Hash::Processor& hp, const beam::PeerID& pid) {
	return hp << Cast::Down<ECC::Hash::Value>(pid);
}
