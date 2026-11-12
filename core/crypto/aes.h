#pragma once
#include "core/base/uintBig.h"

struct AES
{
	static const int s_KeyBits = 256;
	static const int s_KeyBytes = s_KeyBits >> 3;
	static const int Nr = 14; // num-rounds
	static const int s_BlockSize = 16;

	struct Encoder
	{
		uint32_t m_erk[64]; // encryption round keys. Actually needed 60, but during init extra space is used
		uint8_t m_pRkBytes[(Nr + 1) * s_BlockSize]; // the same round keys as bytes, for the AES instructions of the CPU (if used)
		void Init(const uint8_t* pKey);
		void Proceed(uint8_t* pDst, const uint8_t* pSrc) const; // uses the AES instructions of the CPU if supported
		void ProceedSw(uint8_t* pDst, const uint8_t* pSrc) const; // software only

		static bool IsHw(); // the AES instructions are used
	};

	struct Decoder
	{
		uint32_t m_drk[60]; // decryption round keys
		void Init(const Encoder&);
		void Proceed(uint8_t* pDst, const uint8_t* pSrc) const;
	};

	struct StreamCipher
	{
		beam::uintBig_t<s_BlockSize> m_Counter; // CTR mode

		// generated cipherstream
		uint8_t m_pBuf[s_BlockSize];
		uint8_t m_nBuf;

		void PerfXor(uint8_t* pBuf, uint32_t nSize);

		void Reset();
		void XCrypt(const Encoder&, uint8_t* pBuf, uint32_t nSize);
	};

};
