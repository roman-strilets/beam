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

#include "core/crypto/ecc_internal.h"

#if defined(__aarch64__) && defined(__ARM_FEATURE_SHA2)
#	include <arm_neon.h>
#	define BEAM_SHA256_HW_ARM
#endif

namespace ECC {

	/////////////////////
	// SHA-256. Same as secp256k1_sha256_write/finalize, but the transform uses the SHA-256 instructions of the CPU if supported,
	// and the full blocks are processed in place
	namespace Sha256
	{
#ifdef BEAM_SHA256_HW_ARM
		static const uint32_t s_pK[64] = {
			0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
			0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
			0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
			0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
			0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
			0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
			0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
			0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
		};

		static void Transform(uint32_t* s, const uint8_t* p, size_t nBlocks)
		{
			uint32x4_t abcd = vld1q_u32(s);
			uint32x4_t efgh = vld1q_u32(s + 4);

			for (; nBlocks--; p += 64)
			{
				const uint32x4_t abcd0 = abcd, efgh0 = efgh;

				// the message words are big-endian
				uint32x4_t pW[4];
				for (int i = 0; i < 4; i++)
					pW[i] = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(p + i * 16)));

				for (int i = 0; i < 16; i++)
				{
					// rounds 4*i .. 4*i+3. pW[i % 4] has the words 4*i .. 4*i+3
					uint32x4_t wk = vaddq_u32(pW[i & 3], vld1q_u32(s_pK + i * 4));
					uint32x4_t abcdPrev = abcd;
					abcd = vsha256hq_u32(abcd, efgh, wk);
					efgh = vsha256h2q_u32(efgh, abcdPrev, wk);

					if (i < 12) // the words 4*i+16 .. 4*i+19
						pW[i & 3] = vsha256su1q_u32(vsha256su0q_u32(pW[i & 3], pW[(i + 1) & 3]), pW[(i + 2) & 3], pW[(i + 3) & 3]);
				}

				abcd = vaddq_u32(abcd, abcd0);
				efgh = vaddq_u32(efgh, efgh0);
			}

			vst1q_u32(s, abcd);
			vst1q_u32(s + 4, efgh);
		}
#else
		static void Transform(uint32_t* s, const uint8_t* p, size_t nBlocks)
		{
			for (; nBlocks--; p += 64)
				secp256k1_sha256_transform(s, p);
		}
#endif

		static void Write(secp256k1_sha256* pHash, const uint8_t* p, size_t n)
		{
			size_t nBuf = pHash->bytes & 0x3f;
			pHash->bytes += n;

			if (nBuf)
			{
				size_t nPortion = 64 - nBuf;
				if (n < nPortion)
				{
					memcpy(pHash->buf + nBuf, p, n);
					return;
				}

				memcpy(pHash->buf + nBuf, p, nPortion);
				Transform(pHash->s, pHash->buf, 1);

				p += nPortion;
				n -= nPortion;
			}

			size_t nBlocks = n >> 6;
			if (nBlocks)
			{
				Transform(pHash->s, p, nBlocks);
				p += nBlocks << 6;
				n -= nBlocks << 6;
			}

			if (n)
				memcpy(pHash->buf, p, n);
		}

		static void Finalize(secp256k1_sha256* pHash, uint8_t* pOut32)
		{
			static const uint8_t pPad[64] = { 0x80 };
			uint8_t pSize[8];

			secp256k1_write_be32(pSize, (uint32_t) (pHash->bytes >> 29));
			secp256k1_write_be32(pSize + 4, (uint32_t) (pHash->bytes << 3));
			Write(pHash, pPad, 1 + ((119 - (pHash->bytes % 64)) % 64));
			Write(pHash, pSize, sizeof(pSize));

			for (int i = 0; i < 8; i++)
			{
				secp256k1_write_be32(pOut32 + 4 * i, pHash->s[i]);
				pHash->s[i] = 0;
			}
		}

	} // namespace Sha256

	bool Hash::IsHw()
	{
#ifdef BEAM_SHA256_HW_ARM
		return true;
#else
		return false;
#endif
	}

	/////////////////////
	// Hash
	Hash::Processor::Processor()
	{
		Reset();
	}

	Hash::Processor::~Processor()
	{
		if (m_bInitialized)
			SecureErase(*this);
	}

	void Hash::Processor::Reset()
	{
		secp256k1_sha256_initialize(this);
		m_bInitialized = true;
	}

	void Hash::Processor::Reset(beam::Uninitialized_)
	{
		if (m_bInitialized)
		{
			SecureErase(*this);
			m_bInitialized = false;
		}
	}

	void Hash::Processor::Write(const void* p, uint32_t n)
	{
		assert(m_bInitialized);
		Sha256::Write(this, (const uint8_t*) p, n);
	}

	void Hash::Processor::Finalize(Value& v)
	{
		assert(m_bInitialized);
		Sha256::Finalize(this, v.m_pData);
		
		m_bInitialized = false;
	}

	void Hash::Processor::FinalizeTruncated(uint8_t* p, uint32_t nSize)
	{
		assert(nSize < Value::nBytes);

		Value hv;
		Finalize(hv);

		memcpy(p, hv.m_pData, nSize);
	}

	void Hash::Processor::Write(const beam::Blob& v)
	{
		Write(v.p, v.n);
	}

	void Hash::Processor::Write(bool b)
	{
		uint8_t n = (false != b);
		Write(n);
	}

	void Hash::Processor::Write(uint8_t n)
	{
		Write(&n, sizeof(n));
	}

	void Hash::Processor::Write(const Scalar& v)
	{
		Write(v.m_Value);
	}

	void Hash::Processor::Write(const Scalar::Native& v)
	{
		NoLeak<Scalar> s_;
		s_.V = v;
		Write(s_.V);
	}

	void Hash::Processor::Write(const Point& v)
	{
		Write(v.m_X);
		Write(v.m_Y);
	}

	void Hash::Processor::Write(const Point::Native& v)
	{
		Write(Point(v));
	}

	// Same as secp256k1_hmac_sha256_initialize/write/finalize, via Sha256
	void Hash::Mac::Reset(const void* pSecret, uint32_t nSecret)
	{
		uint8_t pKey[64];
		if (nSecret <= sizeof(pKey))
		{
			memcpy(pKey, pSecret, nSecret);
			memset(pKey + nSecret, 0, sizeof(pKey) - nSecret);
		}
		else
		{
			secp256k1_sha256 sha;
			secp256k1_sha256_initialize(&sha);
			Sha256::Write(&sha, (const uint8_t*) pSecret, nSecret);
			Sha256::Finalize(&sha, pKey);
			memset(pKey + 32, 0, 32);
		}

		secp256k1_sha256_initialize(&outer);
		for (uint32_t i = 0; i < sizeof(pKey); i++)
			pKey[i] ^= 0x5c;
		Sha256::Write(&outer, pKey, sizeof(pKey));

		secp256k1_sha256_initialize(&inner);
		for (uint32_t i = 0; i < sizeof(pKey); i++)
			pKey[i] ^= 0x5c ^ 0x36;
		Sha256::Write(&inner, pKey, sizeof(pKey));

		SecureErase(pKey, sizeof(pKey));
	}

	void Hash::Mac::Write(const void* p, uint32_t n)
	{
		Sha256::Write(&inner, (const uint8_t*) p, n);
	}

	void Hash::Mac::Finalize(Value& hv)
	{
		uint8_t pTemp[32];
		Sha256::Finalize(&inner, pTemp);
		Sha256::Write(&outer, pTemp, sizeof(pTemp));
		SecureErase(pTemp, sizeof(pTemp));
		Sha256::Finalize(&outer, hv.m_pData);
	}

	/////////////////////
	// NonceGenerator
	void NonceGenerator::Reset()
	{
		ZeroObject(m_Context);
		m_bFirstTime = true;
		m_Counter = Zero;
	}

	void NonceGenerator::WriteIkm(const beam::Blob& b)
	{
		assert(m_bFirstTime);
		m_HMac.Write(b.p, b.n);
	}

	const Hash::Value& NonceGenerator::get_Okm()
	{
		if (m_bFirstTime)
			// Extract
			m_HMac >> m_Prk;

		// Expand
		m_HMac.Reset(m_Prk.m_pData, m_Prk.nBytes);

		if (m_bFirstTime)
			m_bFirstTime = false;
		else
			m_HMac.Write(m_Okm.m_pData, m_Okm.nBytes);

		m_HMac.Write(m_Context.p, m_Context.n);

		m_Counter.Inc();
		m_HMac.Write(m_Counter.m_pData, m_Counter.nBytes);

		m_HMac >> m_Okm;
		return m_Okm;
	}

	NonceGenerator& NonceGenerator::operator >> (Hash::Value& hv)
	{
		hv = get_Okm();
		return *this;
	}

	NonceGenerator& NonceGenerator::operator >> (Scalar::Native& sk)
	{
		static_assert(sizeof(Scalar) == sizeof(m_Okm), "");
		const Scalar& s = reinterpret_cast<const Scalar&>(m_Okm);

		do
			get_Okm();
		while (!sk.ImportNnz(s));

		return *this;
	}

	/////////////////////
	// Oracle
	void Oracle::Reset()
	{
		m_hp.Reset();
	}

	void Oracle::operator >> (Hash::Value& out)
	{
		Hash::Processor(m_hp) >> out;
		operator << (out);
	}

	void Oracle::operator >> (Scalar::Native& out)
	{
		NoLeak<Scalar> s;

		do
			operator >> (s.V.m_Value);
		while (!out.ImportNnz(s.V));
	}

} // namespace ECC
