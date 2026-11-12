#include <assert.h>
#include "core/crypto/aes.h"

#if defined(__aarch64__) && defined(__ARM_FEATURE_AES)
#	include <arm_neon.h>
#	define BEAM_AES_HW_ARM
#endif

/*
*  FIPS-197 compliant AES implementation
*
*  Copyright (C) 2001-2004  Christophe Devine
*
*  This program is free software; you can redistribute it and/or modify
*  it under the terms of the GNU General Public License as published by
*  the Free Software Foundation; either version 2 of the License, or
*  (at your option) any later version.
*
*  This program is distributed in the hope that it will be useful,
*  but WITHOUT ANY WARRANTY; without even the implied warranty of
*  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*  GNU General Public License for more details.
*
*  You should have received a copy of the GNU General Public License
*  along with this program; if not, write to the Free Software
*  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

/* forward S-box & tables */

uint32_t FSb[256];
uint32_t FT0[256];
uint32_t FT1[256];
uint32_t FT2[256];
uint32_t FT3[256];

/* reverse S-box & tables */

uint32_t RSb[256];
uint32_t RT0[256];
uint32_t RT1[256];
uint32_t RT2[256];
uint32_t RT3[256];

/* round constants */

uint32_t RCON[10];

/* tables generation routine */

#define ROTR8(x) ( ( ( x << 24 ) & 0xFFFFFFFF ) | \
                   ( ( x & 0xFFFFFFFF ) >>  8 ) )

#define XTIME(x) ( ( x <<  1 ) ^ ( ( x & 0x80 ) ? 0x1B : 0x00 ) )
#define MUL(x,y) ( ( x &&  y ) ? pow[(log[x] + log[y]) % 255] : 0 )

void aes_gen_tables(void)
{
	int i;
	uint8_t x, y;
	uint8_t pow[256];
	uint8_t log[256];

	/* compute pow and log tables over GF(2^8) */

	for (i = 0, x = 1; i < 256; i++, x ^= XTIME(x))
	{
		pow[i] = x;
		log[x] = static_cast<uint8_t>(i);
	}

	/* calculate the round constants */

	for (i = 0, x = 1; i < 10; i++, x = XTIME(x))
	{
		RCON[i] = (uint32_t)x << 24;
	}

	/* generate the forward and reverse S-boxes */

	FSb[0x00] = 0x63;
	RSb[0x63] = 0x00;

	for (i = 1; i < 256; i++)
	{
		x = pow[255 - log[i]];

		y = x;  y = (y << 1) | (y >> 7);
		x ^= y; y = (y << 1) | (y >> 7);
		x ^= y; y = (y << 1) | (y >> 7);
		x ^= y; y = (y << 1) | (y >> 7);
		x ^= y ^ 0x63;

		FSb[i] = x;
		RSb[x] = i;
	}

	/* generate the forward and reverse tables */

	for (i = 0; i < 256; i++)
	{
		x = (unsigned char)FSb[i]; y = XTIME(x);

		FT0[i] = (uint32_t)(x ^ y) ^
			((uint32_t)x << 8) ^
			((uint32_t)x << 16) ^
			((uint32_t)y << 24);

		FT0[i] &= 0xFFFFFFFF;

		FT1[i] = ROTR8(FT0[i]);
		FT2[i] = ROTR8(FT1[i]);
		FT3[i] = ROTR8(FT2[i]);

		y = (unsigned char)RSb[i];

		RT0[i] = ((uint32_t)MUL(0x0B, y)) ^
			((uint32_t)MUL(0x0D, y) << 8) ^
			((uint32_t)MUL(0x09, y) << 16) ^
			((uint32_t)MUL(0x0E, y) << 24);

		RT0[i] &= 0xFFFFFFFF;

		RT1[i] = ROTR8(RT0[i]);
		RT2[i] = ROTR8(RT1[i]);
		RT3[i] = ROTR8(RT2[i]);
	}
}

/* platform-independant 32-bit integer manipulation macros */

#define GET_UINT32(n,b,i)                       \
{                                               \
    (n) = ( (uint32_t) (b)[(i)    ] << 24 )       \
        | ( (uint32_t) (b)[(i) + 1] << 16 )       \
        | ( (uint32_t) (b)[(i) + 2] <<  8 )       \
        | ( (uint32_t) (b)[(i) + 3]       );      \
}

#define PUT_UINT32(n,b,i)                       \
{                                               \
    (b)[(i)    ] = (uint8_t) ( (n) >> 24 );       \
    (b)[(i) + 1] = (uint8_t) ( (n) >> 16 );       \
    (b)[(i) + 2] = (uint8_t) ( (n) >>  8 );       \
    (b)[(i) + 3] = (uint8_t) ( (n)       );       \
}

/* decryption key schedule tables */

int KT_init = 1;

uint32_t KT0[256];
uint32_t KT1[256];
uint32_t KT2[256];
uint32_t KT3[256];

/* AES key scheduling routine */


void AES::Encoder::Init(const uint8_t* pKey)
{
	int i;
	uint32_t *RK;

	[[maybe_unused]] static const bool s_bTablesReady = (aes_gen_tables(), true); // thread-safe one-time initialization

	RK = m_erk;

	for (i = 0; i < (s_KeyBits >> 5); i++)
	{
		GET_UINT32(RK[i], pKey, i * 4);
	}

	/* setup encryption round keys */

	for (i = 0; i < 7; i++, RK += 8)
	{
		RK[8] = RK[0] ^ RCON[i] ^
			(FSb[(uint8_t)(RK[7] >> 16)] << 24) ^
			(FSb[(uint8_t)(RK[7] >> 8)] << 16) ^
			(FSb[(uint8_t)(RK[7])] << 8) ^
			(FSb[(uint8_t)(RK[7] >> 24)]);

		RK[9] = RK[1] ^ RK[8];
		RK[10] = RK[2] ^ RK[9];
		RK[11] = RK[3] ^ RK[10];

		RK[12] = RK[4] ^
			(FSb[(uint8_t)(RK[11] >> 24)] << 24) ^
			(FSb[(uint8_t)(RK[11] >> 16)] << 16) ^
			(FSb[(uint8_t)(RK[11] >> 8)] << 8) ^
			(FSb[(uint8_t)(RK[11])]);

		RK[13] = RK[5] ^ RK[12];
		RK[14] = RK[6] ^ RK[13];
		RK[15] = RK[7] ^ RK[14];
	}

	// the round keys are big-endian words
	for (i = 0; i < (Nr + 1) * 4; i++)
		PUT_UINT32(m_erk[i], m_pRkBytes, i * 4);
}

bool AES::Encoder::IsHw()
{
#ifdef BEAM_AES_HW_ARM
	return true;
#else
	return false;
#endif
}

void AES::Encoder::Proceed(uint8_t* pDst, const uint8_t* pSrc) const
{
#ifdef BEAM_AES_HW_ARM
	// AESE: AddRoundKey, SubBytes, ShiftRows. AESMC: MixColumns. The last round has no MixColumns, and is followed by AddRoundKey
	uint8x16_t x = vld1q_u8(pSrc);

	for (int iRound = 0; iRound < Nr - 1; iRound++)
		x = vaesmcq_u8(vaeseq_u8(x, vld1q_u8(m_pRkBytes + iRound * s_BlockSize)));

	x = vaeseq_u8(x, vld1q_u8(m_pRkBytes + (Nr - 1) * s_BlockSize));
	x = veorq_u8(x, vld1q_u8(m_pRkBytes + Nr * s_BlockSize));

	vst1q_u8(pDst, x);
#else
	ProceedSw(pDst, pSrc);
#endif
}

void AES::Decoder::Init(const Encoder& enc)
{
	int i;
	// setup decryption round keys

	if (KT_init)
	{
		for (i = 0; i < 256; i++)
		{
			KT0[i] = RT0[FSb[i]];
			KT1[i] = RT1[FSb[i]];
			KT2[i] = RT2[FSb[i]];
			KT3[i] = RT3[FSb[i]];
		}

		KT_init = 0;
	}

	uint32_t* SK = m_drk;
	const uint32_t* RK = enc.m_erk + _countof(enc.m_erk) - 8;

	*SK++ = *RK++;
	*SK++ = *RK++;
	*SK++ = *RK++;
	*SK++ = *RK++;

	for (i = 1; i < Nr; i++)
	{
		RK -= 8;

		*SK++ = KT0[(uint8_t)(*RK >> 24)] ^
			KT1[(uint8_t)(*RK >> 16)] ^
			KT2[(uint8_t)(*RK >> 8)] ^
			KT3[(uint8_t)(*RK)]; RK++;

		*SK++ = KT0[(uint8_t)(*RK >> 24)] ^
			KT1[(uint8_t)(*RK >> 16)] ^
			KT2[(uint8_t)(*RK >> 8)] ^
			KT3[(uint8_t)(*RK)]; RK++;

		*SK++ = KT0[(uint8_t)(*RK >> 24)] ^
			KT1[(uint8_t)(*RK >> 16)] ^
			KT2[(uint8_t)(*RK >> 8)] ^
			KT3[(uint8_t)(*RK)]; RK++;

		*SK++ = KT0[(uint8_t)(*RK >> 24)] ^
			KT1[(uint8_t)(*RK >> 16)] ^
			KT2[(uint8_t)(*RK >> 8)] ^
			KT3[(uint8_t)(*RK)]; RK++;
	}

	RK -= 8;

	*SK++ = *RK++;
	*SK++ = *RK++;
	*SK++ = *RK++;
	*SK++ = *RK++;
}


/* AES 128-bit block encryption routine */

void AES::Encoder::ProceedSw(uint8_t* pDst, const uint8_t* pSrc) const
{
	uint32_t X0, X1, X2, X3, Y0, Y1, Y2, Y3;

	const uint32_t* RK = m_erk;

	GET_UINT32(X0, pSrc, 0); X0 ^= RK[0];
	GET_UINT32(X1, pSrc, 4); X1 ^= RK[1];
	GET_UINT32(X2, pSrc, 8); X2 ^= RK[2];
	GET_UINT32(X3, pSrc, 12); X3 ^= RK[3];

#define AES_FROUND(X0,X1,X2,X3,Y0,Y1,Y2,Y3)     \
{                                               \
    RK += 4;                                    \
                                                \
    X0 = RK[0] ^ FT0[ (uint8_t) ( Y0 >> 24 ) ] ^  \
                 FT1[ (uint8_t) ( Y1 >> 16 ) ] ^  \
                 FT2[ (uint8_t) ( Y2 >>  8 ) ] ^  \
                 FT3[ (uint8_t) ( Y3       ) ];   \
                                                \
    X1 = RK[1] ^ FT0[ (uint8_t) ( Y1 >> 24 ) ] ^  \
                 FT1[ (uint8_t) ( Y2 >> 16 ) ] ^  \
                 FT2[ (uint8_t) ( Y3 >>  8 ) ] ^  \
                 FT3[ (uint8_t) ( Y0       ) ];   \
                                                \
    X2 = RK[2] ^ FT0[ (uint8_t) ( Y2 >> 24 ) ] ^  \
                 FT1[ (uint8_t) ( Y3 >> 16 ) ] ^  \
                 FT2[ (uint8_t) ( Y0 >>  8 ) ] ^  \
                 FT3[ (uint8_t) ( Y1       ) ];   \
                                                \
    X3 = RK[3] ^ FT0[ (uint8_t) ( Y3 >> 24 ) ] ^  \
                 FT1[ (uint8_t) ( Y0 >> 16 ) ] ^  \
                 FT2[ (uint8_t) ( Y1 >>  8 ) ] ^  \
                 FT3[ (uint8_t) ( Y2       ) ];   \
}

	AES_FROUND(Y0, Y1, Y2, Y3, X0, X1, X2, X3);	// round 1
	AES_FROUND(X0, X1, X2, X3, Y0, Y1, Y2, Y3);	// round 2
	AES_FROUND(Y0, Y1, Y2, Y3, X0, X1, X2, X3);	// round 3
	AES_FROUND(X0, X1, X2, X3, Y0, Y1, Y2, Y3);	// round 4
	AES_FROUND(Y0, Y1, Y2, Y3, X0, X1, X2, X3);	// round 5
	AES_FROUND(X0, X1, X2, X3, Y0, Y1, Y2, Y3);	// round 6
	AES_FROUND(Y0, Y1, Y2, Y3, X0, X1, X2, X3);	// round 7
	AES_FROUND(X0, X1, X2, X3, Y0, Y1, Y2, Y3);	// round 8
	AES_FROUND(Y0, Y1, Y2, Y3, X0, X1, X2, X3);	// round 9
	AES_FROUND(X0, X1, X2, X3, Y0, Y1, Y2, Y3);	// round 10
	AES_FROUND(Y0, Y1, Y2, Y3, X0, X1, X2, X3);	// round 11
	AES_FROUND(X0, X1, X2, X3, Y0, Y1, Y2, Y3);	// round 12
	AES_FROUND(Y0, Y1, Y2, Y3, X0, X1, X2, X3);	// round 13

	/* last round */

	RK += 4;

	X0 = RK[0] ^ (FSb[(uint8_t)(Y0 >> 24)] << 24) ^
		(FSb[(uint8_t)(Y1 >> 16)] << 16) ^
		(FSb[(uint8_t)(Y2 >> 8)] << 8) ^
		(FSb[(uint8_t)(Y3)]);

	X1 = RK[1] ^ (FSb[(uint8_t)(Y1 >> 24)] << 24) ^
		(FSb[(uint8_t)(Y2 >> 16)] << 16) ^
		(FSb[(uint8_t)(Y3 >> 8)] << 8) ^
		(FSb[(uint8_t)(Y0)]);

	X2 = RK[2] ^ (FSb[(uint8_t)(Y2 >> 24)] << 24) ^
		(FSb[(uint8_t)(Y3 >> 16)] << 16) ^
		(FSb[(uint8_t)(Y0 >> 8)] << 8) ^
		(FSb[(uint8_t)(Y1)]);

	X3 = RK[3] ^ (FSb[(uint8_t)(Y3 >> 24)] << 24) ^
		(FSb[(uint8_t)(Y0 >> 16)] << 16) ^
		(FSb[(uint8_t)(Y1 >> 8)] << 8) ^
		(FSb[(uint8_t)(Y2)]);

	PUT_UINT32(X0, pDst, 0);
	PUT_UINT32(X1, pDst, 4);
	PUT_UINT32(X2, pDst, 8);
	PUT_UINT32(X3, pDst, 12);
}


/* AES 128-bit block decryption routine */

void AES::Decoder::Proceed(uint8_t* pDst, const uint8_t* pSrc) const
{
	uint32_t X0, X1, X2, X3, Y0, Y1, Y2, Y3;

	const uint32_t* RK = m_drk;

	GET_UINT32(X0, pSrc, 0); X0 ^= RK[0];
	GET_UINT32(X1, pSrc, 4); X1 ^= RK[1];
	GET_UINT32(X2, pSrc, 8); X2 ^= RK[2];
	GET_UINT32(X3, pSrc, 12); X3 ^= RK[3];

#define AES_RROUND(X0,X1,X2,X3,Y0,Y1,Y2,Y3)     \
{                                               \
    RK += 4;                                    \
                                                \
    X0 = RK[0] ^ RT0[ (uint8_t) ( Y0 >> 24 ) ] ^  \
                 RT1[ (uint8_t) ( Y3 >> 16 ) ] ^  \
                 RT2[ (uint8_t) ( Y2 >>  8 ) ] ^  \
                 RT3[ (uint8_t) ( Y1       ) ];   \
                                                \
    X1 = RK[1] ^ RT0[ (uint8_t) ( Y1 >> 24 ) ] ^  \
                 RT1[ (uint8_t) ( Y0 >> 16 ) ] ^  \
                 RT2[ (uint8_t) ( Y3 >>  8 ) ] ^  \
                 RT3[ (uint8_t) ( Y2       ) ];   \
                                                \
    X2 = RK[2] ^ RT0[ (uint8_t) ( Y2 >> 24 ) ] ^  \
                 RT1[ (uint8_t) ( Y1 >> 16 ) ] ^  \
                 RT2[ (uint8_t) ( Y0 >>  8 ) ] ^  \
                 RT3[ (uint8_t) ( Y3       ) ];   \
                                                \
    X3 = RK[3] ^ RT0[ (uint8_t) ( Y3 >> 24 ) ] ^  \
                 RT1[ (uint8_t) ( Y2 >> 16 ) ] ^  \
                 RT2[ (uint8_t) ( Y1 >>  8 ) ] ^  \
                 RT3[ (uint8_t) ( Y0       ) ];   \
}

	AES_RROUND(Y0, Y1, Y2, Y3, X0, X1, X2, X3);       /* round 1 */
	AES_RROUND(X0, X1, X2, X3, Y0, Y1, Y2, Y3);       /* round 2 */
	AES_RROUND(Y0, Y1, Y2, Y3, X0, X1, X2, X3);       /* round 3 */
	AES_RROUND(X0, X1, X2, X3, Y0, Y1, Y2, Y3);       /* round 4 */
	AES_RROUND(Y0, Y1, Y2, Y3, X0, X1, X2, X3);       /* round 5 */
	AES_RROUND(X0, X1, X2, X3, Y0, Y1, Y2, Y3);       /* round 6 */
	AES_RROUND(Y0, Y1, Y2, Y3, X0, X1, X2, X3);       /* round 7 */
	AES_RROUND(X0, X1, X2, X3, Y0, Y1, Y2, Y3);       /* round 8 */
	AES_RROUND(Y0, Y1, Y2, Y3, X0, X1, X2, X3);       /* round 9 */

	AES_RROUND(X0, X1, X2, X3, Y0, Y1, Y2, Y3);   /* round 10 */
	AES_RROUND(Y0, Y1, Y2, Y3, X0, X1, X2, X3);   /* round 11 */

	AES_RROUND(X0, X1, X2, X3, Y0, Y1, Y2, Y3);   /* round 12 */
	AES_RROUND(Y0, Y1, Y2, Y3, X0, X1, X2, X3);   /* round 13 */

	/* last round */

	RK += 4;

	X0 = RK[0] ^ (RSb[(uint8_t)(Y0 >> 24)] << 24) ^
		(RSb[(uint8_t)(Y3 >> 16)] << 16) ^
		(RSb[(uint8_t)(Y2 >> 8)] << 8) ^
		(RSb[(uint8_t)(Y1)]);

	X1 = RK[1] ^ (RSb[(uint8_t)(Y1 >> 24)] << 24) ^
		(RSb[(uint8_t)(Y0 >> 16)] << 16) ^
		(RSb[(uint8_t)(Y3 >> 8)] << 8) ^
		(RSb[(uint8_t)(Y2)]);

	X2 = RK[2] ^ (RSb[(uint8_t)(Y2 >> 24)] << 24) ^
		(RSb[(uint8_t)(Y1 >> 16)] << 16) ^
		(RSb[(uint8_t)(Y0 >> 8)] << 8) ^
		(RSb[(uint8_t)(Y3)]);

	X3 = RK[3] ^ (RSb[(uint8_t)(Y3 >> 24)] << 24) ^
		(RSb[(uint8_t)(Y2 >> 16)] << 16) ^
		(RSb[(uint8_t)(Y1 >> 8)] << 8) ^
		(RSb[(uint8_t)(Y0)]);

	PUT_UINT32(X0, pDst, 0);
	PUT_UINT32(X1, pDst, 4);
	PUT_UINT32(X2, pDst, 8);
	PUT_UINT32(X3, pDst, 12);
}


void AES::StreamCipher::Reset()
{
	m_nBuf = 0;
	m_Counter = beam::Zero;
}

void AES::StreamCipher::PerfXor(uint8_t* pBuf, uint32_t nSize)
{
	assert(m_nBuf >= nSize);

	memxor(pBuf, m_pBuf + _countof(m_pBuf) - m_nBuf, nSize);
	m_nBuf -= (uint8_t) nSize;
}

void AES::StreamCipher::XCrypt(const Encoder& enc, uint8_t* pBuf, uint32_t nSize)
{
	while (true)
	{
		if (!m_nBuf)
		{
			enc.Proceed(m_pBuf, m_Counter.m_pData);
			m_nBuf = _countof(m_pBuf);
			m_Counter.Inc();
		}

		if (m_nBuf >= nSize)
		{
			PerfXor(pBuf, nSize);
			break;
		}

		uint8_t n = m_nBuf;
		PerfXor(pBuf, m_nBuf);

		pBuf += n;
		nSize -= n;
	}
}
