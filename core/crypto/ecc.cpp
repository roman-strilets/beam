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

#ifdef WIN32
#	pragma comment (lib, "Bcrypt.lib")
#else // WIN32
#    include <unistd.h>
#    include <fcntl.h>
#endif // WIN32

//#ifdef __linux__
//#	include <sys/syscall.h>
//#	include <linux/random.h>
//#endif // __linux__


namespace ECC {

	//void* NoErase(void*, size_t) { return NULL; }

	// Pointer to the 'eraser' function. The pointer should be non-const (i.e. variable that can be changed at run-time), so that optimizer won't remove this.
	void (*g_pfnEraseFunc)(void*, size_t) = memset0/*NoErase*/;

	void SecureErase(void* p, uint32_t n)
	{
		g_pfnEraseFunc(p, n);
	}

	template void data_cmov_as<uint32_t>(uint32_t* pDst, const uint32_t* pSrc, int nWords, int flag);

	thread_local Mode::Enum g_Mode = Mode::Secure; // default

	Mode::Scope::Scope(Mode::Enum val)
		:m_PrevMode(g_Mode)
	{
		g_Mode = val;
	}

	Mode::Scope::~Scope()
	{
		g_Mode = m_PrevMode;
	}

	std::ostream& operator << (std::ostream& s, const Scalar& x)
	{
		return operator << (s, x.m_Value);
	}

    std::ostream& operator << (std::ostream& s, const Scalar::Native& x)
    {
        Scalar scalar;
        x.Export(scalar);
        return operator << (s, scalar);
    }

	std::ostream& operator << (std::ostream& s, const Point& x)
	{
		return operator << (s, x.m_X);
	}

    std::ostream& operator << (std::ostream& s, const Point::Native& x)
    {
        Point point;

        x.Export(point);
        return operator << (s, point);
    }

	thread_local PseudoRandomGenerator* PseudoRandomGenerator::s_pOverride = nullptr;

	PseudoRandomGenerator::PseudoRandomGenerator()
	{
		ZeroObject(*this);
	}

	void PseudoRandomGenerator::Generate(void* p, uint32_t nSize)
	{
		while (true)
		{
			if (!m_Remaining)
			{
				Hash::Processor() << m_hv >> m_hv;
				m_Remaining = m_hv.nBytes;
			}

			if (m_Remaining >= nSize)
			{
				memcpy(p, m_hv.m_pData + m_hv.nBytes - m_Remaining, nSize);
				m_Remaining -= nSize;
				break;
			}

			memcpy(p, m_hv.m_pData + m_hv.nBytes - m_Remaining, m_Remaining);

			((uint8_t*&) p) += m_Remaining;
			nSize -= m_Remaining;

			m_Remaining = 0;
		}
	}

	void GenRandom(void* p, uint32_t nSize)
	{
		// checkpoint?

		if (PseudoRandomGenerator::s_pOverride)
		{
			PseudoRandomGenerator::s_pOverride->Generate(p, nSize);
			return;
		}

#ifdef WIN32

		NTSTATUS ntStatus = BCryptGenRandom(NULL, (PUCHAR) p, nSize, BCRYPT_USE_SYSTEM_PREFERRED_RNG);
		if (ntStatus)
			beam::ThrowSystemError(ntStatus);

#else // WIN32

		bool bRet = false;

//#	ifdef __linux__
//
//		ssize_t nRet = syscall(SYS_getrandom, p, nSize, 0);
//		bRet = (nRet == nSize);
//
//#	else // __linux__

		// use standard posix
		int hFile = open("/dev/urandom", O_RDONLY);
		if (hFile >= 0)
		{
			if (read(hFile, p, nSize) == static_cast<ssize_t>(nSize))
				bRet = true;

			close(hFile);
		}

//#	endif // __linux__

		if (!bRet)
			beam::ThrowLastError();

#endif // WIN32
	}

	/////////////////////
	// Curve constants
	// Initialized dynamically, and the initialization order is defined only within one
	// translation unit. Keep them here, ahead of g_Initializer, which builds the context.
	const uintBig Scalar::s_Order = { // fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141
		0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFE,
		0xBA,0xAE,0xDC,0xE6,0xAF,0x48,0xA0,0x3B,0xBF,0xD2,0x5E,0x8C,0xD0,0x36,0x41,0x41
	};

	const uintBig Point::s_FieldOrder = { // fffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2f
		0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFE,0xFF,0xFF,0xFC,0x2F
	};

	/////////////////////
	// Context
	AlignedBuf<Context> g_ContextBuf;

	// Currently - auto-init in global obj c'tor
	Initializer g_Initializer;

#ifndef NDEBUG
	bool g_bContextInitialized = false;
#endif // NDEBUG

	const Context& Context::get()
	{
		assert(g_bContextInitialized);
		return g_ContextBuf.get();
	}

	void InitializeContext()
	{
		Context& ctx = g_ContextBuf.get();

		Mode::Scope scope(Mode::Fast);

		Oracle oracle;
		oracle << "Let the generator generation begin!";

		Point::Compact::Converter cpc;

		// make sure we get the same G,H for different generator kinds
		Point::Native G_raw, H_raw, J_raw;

		secp256k1_gej_set_ge(&G_raw.get_Raw(), &secp256k1_ge_const_g);
		Point ptG;
		Point::Native::ExportEx(ptG, secp256k1_ge_const_g);

		Hash::Processor hpRes;
		hpRes << ptG;

		Generator::CreatePointNnz(H_raw, oracle, &hpRes);
		Generator::CreatePointNnz(J_raw, oracle, &hpRes);

		ctx.G.Initialize(G_raw, oracle, cpc);

		// for historical reasons
		for (uint32_t i = 0; i < 2; i++)
		{
			Point::Native ptDummy;
			Generator::CreatePointNnz(ptDummy, oracle, nullptr);
		}
		ctx.H.Initialize(H_raw, cpc);
		ctx.J.Initialize(J_raw, oracle, cpc);

		cpc.Flush();

		Point::Native pt, ptAux2(Zero);

		ctx.m_Ipp.G_.Initialize(G_raw, oracle, cpc);
		ctx.m_Ipp.H_.Initialize(H_raw, oracle, cpc);

		// for historical reasons: make a temp copy of oracle to initialize what was not present earlier
		Oracle o2 = oracle;
		o2 << "v1";

		ctx.m_Ipp.J_.Initialize(J_raw, o2, cpc);

		for (uint32_t i = 0; i < InnerProduct::nDim; i++)
		{
			for (uint32_t j = 0; j < 2; j++)
			{
				MultiMac::Prepared& p = ctx.m_Ipp.m_pGen_[j][i];
				p.Initialize(oracle, hpRes, cpc);

				// fast data is already flushed

				if (!j)
					ptAux2 += p;
			}
		}

		ptAux2 = -ptAux2;
		ctx.m_Ipp.m_Aux2_.Initialize(ptAux2, oracle, cpc);

		ctx.m_Ipp.m_GenDot_.Initialize(oracle, hpRes, cpc);

		const MultiMac::Prepared& genericNums = ctx.m_Ipp.m_GenDot_;
		ctx.m_Casual.m_Nums = genericNums.m_Fast.m_pPt[0]; // whatever

		{
			MultiMac_WithBufs<1, 1> mm;
			Scalar::Native& k = mm.m_Bufs.m_pKPrep[0];
			k = Zero;
			for (int i = ECC::nBits; i--; )
			{
				k = k + k;
				if (!(i % MultiMac::Casual::Secure::nBits))
					k = k + 1U;
			}

			k = -k;

			mm.m_Bufs.m_ppPrepared[0] = &ctx.m_Ipp.m_GenDot_;
			mm.m_Prepared = 1;

			mm.Calculate(pt);
			cpc.set_Deferred(ctx.m_Casual.m_Compensation, pt);
		}

		ctx.m_Ipp.m_2Inv.SetInv(2U);

		cpc.Flush();

		hpRes
			<< uint32_t(2) // increment this each time we change signature formula (rangeproof and etc.)
			>> ctx.m_hvChecksum;

		ctx.m_Sig.m_GenG.m_pGen_s = &ctx.G;
		ctx.m_Sig.m_GenG.m_pGen_f = nullptr;
		ctx.m_Sig.m_GenG.m_pGenPrep = &ctx.m_Ipp.G_;
		ctx.m_Sig.m_GenG.m_nBatchIdx = InnerProduct::BatchContext::s_Idx_G;

		ctx.m_Sig.m_CfgG1.m_nKeys = 1;
		ctx.m_Sig.m_CfgG1.m_nG = 1;
		ctx.m_Sig.m_CfgG1.m_pG = &ctx.m_Sig.m_GenG;

		ctx.m_Sig.m_pGenGJ[0] = ctx.m_Sig.m_GenG;

		ctx.m_Sig.m_pGenGJ[1].m_pGen_s = &ctx.J;
		ctx.m_Sig.m_pGenGJ[1].m_pGen_f = nullptr;
		ctx.m_Sig.m_pGenGJ[1].m_pGenPrep = &ctx.m_Ipp.J_;
		ctx.m_Sig.m_pGenGJ[1].m_nBatchIdx = InnerProduct::BatchContext::s_Idx_J;

		ctx.m_Sig.m_pGenGH[0] = ctx.m_Sig.m_GenG;

		ctx.m_Sig.m_pGenGH[1].m_pGen_s = nullptr;
		ctx.m_Sig.m_pGenGH[1].m_pGen_f = &ctx.H;
		ctx.m_Sig.m_pGenGH[1].m_pGenPrep = &ctx.m_Ipp.H_;
		ctx.m_Sig.m_pGenGH[1].m_nBatchIdx = InnerProduct::BatchContext::s_Idx_H;

		ctx.m_Sig.m_CfgGJ1.m_nKeys = 1;
		ctx.m_Sig.m_CfgGJ1.m_nG = 2;
		ctx.m_Sig.m_CfgGJ1.m_pG = ctx.m_Sig.m_pGenGJ;

		ctx.m_Sig.m_CfgG2.m_nKeys = 2;
		ctx.m_Sig.m_CfgG2.m_nG = 1;
		ctx.m_Sig.m_CfgG2.m_pG = &ctx.m_Sig.m_GenG;

		ctx.m_Sig.m_CfgGH2.m_nKeys = 2;
		ctx.m_Sig.m_CfgGH2.m_nG = 2;
		ctx.m_Sig.m_CfgGH2.m_pG = ctx.m_Sig.m_pGenGH;

#ifndef NDEBUG
		g_bContextInitialized = true;
#endif // NDEBUG
	}

} // namespace ECC
