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
#include "bvm2_internal.h"

namespace beam {
namespace bvm2 {

	void ProcessorManager::DerivePkInternal(ECC::Point::Native& res, const Blob& b)
	{
		ECC::Hash::Value hv;
		DeriveKeyPreimage(hv, b);

		Exc::Test(m_pPKdf != nullptr);
		m_pPKdf->DerivePKeyG(res, hv);
	}

	BVM_METHOD(DerivePk)
	{
		return OnHost_DerivePk(get_AddrAsW<PubKey>(pubKey), get_AddrR(pID, nID), nID);
	}
	BVM_METHOD_HOST(DerivePk)
	{
		ECC::Point::Native pt;
		DerivePkInternal(pt, Blob(pID, nID));
		pt.Export(pubKey);
	}

	BVM_METHOD(get_Pk)
	{
		DerivePkInternal(m_Secp.m_Point.FindStrict(res).m_Val, Blob(get_AddrR(pID, nID), nID));
	}
	BVM_METHOD_HOST(get_Pk)
	{
		DerivePkInternal(m_Secp.m_Point.FindStrict(Secp::Point::From(res)).m_Val, Blob(pID, nID));
	}

	void ProcessorManager::DeriveKeyPreimage(ECC::Hash::Value& hv, const Blob& b)
	{
		ECC::Hash::Processor()
			<< "bvm.m.key"
			<< b
			>> hv;
	}

	BVM_METHOD(get_PkEx)
	{
		ECC::Hash::Value hv;
		DeriveKeyPreimage(hv, Blob(get_AddrR(pID, nID), nID));
		get_DH_Internal(res, hv, gen);
	}
	BVM_METHOD_HOST(get_PkEx)
	{
		ECC::Hash::Value hv;
		DeriveKeyPreimage(hv, Blob(pID, nID));
		get_DH_Internal(Secp::Point::From(res), hv, Secp::Point::From(gen));
	}

	BVM_METHOD(get_SlotImage)
	{
		ECC::Hash::Value hv;
		get_SlotPreimageInternal(hv, iSlot);

		Exc::Test(m_pPKdf != nullptr);
		m_pPKdf->DerivePKeyG(m_Secp.m_Point.FindStrict(res).m_Val, hv);

	}
	BVM_METHOD_HOST_AUTO(get_SlotImage)

	void ProcessorManager::get_DH_Internal(uint32_t res, const ECC::Hash::Value& hv, uint32_t gen)
	{
		Exc::Test(m_pKdf != nullptr);
		ECC::Scalar::Native sk;
		m_pKdf->DeriveKey(sk, hv);

		// careful: res and gen may not be distinct
		ECC::Point::Native pt = m_Secp.m_Point.FindStrict(gen).m_Val * sk;
		m_Secp.m_Point.FindStrict(res).m_Val = pt;
	}

	BVM_METHOD(get_SlotImageEx)
	{
		ECC::Hash::Value hv;
		get_SlotPreimageInternal(hv, iSlot);

		get_DH_Internal(res, hv, gen);
	}
	BVM_METHOD_HOST_AUTO(get_SlotImageEx)

	BVM_METHOD(SlotInit)
	{
		OnHost_SlotInit(get_AddrR(pExtraSeed, nExtraSeed), nExtraSeed, iSlot);
	}

	BVM_METHOD_HOST(SlotInit)
	{
		Exc::Test(iSlot < Shaders::s_NonceSlots);

		ECC::Hash::Value hv;
		SlotRenegerateInternal(hv, iSlot, pExtraSeed, nExtraSeed);
	}

	BVM_METHOD(get_BlindSk)
	{
		get_BlindSkInternal(res, mul, iSlot, Blob(get_AddrR(pID, nID), nID));
	}
	BVM_METHOD_HOST(get_BlindSk)
	{
		get_BlindSkInternal(Secp::Scalar::From(res), Secp::Scalar::From(mul), iSlot, Blob(pID, nID));
	}

	void ProcessorManager::get_SlotPreimageInternal(ECC::Hash::Value& hv, uint32_t iSlot)
	{
		Exc::Test(iSlot < Shaders::s_NonceSlots);

		if (!SlotLoad(hv, iSlot))
			SlotRenegerateInternal(hv, iSlot, nullptr, 0);
	}

	void ProcessorManager::SlotRenegerateInternal(ECC::Hash::Value& hvRes, uint32_t iSlot, const void* pSeedExtra, uint32_t nSeedExtra)
	{
		ECC::GenRandom(hvRes);

		if (nSeedExtra)
		{
			ECC::Hash::Processor()
				<< "bvm.slot.seed"
				<< Blob(pSeedExtra, nSeedExtra)
				<< hvRes
				>> hvRes;
		}

		SlotSave(hvRes, iSlot);
	}

	void ProcessorManager::get_Sk(ECC::Scalar::Native& sk, const ECC::Hash::Value& hv)
	{
		Exc::Test(m_pKdf != nullptr);
		m_pKdf->DeriveKey(sk, hv);
	}

	void ProcessorManager::get_BlindSkInternal(uint32_t iRes, uint32_t iMul, uint32_t iSlot, const Blob& b)
	{
		ECC::Hash::Value hv;
		DeriveKeyPreimage(hv, b);

		// careful, iRes, iMul don't have to be distinct
		ECC::Scalar::Native sk;
		get_Sk(sk, hv);

		sk *= m_Secp.m_Scalar.FindStrict(iMul).m_Val;

		get_SlotPreimageInternal(hv, iSlot);
		SlotErase(iSlot); // important!

		auto& res = m_Secp.m_Scalar.FindStrict(iRes).m_Val;

		get_Sk(res, hv);
		res += sk;
	}

	uint8_t* ProcessorManager::ResizeAux(uint32_t nSize)
	{
		if (m_AuxAlloc.m_Size < nSize)
		{
			FreeAuxAllocGuarded();
			m_AuxAlloc.m_pPtr = ProcessorPlus::From(*this).OnMethod_Heap_Alloc(nSize);
			m_AuxAlloc.m_Size = nSize;
		}

		return get_AddrW(m_AuxAlloc.m_pPtr, nSize);
	}

	void ProcessorManager::FreeAuxAllocGuarded()
	{
		// may raise exc, call this only from within interpretator methods
		if (m_AuxAlloc.m_pPtr)
		{
			ProcessorPlus::From(*this).OnMethod_Heap_Free(m_AuxAlloc.m_pPtr);
			ZeroObject(m_AuxAlloc);
		}
	}

} // namespace bvm2
} // namespace beam
