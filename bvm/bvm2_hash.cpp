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

#if defined(__ANDROID__) || !defined(BEAM_USE_AVX)
#include "crypto/blake/ref/blake2.h"
#else
#include "crypto/blake/sse/blake2.h"
#endif

#include "../core/crypto/keccak.h"

namespace beam {
namespace bvm2 {

	namespace Impl
	{
		namespace Env {
			void Memcpy(void* pDst, const void* pSrc, uint32_t n) {
				memmove(pDst, pSrc, n);
			}
			void Memset(void* pDst, char c, uint32_t n) {
				memset(pDst, c, n);
			}
			uint8_t Memis0(const void* p, uint32_t n) {
				return !!memis0(p, n);
			}
		}

		namespace Utils {
			template <typename T>
			inline T FromLE(T x) {
				return beam::ByteOrder::from_le(x);
			}
		}

		struct HashProcessor {

			struct Blake2b_Base
			{
				blake2b_state m_State;

				bool Init(const void* pPersonal, uint32_t nPersonal, uint32_t nResultSize)
				{
					blake2b_param pars = { 0 };
					pars.digest_length = static_cast<uint8_t>(nResultSize);
					pars.fanout = 1;
					pars.depth = 1;

					memcpy(pars.personal, pPersonal, std::min<size_t>(sizeof(pars.personal), nPersonal));

					return !blake2b_init_param(&m_State, &pars);
				}

				void Write(const void* p, uint32_t n)
				{
					blake2b_update(&m_State, p, n);
				}

				uint32_t Read(void* p, uint32_t n)
				{
					if (blake2b_final(&m_State, p, n))
						return 0;

					assert(m_State.outlen <= n);

					return static_cast<uint32_t>(m_State.outlen);
				}

				template <typename T>
				void operator >> (T& res) {
					Read(&res, sizeof(res));

				}
			};

			struct Blake2b
				:public Blake2b_Base
			{
				Blake2b(const void* pPersonal, uint32_t nPersonal, uint32_t nResultSize)
				{
					BEAM_VERIFY(Init(pPersonal, nPersonal, nResultSize)); // should not fail with internally specified params
				}
			};
		};

#include "Shaders/BeamHashIII.h"
	}

	template<>
	Wasm::Word ProcessorPlus::ToMethod<Wasm::Word, HashObj*>(HashObj* p)
	{
		return static_cast<Wasm::Word>(reinterpret_cast<size_t>(p));
	}

	struct Processor::DataProcessor::Instance
	{
		template <uint32_t nBytes>
		struct FixedResSize
			:public Base
		{
			typedef uintBig_t<nBytes> uintBig;

			virtual void Read(uintBig&) = 0;

			uint32_t Read(uint8_t* p, uint32_t n) override
			{
				if (n >= nBytes)
				{
					Read(*reinterpret_cast<uintBig*>(p));
					return nBytes;
				}

				uintBig hv;
				Read(hv);

				memcpy(p, hv.m_pData, n);
				return n;
			}
		};

		struct Sha256
			:public FixedResSize<ECC::Hash::Value::nBytes>
		{
			ECC::Hash::Processor m_Hp;

			virtual ~Sha256() {}
			void Write(const uint8_t* p, uint32_t n) override
			{
				m_Hp << Blob(p, n);
			}
			void Read(uintBig& hv) override
			{
				ECC::Hash::Processor(m_Hp) >> hv;
			}
			void Clone(std::unique_ptr<Base>& pOut) const override
			{
				pOut = std::make_unique<Sha256>();
				Cast::Up<Sha256>(*pOut).m_Hp = m_Hp;
			}
		};

		struct Blake2b
			:public Base
		{
			bvm2::Impl::HashProcessor::Blake2b_Base m_B2b;

			virtual ~Blake2b() {}
			void Write(const uint8_t* p, uint32_t n) override
			{
				m_B2b.Write(p, n);
			}
			uint32_t Read(uint8_t* p, uint32_t n) override
			{
				auto s = m_B2b; // copy
				return s.Read(p, n);
			}
			void Clone(std::unique_ptr<Base>& pOut) const override
			{
				pOut = std::make_unique<Blake2b>();
				Cast::Up<Blake2b>(*pOut).m_B2b = m_B2b;
			}
		};

		template <uint32_t nBits>
		struct Keccak
			:public FixedResSize<nBits / 8>
		{
			KeccakProcessor<nBits> m_State;

			void Write(const uint8_t* pSrc, uint32_t nSrc) override
			{
				m_State.Write(pSrc, nSrc);
			}

			void Read(uintBig_t<nBits/8>& hv) override
			{
				auto s = m_State; // copy
				s.Read(hv.m_pData);
			}
			void Clone(std::unique_ptr<Base>& pOut) const override
			{
				pOut = std::make_unique<Keccak>();
				Cast::Up<Keccak>(*pOut).m_State = m_State;
			}
		};

	};

	Processor::DataProcessor::Base& Processor::DataProcessor::FindStrict(uint32_t key)
	{
		auto it = m_Map.find(key, Base::Comparator());
		Exc::Test(m_Map.end() != it);
		return *it;
	}

	Processor::DataProcessor::Base& Processor::DataProcessor::FindStrict(HashObj* pObj)
	{
		return FindStrict(static_cast<uint32_t>(reinterpret_cast<size_t>(pObj)));
	}

	uint32_t Processor::AddHash(std::unique_ptr<DataProcessor::Base>&& p)
	{
		if ((Kind::Contract == get_Kind()) && (m_DataProcessor.m_Map.size() >= Limits::HashObjects))
			return 0;

		DischargeUnits(Limits::Cost::HashOp);

		uint32_t ret = m_DataProcessor.m_Map.empty() ? 1 : (m_DataProcessor.m_Map.rbegin()->m_Key + 1);
		p->m_Key = ret;
		m_DataProcessor.m_Map.insert(*p.release());

		return ret;
	}

	uint32_t Processor::CloneHash(const DataProcessor::Base& x)
	{
		std::unique_ptr<DataProcessor::Base> pHash;
		x.Clone(pHash);
		return AddHash(std::move(pHash));
	}

	BVM_METHOD(HashCreateSha256)
	{
		auto pRet = std::make_unique<DataProcessor::Instance::Sha256>();
		return AddHash(std::move(pRet));
	}

	BVM_METHOD_HOST(HashCreateSha256)
	{
		auto val = ProcessorPlus::From(*this).OnMethod_HashCreateSha256();
		return val ? reinterpret_cast<HashObj*>(static_cast<size_t>(val)) : nullptr;
	}

	BVM_METHOD(HashCreateBlake2b)
	{
		auto val = ProcessorPlus::From(*this).OnHost_HashCreateBlake2b(get_AddrR(pPersonal, nPersonal), nPersonal, nResultSize);
		return val ? static_cast<uint32_t>(reinterpret_cast<size_t>(val)) : 0;
	}

	BVM_METHOD_HOST(HashCreateBlake2b)
	{
		auto pRet = std::make_unique<DataProcessor::Instance::Blake2b>();
		if (!pRet->m_B2b.Init(pPersonal, nPersonal, nResultSize))
			return nullptr;

		auto val = AddHash(std::move(pRet));
		return val ? reinterpret_cast<HashObj*>(static_cast<size_t>(val)) : nullptr;
	}

	BVM_METHOD(HashCreateKeccak)
	{
		switch (nBits)
		{
		case 256:
			return AddHash(std::make_unique<DataProcessor::Instance::Keccak<256> >());
		case 384:
			return AddHash(std::make_unique<DataProcessor::Instance::Keccak<384> >());
		case 512:
			return AddHash(std::make_unique<DataProcessor::Instance::Keccak<512> >());
		}

		return 0;
	}

	BVM_METHOD_HOST(HashCreateKeccak)
	{
		auto val = ProcessorPlus::From(*this).OnMethod_HashCreateKeccak(nBits);
		return val ? reinterpret_cast<HashObj*>(static_cast<size_t>(val)) : nullptr;
	}

	BVM_METHOD(HashWrite)
	{
		DischargeUnits(Limits::Cost::HashWrite + Limits::Cost::HashWritePerByte * size);

		m_DataProcessor.FindStrict(pHash).Write(get_AddrR(p, size), size);
	}

	BVM_METHOD_HOST(HashWrite)
	{
		m_DataProcessor.FindStrict(pHash).Write(reinterpret_cast<const uint8_t*>(p), size);
	}

	BVM_METHOD(HashGetValue)
	{
		DischargeUnits(Limits::Cost::HashOp + Limits::Cost::HashWritePerByte * size);

		uint8_t* pDst_ = get_AddrW(pDst, size);
		uint32_t n = m_DataProcessor.FindStrict(pHash).Read(pDst_, size);
		memset0(pDst_ + n, size - n);
	}

	BVM_METHOD_HOST(HashGetValue)
	{
		uint8_t* pDst_ = reinterpret_cast<uint8_t*>(pDst);
		uint32_t n = m_DataProcessor.FindStrict(pHash).Read(pDst_, size);
		memset0(pDst_ + n, size - n);
	}

	BVM_METHOD(HashFree)
	{
		m_DataProcessor.m_Map.Delete(m_DataProcessor.FindStrict(pHash));
	}

	BVM_METHOD_HOST(HashFree)
	{
		m_DataProcessor.m_Map.Delete(m_DataProcessor.FindStrict(pHash));
	}

	BVM_METHOD(HashClone)
	{
		if (Kind::Contract == get_Kind())
			Exc::Test(IsPastFork_<4>());

		return CloneHash(m_DataProcessor.FindStrict(pHash));
	}
	BVM_METHOD_HOST(HashClone)
	{
		auto val = CloneHash(m_DataProcessor.FindStrict(pHash));
		return val ? reinterpret_cast<HashObj*>(static_cast<size_t>(val)) : nullptr;
	}

	BVM_METHOD(VerifyBeamHashIII)
	{
		DischargeUnits(Limits::Cost::BeamHashIII);

		return OnHost_VerifyBeamHashIII(
			get_AddrR(pInp, nInp), nInp,
			get_AddrR(pNonce, nNonce), nNonce,
			get_AddrR(pSol, nSol), nSol);
	}
	BVM_METHOD_HOST(VerifyBeamHashIII)
	{
		return !!Impl::BeamHashIII::Verify(pInp, nInp, pNonce, nNonce, (const uint8_t*) pSol, nSol);
	}

//	BVM_METHOD(get_EthMixHash)
//	{
//		DischargeUnits(Limits::Cost::EthMixHash);
//		return OnHost_get_EthMixHash(get_AddrAsW<HashValue>(hv), iEpoch, get_AddrAsR<HashValue512>(hvSeed));
//	}
//	BVM_METHOD_HOST(get_EthMixHash)
//	{
//		// ban ridiculously high epoch numbers, which may consume too much memory, cause overflows, etc.
//		// Our current limit is 1000. The memory size of this max epoch is ~147MB.
//		// This will be enough up to ethereum block 3mln, which is expected to appear on September 2029. (more than 8 years from now).
//		if (iEpoch > 1000)
//			return 0;
//
//		Blob blob;
//		ZeroObject(blob);
//
//		if (!LoadEthContext(blob, iEpoch))
//			return 0;
//
//#pragma pack (push, 1)
//
//		struct Hdr
//		{
//			uint32_t m_ItemsLight_LE;
//			uint32_t m_ItemsFull_LE;
//		};
//
//#pragma pack (pop)
//
//		struct Guard {
//			ethash_epoch_context* m_pVal = nullptr;
//			~Guard()
//			{
//				if (m_pVal)
//					ethash_destroy_epoch_context(m_pVal);
//			}
//		} g;
//
//		ethash_epoch_context ctx = { 0 };
//
//		if (!blob.n)
//		{
//			g.m_pVal = ethash_create_epoch_context(iEpoch);
//			if (!g.m_pVal)
//				Wasm::Fail("no mem");
//
//			memcpy(reinterpret_cast<void*>(&ctx), g.m_pVal, sizeof(ctx));
//		}
//		else
//		{
//			Wasm::Test(blob.n >= sizeof(Hdr));
//			auto pHdr = reinterpret_cast<const Hdr*>(blob.p);
//
//			ethash_epoch_context ctxInst2 = ethash_epoch_context{
//				static_cast<int>(iEpoch),
//				static_cast<int>(ByteOrder::from_le(pHdr->m_ItemsLight_LE)),
//				reinterpret_cast<const ethash_hash512*>(reinterpret_cast<const uint8_t*>(blob.p) + sizeof(Hdr)),
//				nullptr,
//				static_cast<int>(ByteOrder::from_le(pHdr->m_ItemsFull_LE)) };
//
//			memcpy(reinterpret_cast<void*>(&ctx), &ctxInst2, sizeof(ctx));
//		}
//
//		uint32_t nSizeBuf = sizeof(Hdr) + ctx.light_cache_num_items * sizeof(HashValue512);
//		Wasm::Test(!blob.n || (blob.n == nSizeBuf));
//
//		ethash_get_MixHash((ethash_hash256*) hv.m_pData, &ctx, (ethash_hash512*) hvSeed.m_pData);
//
//		if (!blob.n)
//		{
//			static_assert(sizeof(ctx) >= sizeof(Hdr)); // the context is allocated as ctx + cache at once. It's safe to overwrite the preceeding portion by our header
//			//assert(reinterpret_cast<const uint8_t*>(ctx.light_cache) == reinterpret_cast<const uint8_t*>(g.m_pVal + 1));
//
//			Hdr hdr1;
//			hdr1.m_ItemsFull_LE = ByteOrder::to_le(static_cast<uint32_t>(ctx.full_dataset_num_items));
//			hdr1.m_ItemsLight_LE = ByteOrder::to_le(static_cast<uint32_t>(ctx.light_cache_num_items));
//
//			Hdr& hdrRef = reinterpret_cast<Hdr*>(Cast::NotConst(ctx.light_cache))[-1];
//			TemporarySwap<Hdr> tmpSwap(hdr1, hdrRef);
//
//			blob.p = &hdrRef;
//			blob.n = nSizeBuf;
//
//			SaveEthContext(blob, iEpoch);
//		}
//
//		return 1;
//	}

} // namespace bvm2
} // namespace beam
