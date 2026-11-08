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
#include "common.h"
#include <bit>

namespace beam
{

	namespace ByteOrder
	{
		inline uint8_t bswap(uint8_t x) {
			return x;
		}

		inline uint16_t bswap(uint16_t x) {
#ifdef _MSC_VER
			return _byteswap_ushort(x);
#else
			return __builtin_bswap16(x);
#endif // _MSC_VER
		}

		inline uint32_t bswap(uint32_t x) {
#ifdef _MSC_VER
			static_assert(sizeof(uint32_t) == sizeof(unsigned long));
			return _byteswap_ulong(x);
#else
			return __builtin_bswap32(x);
#endif // _MSC_VER
		}

		inline uint64_t bswap(uint64_t x) {
#ifdef _MSC_VER
			return _byteswap_uint64(x);
#else
			return __builtin_bswap64(x);
#endif // _MSC_VER
		}

		template <typename T, bool bLE, bool bTo>
		inline T Convert(T x)
		{
			static_assert((std::endian::native == std::endian::little) || (std::endian::native == std::endian::big), "mixed endian is not supported");
			constexpr bool bNativeLE = (std::endian::native == std::endian::little);

			// for big/little endian the to/from flag doesn't matter
			// return x;
			if constexpr (bNativeLE == bLE)
				return x;
			else
				return bswap(x);
		}

		template <typename T> inline T to_le(T x) { return Convert<T, true, true>(x); }
		template <typename T> inline T to_be(T x) { return Convert<T, false, true>(x); }
		template <typename T> inline T from_le(T x) { return Convert<T, true, false>(x); }
		template <typename T> inline T from_be(T x) { return Convert<T, false, false>(x); }
	}
}
