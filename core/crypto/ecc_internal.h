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

// Internal header shared by the ecc*.cpp sources that implement ecc_native.h:
// the include list and the few symbols used by more than one of them.
#pragma once

#include "core/base/common.h"
#include "core/crypto/ecc_native.h"
#include "utility/common.h" // Exc

#if defined(__clang__) || defined(__GNUC__) || defined(__GNUG__)
#	pragma GCC diagnostic push
#	pragma GCC diagnostic ignored "-Wunused-function"
#else
#	pragma warning (push, 0) // suppress warnings from secp256k1
#	pragma warning (disable: 4706 4701) // assignment within conditional expression
#endif

#include "secp256k1/src/group_impl.h"
#include "secp256k1/src/scalar_impl.h"
#include "secp256k1/src/field_impl.h"
#include "secp256k1/src/hash_impl.h"
#include "secp256k1/src/int128_impl.h"

#if defined(__clang__) || defined(__GNUC__) || defined(__GNUG__)
#	pragma GCC diagnostic pop
#else
#	pragma warning (default: 4706 4701)
#	pragma warning (pop)
#endif

namespace ECC {

	// Defined here so that it gets inlined into the constant-time table scans: as an
	// external call it slows down G.Multiply by 19% and signature.Sign by 12%.
	template <typename T>
	void data_cmov_as(T* pDst, const T* pSrc, int nWords, int flag)
	{
		const T mask0 = flag + ~((T)0);
		const T mask1 = ~mask0;

		for (int n = 0; n < nWords; n++)
			pDst[n] = (pDst[n] & mask0) | (pSrc[n] & mask1);
	}

	extern thread_local Mode::Enum g_Mode; // ecc.cpp

	namespace Generator
	{
		void CreatePointNnz(Point::Native& out, Oracle& oracle, Hash::Processor* phpRes); // ecc_generator.cpp
	}

} // namespace ECC
