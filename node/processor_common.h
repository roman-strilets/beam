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

// The include set shared by all parts of NodeProcessor implementation.
// Kept identical for all of them, so that each one sees the same serialization adapters.
#include "processor.h"
#include "../core/consensus/treasury.h"
#include "../core/consensus/shielded.h"
#include "../bvm/bvm2.h"
#include "../bvm/evm.h"
#include "../core/consensus/serialization_adapters.h"
#include "../core/crypto/base58.h"
#include "../utility/serialize.h"
#include "../utility/logger.h"
#include "../utility/logger_checkpoints.h"
#include "../utility/blobmap.h"
#include <atomic>
#include <condition_variable>
#include <cctype>

namespace beam {

template <typename T>
bool IsBigger2(T a, T b1, T b2)
{
	b1 += b2;
	return (b1 >= b2) && (a > b1);
}

template <typename T>
bool IsBigger3(T a, T b1, T b2, T b3)
{
	b2 += b3;
	return (b2 >= b3) && IsBigger2(a, b1, b2);
}

} // namespace beam
