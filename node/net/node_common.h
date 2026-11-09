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

// The include set shared by all parts of Node implementation.
// Must be the first include of each part: the macros below have to precede the logger header.
// Kept identical for all of them, so that each one sees the same serialization adapters.
#define LOG_DEBUG_ENABLED 1
#define HOST_BUILD

#include "node.h"
#include "core/consensus/serialization_adapters.h"
#include "core/net/proto.h"
#include "core/crypto/ecc_native.h"
#include "core/storage/block_rw.h"
#include "core/net/fly_client.h" // hdr pack decoding moved there

#include "p2p/protocol.h"
#include "p2p/connection.h"

#include "utility/io/tcpserver.h"
#include "utility/logger.h"
#include "utility/logger_checkpoints.h"

#include "bvm/bvm2.h"

#include "pow/external_pow.h"

#include <charconv>

namespace beam {

struct LogTip
{
	const NodeProcessor::Tip& m_Tip;
	LogTip(const NodeProcessor::Tip& x) :m_Tip(x) {}
};

inline std::ostream& operator << (std::ostream& os, const LogTip& x)
{
	if (!Rules::get().IsConstantSpan())
		os << x.m_Tip.m_Full.m_Number.v << "-";

	os << x.m_Tip.m_hh;
	return os;
}

} // namespace beam
