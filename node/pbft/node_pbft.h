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

// Definitions shared by the parts of Node::Validator implementation.
#include "node/net/node_common.h"

namespace Shaders {
#	define HOST_BUILD
#	include "bvm/Shaders/common.h"
#	include "bvm/Shaders/pbft/i_pbft.h"
} // namespace Shaders

namespace beam {

#define PBFT_LOG(level, expr) BEAM_LOG_##level() << "pbft " << m_iRound << " " << expr
//#define PBFT_LOG(level, expr) std::cout << "PBFT " << m_iRound << " " << expr << std::endl

struct Node::Validator::Assessment::Settings
{
	static void Regress(uint32_t& score)
	{
		score -= (score + 15) / 16; // auto-regression
	}

	static const uint32_t NotProposed = 384;
	static const uint32_t NotRoundStart = 256;
	static const uint32_t NotVotedPre = 128;
	static const uint32_t NotVotedCommit = 96;
	static const uint32_t SuspendDh = 20;
	static const uint32_t SlashMaxLatency = 30;
	static const uint8_t MaxSlash = 4;

	struct Watermark
	{
		static const uint32_t Jail = 768;
		static const uint32_t Unjail = 150;
		static const uint32_t AcceptForMultisig = 50;
	};
};

template <typename TMsg>
void Node::Validator::Broadcast(const TMsg& msg, const Peer* pSrc) const
{
	auto& n = get_ParentObj();
	for (PeerList::iterator it = n.m_lstPeers.begin(); n.m_lstPeers.end() != it; ++it)
	{
		Peer& peer = *it;
		if ((&peer != pSrc) && ShouldSendTo(peer))
			peer.Send(msg);
	}
}

} // namespace beam
