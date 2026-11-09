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

// Internal header shared by the wallet_client*.cpp sources: the include list
// and the constants used by more than one of them.
//
// WalletClient methods implementing IWalletModelAsync are called in the background
// thread and can safely access the wallet DB.
#pragma once

#include "wallet_client.h"
#include "wallet/core/tx/simple_transaction.h"
#include "wallet/core/slatepack/slatepack_endpoint.h"
#ifdef BEAM_ASSET_SWAP_SUPPORT
#include "wallet/transactions/dex/dex_tx.h"
#endif  // BEAM_ASSET_SWAP_SUPPORT
#include "utility/log_rotation.h"
#include "http/http_client.h"
#include "core/storage/block_rw.h"
#include "wallet/core/common/common_utils.h"
#include "extensions/broadcast_gateway/broadcast_router.h"
#include "extensions/export/tx_history_to_csv.h"
#include "extensions/news_channels/wallet_updates_provider.h"
#include "extensions/news_channels/exchange_rate_provider.h"
#include "utility/fsutils.h"
#ifdef BEAM_LELANTUS_SUPPORT
#include "wallet/transactions/lelantus/push_transaction.h"
#endif // BEAM_LELANTUS_SUPPORT

#include "filter.h"
#include <regex>

using namespace std;

namespace beam::wallet
{
    constexpr size_t kShieldedPer24hFilterSize = 20;
}
