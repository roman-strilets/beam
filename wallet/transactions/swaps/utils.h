// Copyright 2019 The Beam Team
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

#ifdef BEAM_ATOMIC_SWAP_SUPPORT
#include "wallet/transactions/swaps/swap_transaction.h"
#include "wallet/transactions/swaps/common.h"
#include "wallet/core/wallet.h"
#endif // BEAM_ATOMIC_SWAP_SUPPORT
namespace beam::wallet
{
#ifdef BEAM_ATOMIC_SWAP_SUPPORT
const char* getSwapTxStatus(AtomicSwapTransaction::State state);

TxParameters InitNewSwap(
    const WalletID& myID, Height minHeight, Amount amount,
    Amount fee, AtomicSwapCoin swapCoin, Amount swapAmount, Amount swapFee,
    bool isBeamSide = true, Height lifetime = kDefaultTxLifetime,
    Height responseTime = kDefaultTxResponseTime);

void RegisterSwapTxCreators(Wallet::Ptr wallet, IWalletDB::Ptr walletDB);

Amount GetSwapFeeRate(IWalletDB::Ptr walletDB, AtomicSwapCoin swapCoin);
bool IsSwapAmountValid(
    AtomicSwapCoin swapCoin, Amount swapAmount, Amount swapFeeRate);
#endif // BEAM_ATOMIC_SWAP_SUPPORT
} // namespace beam::wallet
