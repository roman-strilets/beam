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

// Definitions shared by all parts of the tx builders implementation (base_tx_builder*.cpp).
// Must be the first include of each part. Not intended for use outside of them.

#include "base_tx_builder.h"
#include "core/consensus/block_crypt.h"
#include "core/consensus/shielded.h"
#include "base_transaction.h"
#include "wallet/core/common/strings_resources.h"
#include "utility/executor.h"

// TODO: getrandom not available until API 28 in the Android NDK 17b
// https://github.com/boostorg/uuid/issues/76
#if defined(__ANDROID__)
#define BOOST_UUID_RANDOM_PROVIDER_DISABLE_GETRANDOM 1
#endif

#include <boost/uuid/uuid_generators.hpp>
#include <numeric>
#include "utility/logger.h"

namespace beam::wallet
{
    using namespace ECC;
    using namespace std;

    template <typename TDst, typename TSrc>
    void MoveIntoVec1(std::vector<TDst>& vDst, std::vector<TSrc>& vSrc)
    {
        std::move(vSrc.begin(), vSrc.end(), back_inserter(vDst));
    }

    template <typename T>
    void MoveIntoVec(std::vector<T>& vDst, std::vector<T>& vSrc)
    {
        if (vDst.empty())
            vDst = std::move(vSrc);
        else
            MoveIntoVec1(vDst, vSrc);
    }
}
