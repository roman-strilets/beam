// Copyright 2019 The Beam Team;
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

// Definitions shared by the parts of the common.h implementation: common.cpp, common_to_string.cpp,
// common_tx_*.cpp, common_token_json.cpp and common_version.cpp.
// Must be the first include of each part. Not intended for use outside of them.

#include "common.h"
#include "utility/logger.h"
#include "core/crypto/ecc_native.h"
#include "core/crypto/base58.h"
#include "core/consensus/serialization_adapters.h"
#include "base58.h"
#include "utility/string_helpers.h"
#include "strings_resources.h"
#include "core/consensus/shielded.h"
#include "3rdparty/nlohmann/json.hpp"
#include "wallet_db.h"
#include "version.h"

#include <algorithm>
#include <iomanip>
#include <regex>
#include <boost/algorithm/string.hpp>

#include <boost/serialization/nvp.hpp>
#include <boost/multiprecision/cpp_dec_float.hpp>

using namespace std;
using namespace ECC;
using namespace beam;

namespace beam
{
    inline string SkipLeadingZeroes(const char* szPtr)
    {
        while (*szPtr == '0')
            szPtr++;

        if (!*szPtr)
            szPtr--; // leave at least 1 symbol

        return szPtr;
    }

    // skips leading zeroes
    template<typename T>
    string EncodeToHex(const T& v)
    {
        char szBuf[sizeof(v) * 2 + 1];
        beam::to_hex(szBuf, &v, sizeof(v));
        return SkipLeadingZeroes(szBuf);
    }

    inline string EncodeToHex(const ByteBuffer& v)
    {
        if (v.empty())
        {
            return {};
        }
        char* szBuf = (char*)alloca(2 * v.size() + 1);
        beam::to_hex(szBuf, &v[0], v.size());
        return SkipLeadingZeroes(szBuf);
    }
}  // namespace beam
