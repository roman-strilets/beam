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

#include "core/base/uintBig.h"

namespace beam
{
	// std::hash may not be specialized for std::array of built-in types
	struct ArrayHash
	{
	    template<class T, size_t N>
	    size_t operator() (const std::array<T, N>& key) const
	    {
	        std::hash<T> hasher;
	        size_t result = 0;
	        for(size_t i = 0; i < N; ++i)
	        {
	            result = (result << 1) ^ hasher(key[i]);
	        }
	        return result;
	    }
	};
}

namespace std
{
    template<>
    struct hash<ECC::uintBig>
    {
        size_t operator() (const ECC::uintBig& key) const noexcept
        {
            std::hash<uint8_t> hasher;
            size_t result = 0;
            for(size_t i = 0; i < ECC::uintBig::nBytes; ++i)
            {
                result = (result << 1) ^ hasher(key.m_pData[i]);
            }
            return result;
        }
    };
}
