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

// Helpers shared by the .cpp files of core/consensus. Do not include from headers.

#pragma once
#include <memory>
#include <vector>

#define CMP_SIMPLE(a, b) \
		if (a < b) \
			return -1; \
		if (a > b) \
			return 1;

#define CMP_MEMBER(member) CMP_SIMPLE(member, v.member)

#define CMP_MEMBER_EX(member) \
		{ \
			int n = member.cmp(v.member); \
			if (n) \
				return n; \
		}

#define CMP_PTRS(a, b) \
		if (a) \
		{ \
			if (!b) \
				return 1; \
			int n = a->cmp(*b); \
			if (n) \
				return n; \
		} else \
			if (b) \
				return -1;

#define CMP_MEMBER_PTR(member) CMP_PTRS(member, v.member)

namespace beam
{
	template <typename T>
	void ClonePtr(std::unique_ptr<T>& trg, const std::unique_ptr<T>& src)
	{
		if (src)
		{
			trg.reset(new T);
			*trg = *src;
		}
		else
			trg.reset();
	}

	template <typename T>
	void PushVectorPtr(std::vector<typename T::Ptr>& vec, const T& val)
	{
		vec.resize(vec.size() + 1);
		typename T::Ptr& p = vec.back();
		p.reset(new T);
		*p = val;
	}
}
