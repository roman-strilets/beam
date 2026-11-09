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

// Internal header shared by the wasm_*.cpp sources: the Reader templates,
// the Type helpers and the instruction tables.
#pragma once
#define _CRT_SECURE_NO_WARNINGS // sprintf
#include "wasm_interpreter.h"
#include "../core/base/uintBig.h"
#include <bit>
#include <sstream>

namespace beam {
namespace Wasm {

	/////////////////////////////////////////////
	// Reader

	template <typename T, bool bSigned>
	T Reader::ReadInternal()
	{
		static_assert(!std::numeric_limits<T>::is_signed); // the sign flag must be specified separately

		T ret = 0;
		constexpr unsigned int nBitsMax = sizeof(ret) * 8;

		for (unsigned int nShift = 0; ; )
		{
			uint8_t n = Read1();
			bool bEnd = !(0x80 & n);
			if (!bEnd)
				n &= ~0x80;

			ret |= T(n) << nShift;
			nShift += 7;

			if (bEnd)
			{
				if constexpr (bSigned)
				{
					if (0x40 & n)
					{
						// Attention: Bug workaround.
						// According to the standard we must pad the remaining bits of the result with 1's.
						// However this should only be done if there are bits left. That is, only if nShift is lesser than the number of result bits.
						// Original code didn't take care of this.

						if (nShift >= nBitsMax)
						{
							m_ModeTriggered = true;

							constexpr uint32_t nBitsJustFed = ((nBitsMax - 1) % 7) + 1; // how many bits were just fed into result
							static_assert(nBitsJustFed < 6); // the 0x40 bit was not fed. It's unnecessary

							switch (m_Mode)
							{
							case Mode::AutoWorkAround:
								n &= ~0x40; // safe to remove this flag, it's redundant and didn't have to appear at all
								Cast::NotConst(m_p0[-1]) = n; // replace it back!
								break;

							case Mode::Emulate_x86:
								// emulate the unfixed behavior. On x86 bitshift is effective modulo size of operand
								ret |= (~static_cast<T>(0) << (nShift % nBitsMax));
								break;

							case Mode::Standard:
								// Standard behavior, ignore padding
								break;

							default:
								assert(false);
								// no break;
							case Mode::Restrict:
								Exc::Fail("Conflicting flags");
							}
						}
						else
							// standard padding
							ret |= (~static_cast<T>(0) << nShift);
					}
				}
				break;
			}

			Exc::Test(nShift < nBitsMax);
		}

		return ret;
	}

	/////////////////////////////////////////////
	// Common

	struct Type
		:public TypeCode
	{
		static const uint8_t s_Base = 0x7C; // for the 2-bit type encoding

		static uint8_t Words(uint8_t t) {
			switch (t) {
			default:
				Exc::Fail();
			case i32:
			case f32:
				return 1;
			case i64:
			case f64:
				return 2;
			}
		}

		template <uint8_t nType> struct Code2Type;
		template <typename T, bool bSigned> struct ToFlexible;

		template <typename TIn>
		static typename ToFlexible<TIn, true>::T SignedFrom(TIn x) { return x; }

		template <typename TOut, typename TIn>
		static TOut Extend(TIn x)
		{
			// promote w.r.t. sign
			static_assert(sizeof(TOut) >= sizeof(TIn));
			return (typename ToFlexible<TOut, std::numeric_limits<TIn>::is_signed>::T) x;
		}
	};

	template <> struct Type::Code2Type<Type::i32> { typedef uint32_t T; };
	template <> struct Type::Code2Type<Type::i64> { typedef uint64_t T; };

	template <> struct Type::ToFlexible<uint8_t, true> { typedef int8_t T; };
	template <> struct Type::ToFlexible<int8_t, true> { typedef int8_t T; };
	template <> struct Type::ToFlexible<uint8_t, false> { typedef uint8_t T; };
	template <> struct Type::ToFlexible<int8_t, false> { typedef uint8_t T; };

	template <> struct Type::ToFlexible<uint16_t, true> { typedef int16_t T; };
	template <> struct Type::ToFlexible<int16_t, true> { typedef int16_t T; };
	template <> struct Type::ToFlexible<uint16_t, false> { typedef uint16_t T; };
	template <> struct Type::ToFlexible<int16_t, false> { typedef uint16_t T; };

	template <> struct Type::ToFlexible<uint32_t, true> { typedef int32_t T; };
	template <> struct Type::ToFlexible<int32_t, true> { typedef int32_t T; };
	template <> struct Type::ToFlexible<uint32_t, false> { typedef uint32_t T; };
	template <> struct Type::ToFlexible<int32_t, false> { typedef uint32_t T; };

	template <> struct Type::ToFlexible<uint64_t, true> { typedef int64_t T; };
	template <> struct Type::ToFlexible<int64_t, true> { typedef int64_t T; };
	template <> struct Type::ToFlexible<uint64_t, false> { typedef uint64_t T; };
	template <> struct Type::ToFlexible<int64_t, false> { typedef uint64_t T; };


	template <typename T>
	T Reader::Read()
	{
		return ReadInternal<typename Type::ToFlexible<T, false>::T, std::numeric_limits<T>::is_signed>();
	}


#define WasmInstructions_unop_Polymorphic_32(macro) \
	macro(eqz   , 0x45, 0x50) \

#define WasmInstructions_binop_Polymorphic_32(macro) \
	macro(eq    , 0x46, 0x51) \
	macro(ne    , 0x47, 0x52) \
	macro(lt_s  , 0x48, 0x53) \
	macro(lt_u  , 0x49, 0x54) \
	macro(gt_s  , 0x4A, 0x55) \
	macro(gt_u  , 0x4B, 0x56) \
	macro(le_s  , 0x4C, 0x57) \
	macro(le_u  , 0x4D, 0x58) \
	macro(ge_s  , 0x4E, 0x59) \
	macro(ge_u  , 0x4F, 0x5A) \

#define WasmInstructions_binop_Polymorphic_x(macro) \
	macro(add   , 0x6A, 0x7C) \
	macro(sub   , 0x6B, 0x7D) \
	macro(mul   , 0x6C, 0x7E) \
	macro(div_s , 0x6D, 0x7F) \
	macro(div_u , 0x6E, 0x80) \
	macro(rem_s , 0x6F, 0x81) \
	macro(rem_u , 0x70, 0x82) \
	macro(and   , 0x71, 0x83) \
	macro(or    , 0x72, 0x84) \
	macro(xor   , 0x73, 0x85) \
	macro(shl   , 0x74, 0x86) \
	macro(shr_s , 0x75, 0x87) \
	macro(shr_u , 0x76, 0x88) \
	macro(rotl  , 0x77, 0x89) \
	macro(rotr  , 0x78, 0x8A) \

#define WasmInstructions_Store(macro) \
	macro(0x36, i32, store  , uint32_t) \
	macro(0x37, i64, store  , uint64_t) \
	macro(0x3A, i32, store8 , uint8_t) \
	macro(0x3B, i32, store16, uint16_t) \
	macro(0x3C, i64, store8 , uint8_t) \
	macro(0x3D, i64, store16, uint16_t) \
	macro(0x3E, i64, store32, uint32_t) \

#define WasmInstructions_Load(macro) \
	macro(0x28, i32, load    , uint32_t) \
	macro(0x29, i64, load    , uint64_t) \
	macro(0x2C, i32, load8_s , int8_t) \
	macro(0x2D, i32, load8_u , uint8_t) \
	macro(0x2E, i32, load16_s, int16_t) \
	macro(0x2F, i32, load16_u, uint16_t) \
	macro(0x30, i64, load8_s , int8_t) \
	macro(0x31, i64, load8_u , uint8_t) \
	macro(0x32, i64, load16_s, int16_t) \
	macro(0x33, i64, load16_u, uint16_t) \
	macro(0x34, i64, load32_s, int32_t) \
	macro(0x35, i64, load32_u, uint32_t) \


#define WasmInstructions_CustomPorted(macro) \
	macro(0x1A, drop) \
	macro(0x1B, select) \
	macro(0x20, local_get) \
	macro(0x21, local_set) \
	macro(0x22, local_tee) \
	macro(0x23, global_get) \
	macro(0x24, global_set) \
	macro(0xA7, i32_wrap_i64) \
	macro(0xAC, i64_extend_i32_s) \
	macro(0xAD, i64_extend_i32_u) \
	macro(0x10, call) \
	macro(0x11, call_indirect) \
	macro(0x0C, br) \
	macro(0x0D, br_if) \
	macro(0x0E, br_table) \
	macro(0x0F, ret) \
	macro(0x41, i32_const) \
	macro(0x42, i64_const) \

#define WasmInstructions_Proprietary(macro) \
	macro(0xf1, prolog) \
	macro(0xf3, call_ext) \
	macro(0xf8, global_get_imp) \
	macro(0xf9, global_set_imp) \

#define WasmInstructions_NotPorted(macro) \
	macro(0x00, unreachable) \
	macro(0x01, nop) \
	macro(0x02, block) \
	macro(0x03, loop) \
	macro(0x0B, end_block) \

	enum Instruction
	{
#define THE_MACRO(id, name) name = id,
		WasmInstructions_CustomPorted(THE_MACRO) \
		WasmInstructions_NotPorted(THE_MACRO) \
		WasmInstructions_Proprietary(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(name, id32, id64) \
		i32_##name = id32, \
		i64_##name = id64,

		WasmInstructions_unop_Polymorphic_32(THE_MACRO)
		WasmInstructions_binop_Polymorphic_32(THE_MACRO)
		WasmInstructions_binop_Polymorphic_x(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(id, type, name, tmem) type##_##name = id,
		WasmInstructions_Store(THE_MACRO)
		WasmInstructions_Load(THE_MACRO)
#undef THE_MACRO
	};

} // namespace Wasm
} // namespace beam
