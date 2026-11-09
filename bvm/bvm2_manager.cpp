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
#include "bvm2_internal.h"
#include <boost/algorithm/string/replace.hpp>

namespace beam {
namespace bvm2 {

	void ProcessorManager::InitMem(uint32_t nStackBytesExtra /* = 0 */)
	{
		InitBase(0x20000 + nStackBytesExtra); // 128K

		ZeroObject(m_AuxAlloc);
		m_NeedComma = false;
	}

	/////////////////////////////////////////////
	// Manager
	void ProcessorManager::ResetBase()
	{
		InitMem();
		m_InvokeData.Reset();

		m_Comms.Clear();
		m_Context.Reset();
		m_fmSpendMax.clear();

		m_mapReadVars.Clear();
		m_mapReadLogs.Clear();
		m_mapReadAssets.Clear();

		m_DbgCallstack = DebugCallstack();

		m_ApiVersion = Shaders::ApiVersion::Current;
	}

	void ProcessorManager::CallMethod(uint32_t iMethod)
	{
		const Header& hdr = ParseMod();
		Exc::Test(iMethod < ByteOrder::from_le(hdr.m_NumMethods));
		uint32_t nAddr = ByteOrder::from_le(hdr.m_pMethod[iMethod]);
		Call(nAddr, 0);
	}

	void ProcessorManager::set_ArgBlob(const char* sz, const Blob& b)
	{
		auto& trg = m_Args[sz];
		trg.resize(b.n * 2);
		if (b.n)
			uintBigImpl::_Print(reinterpret_cast<const uint8_t*>(b.p), b.n, &trg.front());
	}

	uint32_t ProcessorManager::get_HeapLimit()
	{
		return 0x800000; // 8MB
	}

	namespace
	{
		// Parser for "key=value, key2=\"quoted, value\"" argument strings.
		// A key is [A-Za-z0-9_]+. A value is either unquoted (any chars except ',', '"' and ' ')
		// or double-quoted, where \" stands for a quote. Spaces around keys, '=' and values are ignored.
		// Text that doesn't form a valid pair is skipped, and anything after a pair up to the next comma is ignored.
		class ArgsParser
		{
		public:
			explicit ArgsParser(std::string_view s) : m_S(s) {}

			// Finds the next pair, starting from the current position
			bool Next(std::string& key, std::string& value)
			{
				for (size_t pos = m_Pos; pos < m_S.size(); )
				{
					size_t keyBegin = SkipSpaces(pos);
					size_t keyEnd = keyBegin;
					while (keyEnd < m_S.size() && IsKeyChar(m_S[keyEnd]))
						++keyEnd;

					size_t valueEnd = (keyEnd > keyBegin) ? ReadAssignment(keyEnd, value) : npos;
					if (valueEnd != npos)
					{
						key.assign(m_S.substr(keyBegin, keyEnd - keyBegin));
						m_Pos = m_S.find(',', valueEnd);
						return true;
					}

					// A match starting anywhere inside the scanned spaces or key would fail the same way
					pos = std::max(pos + 1, keyEnd);
				}

				m_Pos = npos;
				return false;
			}

		private:
			static constexpr size_t npos = std::string_view::npos;

			std::string_view m_S;
			size_t m_Pos = 0;
			// Once a quoted value at some position has no closing quote, none further on has either
			size_t m_UnclosedQuote = npos;

			static bool IsSpace(char c)
			{
				return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f';
			}

			static bool IsKeyChar(char c)
			{
				return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
			}

			static bool IsUnquotedChar(char c)
			{
				return c != ',' && c != '"' && c != ' ';
			}

			size_t SkipSpaces(size_t pos) const
			{
				while (pos < m_S.size() && IsSpace(m_S[pos]))
					++pos;
				return pos;
			}

			// Reads "\s*=\s*value" at pos. Returns the end of the value, or npos
			size_t ReadAssignment(size_t pos, std::string& value)
			{
				pos = SkipSpaces(pos);
				if (pos >= m_S.size() || m_S[pos] != '=')
					return npos;

				size_t spacesBegin = pos + 1;
				size_t spacesEnd = SkipSpaces(spacesBegin);

				size_t valueEnd = ReadValue(spacesEnd, value);
				if (valueEnd != npos)
					return valueEnd;

				// No value after the spaces. Whitespace other than ' ' (tab, newline, ...) is a valid
				// unquoted value char, so the value may start at the last such char among the spaces.
				for (size_t i = spacesEnd; i > spacesBegin; )
				{
					if (m_S[--i] != ' ')
						return ReadValue(i, value);
				}
				return npos;
			}

			// Reads an unquoted or a quoted value at pos. Returns its end, or npos
			size_t ReadValue(size_t pos, std::string& value)
			{
				if (pos >= m_S.size())
					return npos;

				if (IsUnquotedChar(m_S[pos]))
				{
					size_t end = pos;
					while (end < m_S.size() && IsUnquotedChar(m_S[end]))
						++end;
					value.assign(m_S.substr(pos, end - pos));
					return end;
				}

				if (m_S[pos] != '"' || pos >= m_UnclosedQuote)
					return npos;

				// The closing quote is the first unescaped one, the quoted text is not empty
				for (size_t i = pos + 2; i < m_S.size(); ++i)
				{
					if (m_S[i] == '"' && m_S[i - 1] != '\\')
					{
						value.assign(m_S.substr(pos + 1, i - pos - 1));
						boost::algorithm::replace_all(value, "\\\"", "\"");
						return i + 1;
					}
				}

				m_UnclosedQuote = pos;
				return npos;
			}
		};
	}

	uint32_t ProcessorManager::AddArgs(const std::string& commaSeparatedPairs)
	{
		uint32_t ret = 0;
		ArgsParser parser(commaSeparatedPairs);
		std::string key, value;
		while (parser.Next(key, value))
		{
			m_Args.emplace(std::move(key), std::move(value));
			++ret;
		}
		return ret;
	}

	void ProcessorManager::DumpCallstack(std::ostream& os, const Wasm::Compiler::DebugInfo* pDbgInfo /* = nullptr */) const
	{
		Wasm::Word ip = get_Ip();
		m_DbgCallstack.Dump(os, ip, pDbgInfo);
	}

	void ProcessorManager::OnCall(Wasm::Word nAddr)
	{
		if (m_Debug)
		{
			Wasm::Word nRetAddr = m_DbgCallstack.m_v.empty() ? 0 : get_Ip();
			m_DbgCallstack.OnCall(nAddr, nRetAddr);
		}

		Processor::OnCall(nAddr);
	}

	void ProcessorManager::OnRet(Wasm::Word nRetAddr)
	{
		if (m_Debug)
			m_DbgCallstack.OnRet();

		Processor::OnRet(nRetAddr);
	}

	BVM_METHOD(Comm_Listen)
	{
		OnHost_Comm_Listen(get_AddrR(pID, nID), nID, nCookie);
	}
	BVM_METHOD_HOST(Comm_Listen)
	{
		auto it = m_Comms.m_Map.find(nCookie, Comm::Channel::Comparator());
		if (m_Comms.m_Map.end() == it)
		{
			if (nID)
			{
				ECC::Hash::Value hv;
				DeriveKeyPreimage(hv, Blob(pID, nID));

				Comm::Channel::Ptr pCh;
				Comm_CreateListener(pCh, hv);

				if (pCh)
				{
					pCh->m_Key = nCookie;
					m_Comms.m_Map.insert(*pCh.release());
				}
			}
		}
		else
		{
			Exc::Test(!nID);
			m_Comms.m_Map.Delete(*it);
		}
	}
	BVM_METHOD(Comm_Send)
	{
		OnHost_Comm_Send(get_AddrAsR<PubKey>(pkRemote), get_AddrR(pBuf, nSize), nSize);
	}
	BVM_METHOD_HOST(Comm_Send)
	{
		Comm_Send(pkRemote, Blob(pBuf, nSize));
	}
	BVM_METHOD(Comm_Read)
	{
		uint32_t* pCookie_ = pCookie ? &get_AddrAsW<uint32_t>(pCookie) : nullptr;
		auto nRet = OnHost_Comm_Read(get_AddrW(pBuf, nSize), nSize, pCookie_, bKeep);

		if (nRet && pCookie_)
			*pCookie_ = Wasm::to_wasm(*pCookie_);

		return nRet;
	}
	BVM_METHOD_HOST(Comm_Read)
	{
		if (m_Comms.m_Rcv.empty())
			return 0;

		auto& x = m_Comms.m_Rcv.front().get_ParentObj();

		uint32_t nRet = static_cast<uint32_t>(x.m_Msg.size());
		if (nRet)
			memcpy(pBuf, &x.m_Msg.front(), std::min(nRet, nSize));

		auto& c = *x.m_pChannel;
		if (pCookie)
			*pCookie = c.m_Key;

		if (!bKeep)
			c.m_List.Delete(x);

		return nRet;
	}

	BVM_METHOD(Comm_WaitMsg)
	{
		return OnHost_Comm_WaitMsg(nTimeout_ms);
	}
	BVM_METHOD_HOST(Comm_WaitMsg)
	{
		if (m_Comms.m_Rcv.empty())
		{
			if (m_Comms.m_TimedOut)
				m_Comms.m_TimedOut = false;
			else
				Comm_Wait(nTimeout_ms);
		}
	}

} // namespace bvm2
} // namespace beam
