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

namespace beam {
namespace bvm2 {

	BVM_METHOD_VIA_HOST(DocAddGroup)
	BVM_METHOD_HOST(DocAddGroup)
	{
		DocID(szID);
		*m_pOut << '{';
		m_NeedComma = false;
	}

	BVM_METHOD_VIA_HOST(DocCloseGroup)
	BVM_METHOD_HOST(DocCloseGroup)
	{
		*m_pOut << '}';
		m_NeedComma = true;
	}

	BVM_METHOD_VIA_HOST(DocAddText)
	BVM_METHOD_HOST(DocAddText)
	{
		DocID(szID);
		DocQuotedText(val);
	}

	BVM_METHOD_VIA_HOST(DocAddNum32)
	BVM_METHOD_HOST(DocAddNum32)
	{
		DocID(szID);
		*m_pOut << val;
	}

	BVM_METHOD_VIA_HOST(DocAddNum64)
	BVM_METHOD_HOST(DocAddNum64)
	{
		DocID(szID);
		*m_pOut << val;
	}

	BVM_METHOD(DocAddBlob)
	{
		return OnHost_DocAddBlob(RealizeStr(szID), get_AddrR(pBlob, nBlob), nBlob);
	}
	BVM_METHOD_HOST(DocAddBlob)
	{
		DocID(szID);
		*m_pOut << '"';
		uintBigImpl::_PrintFull(reinterpret_cast<const uint8_t*>(pBlob), nBlob, *m_pOut);
		*m_pOut << '"';
	}

	BVM_METHOD_VIA_HOST(DocAddArray)
	BVM_METHOD_HOST(DocAddArray)
	{
		DocID(szID);
		*m_pOut << '[';
		m_NeedComma = false;
	}

	BVM_METHOD_VIA_HOST(DocCloseArray)
	BVM_METHOD_HOST(DocCloseArray)
	{
		*m_pOut << ']';
		m_NeedComma = true;
	}

	BVM_METHOD(DocGetText)
	{
		return OnHost_DocGetText(RealizeStr(szID), reinterpret_cast<char*>(get_AddrW(szRes, nLen)), nLen);
	}
	BVM_METHOD_HOST(DocGetText)
	{
		auto pVal = FindArg(szID);
		if (!pVal)
		{
			if (nLen)
				szRes[0] = 0;
			return 0;
		}

		uint32_t n = static_cast<uint32_t>(pVal->size()) + 1;
		if (nLen)
		{
			beam::setmin(nLen, n);

			memcpy(szRes, pVal->c_str(), nLen - 1);
			szRes[nLen - 1] = 0;
		}
		return n;
	}

	BVM_METHOD(DocGetBlob)
	{
		return OnHost_DocGetBlob(RealizeStr(szID), reinterpret_cast<char*>(get_AddrW(pOut, nLen)), nLen);
	}
	BVM_METHOD_HOST(DocGetBlob)
	{
		auto pVal = FindArg(szID);
		if (!pVal)
		{
			Blob b;
			if (!get_SpecialParam(szID, b))
				return 0;

			memcpy(pOut, b.p, std::min(nLen, b.n));
			return b.n;
		}

		uint32_t nSrcLen = static_cast<uint32_t>(pVal->size());
		const char* str = pVal->c_str();
		// ignore hex prefix "0x"
		if (nSrcLen > 2 && str[0] == '0' && tolower(str[1]) == 'x')
		{
			const uint32_t nPrefixLen = 2;
			nSrcLen -= nPrefixLen;
			str += nPrefixLen;
		}
		
		uint32_t nMax = nLen * 2;
		uint32_t nRes = uintBigImpl::_Scan(reinterpret_cast<uint8_t*>(pOut), str, std::min(nSrcLen, nMax));

		// if src len is bigger than the requested size - return the src size
		// if parsed less than requested - return size parsed.
		// The requested size is returned only if it equals to the src size, and was successfully parsed
		return ((nRes == nMax) ? nSrcLen : nRes) / 2;
	}

	BVM_METHOD(DocGetNum32)
	{
		uint32_t val = 0;
		auto nRet = OnHost_DocGetNum32(RealizeStr(szID), &val);
		Wasm::to_wasm(get_AddrW(pOut, sizeof(val)), val);
		return nRet;
	}
	BVM_METHOD_HOST(DocGetNum32)
	{
		auto pVal = FindArg(szID);
		if (!pVal)
			return 0;

		*pOut = atoi(pVal->c_str());
		return sizeof(*pOut); // ignore errors
	}

	BVM_METHOD(DocGetNum64)
	{
		uint64_t val = 0;
		auto nRet = OnHost_DocGetNum64(RealizeStr(szID), &val);
		Wasm::to_wasm(get_AddrW(pOut, sizeof(val)), val);
		return nRet;
	}
	BVM_METHOD_HOST(DocGetNum64)
	{
		auto pVal = FindArg(szID);
		if (!pVal)
			return 0;

		*pOut = strtoull(pVal->c_str(), nullptr, 0);
		return sizeof(*pOut); // ignore errors
	}

	const std::string* ProcessorManager::FindArg(const char* szID)
	{
		auto it = m_Args.find(szID);
		return (m_Args.end() == it) ? nullptr : &it->second;
	}

	void ProcessorManager::DocID(const char* sz)
	{
		DocOnNext();
		if (*sz)
		{
			DocQuotedText(sz);
			*m_pOut << ": ";
		}
	}

	void ProcessorManager::DocQuotedText(const char* sz)
	{
		*m_pOut << '"';
		DocEncodedText(sz);
		*m_pOut << '"';
	}

	void ProcessorManager::DocEncodedText(const char* sz)
	{
		while (true)
		{
            //
            // All Unicode characters may be placed within the quotation marks except for the characters
            // that must be escaped: quotation mark, reverse solidus, and the control characters (U+0000 through U+001F)
			//
            char ch = *sz++;
            if (ch == 0) {
                return;
            }

            if (ch <= 0x1F)
            {
                const auto flags = m_pOut->flags();
                *m_pOut << "\\u" << std::setfill('0') << std::setw(4) << std::hex << static_cast<int>(ch);
                m_pOut->flags(flags);
            }
            else if (ch == '"' || ch == '\\')
            {
                *m_pOut << '\\' << ch;
            }
            else
            {
                *m_pOut << ch;
            }
		}
	}



	void ProcessorManager::DocOnNext()
	{
		if (m_NeedComma)
			*m_pOut << ',';
		else
			m_NeedComma = true;
	}

} // namespace bvm2
} // namespace beam
