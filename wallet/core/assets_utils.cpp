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
#include "assets_utils.h"
#include "wallet/core/common.h"
#include "wallet/core/strings_resources.h"
#include "utility/logger.h"
#include <regex>
#include <set>

namespace beam::wallet {
    namespace
    {
        const char STD_META_MARK[]     = "STD:";
        const char VERSION_KEY[]       = "SCH_VER";
        const char NAME_KEY[]          = "N";
        const char SHORT_NAME_KEY[]    = "SN";
        const char UNIT_NAME_KEY[]     = "UN";
        const char NTH_UNIT_NAME_KEY[] = "NTHUN";
        const char OPT_SDESC_KEY[]     = "OPT_SHORT_DESC";
        const char OPT_LDESC_KEY[]     = "OPT_LONG_DESC";
        const char ALLOWED_SYMBOLS[]   = " .,-_";
        const unsigned CURRENT_META_VERSION = 1;
    }

    WalletAssetMeta::WalletAssetMeta(std::string meta)
        : _std(false)
        , _std_v5_0(false)
        , _parsed(false)
        , _meta(std::move(meta))
    {
        Parse();
    }

    WalletAssetMeta::WalletAssetMeta(const Asset::Full& info)
        : _std(false)
        , _std_v5_0(false)
        , _parsed(false)
    {
        const auto& mval = info.m_Metadata.m_Value;
        if (mval.empty())
        {
            return;
        }

        if(!fromByteBuffer(mval, _meta))
        {
            LOG_WARNING() << "AssetID " << info.m_ID << " failed to deserialize from Asset::Full";
            return;
        }

        Parse();
    }

    void WalletAssetMeta::Parse()
    {
        _std = false;
        _parsed = false;

        const auto STD_LEN = std::size(STD_META_MARK) - 1;
        if(strncmp(_meta.c_str(), STD_META_MARK, STD_LEN) != 0) return;

        std::regex rg{R"([^;]+)"};
        std::set<std::string> tokens{
            std::sregex_token_iterator{_meta.begin() + STD_LEN, _meta.end(), rg},
            std::sregex_token_iterator{}
        };

        for(const auto& it: tokens)
        {
            auto eq = it.find_first_of('=');
            if (eq == std::string::npos) continue;
            auto key = std::string(it.begin(), it.begin() + eq);
            auto val = std::string(it.begin() + eq + 1, it.end());
            _values[key] = val;
        }

        const auto fieldValid = [&](const char* name) -> bool {
            const auto it = _values.find(name);
            if (it == _values.end()) return false;

            return std::all_of(it->second.begin(), it->second.end(), [](const char ch) -> bool {
                return std::isalnum(ch, std::locale::classic()) || std::string(ALLOWED_SYMBOLS).find(ch) != std::string::npos;
            });
        };

        _std_v5_0 =
               fieldValid(NAME_KEY) &&
               fieldValid(SHORT_NAME_KEY) &&
               fieldValid(UNIT_NAME_KEY) &&
               fieldValid(NTH_UNIT_NAME_KEY);

        const auto versionValid = [&] () -> bool {
            const auto it = _values.find(VERSION_KEY);
            if (it == _values.end()) return false;

            const auto version = std::to_unsigned(it->second, false);
            return version == CURRENT_META_VERSION;
        };

        const auto optSDescValid = [&] () -> bool {
            const auto it = _values.find(OPT_SDESC_KEY);
            if (it == _values.end()) return true;
            return it->second.length () <= 128;
        };

        const auto optLDescValid = [&] () -> bool {
            const auto it = _values.find(OPT_LDESC_KEY);
            if (it == _values.end()) return true;
            return it->second.length () <= 1024;
        };

        _std = _std_v5_0 && versionValid() && optSDescValid() && optLDescValid();
        _parsed = true;
    }

    void WalletAssetMeta::LogInfo(const std::string& pref) const
    {
        assert(_parsed);

        const auto prefix = pref.empty() ? pref : pref + " ";
        const auto isPrintable = [](const std::string& str) -> bool {
            std::locale loc("");
            return std::all_of(str.begin(), str.end(), [&loc](const char ch) -> bool {
                return std::isprint(ch, loc);
            });
        };

        for(const auto& it: _values)
        {
            std::string value = it.second;
            if (!isPrintable(value))
            {
                std::stringstream  ss;
                ss << "[CANNOT BE PRINTED, size is " << value.size() << " bytes]";
                value = ss.str();
            }
            LOG_INFO() << prefix << it.first << "=" << value;
        }
    }

    bool WalletAssetMeta::isStd() const
    {
        assert(_parsed);
        return _std;
    }

    bool WalletAssetMeta::isStd_v5_0() const
    {
        assert(_parsed);
        return _std_v5_0;
    }

    std::string WalletAssetMeta::GetUnitName() const
    {
        assert(_parsed);
        const auto it = _values.find(UNIT_NAME_KEY);
        return it != _values.end() ? it->second : std::string(kAmountASSET);
    }

    std::string WalletAssetMeta::GetNthUnitName() const
    {
        assert(_parsed);
        const auto it = _values.find(NTH_UNIT_NAME_KEY);
        return it != _values.end() ? it->second : std::string(kAmountAGROTH);
    }

    std::string WalletAssetMeta::GetName() const
    {
        assert(_parsed);
        const auto it = _values.find(NAME_KEY);
        return it != _values.end() ? it->second : std::string(kNA);
    }

    std::string WalletAssetMeta::GetShortName() const
    {
        assert(_parsed);
        const auto it = _values.find(SHORT_NAME_KEY);
        return it != _values.end() ? it->second : std::string(kNA);
    }

    unsigned WalletAssetMeta::GetSchemaVersion() const
    {
        assert(_parsed);
        const auto it = _values.find(SHORT_NAME_KEY);
        return it != _values.end() ? std::to_unsigned(it->second, false) : 0;
    }

    WalletAsset::WalletAsset(const Asset::Full& full, Height refreshHeight)
        : Asset::Full(full)
        , m_RefreshHeight(refreshHeight)
    {
    }

    bool WalletAsset::CanRollback(Height from) const
    {
        const auto maxRollback = Rules::get().MaxRollback;
        return m_LockHeight + maxRollback > from;
    }

    void WalletAsset::LogInfo(const std::string& pref) const
    {
        const auto prefix = pref.empty() ? pref : pref + " ";

        LOG_INFO() << prefix << "Asset ID: "       << m_ID;
        LOG_INFO() << prefix << "Owner ID: "       << m_Owner;
        LOG_INFO() << prefix << "Issued amount: "  << PrintableAmount(m_Value, false, kAmountASSET, kAmountAGROTH);
        LOG_INFO() << prefix << "Lock Height: "    << m_LockHeight;
        LOG_INFO() << prefix << "Refresh height: " << m_RefreshHeight;
        LOG_INFO() << prefix << "Metadata size: "  << m_Metadata.m_Value.size() << " bytes";

        const WalletAssetMeta meta(*this);
        meta.LogInfo(pref + "\t");

        if(m_IsOwned)
        {
            LOG_INFO() << prefix << "You own this asset";
        }
    }

    void WalletAsset::LogInfo(const TxID& txId, const SubTxID& subTxId) const
    {
        std::stringstream ss;
        ss << txId << "[" << subTxId << "]";
        const auto prefix = ss.str();
        LogInfo(prefix);
    }
}
