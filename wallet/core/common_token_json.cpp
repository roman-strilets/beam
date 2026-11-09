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

#include "common_internal.h"

namespace beam::wallet
{
    using nlohmann::json;

#define BEAM_IGNORED_JSON_TYPES2(MACRO) \
        MACRO(ECC::RangeProof::Confidential::Part2) \
        MACRO(ECC::RangeProof::Confidential::Part3)

#define MACRO(type) \
    static bool fromByteBuffer(const ByteBuffer&, type&) \
    { \
        return false; \
    } \

    BEAM_IGNORED_JSON_TYPES2(MACRO)
#undef MACRO

#define MACRO(type) \
    static ByteBuffer toByteBuffer(const type&) \
    { \
        return {}; \
    } \

    BEAM_IGNORED_JSON_TYPES2(MACRO)
#undef MACRO

    namespace
    {
        using Names = std::array<string, uint8_t(128)>;
        Names GetParameterNames()
        {
            Names names;
#define MACRO(name, index, type) names[index] = #name;
            BEAM_TX_PUBLIC_PARAMETERS_MAP(MACRO)
#undef MACRO
            return names;
        }

        using NameIDs = std::map<string, TxParameterID>;
        NameIDs GetParameterNameIDs()
        {
            NameIDs names;
#define MACRO(name, index, type) names[#name] = TxParameterID(index);
            BEAM_TX_PUBLIC_PARAMETERS_MAP(MACRO)
#undef MACRO
            return names;
        }

        template<typename T>
        string ToJsonValue(const T& v)
        {
            return beam::to_string(v);
        }

        json ToJsonValue(TxType v)
        {
            return uint8_t(v);
        }

        json ToJsonValue(bool v)
        {
            return v;
        }

        string ToJsonValue(const ByteBuffer& buf)
        {
            return EncodeToHex(buf);
        }

        json ToJsonValue(const AmountList& amountList)
        {
            json list = json::array();
            for (const auto& a : amountList)
            {
                list.push_back(std::to_string(a));
            }
            return list;
        }

        json ToJsonValue(const CoinIDList& coinList)
        {
            json list = json::array();
            for (const auto& c : coinList)
            {
                list.push_back(toString(c));
            }
            return list;
        }

#define BEAM_IGNORED_JSON_TYPES(MACRO) \
        MACRO(ECC::Point) \
        MACRO(ECC::Scalar) \
        MACRO(ECC::Signature) \
        MACRO(std::vector<Input::Ptr>) \
        MACRO(std::vector<Output::Ptr>) \
        MACRO(std::vector<ExchangeRate>) \
        MACRO(ShieldedTxo::Voucher) \
        MACRO(ShieldedVoucherList) \
        MACRO(ShieldedTxo::PublicGen)

#define MACRO(type) \
        json ToJsonValue(const type&) \
        { \
            return {}; \
        } \

        BEAM_IGNORED_JSON_TYPES(MACRO)
        BEAM_IGNORED_JSON_TYPES2(MACRO)

#undef MACRO

#define MACRO(type) \
        bool FromJson(const json&, type&) \
        { \
            return false; \
        } \

        BEAM_IGNORED_JSON_TYPES(MACRO)
        BEAM_IGNORED_JSON_TYPES2(MACRO)
#undef MACRO

        const string& ToJsonValue(const std::string& s)
        {
            return s;
        }

        const string& GetParameterName(TxParameterID id)
        {
            static auto names = GetParameterNames();
            return names[uint8_t(id)];
        }

        TxParameterID GetParameterID(const std::string& n)
        {
            static auto names = GetParameterNameIDs();
            return names[n];
        }

        json GetParameterValue(TxParameterID id, const ByteBuffer& buffer)
        {
            switch (id)
            {
#define MACRO(name, index, type) \
            case TxParameterID::name: \
                { \
                    type value; \
                    if (fromByteBuffer(buffer, value)) \
                    { \
                        return ToJsonValue(value); \
                    } \
                } break; 
             BEAM_TX_PUBLIC_PARAMETERS_MAP(MACRO)
#undef MACRO
            default:
                break;
            }
            return {};
        }

        bool FromJson(const json& s, WalletID& v)
        {
            if (!s.is_string())
            {
                return false;
            }
            return v.FromHex(s.get<string>());
        }

        bool FromJson(const json& s, DexOrderID& v)
        {
            if (!s.is_string())
            {
                return false;
            }
            return v.FromHex(s.get<string>());
        }

        template<typename T>
        bool FromJson(const json& s, T& v)
        {
            if (!s.is_string())
            {
                return false;
            }
            v = T(stoll(s.get<string>()));
            return true;
        }

        bool FromJson(const json& s, TxType& v)
        {
            if (!s.is_number_integer())
            {
                return false;
            }

            v = TxType(s.get<uint8_t>());
            return true;
        }

        bool FromJson(const json& s, bool& v)
        {
            if (!s.is_boolean())
            {
                return false;
            }

            v = s.get<bool>();
            return true;
        }

        bool FromJson(const json& s, uintBig& v)
        {
            if (!s.is_string())
            {
                return false;
            }
            bool isValid = false;
            auto buf = from_hex(s.get<std::string>(), &isValid);
            if (!isValid)
            {
                return false;
            }
            v = Blob(buf.data(), static_cast<uint32_t>(buf.size()));
            return true;
        }

        bool FromJson(const json& s, PeerID& p)
        {
            if (!s.is_string())
            {
                return false;
            }
            return FromJson(s, static_cast<uintBig&>(p));
        }

        bool FromJson(const json& s, CoinID& v)
        {
            if (!s.is_string())
            {
                return false;
            }
            auto t = Coin::FromString(s.get<std::string>());
            if (!t)
            {
                return false;
            }
            v = *t;
            return true;
        }

        bool FromJson(const json& s, std::string& v)
        {
            if (!s.is_string())
            {
                return false;
            }
            v = s.get<std::string>();
            return true;
        }

        bool FromJson(const json& s, ByteBuffer& v)
        {
            if (!s.is_string())
            {
                return false;
            }
            bool isValid = false;
            v = from_hex(s.get<std::string>(), &isValid);
            return isValid;
        }

        template<typename T>
        bool VectorFromJson(const json& s, T& v)
        {
            if (!s.is_array())
            {
                return false;
            }
            for (const auto& item : s)
            {
                typename T::value_type t;
                if (FromJson(item, t))
                {
                    v.push_back(t);
                }
            }
            return true;
        }

        bool FromJson(const json& s, AmountList& v)
        {
            return VectorFromJson(s, v);
        }

        bool FromJson(const json& s, CoinIDList& v)
        {
            return VectorFromJson(s, v);
        }

        ByteBuffer GetParameterValue(TxParameterID id, const json& jsonValue)
        {
            switch (id)
            {
#define MACRO(name, index, type) \
            case TxParameterID::name: \
                { \
                    type value; \
                    if (FromJson(jsonValue, value)) \
                    { \
                        return toByteBuffer(value); \
                    } \
                } break; 
                BEAM_TX_PUBLIC_PARAMETERS_MAP(MACRO)
#undef MACRO
            default:
                break;
            }
            return {};
        }

        const std::string ParamsName = "params";
        const std::string TxIDName = "tx_id";
        const std::string IdName = "id";
        const std::string ValueName = "value";
    }

    std::string ConvertTokenToJson(const std::string& token)
    {
        auto p = ParseParameters(token);
        if (!p)
        {
            return {};
        }
        auto packedParams = p->Pack();
        json params = json::object();

        for (const auto& pair : packedParams)
        {
            const auto& v = GetParameterValue(pair.first, pair.second);
            if (v.is_null())
            {
                continue;
            }
            params.push_back(
            {
                GetParameterName(pair.first),
                v
            });
        }
        auto txID = p->GetTxID();

        json res = json
        {
            { TxIDName, txID.is_initialized() ? json(beam::to_string(*txID)) : json{}},
            { ParamsName, params }
        };
        return res.dump();
    }

    std::string ConvertJsonToToken(const std::string& jsonParams)
    {
        try
        {
            json obj = json::parse(jsonParams.data(), jsonParams.data() + jsonParams.size());
            auto paramsIt = obj.find(ParamsName);
            if (paramsIt == obj.end())
            {
                return {};
            }
            boost::optional<TxID> txID;
            auto txIDIt = obj.find(TxIDName);
            if (txIDIt != obj.end() && !txIDIt->is_null())
            {
                auto txIdVec = from_hex(*txIDIt);

                if (txIdVec.size() >= 16)
                {
                    txID.emplace();
                    std::copy_n(txIdVec.begin(), 16, txID->begin());
                }
            }
            TxParameters txParams(txID);
            for (const auto& p : paramsIt->items())
            {
                auto id = GetParameterID(p.key());
                ByteBuffer value = GetParameterValue(id, p.value());
                txParams.SetParameter(id, value);
            }
            return beam::to_string(txParams);
        }
        catch (const nlohmann::detail::exception&)
        {
        }
        return {};
    }
}  // namespace beam::wallet
