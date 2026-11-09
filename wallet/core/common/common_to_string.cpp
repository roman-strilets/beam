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

namespace beam
{
    string to_string(const beam::wallet::WalletID& id)
    {
        if (id == Zero)
            return {};

        static_assert(sizeof(id) == sizeof(id.m_Channel) + sizeof(id.m_Pk), "");
        return EncodeToHex(id);
    }

    string to_string(const Merkle::Hash& hash)
    {
        if (memis0(hash.m_pData, hash.nBytes))
            return string();

        char sz[Merkle::Hash::nTxtLen + 1];
        hash.Print(sz);
        return string(sz);
    }

    string to_string(const beam::wallet::PrintableAmount& amount)
    {
        std::ostringstream os;
        beam::operator << (os, amount);
        return os.str();
    }

    string to_string(const beam::wallet::TxParameters& value)
    {
        beam::wallet::TxToken token(value);
        Serializer s;
        s & token;
        ByteBuffer buffer;
        s.swap_buf(buffer);
        return beam::wallet::EncodeToBase58(buffer);
    }

    string to_string(const beam::wallet::TxID& id)
    {
        return to_hex(id.data(), id.size());
    }

    string to_string(const beam::PeerID& id)
    {
        return EncodeToHex(id);
    }

    string to_base58(const beam::PeerID& id)
    {
        return Base58::to_string(id);
    }

     unsigned to_unsigned(const std::string& what, bool throws)
     {
        try
        {
            return boost::lexical_cast<unsigned>(what);
        }
        catch(...)
        {
            if (throws) throw;
            return 0;
        }
     }

     string to_string(const beam::AmountBig::Number& amount)
     {
         char sz[beam::AmountBig::Number::nTxtLen10Max + 1];
         auto len = amount.PrintDecimal(sz);
         return string(sz, len);
     }

    string to_string(const beam::AmountBig::Type& amount)
    {
        return to_string(amount.ToNumber());
    }

}  // namespace beam

namespace beam
{
    std::ostream& operator<<(std::ostream& os, const wallet::TxID& uuid)
    {
        stringstream ss;
        ss << "[" << beam::to_string(uuid) << "]";
        os << ss.str();
        return os;
    }

    std::ostream& operator<<(std::ostream& os, const wallet::PrintableAmount& x)
    {
        AmountBig::Print(os, x.m_value);

        if (static_cast<Asset::ID>(-1) != x.m_Aid)
        {
            os << ' ';

            if (x.m_Aid)
            {
                os << kASSET << '-' << x.m_Aid;
                if (x.m_szCoinName && *x.m_szCoinName)
                    os << " (" << x.m_szCoinName << ')';
            }
            else
                os << kBEAM;
        }

        return os;
    }

}  // namespace beam
