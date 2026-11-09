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

#include "wallet_db_common.h"

namespace beam::wallet
{
    Coin::Coin(Amount amount /* = 0 */, Key::Type keyType /* = Key::Type::Regular */, Asset::ID assetId /* = 0 */)
        : m_status{ Status::Unavailable }
        , m_maturity{ MaxHeight }
        , m_confirmHeight{ MaxHeight }
        , m_spentHeight{ MaxHeight }
        , m_OBSOLETTEsessionId(EmptyCoinSession)
    {
        m_ID = Zero;
        m_ID.m_Value = amount;
        m_ID.m_Type = keyType;
        m_ID.m_AssetID = assetId;
    }

    bool Coin::isReward() const
    {
        switch (m_ID.m_Type)
        {
        case Key::Type::Coinbase:
        case Key::Type::Comission:
            return true;
        default:
            return false;
        }
    }

    bool Coin::isChange() const
    {
        return m_ID.m_Type == Key::Type::Change;
    }

    bool Coin::isAsset() const
    {
        return m_ID.m_AssetID != 0;
    }

    bool Coin::isAsset(Asset::ID assetId) const
    {
        return m_ID.m_AssetID == assetId;
    }

    bool Coin::IsMaturityValid() const
    {
        switch (m_status)
        {
        case Unavailable:
        case Incoming:
            return false;

        default:
            return true;
        }
    }

    Height Coin::get_Maturity() const
    {
        return IsMaturityValid() ? m_maturity + m_offset : MaxHeight;
    }

    bool Coin::operator==(const Coin& other) const
    {
        return other.m_ID == m_ID;
    }

    bool Coin::operator!=(const Coin& other) const
    {
        return !(other == *this);
    }

#pragma pack (push, 1)
    struct CoinIDPacked
    {
        // for historical reasons - make AssetID 1st member to keep bkwd compatibility
        // during serialization/deserialization leading zeroes are trimmed
        uintBigFor<Asset::ID>::Type m_AssetID;
        Key::ID::Packed m_Kid;
        uintBigFor<Amount>::Type m_Value;
    };
#pragma pack (pop)

    std::string toString(const CoinID& id)
    {
        CoinIDPacked packed;
        packed.m_Kid = id;
        packed.m_Value = id.m_Value;

        if (!id.m_AssetID)
            return to_hex(&packed.m_Kid, sizeof(packed) - sizeof(packed.m_AssetID));

        packed.m_AssetID = id.m_AssetID;
        return to_hex(&packed, sizeof(packed));
    }

    string Coin::toStringID() const
    {
        return toString(m_ID);
    }

    Amount Coin::getAmount() const
    {
        return m_ID.m_Value;
    }

    Asset::ID Coin::getAssetID() const
    {
        return m_ID.m_AssetID;
    }

    std::string Coin::getType() const
    {
        return (const char*)FourCC::Text(m_ID.m_Type);
    }

    std::string Coin::getStatusString() const
    {
        static std::map<Status, std::string> Strings 
        {
            {Unavailable,   "unavailable"},
            {Available,     "available"},
            {Maturing,      "maturing"},
            {Outgoing,      "outgoing"},
            {Incoming,      "incoming"},
            {Spent,         "spent"},
            {Consumed,      "consumed"},
        };

        return Strings[m_status];
    }

    boost::optional<Coin::ID> Coin::FromString(const std::string& str)
    {
        bool isValid = false;
        auto byteBuffer = from_hex(str, &isValid);
        if (isValid && byteBuffer.size() <= sizeof(CoinIDPacked))
        {
            CoinIDPacked packed;
            ZeroObject(packed);
            uint8_t* p = reinterpret_cast<uint8_t*>(&packed) + sizeof(CoinIDPacked) - byteBuffer.size();
            copy_n(byteBuffer.begin(), byteBuffer.size(), p);
            Coin::ID id;
            Cast::Down<Key::ID>(id) = packed.m_Kid;
            packed.m_Value.Export(id.m_Value);
            packed.m_AssetID.Export(id.m_AssetID);
            return id;
        }
        return boost::optional<Coin::ID>();
    }

    void Coin::setOffset(uint32_t offset)
    {
        m_offset = offset;
    }

    ////////////////////////
    // WalletAddress
    WalletAddress::WalletAddress(const std::string& label, const std::string& category)
        : m_BbsAddr(Zero)
        , m_label(label)
        , m_category(category)
        , m_createTime(0)
        , m_duration(AddressExpiration24h)
        , m_OwnID(0)
        , m_Endpoint(Zero)
    {}

    bool WalletAddress::operator == (const WalletAddress& other) const
    {
        return m_BbsAddr == other.m_BbsAddr && m_OwnID == other.m_OwnID;
    }

    bool WalletAddress::operator != (const WalletAddress& other) const
    {
        return !(*this == other);
    }

    bool WalletAddress::isExpired() const
    {
        return getTimestamp() > getExpirationTime();
    }

    bool WalletAddress::isOwn() const
    {
        return m_OwnID != 0;
    }

    bool WalletAddress::isPermanent() const
    {
        return m_duration == AddressExpirationNever;
    }

    Timestamp WalletAddress::getCreateTime() const
    {
        return m_createTime;
    }

    Timestamp WalletAddress::getExpirationTime() const
    {
        if (isPermanent())
        {
            return Timestamp(-1);
        }
        return m_createTime + m_duration;
    }

    void WalletAddress::setLabel(const std::string& label)
    {
        m_label = label;
    }

    void WalletAddress::setCategory(const std::string& category)
    {
        m_category = category;
    }

    void WalletAddress::setExpirationTime(beam::Timestamp expirationTime)
    {
        if (m_createTime <= expirationTime)
        {
            m_duration = expirationTime - getCreateTime();
        }
        else
        {
            assert(!"invalid expiration time");
            setExpirationStatus(ExpirationStatus::Expired);
        }
    }

    void WalletAddress::setExpirationStatus(WalletAddress::ExpirationStatus status)
    {
        switch (status)
        {
        case ExpirationStatus::Expired:
            {
                assert(m_createTime < getTimestamp() - 1);
                m_duration = getTimestamp() - m_createTime - 1;
                break;
            }
        case ExpirationStatus::Auto:
            {
                // set expiration date since current timestamp
                auto delta = getTimestamp() - m_createTime;
                m_duration = delta + WalletAddress::AddressExpirationAuto;
                break;
            }
        case ExpirationStatus::Never:
            {
                m_duration = AddressExpirationNever;
                break;
            }
        default:
            break;
        }
    }

    bool ShieldedCoin::isAsset(Asset::ID assetId) const
    {
        return m_CoinID.m_AssetID == assetId;
    }

    std::string ShieldedCoin::toStringID() const
    {
        return std::to_string(m_TxoID);
    }

    Amount ShieldedCoin::getAmount() const
    {
        return m_CoinID.m_Value;
    }

    Asset::ID ShieldedCoin::getAssetID() const
    {
        return m_CoinID.m_AssetID;
    }

    std::string ShieldedCoin::getType() const
    {
        return "shld";
    }

    uint32_t ShieldedCoin::get_WndIndex(uint32_t N) const
    {
        assert(N);

        uint32_t nIdx;
        m_CoinID.m_Key.m_kSerG.m_Value.ExportWord<0>(nIdx); // pseudo-random

        nIdx %= N; // pseudo-random, how many elements should follow this in the window

        if (nIdx > m_TxoID)
            nIdx = static_cast<uint32_t>(m_TxoID);

        return nIdx;
    }

    std::string ShieldedCoin::getStatusString() const
    {
        static std::map<Status, std::string> Strings
        {
            {Unavailable,   "unavailable"},
            {Available,     "available"},
            {Maturing,      "maturing"},
            {Outgoing,      "outgoing"},
            {Incoming,      "incoming"},
            {Spent,         "spent"},
            {Consumed,      "consumed"},
        };

        return Strings[m_Status];
    }

    void ShieldedCoin::setOffset(uint32_t offset)
    {
        m_offset = offset;
    }

    void ShieldedCoin::UnlinkStatus::Init(const ShieldedCoin& sc, TxoID nShieldedOuts)
    {
        if (kTxoInvalidID == sc.m_TxoID)
        {
            m_Progress = 0;
            m_WndReserve0 = 0;
            m_WndReserve1 = 0;
        }
        else
        {
            const Rules& r = Rules::get();
            const uint32_t N = std::max(r.Shielded.m_ProofMax.get_N(), 1U);


            uint32_t nRemaining = N - sc.get_WndIndex(N) - 1;

            beam::setmax(nShieldedOuts, sc.m_TxoID + 1);
            nShieldedOuts -= sc.m_TxoID; // to relative
            assert(nShieldedOuts);

            if (nShieldedOuts <= nRemaining)
            {
                assert(nRemaining);

                uint32_t n = static_cast<uint32_t>(nShieldedOuts) - 1;
                assert(n < nRemaining);

                m_Progress = 1 + 99U * n / nRemaining;
            }
            else
                m_Progress = 100;

            m_WndReserve0 = get_Reserve(nRemaining + 1, nShieldedOuts);
            m_WndReserve1 = get_Reserve(N, nShieldedOuts);
        }
    }

    template <typename TDst, typename TSrc>
    TDst CastSaturated(const TSrc& x)
    {
        return
            (x > std::numeric_limits<TDst>::max()) ? std::numeric_limits<TDst>::max() :
            (x < std::numeric_limits<TDst>::min()) ? std::numeric_limits<TDst>::min() :
            static_cast<TDst>(x);
    }

    int32_t ShieldedCoin::get_Reserve(uint32_t nEndRel, TxoID nShieldedOutsRel)
    {
        int64_t n = nEndRel;
        n += Rules::get().Shielded.MaxWindowBacklog;
        n -= Rules::get().Shielded.MaxIns; // safety thershold.

        static_assert(sizeof(n) == sizeof(nShieldedOutsRel)); // both should be 64bit
        n -= nShieldedOutsRel;

        return CastSaturated<int32_t>(n);
    }

    bool ShieldedCoin::UnlinkStatus::IsLargeSpendWindowLost() const
    {
        return m_WndReserve1 < 0;
    }

    int ShieldedCoin::UnlinkStatus::get_SpendPriority() const
    {
        if (IsLargeSpendWindowLost())
            return 0;

        const int32_t nWndThresholdHi = 100; // Urgently spend if window will close in less than this
        if (m_WndReserve1 < nWndThresholdHi)
            return 2;

        if (100 == m_Progress)
            return 1;

        return -1;
    }

    void ShieldedCoin::Sort(std::vector<ShieldedCoin::WithStatus>& v)
    {
        struct MyCmp
        {
            bool operator ()(const WithStatus& a, const WithStatus& b) const
            {
                // return if a should be spend before b
                int na = a.second.get_SpendPriority();
                int nb = b.second.get_SpendPriority();
                if (na < nb)
                    return false;
                if (na > nb)
                    return true;

                if (-1 == na)
                {
                    // if both are washing - spend the one that is cleaner
                    if (a.second.m_WndReserve0 < b.second.m_WndReserve0)
                        return true;
                    if (a.second.m_WndReserve0 > b.second.m_WndReserve0)
                        return false;
                }

                // not really important, but for steady sort let's resolve ambiguity by coin unique ID
                return a.first.m_TxoID < b.first.m_TxoID;
            }
        } mcmp;

        std::sort(v.begin(), v.end(), mcmp);
    }
}
