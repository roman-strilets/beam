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
    const char* get_BroadcastValidatorPublicKey()
    {
        switch (Rules::get().m_Network)
        {
        case Rules::Network::testnet: return "dc3df1d8cd489c3fe990eb8b4b8a58089a7706a5fc3b61b9c098047aac2c2812";
        case Rules::Network::mainnet: return "8ea783eced5d65139bbdf432814a6ed91ebefe8079395f63a13beed1dfce39da";
        case Rules::Network::dappnet: return "4c5b0b58caf69542490d1bef077467010a396cd20a4d1bbba269c8dff41da44e";
        case Rules::Network::masternet: return "db617cedb17543375b602036ab223b67b06f8648de2bb04de047f485e7a9daec";
        default:
            break; // suppress warning
        }
        return "";
    }

    const char kTimeStampFormat3x3[] = "%Y.%m.%d %H:%M:%S";
    const char kTimeStampFormatCsv[] = "%d %b %Y | %H:%M";

    int WalletID::cmp(const WalletID& x) const
    {
        int n = m_Channel.cmp(x.m_Channel);
        if (n)
            return n;
        return m_Pk.cmp(x.m_Pk);
    }

    bool WalletID::FromBuf(const ByteBuffer& x)
    {
        if (x.size() > sizeof(*this))
            return false;

        typedef uintBig_t<sizeof(*this)> BigSelf;
        static_assert(sizeof(BigSelf) == sizeof(*this), "");

        *reinterpret_cast<BigSelf*>(this) = Blob(x);
        return true;
    }

    bool WalletID::FromHex(const std::string& s)
    {
        bool bValid = true;
        ByteBuffer bb = from_hex(s, &bValid);

        return bValid && FromBuf(bb);
    }

    bool WalletID::IsValid() const
    {
        BbsChannel channel;
        m_Channel.Export(channel);
        Point::Native p;
        return m_Pk.ExportNnz(p) && channel < proto::Bbs::s_MaxWalletChannels;
    }

    BbsChannel WalletID::get_Channel() const
    {
        BbsChannel ret;
        m_Channel.Export(ret);
        return ret;
    }

    void WalletID::SetChannelFromPk()
    {
        // derive the channel from the address
        BbsChannel ch;
        m_Pk.ExportWord<0>(ch);
        ch %= proto::Bbs::s_MaxWalletChannels;

        m_Channel = ch;
    }

    boost::optional<PeerID> GetPeerIDFromHex(const std::string& s)
    {
        boost::optional<PeerID> res;
        bool isValid = false;
        auto buf = from_hex(s, &isValid);
        if (!isValid)
        {
            return res;
        }
        res.emplace();
        *res = Blob(buf);
        return res;
    }

    bool fromByteBuffer(const ByteBuffer& b, ByteBuffer& value)
    {
        value = b;
        return true;
    }

    ByteBuffer toByteBuffer(const ECC::Point::Native& value)
    {
        ECC::Point pt;
        if (value.Export(pt))
        {
            return toByteBuffer(pt);
        }
        return ByteBuffer();
    }

    ByteBuffer toByteBuffer(const ECC::Scalar::Native& value)
    {
        ECC::Scalar s;
        value.Export(s);
        return toByteBuffer(s);
    }

    ByteBuffer toByteBuffer(const ByteBuffer& value)
    {
        return value;
    }

    ErrorType getWalletError(proto::NodeProcessingException::Type exceptionType)
    {
        switch (exceptionType)
        {
        case proto::NodeProcessingException::Type::Incompatible:
            return ErrorType::NodeProtocolIncompatible;
        case proto::NodeProcessingException::Type::TimeOutOfSync:
            return ErrorType::TimeOutOfSync;
        default:
            return ErrorType::NodeProtocolBase;
        }
    }

    ErrorType getWalletError(io::ErrorCode errorCode)
    {
        switch (errorCode)
        {
        case io::ErrorCode::EC_ETIMEDOUT:
            return ErrorType::ConnectionTimedOut;
        case io::ErrorCode::EC_ECONNREFUSED:
            return ErrorType::ConnectionRefused;
        case io::ErrorCode::EC_EHOSTUNREACH:
            return ErrorType::ConnectionHostUnreach;
        case io::ErrorCode::EC_EADDRINUSE:
            return ErrorType::ConnectionAddrInUse;
        case io::ErrorCode::EC_HOST_RESOLVED_ERROR:
            return ErrorType::HostResolvedError;
        default:
            return ErrorType::ConnectionBase;
        }
    }

    bool ConfirmationBase::IsValid(const PeerID& pid) const
    {
        Point::Native pk;
        if (!pid.ExportNnz(pk))
            return false;

        Hash::Value hv;
        get_Hash(hv);

        return m_Signature.IsValid(hv, pk);
    }

    void ConfirmationBase::Sign(const Scalar::Native& sk)
    {
        Hash::Value hv;
        get_Hash(hv);

        m_Signature.Sign(hv, sk);
    }

    void PaymentConfirmation::get_Hash(Hash::Value& hv) const
    {
        Hash::Processor hp;
        hp
            << "PaymentConfirmation"
            << m_KernelID
            << m_Sender
            << m_Value;

        if (m_AssetID != Asset::s_InvalidID)
        {
            hp
                << "asset"
                << m_AssetID;
        }

        hp >> hv;
    }

    void SignatureHandler::get_Hash(Hash::Value& hv) const
    {
        beam::Blob data(m_data);
        Hash::Processor()
            << "Undersign"
            << data
            >> hv;
    }

    bool IsValidTimeStamp(Timestamp currentBlockTime_s, Timestamp tolerance_s)
    {
        Timestamp currentTime_s = getTimestamp();

        if (currentTime_s > currentBlockTime_s + tolerance_s)
        {
            BEAM_LOG_INFO() << "It seems that last known blockchain tip is not up to date";
            return false;
        }
        return true;
    }

    uint64_t get_RandomID()
    {
        uintBigFor<uint64_t>::Type val;
        ECC::GenRandom(val);

        uint64_t ret;
        val.Export(ret);
        return ret;
    }

    std::string TimestampFile(const std::string& fileName)
    {
        size_t dotPos = fileName.find_last_of('.');

        stringstream ss;
        ss << fileName.substr(0, dotPos);
        ss << getTimestamp();

        if (dotPos != string::npos)
        {
            ss << fileName.substr(dotPos);
        }

        string timestampedPath = ss.str();
        return timestampedPath;
    }

    bool g_AssetsEnabled = false; // OFF by default

    TxFailureReason CheckAssetsEnabled(Height h)
    {
        const Rules& r = Rules::get();
        if (!r.IsPastFork_<2>(h))
            return TxFailureReason::AssetsDisabledFork2;

        if (!r.CA.Enabled)
            return TxFailureReason::AssetsDisabledInRules;

        if (!g_AssetsEnabled)
            return TxFailureReason::AssetsDisabledInWallet;

        return TxFailureReason::Count;
    }

    uint32_t GetShieldedInputsNum(const std::vector<TxKernel::Ptr>& v)
    {
        uint32_t ret = 0;
        for (uint32_t i = 0; i < v.size(); i++)
            if (TxKernel::Subtype::ShieldedInput == v[i]->get_Subtype())
                ret++;
        return ret;
    }
}  // namespace beam::wallet
