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

namespace
{
    bool CopyParameter(beam::wallet::TxParameterID paramID, const beam::wallet::TxParameters& input, beam::wallet::TxParameters& dest)
    {
        ByteBuffer buf;
        if (input.GetParameter(paramID, buf))
        {
            dest.SetParameter(paramID, buf);
            return true; // copied
        }
        return false;
    }
}

namespace beam::wallet
{
    namespace
    {
        TxAddressType GetAddressTypeImpl(const TxParameters& params)
        {
            auto type = params.GetParameter<TxType>(TxParameterID::TransactionType);
            if (type)
            {
                if (*type == TxType::Simple)
                    return TxAddressType::Regular;

                if (*type == TxType::AtomicSwap)
                    return TxAddressType::AtomicSwap;

                if (*type != TxType::PushTransaction)
                    return TxAddressType::Unknown;
            }

            auto peerAddr = params.GetParameter<WalletID>(TxParameterID::PeerAddr); // sbbs addres
            auto peerEndpoint = params.GetParameter<PeerID>(TxParameterID::PeerEndpoint);

            auto voucher = params.GetParameter<ShieldedTxo::Voucher>(TxParameterID::Voucher);
            if (voucher && peerEndpoint)
                return TxAddressType::MaxPrivacy;

            auto vouchers = params.GetParameter<ShieldedVoucherList>(TxParameterID::ShieldedVoucherList);
            if (vouchers && !vouchers->empty() && peerEndpoint && peerAddr)
                return TxAddressType::Offline;

            auto gen = params.GetParameter<ShieldedTxo::PublicGen>(TxParameterID::PublicAddreessGen);
            if (gen)
                return TxAddressType::PublicOffline;

            if (peerAddr)
                return TxAddressType::Regular;

            return TxAddressType::Unknown;
        }
    }

    TxParameters::TxParameters(const boost::optional<TxID>& txID)
        : m_ID(txID)
    {

    }

    bool TxParameters::operator==(const TxParameters& other) const
    {
        return m_ID == other.m_ID &&
            m_Parameters == other.m_Parameters;
    }

    bool TxParameters::operator!=(const TxParameters& other) const
    {
        return !(*this == other);
    }

    const boost::optional<TxID>& TxParameters::GetTxID() const
    {
        return m_ID;
    }

    TxParameters& TxParameters::SetParameter(TxParameterID parameterID, ByteBuffer&& parameter, SubTxID subTxID)
    {
        m_Parameters[subTxID][parameterID] = std::move(parameter);
        return *this;
    }

    PackedTxParameters TxParameters::Pack() const
    {
        PackedTxParameters parameters;
        for (const auto& subTx : m_Parameters)
        {
            if (subTx.first > kDefaultSubTxID)
            {
                parameters.emplace_back(TxParameterID::SubTxIndex, toByteBuffer(subTx.first));
            }
            for (const auto& p : subTx.second)
            {
                parameters.emplace_back(p.first, p.second);
            }
        }
        return parameters;
    }

    TxToken::TxToken(const TxParameters& parameters)
        : m_Flags(TxToken::TokenFlag)
        , m_TxID(parameters.GetTxID())
        , m_Parameters(parameters.Pack())
    {

    }

    TxParameters TxToken::UnpackParameters() const
    {
        TxParameters result(m_TxID);

        SubTxID subTxID = kDefaultSubTxID;
        Deserializer d;
        for (const auto& p : m_Parameters)
        {
            if (p.first == TxParameterID::SubTxIndex)
            {
                // change subTxID
                d.reset(p.second.data(), p.second.size());
                d & subTxID;
                continue;
            }

            result.SetParameter(p.first, p.second, subTxID);
        }
        return result;
    }

    bool TxToken::IsValid() const
    {
        for (const auto& p : m_Parameters)
        {
            switch (p.first)
            {
#define MACRO(name, index, type) \
            case TxParameterID::name: \
                { \
                    type value; \
                    if (!fromByteBuffer(p.second, value)) \
                    { \
                        return false; \
                    } \
                } break; 
                BEAM_TX_PUBLIC_PARAMETERS_MAP(MACRO)
#undef MACRO
            default:
                break;
            }
        }
        return true;
    }

    boost::optional<TxParameters> ParseParameters(const string& text)
    {
        bool isValid = true;
        ByteBuffer buffer = from_hex(text, &isValid);
        if (!isValid)
        {
            buffer = DecodeBase58(text);
            if (buffer.empty())
            {
                return {};
            }
        }

        if (buffer.size() < 2)
        {
            return {};
        }

        if (buffer.size() > 33 && buffer[0] & TxToken::TokenFlag) // token
        {
            try
            {
                TxToken token;
                // simply deserialize for now
                Deserializer d;
                d.reset(&buffer[0], buffer.size());
                d & token;

                if (token.IsValid())
                {
                    return boost::make_optional<TxParameters>(token.UnpackParameters());
                }
            }
            catch (...)
            {
                // failed to deserialize
            }
        }
        else // plain WalletID
        {
            WalletID walletID;
            if (walletID.FromBuf(buffer) && walletID.IsValid())
            {
                auto result = boost::make_optional<TxParameters>({});
                result->SetParameter(TxParameterID::PeerAddr, walletID);
                return result;
            }
        }
        return {};
    }

    bool LoadReceiverParams(const TxParameters& receiverParams, TxParameters& params, TxAddressType type)
    {
        const TxParameters& p = receiverParams;
        switch (type)
        {
        case TxAddressType::AtomicSwap:
        case TxAddressType::Regular:
            {
                if (type == TxAddressType::Regular)
                {
                    params.SetParameter(TxParameterID::TransactionType, TxType::Simple);
                }
                else if (type == TxAddressType::AtomicSwap)
                {
                    params.SetParameter(TxParameterID::TransactionType, TxType::AtomicSwap);
                }

                if (!CopyParameter(TxParameterID::PeerAddr, p, params))
                {
                    return false;
                }
                CopyParameter(TxParameterID::PeerEndpoint, p, params);
                break;
            }

        case TxAddressType::PublicOffline:
            {
                params.SetParameter(TxParameterID::TransactionType, TxType::PushTransaction);

                ShieldedTxo::PublicGen pgen;
                if (!p.GetParameter(TxParameterID::PublicAddreessGen, pgen))
                    return false;

                PeerID pid;
                ECC::Signature sig;
                if (p.GetParameter(TxParameterID::PublicAddressGenSig, sig) && p.GetParameter(TxParameterID::PeerEndpoint, pid))
                {
                    params.SetParameter(TxParameterID::PublicAddreessGen, pgen);
                    params.SetParameter(TxParameterID::PublicAddressGenSig, sig);
                    params.SetParameter(TxParameterID::PeerEndpoint, pid);
                }
                else
                {
                    // legacy address. Temporarily support them.
                    ECC::Hash::Value hv;
                    ECC::GenRandom(hv); // nonce

                    ShieldedTxo::Voucher voucher;

                    ShieldedTxo::Data::TicketParams tp;
                    tp.Generate(voucher.m_Ticket, pgen, hv);

                    voucher.m_SharedSecret = tp.m_SharedSecret;

                    // fake Endpoint
                    Scalar::Native sk;
                    sk.GenRandomNnz();
                    pid.FromSk(sk);

                    voucher.get_Hash(hv);
                    voucher.m_Signature.Sign(hv, sk);

                    params.SetParameter(TxParameterID::Voucher, voucher);
                    params.SetParameter(TxParameterID::PeerEndpoint, pid);
                }
            }
            break;

        case TxAddressType::Offline:
            {
                params.SetParameter(TxParameterID::TransactionType, TxType::PushTransaction);
                CopyParameter(TxParameterID::PeerAddr, p, params);
                auto peerEndpoint = p.GetParameter<PeerID>(TxParameterID::PeerEndpoint);
                params.SetParameter(TxParameterID::PeerEndpoint, *peerEndpoint);

                if (auto vouchers = p.GetParameter<ShieldedVoucherList>(TxParameterID::ShieldedVoucherList); vouchers)
                {
                    if (!IsValidVoucherList(*vouchers, *peerEndpoint))
                    {
                        BEAM_LOG_ERROR() << "Voucher signature verification failed. Unauthorized voucher was provider.";
                        return false;
                    }
                    params.SetParameter(TxParameterID::ShieldedVoucherList, *vouchers);
                }
            }
            break;

        case TxAddressType::MaxPrivacy:
            {
                params.SetParameter(TxParameterID::TransactionType, TxType::PushTransaction);
                auto peerEndpoint = p.GetParameter<PeerID>(TxParameterID::PeerEndpoint);
                params.SetParameter(TxParameterID::PeerEndpoint, *peerEndpoint);
                auto voucher = p.GetParameter<ShieldedTxo::Voucher>(TxParameterID::Voucher);
                if (!voucher->IsValid(*peerEndpoint))
                {
                    BEAM_LOG_ERROR() << "Voucher signature verification failed. Unauthorized voucher was provider.";
                    return false;
                }
                params.SetParameter(TxParameterID::Voucher, *voucher);
                params.SetParameter(TxParameterID::MaxPrivacyMinAnonimitySet, uint8_t(64));
            }
            break;

        default:
            return false;
        }

        params.SetParameter(TxParameterID::AddressType, type);
        ProcessLibraryVersion(receiverParams);

        return true;
    }

    std::string GetSendToken(const std::string& sbbsAddress, const std::string& endpointStr, Amount amount)
    {
        WalletID walletID;
        if (!walletID.FromHex(sbbsAddress))
        {
            return "";
        }
        auto endPoint = GetPeerIDFromHex(endpointStr);
        if (!endPoint)
        {
            return "";
        }

        TxParameters parameters;
        if (amount > 0)
        {
            parameters.SetParameter(TxParameterID::Amount, amount);
        }

        parameters.SetParameter(TxParameterID::PeerAddr, walletID);
        parameters.SetParameter(TxParameterID::TransactionType, beam::wallet::TxType::Simple);
        parameters.SetParameter(TxParameterID::PeerEndpoint, *endPoint);

        return beam::to_string(parameters);
    }

    ShieldedVoucherList GenerateVoucherList(const std::shared_ptr<IPrivateKeyKeeper2>& pKeeper, uint64_t ownID, size_t count)
    {
        ShieldedVoucherList res;

        if (pKeeper && count)
        {
            IPrivateKeyKeeper2::Method::CreateVoucherShielded m;
            m.m_iEndpoint = ownID;
            m.m_Count = static_cast<uint32_t>(count);
            ECC::GenRandom(m.m_Nonce);

            if (IPrivateKeyKeeper2::Status::Success == pKeeper->InvokeSync(m))
                res = std::move(m.m_Res);
        }

        return res;
    }

    bool IsValidVoucherList(const ShieldedVoucherList& vouchers, const PeerID& identity)
    {
        if (vouchers.empty())
            return false;

        ECC::Point::Native pk;
        if (!identity.ExportNnz(pk))
            return false;

        for (const auto& voucher : vouchers)
        {
            if (!voucher.IsValid(pk))
            {
                return false;
            }
        }
        return true;
    }

    TxAddressType GetAddressType(const TxDescription& tx)
    {
        return GetAddressTypeImpl(tx);
    }

    TxAddressType GetAddressType(const std::string& address)
    {
        auto p = ParseParameters(address);
        if (!p)
            return TxAddressType::Unknown;

        return GetAddressTypeImpl(*p);
    }
}  // namespace beam::wallet
