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
    TxDescription::TxDescription(const TxParameters& p)
        : TxParameters(p)
    {
        fillFromTxParameters(*this);
    }

    void TxDescription::fillFromTxParameters(const TxParameters& parameters)
    {
        boost::optional<TxID> txId = parameters.GetTxID();
        if (txId)
        {
            m_txId = *txId;
        }

        #define MACRO(id, type, field, init) \
        if (auto value = parameters.GetParameter<type>(id); value) \
            field = *value; 
        BEAM_TX_DESCRIPTION_INITIAL_PARAMS(MACRO)
        #undef MACRO
    }

    bool TxDescription::canResume() const
    {
        return m_status == TxStatus::Pending
            || m_status == TxStatus::InProgress
            || m_status == TxStatus::Registering
            || m_status == TxStatus::Confirming;
    }

    bool TxDescription::canCancel() const
    {
        return m_status == TxStatus::InProgress
            || m_status == TxStatus::Pending;
    }

    bool TxDescription::canDelete() const
    {
        return m_status == TxStatus::Failed
            || m_status == TxStatus::Completed
            || m_status == TxStatus::Canceled;
    }

    std::string TxDescription::getTxTypeString() const
    {
        switch(m_txType)
        {
#define MACRO(type, index, s) case TxType::type: return s;
            BEAM_TX_TYPES_MAP(MACRO)
#undef MACRO

        default:
            BOOST_ASSERT_MSG(false, kErrorUnknownTxType);
            return "unknown";
        }
    }

    /// Return empty string if exchange rate is not available
    Amount TxDescription::getExchangeRate(const Currency& targetCurrency, beam::Asset::ID assetId /* = beam::Asset::s_InvalidID*/) const
    {
        auto exchangeRatesOptional = GetParameter<std::vector<ExchangeRate>>(TxParameterID::ExchangeRates);
        if (exchangeRatesOptional)
        {
            std::vector<ExchangeRate>& rates = *exchangeRatesOptional;

            beam::wallet::Currency fromCurrency(beam::Asset::s_BeamID);
            if (assetId == beam::Asset::s_InvalidID || assetId == beam::Asset::s_BeamID)
            {
                auto assetIdOptional = GetParameter<Asset::ID>(TxParameterID::AssetID);
                Asset::ID txAssetId  = assetIdOptional ? *assetIdOptional : beam::Asset::s_BeamID;
                fromCurrency = beam::wallet::Currency(txAssetId);
            } else
            {
                fromCurrency = beam::wallet::Currency(assetId);
            }

            auto search = std::find_if(std::begin(rates), std::end(rates),
                                    [&fromCurrency, &targetCurrency](const ExchangeRate& r)
                                    {
                                        return r.m_from == fromCurrency && r.m_to == targetCurrency;
                                    });

            if (search != std::cend(rates))
            {
                return search->m_rate;
            }
        }

        return 0UL;
    }

    std::string TxDescription::getToken() const
    {
        auto token = GetParameter<std::string>(TxParameterID::OriginalToken);
        if (token)
        {
            return *token;
        }
        return {};
    }

    std::string TxDescription::getSenderEndpoint() const
    {
        return getEndpoint(m_sender);
    }

    std::string TxDescription::getReceiverEndpoint() const
    {
        return getEndpoint(!m_sender);
    }

    std::string TxDescription::getSender() const
    {
        return beam::to_string(m_sender ? m_myAddr : m_peerAddr);
    }

    std::string TxDescription::getReceiver() const
    {
        return beam::to_string(!m_sender ? m_myAddr : m_peerAddr);
    }

    std::string TxDescription::getEndpoint(bool isMy) const
    {
        auto v = GetParameter<PeerID>(isMy ? TxParameterID::MyEndpoint : TxParameterID::PeerEndpoint);
        if (v)
            return beam::to_base58(*v);

        // try from addr
        const WalletID& wid = isMy ? m_myAddr : m_peerAddr;
        if (wid.m_Pk != Zero)
            return beam::to_base58(wid.m_Pk);

        return {};
    }

    std::string TxDescription::getAddressFrom() const
    {
        if (m_txType == wallet::TxType::PushTransaction && !m_sender)
        {
            return getSenderEndpoint();
        }
        return beam::to_string(m_sender ? m_myAddr : m_peerAddr);
    }

    std::string TxDescription::getAddressTo() const
    {
        if (m_sender)
        {
            auto token = getToken();
            if (token.empty())
                return beam::to_string(m_peerAddr);

            return token;
        }
        return beam::to_string(m_myAddr);
    }
}  // namespace beam::wallet
