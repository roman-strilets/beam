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
    beam::wallet::TxFailureReason GetFailtureReason(const beam::wallet::TxParameters& txParams)
    {
        beam::wallet::TxFailureReason failureReason = beam::wallet::TxFailureReason::Unknown;
        if (auto value = txParams.GetParameter<beam::wallet::TxFailureReason>(beam::wallet::TxParameterID::FailureReason); value)
            failureReason = *value;

        return failureReason;
    }
}

namespace beam::wallet
{
    std::tuple<TxStatus, bool, bool> ParseParamsForStatusInterpretation(const TxParameters& txParams)
    {
        TxStatus status = TxStatus::Pending;
        bool sender = false;
        bool selfTx = false;

        if (auto value = txParams.GetParameter<wallet::TxStatus>(TxParameterID::Status); value)
            status = *value;

        if (auto value = txParams.GetParameter<bool>(TxParameterID::IsSender); value)
            sender = *value;

        if (auto value = txParams.GetParameter<bool>(TxParameterID::IsSelfTx); value)
            selfTx = *value;

        return std::make_tuple(status, sender, selfTx);
    }

    std::string GetTxStatusStr(const TxParameters& txParams)
    {
        auto [status, sender, selfTx] = ParseParamsForStatusInterpretation(txParams);

        switch (status)
        {
            case TxStatus::Pending:
                return "pending";
            case TxStatus::InProgress:
                return selfTx  ? "self sending" : (sender ? "waiting for receiver" : "waiting for sender");
            case TxStatus::Registering: 
                return selfTx ? "self sending" : (sender ? "sending" : "receiving" );
            case TxStatus::Failed:
                return TxFailureReason::TransactionExpired == GetFailtureReason(txParams) ? "expired" : "failed";
            case TxStatus::Canceled:
                return "cancelled";
            case TxStatus::Completed:
                return selfTx ? "completed" : (sender ? "sent" : "received");
            case TxStatus::Confirming:
                return "confirming";
            default:
                BOOST_ASSERT_MSG(false, kErrorUnknownTxStatus);
                return "unknown";
        }
    }

    std::string GetSimpleTxStatusStr(const TxParameters& txParams)
    {
        auto [status, sender, selfTx] = ParseParamsForStatusInterpretation(txParams);

        switch (status)
        {
        case TxStatus::InProgress:
            return selfTx ? "sending to own address" : (sender ? "waiting for receiver" : "waiting for sender");
        case TxStatus::Registering:
            return selfTx ? "sending to own address" : "in progress";
        case TxStatus::Completed:
            return selfTx ? "sent to own address" : (sender ? "sent" : "received");
        default:
            break;
        }
        return GetTxStatusStr(txParams);
    }

    std::string GetMaxAnonimityTxStatusStr(const TxParameters& txParams)
    {
        TxAddressType addressType = TxAddressType::Unknown;
        auto storedType = txParams.GetParameter<TxAddressType>(TxParameterID::AddressType);
        if (storedType)
        {
            addressType = *storedType;
        }
        else
        {
            TxDescription tx_desc(txParams);
            addressType = tx_desc.m_sender ? GetAddressType(tx_desc) : TxAddressType::Unknown;
        }

        auto [status, sender, selfTx] = ParseParamsForStatusInterpretation(txParams);

        switch (addressType)
        {
            case TxAddressType::MaxPrivacy:
                switch (status)
                {
                case TxStatus::Registering:
                    return selfTx ? "sending maximum anonymity to own address" : "in progress maximum anonymity";
                case TxStatus::Failed:
                    return TxFailureReason::TransactionExpired == GetFailtureReason(txParams) ? "expired" : "failed maximum anonymity";
                case TxStatus::Canceled:
                    return "canceled maximum anonymity";
                case TxStatus::Completed:
                    return selfTx ? "sent maximum anonymity to own address" : (sender ? "sent maximum anonymity" : "received maximum anonymity");
                default:
                    break;
                }
                break;
            case TxAddressType::Offline:
                switch (status)
                {
                case TxStatus::Registering:
                    return selfTx ? "sending offline to own address" : "in progress offline";
                case TxStatus::Failed:
                    return TxFailureReason::TransactionExpired == GetFailtureReason(txParams) ? "expired" : "failed offline";
                case TxStatus::Canceled:
                    return "canceled offline";
                case TxStatus::Completed:
                    return selfTx ? "sent offline to own address" : (sender ? "sent offline" : "received offline");
                default:
                    break;
                }
                break;
            case TxAddressType::PublicOffline:
                switch (status)
                {
                case TxStatus::Registering:
                    return selfTx ? "sending public offline to own address" : "in progress public offline";
                case TxStatus::Failed:
                    return TxFailureReason::TransactionExpired == GetFailtureReason(txParams) ? "expired" : "failed public offline";
                case TxStatus::Canceled:
                    return "canceled public offline";
                case TxStatus::Completed:
                    return selfTx ? "sent public offline to own address" : (sender ? "sent public offline" : "received public offline");
                default:
                    break;
                }
                break;
            default:
                break;
        }

        return GetTxStatusStr(txParams);
    }

    std::string GetAssetTxStatusStr(const TxParameters& txParams)
    {
        wallet::TxType txType = wallet::TxType::AssetInfo;
        if (auto value = txParams.GetParameter<TxType>(TxParameterID::TransactionType); value)
            txType = *value;

        TxStatus status = TxStatus::Pending;
        if (auto value = txParams.GetParameter<wallet::TxStatus>(TxParameterID::Status); value)
            status = *value;

        if (status == TxStatus::InProgress && txType == TxType::AssetInfo) return "getting info";
        if (status == TxStatus::Completed)
        {
            switch (txType)
            {
                case TxType::AssetIssue: return "asset issued";
                case TxType::AssetConsume: return "asset consumed";
                case TxType::AssetReg: return "asset registered";
                case TxType::AssetUnreg: return "asset unregistered";
                case TxType::AssetInfo: return "asset confirmed";
                default: break;
            }
        }

        return GetTxStatusStr(txParams);
    }

    std::string GetContractTxStatusStr(const TxParameters& txParams)
    {
        TxStatus status = TxStatus::Pending;
        if (auto value = txParams.GetParameter<wallet::TxStatus>(TxParameterID::Status); value)
            status = *value;

        switch (status)
        {
            case TxStatus::InProgress:
            case TxStatus::Registering:
                return "in progress";
            case TxStatus::Completed:
                return "completed";
            default:
                break;
        }
        return GetTxStatusStr(txParams);
    }

    std::string interpretStatus(const TxDescription& tx)
    {
        switch (tx.m_txType)
        {
            case wallet::TxType::Simple:
                return GetSimpleTxStatusStr(tx);
            case wallet::TxType::AssetIssue:
            case wallet::TxType::AssetConsume:
            case wallet::TxType::AssetReg:
            case wallet::TxType::AssetUnreg:
            case wallet::TxType::AssetInfo:
                return GetAssetTxStatusStr(tx);
            case wallet::TxType::PushTransaction:
                return GetMaxAnonimityTxStatusStr(tx);
            case wallet::TxType::Contract:
                return GetContractTxStatusStr(tx);
            case wallet::TxType::DexSimpleSwap:
                return GetSimpleTxStatusStr(tx);
            default:
                BOOST_ASSERT_MSG(false, kErrorUnknownTxType);
                return "unknown";
        }
    }
}  // namespace beam::wallet
