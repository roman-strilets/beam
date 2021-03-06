// Copyright 2020 The Beam Team
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

#include "pull_transaction.h"
#include "core/shielded.h"
#include "wallet/core/strings_resources.h"

namespace beam::wallet::lelantus
{
    TxParameters CreatePullTransactionParameters(const WalletID& myID, const boost::optional<TxID>& txId)
    {
        return CreateTransactionParameters(TxType::PullTransaction, txId)
            .SetParameter(TxParameterID::MyID, myID)
            .SetParameter(TxParameterID::IsSender, false);
    }

    BaseTransaction::Ptr PullTransaction::Creator::Create(const TxContext& context)
    {
        return BaseTransaction::Ptr(new PullTransaction(context, m_withAssets));
    }

    TxParameters PullTransaction::Creator::CheckAndCompleteParameters(const TxParameters& parameters)
    {
        // TODO roman.strilets implement this
        return parameters;
    }

    PullTransaction::PullTransaction(const TxContext& context
        , bool withAssets)
        : BaseTransaction(context)
        , m_withAssets(withAssets)
    {
    }

    TxType PullTransaction::GetType() const
    {
        return TxType::PullTransaction;
    }

    bool PullTransaction::IsInSafety() const
    {
        // TODO roman.strilets implement this
        return true;
    }

    void PullTransaction::UpdateImpl()
    {
        Transaction::FeeSettings fs;
        Amount feeShielded = fs.m_ShieldedInput + fs.m_Kernel;

        if (!m_TxBuilder)
        {
            AmountList amoutList; // dummy
            amoutList.push_back(0);

            Amount fee = GetMandatoryParameter<Amount>(TxParameterID::Fee);

            // by convention the fee now includes ALL the fee, whereas our code will add the minimal shielded fee.
            if (fee >= feeShielded)
                fee -= feeShielded;
            std::setmax(fee, fs.m_Kernel);


            m_TxBuilder = std::make_shared<BaseTxBuilder>(*this, GetSubTxID(), amoutList, fee);
        }

        if (!m_TxBuilder->GetInitialTxParams())
        {
            UpdateTxDescription(TxStatus::InProgress);

            TxoID shieldedId = GetMandatoryParameter<TxoID>(TxParameterID::ShieldedOutputId);
            auto shieldedCoin = GetWalletDB()->getShieldedCoin(shieldedId);

            if (shieldedCoin->m_CoinID.m_AssetID && !m_withAssets)
                throw TransactionFailedException(false, TxFailureReason::AssetsDisabled);

            auto& vInp = m_TxBuilder->get_InputCoinsShielded();
            if (vInp.empty())
            {
                if (!shieldedCoin || !shieldedCoin->IsAvailable())
                    throw TransactionFailedException(false, TxFailureReason::NoInputs);

                const auto unitName = m_TxBuilder->IsAssetTx() ? kAmountASSET : "";
                const auto nthName = m_TxBuilder->IsAssetTx() ? kAmountAGROTH : "";

                LOG_INFO() << m_Context << " Extracting from shielded pool:"
                    << " ID - " << shieldedId << ", amount - " << PrintableAmount(shieldedCoin->m_CoinID.m_Value, false, unitName, nthName)
                    << ", receiving amount - " << PrintableAmount(m_TxBuilder->GetAmount(), false, unitName, nthName)
                    << " (fee: " << PrintableAmount(m_TxBuilder->GetFee()) << ")";

                Cast::Down<ShieldedTxo::ID>(vInp.emplace_back()) = shieldedCoin->m_CoinID;
                vInp.back().m_Fee = feeShielded;

                m_TxBuilder->SelectInputs();
                m_TxBuilder->AddChange();
            }
        }

        if (m_TxBuilder->CreateInputs())
        {
            return;
        }

        if (m_TxBuilder->CreateOutputs())
        {
            return;
        }

        if (m_TxBuilder->SignSplit())
        {
            return;
        }

        uint8_t nRegistered = proto::TxStatus::Unspecified;
        if (!GetParameter(TxParameterID::TransactionRegistered, nRegistered))
        {
            if (CheckExpired())
            {
                return;
            }

            // Construct transaction
            auto transaction = m_TxBuilder->CreateTransaction();

            // Verify final transaction
            TxBase::Context::Params pars;
            TxBase::Context ctx(pars);
            ctx.m_Height.m_Min = m_TxBuilder->GetMinHeight();
            if (!transaction->IsValid(ctx))
            {
                OnFailed(TxFailureReason::InvalidTransaction, true);
                return;
            }

            // register TX
            GetGateway().register_tx(GetTxID(), transaction, GetSubTxID());
            return;
        }

        if (proto::TxStatus::InvalidContext == nRegistered) // we have to ensure that this transaction hasn't already added to blockchain)
        {
            Height lastUnconfirmedHeight = 0;
            if (GetParameter(TxParameterID::KernelUnconfirmedHeight, lastUnconfirmedHeight) && lastUnconfirmedHeight > 0)
            {
                OnFailed(TxFailureReason::FailedToRegister, true);
                return;
            }
        }
        else if (proto::TxStatus::Ok != nRegistered)
        {
            OnFailed(TxFailureReason::FailedToRegister, true);
            return;
        }

        // get Kernel proof
        Height hProof = 0;
        GetParameter(TxParameterID::KernelProofHeight, hProof);
        if (!hProof)
        {
            ConfirmKernel(m_TxBuilder->GetKernelID());
            return;
        }

        // update "m_spentHeight" for shieldedCoin
        auto shieldedCoinModified = GetWalletDB()->getShieldedCoin(GetTxID());
        if (shieldedCoinModified)
        {
            shieldedCoinModified->m_spentHeight = std::min(shieldedCoinModified->m_spentHeight, hProof);
            GetWalletDB()->saveShieldedCoin(shieldedCoinModified.get());
        }

        SetCompletedTxCoinStatuses(hProof);
        CompleteTx();
    }

    void PullTransaction::RollbackTx()
    {
        LOG_INFO() << m_Context << " Transaction failed. Rollback...";
        GetWalletDB()->restoreShieldedCoinsSpentByTx(GetTxID());
        GetWalletDB()->deleteCoinsCreatedByTx(GetTxID());
    }
} // namespace beam::wallet::lelantus