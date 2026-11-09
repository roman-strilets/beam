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

#include "base_tx_builder_common.h"

namespace beam::wallet
{
    ///////////////////////////////////////
    // BaseTxBuilder::KeyKeeperHandler

    BaseTxBuilder::KeyKeeperHandler::KeyKeeperHandler(BaseTxBuilder& b, Stage& s)
    {
        m_pBuilder = b.weak_from_this();

        m_pStage = &s;
        assert(Stage::None == s);
        s = Stage::InProgress;
    }

    BaseTxBuilder::KeyKeeperHandler::~KeyKeeperHandler()
    {
        if (m_pStage)
        {
            std::shared_ptr<BaseTxBuilder> pBld = m_pBuilder.lock();
            if (pBld)
                Detach(*pBld, Stage::None);
        }
    }

    void BaseTxBuilder::KeyKeeperHandler::Detach(BaseTxBuilder&, Stage s)
    {
        if (m_pStage)
        {
            assert(Stage::InProgress == *m_pStage);
            *m_pStage = s;
            m_pStage = nullptr;
        }
    }

    void BaseTxBuilder::KeyKeeperHandler::OnDone(IPrivateKeyKeeper2::Status::Type n)
    {
        if (m_pStage)
        {
            std::shared_ptr<BaseTxBuilder> pBld = m_pBuilder.lock();
            if (pBld)
            {
                BaseTxBuilder& b = *pBld;
                if (IPrivateKeyKeeper2::Status::Success == n)
                {
                    ITransaction::Ptr pGuard(b.m_Tx.shared_from_this()); // extra ref on transaction object.
                    // Otherwise it can crash in Update() -> CompleteTx(), which will remove its ptr from live tx map

                    try {
                        OnSuccess(b);
                    }
                    catch (const TransactionFailedException& ex) {
                        Detach(b, Stage::None);
                        pBld->m_Tx.OnFailed(ex.GetReason(), ex.ShouldNofify());
                    }
                }
                else
                    OnFailed(b, n);
            }
            else
                m_pStage = nullptr;
        }
    }

    void BaseTxBuilder::KeyKeeperHandler::OnFailed(BaseTxBuilder& b, IPrivateKeyKeeper2::Status::Type n)
    {
        Detach(b, Stage::None);
        b.m_Tx.OnFailed(BaseTransaction::KeyKeeperErrorToFailureReason(n), true);
    }

    void BaseTxBuilder::KeyKeeperHandler::OnAllDone(BaseTxBuilder& b)
    {
        Detach(b, Stage::Done);
        b.m_Tx.Update(); // may complete transaction
    }

    ///////////////////////////////////////
    // BaseTxBuilder

    BaseTxBuilder::BaseTxBuilder(BaseTransaction& tx, SubTxID subTxID)
        :m_Tx(tx)
        ,m_SubTxID(subTxID)
    {
        GetParameter(TxParameterID::MinHeight, m_Height.m_Min);
        if (!m_Height.m_Min)
        {
            // automatically set current height
            Block::SystemState::Full s;
            if (m_Tx.GetTip(s))
                SaveAndStore(m_Height.m_Min, TxParameterID::MinHeight, s.get_Height());

        }

        GetParameter(TxParameterID::InputCoins, m_Coins.m_Input);
        GetParameter(TxParameterID::InputCoinsShielded, m_Coins.m_InputShielded);
        GetParameter(TxParameterID::OutputCoins, m_Coins.m_Output);

        m_pTransaction = std::make_shared<Transaction>();
        auto& trans = *m_pTransaction; // alias

        GetParameter(TxParameterID::Inputs, trans.m_vInputs);
        GetParameter(TxParameterID::ExtraKernels, trans.m_vKernels);
        GetParameter(TxParameterID::Outputs, trans.m_vOutputs);

        if (!GetParameter(TxParameterID::Offset, trans.m_Offset))
            trans.m_Offset = Zero;

        GetParameter(TxParameterID::MaxHeight, m_Height.m_Max);
        GetParameter(TxParameterID::Fee, m_Fee);

        bool bNoInOuts = trans.m_vInputs.empty() && trans.m_vOutputs.empty() && !wallet::GetShieldedInputsNum(trans.m_vKernels);
        m_GeneratingInOuts = bNoInOuts ? Stage::None : Stage::Done;

        GetParameter(TxParameterID::MutualTxState, m_Status);

        TxKernel::Ptr pKrn;
        GetParameter(TxParameterID::Kernel, pKrn);
        if (pKrn)
        {
            AddKernel(std::move(pKrn));

            if (Status::FullTx == m_Status)
                trans.NormalizeE(); // everything else must have already been normalized
        }
    }

    void BaseTxBuilder::SaveCoins()
    {
        bool bAssets = false;

        for (const auto& cid : m_Coins.m_Input)
            if (cid.m_AssetID)
                bAssets = true;

        for (const auto& si : m_Coins.m_InputShielded)
            if (si.m_AssetID)
                bAssets = true;

        for (const auto& cid : m_Coins.m_Output)
            if (cid.m_AssetID)
                bAssets = true;

        if (bAssets)
            VerifyAssetsEnabled();

        SetParameter(TxParameterID::InputCoins, m_Coins.m_Input);
        SetParameter(TxParameterID::InputCoinsShielded, m_Coins.m_InputShielded);
        SetParameter(TxParameterID::OutputCoins, m_Coins.m_Output);

        // tag inputs
        for (const auto& cid : m_Coins.m_Input)
        {
            Coin coin;
            coin.m_ID = cid;
            if (m_Tx.GetWalletDB()->findCoin(coin) && coin.m_status == Coin::Status::Available)
            {
                coin.m_spentTxId = m_Tx.GetTxID();
                m_Tx.GetWalletDB()->saveCoin(coin);
            }
            else
            {
                auto message("Coin with ID: " + toString(coin.m_ID) + " is unreachable.");
                BEAM_LOG_ERROR() << message;
                throw TransactionFailedException(true, TxFailureReason::NoInputs, message.c_str());
            }
        }

        for (auto& cid : m_Coins.m_InputShielded)
        {
            auto pCoin = m_Tx.GetWalletDB()->getShieldedCoin(cid.m_Key);
            if (pCoin)
            {
                pCoin->m_spentTxId = m_Tx.GetTxID();
                m_Tx.GetWalletDB()->saveShieldedCoin(*pCoin);
            }
        }
    }

    bool BaseTxBuilder::IsGeneratingInOuts() const
    {
        return (Stage::InProgress == m_GeneratingInOuts);
    }

    bool BaseTxBuilder::IsSigning() const
    {
        return (Stage::InProgress == m_Signing);
    }

    void BaseTxBuilder::AddOffset(const ECC::Scalar& k1)
    {
        AddOffset(ECC::Scalar::Native(k1));
    }

    void BaseTxBuilder::AddOffset(const ECC::Scalar::Native& k1)
    {
        ECC::Scalar::Native k(m_pTransaction->m_Offset);
        k += k1;
        m_pTransaction->m_Offset = k;

        SetParameter(TxParameterID::Offset, m_pTransaction->m_Offset);
    }

    bool BaseTxBuilder::Aggregate(ECC::Point& ptDst, const ECC::Point::Native& ptSrcN)
    {
        ECC::Point::Native pt;
        if (!pt.Import(ptDst))
            return false;

        pt += ptSrcN;
        pt.Export(ptDst);
        return true;
    }

    bool BaseTxBuilder::Aggregate(ECC::Point& ptDst, ECC::Point::Native& ptSrcN, const ECC::Point& ptSrc)
    {
        return
            ptSrcN.Import(ptSrc) &&
            Aggregate(ptDst, ptSrcN);
    }

    void BaseTxBuilder::SaveKernel()
    {
        // this is ugly hack, but should work without extra code generated
        SetParameter(TxParameterID::Kernel, Cast::Reinterpret<TxKernel::Ptr>(m_pKrn));
    }

    void BaseTxBuilder::SaveKernelID()
    {
        assert(m_pKrn);
        SetParameter(TxParameterID::KernelID, m_pKrn->get_ID());
    }

    void BaseTxBuilder::SetInOuts(IPrivateKeyKeeper2::Method::TxCommon& m)
    {
        m.m_vInputs = m_Coins.m_Input;
        m.m_vOutputs = m_Coins.m_Output;
        m.m_vInputsShielded = m_Coins.m_InputShielded;

        m.m_NonConventional = !IsConventional();
    }

    void BaseTxBuilder::SetCommon(IPrivateKeyKeeper2::Method::TxCommon& m)
    {
        BaseTxBuilder::SetInOuts(m);
        m.m_pKernel.reset(new TxKernelStd);
        m.m_pKernel->m_Fee = m_Fee;
        m.m_pKernel->m_Height = m_Height;
    }

    void BaseTxBuilder::VerifyTx()
    {
        TxBase::Context ctx;
        ctx.m_Height.m_Min = m_Height.m_Min;
        // TODO:DEX set to false, other side will not see it!!!

        std::string sErr;
        if (!m_pTransaction->IsValid(ctx, &sErr))
            throw TransactionFailedException(false, TxFailureReason::InvalidTransaction, sErr.c_str());
    }

    void BaseTxBuilder::VerifyAssetsEnabled()
    {
        TxFailureReason res = CheckAssetsEnabled(m_Height.m_Min);
        if (TxFailureReason::Count != res)
            throw TransactionFailedException(!m_Tx.IsInitiator(), res);
    }

    bool BaseTxBuilder::SignTx()
    {
        return true;
    }

    void BaseTxBuilder::FinalyzeTx()
    {
        if (Status::FullTx == m_Status)
            return;

        assert(!IsGeneratingInOuts());

        FinalizeTxInternal();
    }

    void BaseTxBuilder::FinalizeTxInternal()
    {
        m_pTransaction->Normalize();
        VerifyTx();

        SaveInOuts();

        SetStatus(Status::FullTx);
    }

    void BaseTxBuilder::AddKernel(TxKernel::Ptr&& pKrn)
    {
        m_pKrn = pKrn.get();
        m_pTransaction->m_vKernels.push_back(std::move(pKrn));
    }

    void BaseTxBuilder::SetStatus(Status::Type s)
    {
        SaveAndStore(m_Status, TxParameterID::MutualTxState, s);
    }

    void BaseTxBuilder::FillUserData(Output::User::Packed* user)
    {
        user->m_TxID = Blob(m_Tx.GetTxID().data(), (uint32_t)m_Tx.GetTxID().size());
        user->m_Fee = m_Fee;
    }

    string BaseTxBuilder::GetKernelIDString() const
    {
        Merkle::Hash kernelID;
        GetParameter(TxParameterID::KernelID, kernelID);
        char sz[Merkle::Hash::nTxtLen + 1];
        kernelID.Print(sz);
        return string(sz);
    }

    void BaseTxBuilder::CheckMinimumFee(const TxStats* pFromPeer /* = nullptr */)
    {
        // after 1st fork fee should be >= minimal fee
        if (Rules::get().IsPastFork_<1>(m_Height.m_Min))
        {
            Amount feeInps = 0;
            for (const auto& si : m_Coins.m_InputShielded)
                feeInps += si.m_Fee; // account for shielded inputs, they carry their fee individually

            TxStats ts;
            ts.m_Outputs = static_cast<uint32_t>(m_Coins.m_Output.size());
            ts.m_InputsShielded = static_cast<uint32_t>(m_Coins.m_InputShielded.size());
            ts.m_Kernels = 1 + static_cast<uint32_t>(m_Coins.m_InputShielded.size());

            if (pFromPeer)
                ts += *pFromPeer;

            auto& fs = Transaction::FeeSettings::get(m_Height.m_Min);
            Amount minFee = fs.Calculate(ts);

            if (m_Fee + feeInps < minFee)
            {
                stringstream ss;
                ss << "The minimum fee must be: " << (minFee - feeInps) << " .";
                throw TransactionFailedException(false, TxFailureReason::FeeIsTooSmall, ss.str().c_str());
            }
        }
    }
}
