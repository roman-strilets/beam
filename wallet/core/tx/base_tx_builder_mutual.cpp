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
    // MutualTxBuilder

    MutualTxBuilder::MutualTxBuilder(BaseTransaction& tx, SubTxID subTxID)
        :SimpleTxBuilder(tx, subTxID)
    {
        GetParameter(TxParameterID::IsSender, m_IsSender);

        Height responseTime = 0;
        Height responseHeight = 0;
        if (GetParameter(TxParameterID::PeerResponseTime, responseTime)
          && !GetParameter(TxParameterID::PeerResponseHeight, responseHeight))
        {
            auto currentHeight = m_Tx.GetWalletDB()->getCurrentHeight();
            // adjust response height, if min height did not set then it should be equal to responce time
            SetParameter(TxParameterID::PeerResponseHeight, responseTime + currentHeight);
        }
    }

    void MutualTxBuilder::CreateKernel(TxKernelStd::Ptr& pKrn)
    {
        pKrn = make_unique<TxKernelStd>();
        pKrn->m_Fee = m_Fee;
        pKrn->m_Height.m_Min = m_Height.m_Min;
        pKrn->m_Height.m_Max = m_Height.m_Max;
        pKrn->m_Commitment = Zero;
        ZeroObject(pKrn->m_Signature);

        // load kernel's extra data
        Hash::Value hv;
        if (GetParameter(TxParameterID::PeerLockImage, hv))
        {
			pKrn->m_pHashLock = make_unique<TxKernelStd::HashLock>();
			pKrn->m_pHashLock->m_IsImage = true;
			pKrn->m_pHashLock->m_Value = hv;
        }

        if (GetParameter(TxParameterID::PreImage, hv))
        {
			pKrn->m_pHashLock = make_unique<TxKernelStd::HashLock>();
			pKrn->m_pHashLock->m_Value = hv;
		}
    }

    void MutualTxBuilder::AddPeerSignature(const ECC::Point::Native& ptNonce, const ECC::Point::Native& ptExc)
    {
        GetParameterStrict(TxParameterID::PeerSignature, m_pKrn->CastTo_Std().m_Signature.m_k);

        if (!m_pKrn->CastTo_Std().m_Signature.IsValidPartial(m_pKrn->get_ID(), ptNonce, ptExc))
            throw TransactionFailedException(true, TxFailureReason::InvalidPeerSignature);

    }

    bool MutualTxBuilder::LoadPeerPart(ECC::Point::Native& ptNonce, ECC::Point::Native& ptExc)
    {
        ECC::Point pt;
        return
            GetParameter(TxParameterID::PeerPublicNonce, pt) &&
            ptNonce.Import(pt) &&
            GetParameter(TxParameterID::PeerPublicExcess, pt) &&
            ptExc.Import(pt);
    }

    void MutualTxBuilder::AddPeerOffset()
    {
        ECC::Scalar k;
        if (GetParameter(TxParameterID::PeerOffset, k))
            AddOffset(k);
    }

    void MutualTxBuilder::FinalizeTxInternal()
    {
        // add peer in/out/offs
        AddPeerOffset();

        std::vector<Input::Ptr> vIns;
        if (GetParameter(TxParameterID::PeerInputs, vIns))
            MoveIntoVec(m_pTransaction->m_vInputs, vIns);

        std::vector<Output::Ptr> vOuts;
        if (GetParameter(TxParameterID::PeerOutputs, vOuts))
            MoveIntoVec(m_pTransaction->m_vOutputs, vOuts);

        SimpleTxBuilder::FinalizeTxInternal();
    }

    void MutualTxBuilder::PrepareMutualParams(IPrivateKeyKeeper2::Method::TxMutual& m)
    {
        m.m_Peer = Zero;
        m.m_iEndpoint = 0;

        if (!m.m_NonConventional)
        {
            GetParameterStrict(TxParameterID::MyAddressID, m.m_iEndpoint);

            if (!GetParameter(TxParameterID::PeerEndpoint, m.m_Peer))
            {
                WalletID wid;
                GetParameterStrict(TxParameterID::PeerAddr, wid);
                m.m_Peer = wid.m_Pk;
            }

            PeerID epMy;
            if (!GetParameter(TxParameterID::MyEndpoint, epMy))
                m.m_IsBbs = true; // legacy. Will fail for trustless key keeper.
        }

    }

    void MutualTxBuilder::SignSender(bool initial)
    {
        if (Stage::InProgress == m_Signing)
            return;
        m_Signing = Stage::None;

        struct MyHandler
            :public KeyKeeperHandler
        {
            using KeyKeeperHandler::KeyKeeperHandler;

            IPrivateKeyKeeper2::Method::SignSender m_Method;

            virtual ~MyHandler() {} // auto

            void OnSuccess(BaseTxBuilder& b_) override
            {
                MutualTxBuilder& b = Cast::Up<MutualTxBuilder>(b_);

                if (b.m_pKrn)
                {
                    // final, update the signature only
                    b.m_pKrn->CastTo_Std().m_Signature.m_k = m_Method.m_pKernel->m_Signature.m_k;
                    b.AddOffset(m_Method.m_kOffset);

                    b.m_Tx.FreeSlotSafe(); // release it ASAP

                    b.SetStatus(Status::SndFull);
                }
                else
                {
                    // initial
                    b.SetParameter(TxParameterID::UserConfirmationToken, m_Method.m_UserAgreement);

                    b.AddKernel(std::move(m_Method.m_pKernel));
                    b.SetStatus(Status::SndHalf);
                }

                b.SaveKernel();
                OnAllDone(b);
            }
        };

        KeyKeeperHandler::Ptr pHandler = std::make_shared<MyHandler>(*this, m_Signing);
        MyHandler& x = Cast::Up<MyHandler>(*pHandler);
        IPrivateKeyKeeper2::Method::SignSender& m = x.m_Method;

        SetInOuts(m);

        m.m_Slot = m_Tx.GetSlotSafe(true);

        PrepareMutualParams(m);

        ZeroObject(m.m_PaymentProofSignature);
        m.m_UserAgreement = Zero;

        if (initial)
            CreateKernel(m.m_pKernel);
        else
        {
            m_pKrn->Clone(Cast::Reinterpret<TxKernel::Ptr>(m.m_pKernel));

            GetParameter(TxParameterID::UserConfirmationToken, m.m_UserAgreement);
            if (m.m_UserAgreement == Zero)
                throw TransactionFailedException(true, TxFailureReason::FailedToGetParameter);

            GetParameter(TxParameterID::PaymentConfirmation, m.m_PaymentProofSignature);
        }


        m_Tx.get_KeyKeeperStrict()->InvokeAsync(x.m_Method, pHandler);
    }

    void MutualTxBuilder::SignReceiver()
    {
        if (Stage::InProgress == m_Signing)
            return;
        m_Signing = Stage::None;

        struct MyHandler
            :public KeyKeeperHandler
        {
            using KeyKeeperHandler::KeyKeeperHandler;

            IPrivateKeyKeeper2::Method::SignReceiver m_Method;

            virtual ~MyHandler() {} // auto

            void AssignExtractDiff(BaseTxBuilder& b, ECC::Point& dst, const ECC::Point& src, TxParameterID par)
            {
                dst.m_Y ^= 1;

                ECC::Point::Native pt;
                b.Aggregate(dst, pt, src);
                b.SetParameter(par, dst);

                dst = src;
            }

            void OnSuccess(BaseTxBuilder& b_) override
            {
                MutualTxBuilder& b = Cast::Up<MutualTxBuilder>(b_);

                AssignExtractDiff(b, b.m_pKrn->CastTo_Std().m_Commitment, m_Method.m_pKernel->m_Commitment, TxParameterID::PublicExcess);
                AssignExtractDiff(b, b.m_pKrn->CastTo_Std().m_Signature.m_NoncePub, m_Method.m_pKernel->m_Signature.m_NoncePub, TxParameterID::PublicNonce);
                b.m_pKrn->CastTo_Std().m_Signature.m_k = m_Method.m_pKernel->m_Signature.m_k;

                b.m_pKrn->CastTo_Std().m_Lazy_ID.Invalidate();
                b.SaveKernel();
                b.SaveKernelID();
                b.SetStatus(Status::RcvFullHalfSig);

                b.AddOffset(m_Method.m_kOffset);

                if (m_Method.m_iEndpoint)
                    b.SetParameter(TxParameterID::PaymentConfirmation, m_Method.m_PaymentProofSignature);

                OnAllDone(b);
            }
        };

        KeyKeeperHandler::Ptr pHandler = std::make_shared<MyHandler>(*this, m_Signing);
        MyHandler& x = Cast::Up<MyHandler>(*pHandler);
        IPrivateKeyKeeper2::Method::SignReceiver& m = x.m_Method;

        SetInOuts(m);
        m_pKrn->Clone(Cast::Reinterpret<TxKernel::Ptr>(m.m_pKernel));

        PrepareMutualParams(m);

        m_Tx.get_KeyKeeperStrict()->InvokeAsync(x.m_Method, pHandler);
    }

    void MutualTxBuilder::FinalyzeMaxHeight()
    {
        if (MaxHeight != m_Height.m_Max)
            return; // already decided

        GetParameter(TxParameterID::PeerMaxHeight, m_Height.m_Max);
        GetParameter(TxParameterID::Lifetime, m_Lifetime); // refresh it too

        // receiver is allowed to adjust what was suggested by the sender
        if (!m_IsSender && m_Lifetime)
        {
            // we're allowed to adjust peer's sugggested max height
            Block::SystemState::Full s;
            if (m_Tx.GetTip(s))
                m_Height.m_Max = s.get_Height() + m_Lifetime;
        }

        // sanity check
        Height hPeerResponce = 0;
        GetParameter(TxParameterID::PeerResponseHeight, hPeerResponce);

        if (hPeerResponce && (m_Height.m_Max > m_Lifetime + hPeerResponce))
            throw TransactionFailedException(true, TxFailureReason::MaxHeightIsUnacceptable);

        SetParameter(TxParameterID::MaxHeight, m_Height.m_Max);
    }


    bool MutualTxBuilder::SignTx()
    {
        GenerateInOuts();

        bool bRes = m_IsSender ?
            SignTxSender() :
            SignTxReceiver();

        if (!bRes)
            m_Tx.UpdateOnNextTip();

        return bRes;
    }

    bool MutualTxBuilder::SignTxSender()
    {
        switch (m_Status)
        {
        case Status::None:
            SignSender(true);
            break;

        case Status::SndHalf:
            {
                SetTxParameter msg;
                msg
                    .AddParameter(TxParameterID::PeerPublicExcess, m_pKrn->CastTo_Std().m_Commitment)
                    .AddParameter(TxParameterID::PeerPublicNonce, m_pKrn->CastTo_Std().m_Signature.m_NoncePub);

                if (m_pKrn->CastTo_Std().m_pHashLock)
                {
                    ECC::Hash::Value lockImage = m_pKrn->CastTo_Std().m_pHashLock->get_Image(lockImage);
                    msg.AddParameter(TxParameterID::PeerLockImage, lockImage);
                }

                SendToPeer(std::move(msg));

                SetStatus(Status::SndHalfSent);
            }
            // no break;

        case Status::SndHalfSent:
            {
                // check if receiver part is ready
                ECC::Point::Native ptNonce, ptExc;
                if (!LoadPeerPart(ptNonce, ptExc))
                    break;

                Aggregate(m_pKrn->CastTo_Std().m_Commitment, ptExc);
                Aggregate(m_pKrn->CastTo_Std().m_Signature.m_NoncePub, ptNonce);

                FinalyzeMaxHeight();
                m_pKrn->m_Height.m_Max = m_Height.m_Max; // can be different from the original

                m_pKrn->CastTo_Std().m_Lazy_ID.Invalidate(); // must be valid already
                SaveKernelID();

                AddPeerSignature(ptNonce, ptExc);

                SaveKernel();

                SetStatus(Status::SndFullHalfSig);

            }
            // no break;

        case Status::SndFullHalfSig:
            SignSender(false);
        }

        return (m_Status >= Status::SndFull) && !IsGeneratingInOuts();

    }

    bool MutualTxBuilder::SignTxReceiver()
    {
        switch (m_Status)
        {
        case Status::None:
            {
                ECC::Point ptNonce, ptExc;

                if (!GetParameter(TxParameterID::PeerPublicNonce, ptNonce) ||
                    !GetParameter(TxParameterID::PeerPublicExcess, ptExc))
                    break;

                FinalyzeMaxHeight();

                TxKernelStd::Ptr pKrn;
                CreateKernel(pKrn);
                pKrn->m_Commitment = ptExc;
                pKrn->m_Signature.m_NoncePub = ptNonce;

                AddKernel(std::move(pKrn));
                SaveKernel();

                SetStatus(Status::RcvHalf);
            }
            // no break;

        case Status::RcvHalf:
            SignReceiver();
            break;

        case Status::RcvFullHalfSig:
            {
                if (IsGeneratingInOuts())
                    break;

                SetTxParameter msg;
                msg
                    .AddParameter(TxParameterID::PeerPublicExcess, GetParameterStrict<ECC::Point>(TxParameterID::PublicExcess))
                    .AddParameter(TxParameterID::PeerPublicNonce, GetParameterStrict<ECC::Point>(TxParameterID::PublicNonce))
                    .AddParameter(TxParameterID::PeerSignature, m_pKrn->CastTo_Std().m_Signature.m_k)
                    .AddParameter(TxParameterID::PeerInputs, m_pTransaction->m_vInputs)
                    .AddParameter(TxParameterID::PeerOutputs, m_pTransaction->m_vOutputs)
                    .AddParameter(TxParameterID::PeerOffset, m_pTransaction->m_Offset);

                Signature sig;
                if (GetParameter(TxParameterID::PaymentConfirmation, sig))
                    msg.AddParameter(TxParameterID::PaymentConfirmation, sig);

                SendToPeer(std::move(msg));

                SetStatus(Status::RcvFullHalfSigSent);
            }

        }

        return (m_Status >= Status::RcvFullHalfSigSent);
    }
    
    void MutualTxBuilder::FillUserData(Output::User::Packed* user)
    {
        SimpleTxBuilder::FillUserData(user);
        PeerID peerID = Zero;
        m_Tx.GetParameter(TxParameterID::PeerEndpoint, peerID);
        user->m_Peer = peerID;
    }
}
