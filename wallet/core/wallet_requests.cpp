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

#include "wallet_common.h"

namespace beam::wallet
{
#define WALLET_REQUEST_Single(type) \
    bool Wallet::MyRequest##type::operator < (const MyRequest##type& x) const { return false; }

    WALLET_REQUEST_Single(Events)
    WALLET_REQUEST_Single(StateSummary)
    WALLET_REQUEST_Single(ShieldedOutputsAt)
    WALLET_REQUEST_Single(BodyPack)
    WALLET_REQUEST_Single(Body)
    WALLET_REQUEST_Single(AssetsListAt)

    bool Wallet::MyRequestUtxo::operator < (const MyRequestUtxo& x) const
    {
        return m_Msg.m_Utxo < x.m_Msg.m_Utxo;
    }

    bool Wallet::MyRequestKernel::operator < (const MyRequestKernel& x) const
    {
        return m_TxID < x.m_TxID;
    }

    bool Wallet::MyRequestKernel2::operator < (const MyRequestKernel2& x) const
    {
        return m_TxID < x.m_TxID;
    }

    bool Wallet::MyRequestAsset::operator < (const MyRequestAsset& x) const
    {
        if (m_TxID < x.m_TxID) {
            return true;
        }

        if (m_Msg.m_AssetID < x.m_Msg.m_AssetID) {
            return true;
        }

        if (m_Msg.m_Owner < x.m_Msg.m_Owner) {
            return true;
        }

        return false;
    }

    bool Wallet::MyRequestTransaction::operator < (const MyRequestTransaction& x) const
    {
        return m_TxID < x.m_TxID;
    }

    bool Wallet::MyRequestProofShieldedOutp::operator < (const MyRequestProofShieldedOutp& x) const
    {
        return m_TxID < x.m_TxID;
    }

    bool Wallet::MyRequestShieldedList::operator < (const MyRequestShieldedList& x) const
    {
        return m_TxID < x.m_TxID;
    }

    void Wallet::RequestHandler::OnComplete(Request& r)
    {
        auto n = get_ParentObj().SyncRemains();

        switch (r.get_Type())
        {
#define THE_MACRO(type) \
        case Request::Type::type: \
            { \
                MyRequest##type& x = static_cast<MyRequest##type&>(r); \
                get_ParentObj().DeleteReq(x); \
                get_ParentObj().OnRequestComplete(x); \
            } \
            break;

            WalletFlyClientRequests_All(THE_MACRO)
#undef THE_MACRO

        default:
            assert(false);
        }

        if (n)
            get_ParentObj().CheckSyncDone();
    }

    // Implementation of the INegotiatorGateway::confirm_kernel
    // @param txID : TxID - transaction id
    // @param kernelID : Merkle::Hash& - kernel id
    // @param subTxID : SubTxID - in case of the complex transaction, there could be sub transactions
    void Wallet::confirm_kernel(const TxID& txID, const Merkle::Hash& kernelID, SubTxID subTxID)
    {
        if (auto it = m_ActiveTransactions.find(txID); it != m_ActiveTransactions.end())
        {
            // check if we have already asked for kernel on given height
            Height lastUnconfirmedHeight = 0;
            if (it->second->GetParameter(TxParameterID::KernelUnconfirmedHeight, lastUnconfirmedHeight, subTxID) && lastUnconfirmedHeight > 0)
            {
                Block::SystemState::Full state;
                if (!get_tip(state) || state.get_Height() == lastUnconfirmedHeight)
                {
                    UpdateOnNextTip(it->second);
                    return;
                }
            }

            MyRequestKernel::Ptr pVal(new MyRequestKernel);
            pVal->m_TxID = txID;
            pVal->m_SubTxID = subTxID;
            pVal->m_Msg.m_ID = kernelID;

            if (PostReqUnique(*pVal))
                BEAM_LOG_INFO() << txID << "[" << subTxID << "]" << " Get proof for kernel: " << pVal->m_Msg.m_ID;
        }
    }

    void Wallet::confirm_kernel_ex(const Merkle::Hash& kernelID, IConfirmCallback::Ptr&& pCallback)
    {
        MyRequestKernel::Ptr pVal(new MyRequestKernel);
        pVal->m_Msg.m_ID = kernelID;
        pVal->m_pCallback = std::move(pCallback);

        if (PostReq(*pVal))
        {
            BEAM_LOG_INFO() << " Get proof for kernel: " << kernelID;
        }
    }

    void Wallet::confirm_asset(const TxID& txID, const PeerID& ownerID, SubTxID subTxID)
    {
        MyRequestAsset::Ptr pVal(new MyRequestAsset);
        pVal->m_TxID = txID;
        pVal->m_SubTxID = subTxID;
        pVal->m_Msg.m_Owner = ownerID;
        pVal->m_Msg.m_AssetID = Asset::s_InvalidID;

        if (PostReqUnique(*pVal))
        {
           BEAM_LOG_INFO() << txID << "[" << subTxID << "]" << " Get proof for asset with the owner ID: " << ownerID;
        }
    }

    void Wallet::confirm_asset(const TxID& txID, Asset::ID assetId, SubTxID subTxID)
    {
        MyRequestAsset::Ptr pVal(new MyRequestAsset);
        pVal->m_TxID = txID;
        pVal->m_SubTxID = subTxID;
        pVal->m_Msg.m_Owner = Asset::s_InvalidOwnerID;
        pVal->m_Msg.m_AssetID = assetId;

        if (PostReqUnique(*pVal))
        {
            BEAM_LOG_INFO() << txID << "[" << subTxID << "]" << " Get proof for asset with id: " << assetId;
        }
    }

    void Wallet::confirm_asset(const Asset::ID assetId)
    {
        MyRequestAsset::Ptr pVal(new MyRequestAsset);
        pVal->m_TxID = {0};
        pVal->m_SubTxID = 0;
        pVal->m_Msg.m_Owner = Asset::s_InvalidOwnerID;
        pVal->m_Msg.m_AssetID = assetId;

        if (PostReqUnique(*pVal))
        {
            BEAM_LOG_INFO() << "Get proof for asset with id: " << assetId;
        }
    }

    // Implementation of the INegotiatorGateway::get_kernel
    // @param txID : TxID - transaction id
    // @param kernelID : Merkle::Hash& - kernel id
    // @param subTxID : SubTxID - in case of the complex transaction, there could be sub transactions
    void Wallet::get_kernel(const TxID& txID, const Merkle::Hash& kernelID, SubTxID subTxID)
    {
        if (auto it = m_ActiveTransactions.find(txID); it != m_ActiveTransactions.end())
        {
            MyRequestKernel2::Ptr pVal(new MyRequestKernel2);
            pVal->m_TxID = txID;
            pVal->m_SubTxID = subTxID;
            auto& msg = pVal->m_Msg; // alias
            msg.m_Fetch = true;
            msg.m_ID = kernelID;

            if (PostReqUnique(*pVal))
            {
                BEAM_LOG_INFO() << txID << "[" << subTxID << "]" << " Get details for kernel: " << msg.m_ID;
            }
        }
    }

    // Implementation of the INegotiatorGateway::get_shielded_list
    void Wallet::get_shielded_list(const TxID& txId, TxoID startIndex, uint32_t count, ShieldedListCallback&& callback)
    {
        MyRequestShieldedList::Ptr pVal(new MyRequestShieldedList);
        pVal->m_callback = std::move(callback);
        pVal->m_TxID = txId;

        pVal->m_Msg.m_Id0 = startIndex;
        pVal->m_Msg.m_Count = count;

        if (PostReq(*pVal))
        {
            BEAM_LOG_INFO() << txId << " Get shielded list, start_index = " << startIndex << ", count = " << count;
        }
    }

    void Wallet::get_proof_shielded_output(const TxID& txId, const ECC::Point& serialPublic, ProofShildedOutputCallback&& callback)
    {
        MyRequestProofShieldedOutp::Ptr pVal(new MyRequestProofShieldedOutp);
        pVal->m_callback = std::move(callback);
        pVal->m_TxID = txId;

        pVal->m_Msg.m_SerialPub = serialPublic;

        if (PostReqUnique(*pVal))
        {
            BEAM_LOG_INFO() << txId << " Get proof of shielded output.";
        }
    }

    void Wallet::OnRequestComplete(MyRequestTransaction& r)
    {
        BEAM_LOG_DEBUG() << r.m_TxID << "[" << r.m_SubTxID << "]" << " register status " << static_cast<uint32_t>(r.m_Res.m_Value);
        if (!r.m_Res.m_ExtraInfo.empty())
            BEAM_LOG_WARNING() << "Extra info: " << r.m_Res.m_ExtraInfo;

        auto it = m_ActiveTransactions.find(r.m_TxID);
        if (it != m_ActiveTransactions.end())
        {
            it->second->SetParameter(TxParameterID::TransactionRegistered, r.m_Res.m_Value, r.m_SubTxID);
            it->second->SetParameter(TxParameterID::TransactionRegisteredExtraInfo, r.m_Res.m_ExtraInfo, r.m_SubTxID);
            it->second->SetParameter(TxParameterID::TransactionRegisteredInternal, r.m_Res.m_Value, r.m_SubTxID);
            UpdateTransaction(r.m_TxID);
        }
    }

    void Wallet::ConfirmAsset(Asset::ID assetId)
    {
        confirm_asset(assetId);
    }

    void Wallet::OnRequestComplete(MyRequestUtxo& r)
    {
        if (r.m_Res.m_Proofs.empty())
            return; // Right now nothing is concluded from empty proofs

        const auto& proof = r.m_Res.m_Proofs.front(); // Currently - no handling for multiple coins for the same commitment.
        CacheCommitment(r.m_Msg.m_Utxo, proof.m_State.m_Maturity, true);
        // we don't know the real height, but it'll be used for logging only. For standard outputs maturity and height are the same
        ProcessEventUtxo(r.m_CoinID, proof.m_State.m_Maturity, proof.m_State.m_Maturity, true, {});
    }

    void Wallet::OnRequestComplete(MyRequestKernel& r)
    {
        if (!r.m_Res.m_Proof.empty())
            m_WalletDB->get_History().AddStates(&r.m_Res.m_Proof.m_State, 1); // why not?

        if (r.m_pCallback)
        {
            Height h = r.m_Res.m_Proof.m_State.get_Height();
            r.m_pCallback->OnDone(r.m_Res.m_Proof.empty() ? nullptr : &h);
            return;
        }

        auto it = m_ActiveTransactions.find(r.m_TxID);
        if (m_ActiveTransactions.end() == it)
            return;

        auto tx = it->second;
        if (!r.m_Res.m_Proof.empty())
        {
            if (tx->SetParameter(TxParameterID::KernelProofHeight, r.m_Res.m_Proof.m_State.get_Height(), r.m_SubTxID))
            {
                UpdateTransaction(tx);
            }
        }
        else
        {
            Block::SystemState::Full sTip;
            get_tip(sTip);
            tx->SetParameter(TxParameterID::KernelUnconfirmedHeight, sTip.get_Height(), r.m_SubTxID);
            UpdateTransaction(tx);
        }
    }

    void Wallet::OnRequestComplete(MyRequestAsset& req)
    {
        BaseTransaction::Ptr tx;
        const auto it = m_ActiveTransactions.find(req.m_TxID);
        if (m_ActiveTransactions.end() != it)
        {
           tx = it->second;
        }

        Block::SystemState::Full sTip;
        get_tip(sTip);

        std::string msgPrefix;
        if (!std::all_of(req.m_TxID.begin(), req.m_TxID.end(), [](const auto& v) { return v == 0; })) // any non-zero val in array
        {
            stringstream ss;
            ss << req.m_TxID << "[" << req.m_SubTxID << "]";
            msgPrefix = ss.str();
        }

        if (!req.m_Res.m_Proof.empty())
        {
            const auto& info  = req.m_Res.m_Info;
            const auto height = sTip.get_Height();
            ProcessAssetInfo(info, height, msgPrefix);

            if (tx)
            {
                tx->SetParameter(TxParameterID::AssetConfirmedHeight, height, req.m_SubTxID);
                tx->SetParameter(TxParameterID::AssetInfoFull, info, req.m_SubTxID);
                tx->SetParameter(TxParameterID::AssetUnconfirmedHeight, Height(0), req.m_SubTxID);
                UpdateTransaction(tx);
            }
            else
            {
                storage::setTxParameter(*m_WalletDB, req.m_TxID, req.m_SubTxID, TxParameterID::AssetConfirmedHeight, height, true);
                storage::setTxParameter(*m_WalletDB, req.m_TxID, req.m_SubTxID, TxParameterID::AssetInfoFull, info, true);
                storage::setTxParameter(*m_WalletDB, req.m_TxID, req.m_SubTxID, TxParameterID::AssetUnconfirmedHeight, Height(0), true);
            }
        }
        else
        {
            const auto& assetId = req.m_Msg.m_AssetID;
            if (assetId != Asset::s_InvalidID)
            {
                m_WalletDB->dropAsset(assetId);
            }
            else
            {
                const auto& assetOwner = req.m_Msg.m_Owner;
                if (assetOwner != Asset::s_InvalidOwnerID)
                {
                    m_WalletDB->dropAsset(assetOwner);
                }
            }

            if (tx)
            {
                tx->SetParameter(TxParameterID::AssetConfirmedHeight, Height(0), req.m_SubTxID);
                tx->SetParameter(TxParameterID::AssetInfoFull, Asset::Full(), req.m_SubTxID);
                tx->SetParameter(TxParameterID::AssetUnconfirmedHeight, sTip.get_Height(), req.m_SubTxID);
                UpdateTransaction(tx);
            }
            else
            {
                storage::setTxParameter(*m_WalletDB, req.m_TxID, req.m_SubTxID, TxParameterID::AssetConfirmedHeight, Height(0), true);
                storage::setTxParameter(*m_WalletDB, req.m_TxID, req.m_SubTxID, TxParameterID::AssetInfoFull, Asset::Full(), true);
                storage::setTxParameter(*m_WalletDB, req.m_TxID, req.m_SubTxID, TxParameterID::AssetUnconfirmedHeight, sTip.get_Height(), true);
            }
        }
    }

    void Wallet::ProcessAssetInfo(const Asset::Full& info, Height height, const std::string& logPrefix)
    {
        m_WalletDB->saveAsset(info, height);
        BEAM_LOG_INFO() << logPrefix << (logPrefix.empty() ? "" : " ") << "Received proof for Asset with ID " << info.m_ID;

        if (Key::IKdf::Ptr maserKdf = m_WalletDB->get_MasterKdf())
        {
            std::string strMeta;
            info.m_Metadata.get_String(strMeta);

            if (beam::wallet::GetAssetOwnerID(maserKdf, strMeta) == info.m_Owner)
            {
                m_WalletDB->markAssetOwned(info.m_ID);
            }
        }
        else
        {
            BEAM_LOG_WARNING() << logPrefix << "Unable to get master key. Asset's " << info.m_ID << " ownership won't be checked.";
        }

        if (const auto wasset = m_WalletDB->findAsset(info.m_ID))
        {
            wasset->LogInfo(logPrefix);
        }
    }

    void Wallet::OnRequestComplete(MyRequestKernel2& r)
    {
        auto it = m_ActiveTransactions.find(r.m_TxID);
        if (m_ActiveTransactions.end() == it)
        {
            return;
        }
        auto tx = it->second;

        if (r.m_Res.m_Kernel)
        {
            tx->SetParameter(TxParameterID::Kernel, r.m_Res.m_Kernel, r.m_SubTxID);
            tx->SetParameter(TxParameterID::KernelProofHeight, r.m_Res.m_Height, r.m_SubTxID);
        }
        else
        {
            Block::SystemState::Full sTip;
            get_tip(sTip);
            tx->SetParameter(TxParameterID::KernelUnconfirmedHeight, sTip.get_Height(), r.m_SubTxID);
        }
    }

    void Wallet::OnRequestComplete(MyRequestShieldedList& r)
    {
        r.m_callback(r.m_Msg.m_Id0, r.m_Msg.m_Count, r.m_Res);
    }

    void Wallet::OnRequestComplete(MyRequestProofShieldedOutp& r)
    {
        r.m_callback(r.m_Res); // either successful or not
    }

    void Wallet::OnRequestComplete(MyRequestStateSummary& r)
    {
        // TODO: save full response?
        m_WalletDB->set_ShieldedOuts(r.m_Res.m_ShieldedOuts);
        m_WalletDB->set_AidMax(r.m_Res.m_AssetsMax);
        if (!IsMobileNodeEnabled())
        {
            m_Extra.m_ShieldedOutputs = r.m_Res.m_ShieldedOuts;
        }
    }

    void Wallet::OnRequestComplete(MyRequestShieldedOutputsAt& r)
    {
        r.m_callback(r.m_Msg.m_Height, r.m_Res.m_ShieldedOuts);
    }

    void Wallet::OnRequestComplete(MyRequestAssetsListAt& r)
    {
        Height tipHeight = get_TipHeight();
        for (const auto& ai : r.m_Res)
        {
            ProcessAssetInfo(ai, tipHeight, "");
        }
        r.m_callback(std::move(r.m_Res));
    }

    void Wallet::getUtxoProof(const Coin& coin)
    {
        MyRequestUtxo::Ptr pReq(new MyRequestUtxo);
        pReq->m_CoinID = coin.m_ID;

        if (!m_WalletDB->get_CommitmentSafe(pReq->m_Msg.m_Utxo, coin.m_ID))
        {
            BEAM_LOG_WARNING() << "You cannot get utxo commitment without private key";
            return;
        }

        BEAM_LOG_DEBUG() << "Get utxo proof: " << pReq->m_Msg.m_Utxo;

        PostReqUnique(*pReq);
    }

    void Wallet::SendTransactionToNode(const TxID& txId, const Transaction::Ptr& data, const Merkle::Hash* pParentCtx, SubTxID subTxID)
    {
        BEAM_LOG_DEBUG() << txId << "[" << subTxID << "]" << " sending tx for registration";

#ifndef NDEBUG
        TxBase::Context ctx;
        ctx.m_Height.m_Min = m_WalletDB->getCurrentHeight();
        assert(data->IsValid(ctx));
#endif // NDEBUG

        MyRequestTransaction::Ptr pReq(new MyRequestTransaction);
        pReq->m_TxID = txId;
        pReq->m_SubTxID = subTxID;
        pReq->m_Msg.m_Transaction = data;

        if (pParentCtx)
        {
            pReq->m_Msg.m_Context = std::make_unique<Merkle::Hash>(*pParentCtx);
            pReq->m_Msg.m_Fluff = true;
        }

        PostReqUnique(*pReq);
    }

    void Wallet::register_tx(const TxID& txId, const Transaction::Ptr& data, const Merkle::Hash* pParentCtx, SubTxID subTxID)
    {
        SendTransactionToNode(txId, data, pParentCtx, subTxID);
    }

    void Wallet::RequestStateSummary()
    {
        MyRequestStateSummary::Ptr pReq(new MyRequestStateSummary);
        PostReqUnique(*pReq);
    }

    void Wallet::RequestShieldedOutputsAt(Height h, std::function<void(Height, TxoID)>&& onRequestComplete)
    {
        MyRequestShieldedOutputsAt::Ptr pVal(new MyRequestShieldedOutputsAt);
        pVal->m_Msg.m_Height = h;
        pVal->m_callback = std::move(onRequestComplete);
        PostReqUnique(*pVal);
    }

    void Wallet::RequestAssetsListAt(Height h, std::function<void(std::vector<beam::Asset::Full>&&)>&& onRequestComplete)
    {
        if (!m_PendingAssetsListAt.empty())
        {
            DeleteReq(*m_PendingAssetsListAt.begin());
        }
        MyRequestAssetsListAt::Ptr pVal(new MyRequestAssetsListAt);
        pVal->m_Msg.m_Height = h;
        pVal->m_callback = std::move(onRequestComplete);
        PostReqUnique(*pVal);
    }
}
