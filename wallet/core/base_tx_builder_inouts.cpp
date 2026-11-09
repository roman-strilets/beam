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
    struct BaseTxBuilder::HandlerInOuts
    {
        struct Shared
            :public std::enable_shared_from_this<Shared>
        {
            typedef std::shared_ptr<Shared> Ptr;

            TxVectors::Full m_InOuts;
            uint32_t m_Remaining;
        };

        struct Base
            :public KeyKeeperHandler
        {
            Base(BaseTxBuilder& b, Stage& s, const Shared::Ptr& pShared)
                :KeyKeeperHandler(b, s)
            {
                m_pShared = pShared;
                pShared->m_Remaining++;
            }

            using KeyKeeperHandler::KeyKeeperHandler;

            Shared::Ptr m_pShared;

            virtual bool OnDoneItem(BaseTxBuilder& b) = 0;

            void OnSuccess(BaseTxBuilder& b) override
            {
                if (OnDoneItem(b))
                {
                    auto& s = *m_pShared;
                    assert(s.m_Remaining);
                    if (!--s.m_Remaining)
                    {
                        MoveIntoVec(b.m_pTransaction->m_vOutputs, s.m_InOuts.m_vOutputs);
                        MoveIntoVec(b.m_pTransaction->m_vInputs, s.m_InOuts.m_vInputs);
                        MoveIntoVec1(b.m_pTransaction->m_vKernels, s.m_InOuts.m_vKernels);

                        b.SaveInOuts();

                        OnAllDone(b);
                    }
                    else
                        m_pStage = nullptr; // detach silently
                }
                else
                    OnFailed(b, IPrivateKeyKeeper2::Status::Unspecified); // although shouldn't happen
            }
        };

        struct HandlerOutput
            :public Base
            ,public std::enable_shared_from_this<HandlerOutput>
        {
            using Base::Base;

            virtual ~HandlerOutput() {} // auto

            IPrivateKeyKeeper2::Method::CreateOutput m_Method;

            bool OnDoneItem(BaseTxBuilder& b) override
            {
                assert(m_Method.m_pResult);
                m_pShared->m_InOuts.m_vOutputs.push_back(std::move(m_Method.m_pResult));
                return true;

            }
        };

        struct HandlerInput
            :public Base
            ,public std::enable_shared_from_this<HandlerInput>
        {
            using Base::Base;

            virtual ~HandlerInput() {} // auto

            IPrivateKeyKeeper2::Method::get_Commitment m_Method;

            bool OnDoneItem(BaseTxBuilder& b) override
            {
                auto& s = *m_pShared;
                s.m_InOuts.m_vInputs.emplace_back();
                s.m_InOuts.m_vInputs.back().reset(new Input);
                s.m_InOuts.m_vInputs.back()->m_Commitment = m_Method.m_Result;
                return true;
            }
        };

        struct HandlerInputShielded
            :public Base
            ,public std::enable_shared_from_this<HandlerInputShielded>
        {
            using Base::Base;

            virtual ~HandlerInputShielded() {} // auto


            struct MyList
                :public Sigma::CmList
            {
                std::vector<ECC::Point::Storage> m_vec;

                ECC::Point::Storage* m_p0;
                uint32_t m_Skip;

                bool get_At(ECC::Point::Storage& res, uint32_t iIdx) override
                {
                    if (iIdx < m_Skip)
                    {
                        res.m_X = Zero;
                        res.m_Y = Zero;
                    }
                    else
                        res = m_p0[iIdx - m_Skip];

                    return true;
                }
            };

            IPrivateKeyKeeper2::Method::CreateInputShielded m_Method;
            MyList m_Lst;

            TxoID m_Wnd0;
            uint32_t m_N;
            uint32_t m_Count;

            bool OnDoneItem(BaseTxBuilder& b) override
            {
                assert(m_Method.m_pKernel);
                m_pShared->m_InOuts.m_vKernels.push_back(std::move(m_Method.m_pKernel));
                return true;
            }

            bool InvokeSafe(BaseTxBuilder&, const IPrivateKeyKeeper2::ShieldedInput&);
            bool OnList(BaseTxBuilder&, proto::ShieldedList& msg);

        };
    };

    struct MyWrapper
    {
        const TxKernel::Ptr* m_p0;
        size_t m_Count;
        TxKernel* m_pSkip;

        template <typename Archive>
        void serialize(Archive& ar)
        {
            size_t n = m_Count - (!!m_pSkip);
            ar & n;

            for (size_t i = 0; i < m_Count; i++)
            {
                const auto& pKrn = m_p0[i];
                if (pKrn.get() != m_pSkip)
                    ar & pKrn;
            }
        }
    };

    void BaseTxBuilder::SaveInOuts()
    {
        SetParameter(TxParameterID::Inputs, m_pTransaction->m_vInputs);
        SetParameter(TxParameterID::Outputs, m_pTransaction->m_vOutputs);

        // serialize kernels, except the 'main' one
        const auto& v = m_pTransaction->m_vKernels;

        MyWrapper wr;
        wr.m_pSkip = m_pKrn;
        wr.m_Count = v.size();
        wr.m_p0 = wr.m_Count ? &v.front() : nullptr;

        SetParameter(TxParameterID::ExtraKernels, wr);

    }

    void BaseTxBuilder::GenerateInOuts()
    {
        if (Stage::None != m_GeneratingInOuts)
            return;

        if (m_Coins.IsEmpty())
        {
            m_GeneratingInOuts = Stage::Done;
            return;
        }

        auto pShared = std::make_shared<HandlerInOuts::Shared>();
        pShared->m_Remaining = 0;

        Asset::ID aidMax = m_Tx.GetWalletDB()->get_AidMax();
        beam::setmax(aidMax, 1u); // insist on hiding asset type anyway

        // in case aidMax wasn't updated - make sure it covers our coins used in this tx (could also use all owned coins)
        for (const auto& cid : m_Coins.m_Output)
            beam::setmax(aidMax, cid.m_AssetID);

        for (const auto& cid : m_Coins.m_Input)
            beam::setmax(aidMax, cid.m_AssetID);

        for (const auto& ins : m_Coins.m_InputShielded)
            beam::setmax(aidMax, ins.m_AssetID);

        Asset::Proof::Params::Override po(aidMax);

        // outputs
        for (const auto& cid : m_Coins.m_Output)
        {
            m_GeneratingInOuts = Stage::None;
            auto pHandler = std::make_shared<HandlerInOuts::HandlerOutput>(*this, m_GeneratingInOuts, pShared);
            auto& h = *pHandler;

            h.m_Method.m_hScheme = m_Height.m_Min;
            h.m_Method.m_Cid = cid;
            FillUserData(Output::User::ToPacked(h.m_Method.m_User));
            m_Tx.get_KeyKeeperStrict()->InvokeAsync(h.m_Method, pHandler);
        }

        // inputs
        for (const auto& cid : m_Coins.m_Input)
        {
            m_GeneratingInOuts = Stage::None;
            auto pHandler = std::make_shared<HandlerInOuts::HandlerInput>(*this, m_GeneratingInOuts, pShared);
            auto& h = *pHandler;

            h.m_Method.m_Cid = cid;
            m_Tx.get_KeyKeeperStrict()->InvokeAsync(h.m_Method, pHandler);
        }

        // inputs shielded
        for (const auto& ins : m_Coins.m_InputShielded)
        {
            m_GeneratingInOuts = Stage::None;
            auto pHandler = std::make_shared<HandlerInOuts::HandlerInputShielded>(*this, m_GeneratingInOuts, pShared);
            auto& h = *pHandler;

            if (!h.InvokeSafe(*this, ins))
                throw TransactionFailedException(true, TxFailureReason::Unknown);
        }

    }

    bool BaseTxBuilder::HandlerInOuts::HandlerInputShielded::InvokeSafe(BaseTxBuilder& b, const IPrivateKeyKeeper2::ShieldedInput& si)
    {
        // currently process inputs 1-by-1
        // don't cache retrieved elements (i.e. ignore overlaps for multiple inputs)
        auto pCoin = b.m_Tx.GetWalletDB()->getShieldedCoin(si.m_Key);
        if (!pCoin)
            return false;
        const ShieldedCoin& c = *pCoin;

        Cast::Down<ShieldedTxo::ID>(m_Method) = c.m_CoinID;

        m_Method.m_pKernel = std::make_unique<TxKernelShieldedInput>();
        m_Method.m_pKernel->m_Fee = si.m_Fee;

        TxoID nShieldedCurrently = b.m_Tx.GetWalletDB()->get_ShieldedOuts();
        beam::setmax(nShieldedCurrently, c.m_TxoID + 1); // assume stored shielded count may be inaccurate, the being-spent element must be present

        ShieldedCoin::UnlinkStatus us(c, nShieldedCurrently);

        bool bWndLost = us.IsLargeSpendWindowLost();
        m_Method.m_pKernel->m_SpendProof.m_Cfg = bWndLost ?
            Rules::get().Shielded.m_ProofMin :
            Rules::get().Shielded.m_ProofMax;

        m_N = m_Method.m_pKernel->m_SpendProof.m_Cfg.get_N();
        if (!m_N)
            return false;

        m_Method.m_iIdx = c.get_WndIndex(m_N);

        if (!bWndLost && (us.m_WndReserve0 < 0))
        {
            // move the selected window forward
            m_Method.m_iIdx += us.m_WndReserve0; // actually decrease
            assert(m_Method.m_iIdx < m_N);
        }

        m_Wnd0 = c.m_TxoID - m_Method.m_iIdx;
        m_Count = m_N;

        TxoID nWndEnd = m_Wnd0 + m_N;
        if (nWndEnd > nShieldedCurrently)
        {
            // move the selected window backward
            uint32_t nExtra = static_cast<uint32_t>(nWndEnd - nShieldedCurrently);
            if (nExtra < m_Wnd0)
                m_Wnd0 -= nExtra;
            else
            {
                nExtra = static_cast<uint32_t>(m_Wnd0);
                m_Wnd0 = 0;
            }

            m_Method.m_iIdx += nExtra;
            m_Count += nExtra;
        }

        BEAM_LOG_INFO() << "ShieldedInput window N=" << m_N << ", Wnd0=" << m_Wnd0 << ", iIdx=" << m_Method.m_iIdx << ", TxoID=" << c.m_TxoID << ", PoolSize=" << nShieldedCurrently;

        b.m_Tx.GetGateway().get_shielded_list(b.m_Tx.GetTxID(), m_Wnd0, m_Count,
            [pHandler = shared_from_this(), weakTx = b.m_Tx.weak_from_this()](TxoID, uint32_t, proto::ShieldedList& msg)
        {
            auto pBldr = pHandler->m_pBuilder.lock();
            if (!pBldr)
                return;
            BaseTxBuilder& b = *pBldr;

            auto pTx = weakTx.lock();
            if (!pTx)
                return;

            try {
                if (!pHandler->OnList(b, msg))
                    b.m_Tx.OnFailed(TxFailureReason::Unknown);
            }
            catch (const TransactionFailedException& ex) {
                b.m_Tx.OnFailed(ex.GetReason(), ex.ShouldNofify());
            }
        });

        return true;
    }

    bool BaseTxBuilder::HandlerInOuts::HandlerInputShielded::OnList(BaseTxBuilder& b, proto::ShieldedList& msg)
    {
        if (msg.m_Items.size() > m_Count)
        {
            BEAM_LOG_ERROR() << "ShieldedList message returned more coins than requested " << TRACE(msg.m_Items.size()) << TRACE(m_Count);
            return false;
        }

        uint32_t nItems = static_cast<uint32_t>(msg.m_Items.size());

        m_Lst.m_p0 = &msg.m_Items.front();
        m_Lst.m_Skip = 0;
        m_Lst.m_vec.swap(msg.m_Items);

        m_Method.m_pList = &m_Lst;
        m_Method.m_pKernel->m_NotSerialized.m_hvShieldedState = msg.m_State1;

        m_Method.m_pKernel->m_Height = b.m_Height;
        m_Method.m_pKernel->m_WindowEnd = m_Wnd0 + nItems;

        if (nItems > m_N)
        {
            uint32_t nDelta = nItems - m_N;
            m_Lst.m_p0 += nDelta;

            assert(m_Method.m_iIdx >= nDelta);
            m_Method.m_iIdx -= nDelta;
        }

        if (nItems < m_N)
        {
            if (m_Wnd0 || (nItems <= m_Method.m_iIdx))
            {
                BEAM_LOG_ERROR() << "ShieldedList message returned unexpected data " << TRACE(m_Wnd0) << TRACE(nItems) << TRACE(m_Method.m_iIdx);
                return false;
            }

            uint32_t nDelta = m_N - nItems;
            m_Lst.m_Skip = nDelta;

            m_Method.m_iIdx += nDelta;
            assert(m_Method.m_iIdx < m_N);
        }

        b.m_Tx.get_KeyKeeperStrict()->InvokeAsync(m_Method, shared_from_this());

        return true;
    }
}
