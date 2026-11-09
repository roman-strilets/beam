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
    struct Wallet::RecognizerHandler : NodeProcessor::Recognizer::IHandler
    {
        Wallet& m_Wallet;
        NodeProcessor::Account m_Account;
        RecognizerHandler(Wallet& wallet, const Key::IPKdf::Ptr& pKdf)
            : m_Wallet(wallet)
        {
            m_Account.m_pOwner = pKdf;
            m_Account.InitFromOwner();
            m_pAccount = &m_Account;
        }

        void OnEvent(Height h, const proto::Event::Base& evt) override
        {
            switch (evt.get_Type())
            {
            case proto::Event::Type::Utxo:
            {
                auto event = Cast::Up<const proto::Event::Utxo&>(evt);
                m_Wallet.ProcessEventUtxo(event, h);
            }break;
            case proto::Event::Type::Shielded:
            {
                auto event = Cast::Up<const proto::Event::Shielded&>(evt);
                m_Wallet.ProcessEventShieldedUtxo(event, h);
            }break;
            default:
                break;
            }
        }

        void AssetEvtsGetStrict(NodeDB::AssetEvt& event, Height h, uint32_t nKrnIdx) override
        {
        }

        void InsertEvent(const HeightPos& pos, const Blob& b, const Blob& k) override
        {
            m_Wallet.m_WalletDB->insertEvent(pos.m_Height, b, k);
        }
/*
        struct WalletDBWalkerEvent : NodeProcessor::Recognizer::WalkerEventBase
        {
            IWalletDB::Ptr m_WalletDB;
            struct Event
            {
                Height m_Height = 0;
                ByteBuffer m_Body;
            };
            std::queue<Event> m_Events;
            Event m_CurrentEvent;
            Blob m_Body;
            WalletDBWalkerEvent(IWalletDB::Ptr walletDB)
                : m_WalletDB(walletDB)
            {
            }
            void Find(const Blob& key)
            {
                m_WalletDB->visitEvents(0, key, [this](Height h, ByteBuffer&& b)
                {
                    m_Events.push({ h, std::move(b) });
                    return true;
                });
            }
            bool MoveNext() override
            {
                if (m_Events.empty())
                    return false;
                m_CurrentEvent = std::move(m_Events.front());
                m_Body = Blob(m_CurrentEvent.m_Body);
                m_Events.pop();
                return true;
            }
            const Blob& get_Body() const override
            {
                return m_Body;
            }
        };

        std::unique_ptr<NodeProcessor::Recognizer::WalkerEventBase> FindEvents(const Blob& key) override
        {
            auto w = std::make_unique<WalletDBWalkerEvent>(m_Wallet.m_WalletDB);
            w->Find(key);
            return w;
        }*/

        bool FindEvents(const Blob& key, NodeProcessor::Recognizer::IEventHandler& handler) override
        {
            bool bFound = false;

            m_Wallet.m_WalletDB->visitEvents(0, key, [&handler, &bFound](Height h, ByteBuffer&& b)
            {
                Blob body = b;

                if (!handler.OnEvent(h, body))
                    return true; // continue

                bFound = true;
                return false; // stop
            });

            return bFound;
        }
    };

    void Wallet::OnRequestComplete(MyRequestBodyPack& r)
    {
        RecognizerHandler h(*this, m_WalletDB->get_OwnerKdf());
        NodeProcessor::Recognizer recognizer(h, m_Extra);
        try 
        {
            Height startHeight = r.m_StartHeight;
            if (!r.m_Res.m_Bodies.empty())
            {
                RequestBodies(r.m_Msg.m_Block0.v, startHeight + r.m_Res.m_Bodies.size());
            }
            for (const auto& b : r.m_Res.m_Bodies)
            {
                ProcessBody(b, startHeight, recognizer);

                ++startHeight;
            }
            assert(GetEventsHeightNext() == startHeight);

            m_WalletDB->set_ShieldedOuts(m_Extra.m_ShieldedOutputs);
        }
        catch (const std::exception&)
        {
            return;
        }
    }

    void Wallet::OnRequestComplete(MyRequestBody& r)
    {
        RecognizerHandler h(*this, m_WalletDB->get_OwnerKdf());
        NodeProcessor::Recognizer recognizer(h, m_Extra);
        try
        {
            if (r.m_Height == 0)
            {
                if (!r.m_Res.m_Body.m_Eternal.empty())
                {
                    // handle treasury
                    const Blob& blob = r.m_Res.m_Body.m_Eternal;
                    Treasury::Data td;
                    if (!NodeProcessor::ExtractTreasury(blob, td))
                        return;

                    for (size_t iG = 0; iG < td.m_vGroups.size(); iG++)
                    {
                        recognizer.RecognizeBlock(td.m_vGroups[iG].m_Data, 0, false);
                    }
                }

                SetTreasuryHandled(true);
                RequestBodies(0, 1);
                return;
            }

            ProcessBody(r.m_Res.m_Body, r.m_Height, recognizer);

            if (r.m_Height < r.m_Msg.m_Top.m_Number.v)
            {
                RequestBodies(r.m_Height, r.m_Height+1);
            }
        }
        catch (const std::exception&)
        {
            return;
        }
    }

    void Wallet::ProcessBody(const proto::BodyBuffers& b, Height h, NodeProcessor::Recognizer& recognizer)
    {
        Block::Body block;
        Deserializer der;
        der.reset(b.m_Perishable);

        der & block.m_vInputs;

        size_t nOuts = 0;
        der & nOuts;
        block.m_vOutputs.resize(nOuts);

        for (size_t i = 0; i < nOuts; i++)
        {
            auto& pOut = block.m_vOutputs[i];
            pOut = std::make_unique<Output>();

            yas::detail::loadRecovery(der, *pOut, h);
        }

        der.reset(b.m_Eternal);
        der & Cast::Down<TxVectors::Eternal>(block);
        PreprocessBlock(block);

        recognizer.m_Pos.m_Height = h;
        recognizer.RecognizeBlock(block, 0, false);
        SetEventsHeight(h);
        ++m_BlocksDone;
    }

    void Wallet::PreprocessBlock(TxVectors::Full& block)
    {
        // In this method we emulate work performed by NodeProcessor::HandleValidatedBlock
        CacheCommitments();

        // remove asset kernels, we don't support them
        auto& kernels = block.m_vKernels;
        kernels.erase(std::remove_if(kernels.begin(), kernels.end(), [](const auto& k)
        {
            switch (k->get_Subtype())
            {
            case TxKernel::Subtype::AssetCreate:
            case TxKernel::Subtype::AssetDestroy:
            case TxKernel::Subtype::AssetEmit:
                return true;
            default:
                return false;
            }
        }), kernels.end());
    }

    void Wallet::RequestBodies()
    {
        if (!IsMobileNodeEnabled())
            return;

        Block::SystemState::Full tip;
        get_tip(tip);

        if (!storage::isTreasuryHandled(*m_WalletDB))
        {
            m_RequestedBlocks = tip.get_Height();
            RequestTreasury();
        }
        else
        {
            Height nextEvent = GetEventsHeightNext();
            if (nextEvent) 
            {
                --nextEvent;
                if (!m_RequestedBlocks)
                {
                    m_RequestedBlocks = tip.get_Height() - nextEvent;
                }
                else
                {
                    // new tip
                    ++m_RequestedBlocks;
                }
            }
            else
            {
                m_RequestedBlocks = tip.get_Height();
            }
            RequestBodies(nextEvent, nextEvent + 1);
        }
    }

    void Wallet::RequestTreasury()
    {
        if (!IsMobileNodeEnabled())
            return;

        m_Extra.m_ShieldedOutputs = 0;
        m_WalletDB->set_ShieldedOuts(0);

        MyRequestBody::Ptr pReq(new MyRequestBody);

        pReq->m_Msg.m_FlagP = proto::BodyBuffers::Full;
        pReq->m_Msg.m_FlagE = proto::BodyBuffers::Full;

        pReq->m_Height = 0;

        PostReqUnique(*pReq);
    }

    void Wallet::RequestBodies(Height currentHeight, Height startHeight)
    {
        if (!IsMobileNodeEnabled())
            return;

        if (!m_PendingBodyPack.empty() || !m_PendingBody.empty())
            return;

        Block::SystemState::Full newTip;
        m_WalletDB->get_History().get_Tip(newTip);

        if (startHeight > newTip.get_Height())
            return;

        Height hCountExtra = newTip.get_Height() - startHeight;
        if (hCountExtra)
        {
            MyRequestBodyPack::Ptr pReq(new MyRequestBodyPack);

            proto::GetBodyPack& msg = pReq->m_Msg;
            msg.m_FlagP = proto::BodyBuffers::Recovery1;
            msg.m_FlagE = proto::BodyBuffers::Full;

            newTip.get_ID(pReq->m_Msg.m_Top);

            Height r = Rules::get().MaxRollback;
            Height count = std::min(newTip.get_Height() - currentHeight, r * 2);
            pReq->m_StartHeight = startHeight;
            msg.m_CountExtra.v = hCountExtra;
            msg.m_Block0.v = currentHeight;
            msg.m_HorizonLo1.v = newTip.get_Height() - count;
            msg.m_HorizonHi1.v = newTip.get_Height();

            PostReqUnique(*pReq);
        }
        else
        {
            MyRequestBody::Ptr pReq(new MyRequestBody);

            pReq->m_Msg.m_FlagP = proto::BodyBuffers::Recovery1;
            pReq->m_Msg.m_FlagE = proto::BodyBuffers::Full;

            newTip.get_ID(pReq->m_Msg.m_Top);
            pReq->m_Height = pReq->m_Msg.m_Top.m_Number.v;
            pReq->m_Msg.m_CountExtra.v = hCountExtra;

            PostReqUnique(*pReq);
        }
    }

    void Wallet::AbortBodiesRequests()
    {
        if (!m_PendingBodyPack.empty())
            DeleteReq(*m_PendingBodyPack.begin());

        if (!m_PendingBody.empty())
            DeleteReq(*m_PendingBody.begin());
    }

    void Wallet::SetTreasuryHandled(bool value)
    {
        m_IsTreasuryHandled = value;
        storage::setTreasuryHandled(*m_WalletDB, value);
    }

    void Wallet::CacheCommitments()
    {
        if (m_IsCommitmentsCached)
            return;

        m_WalletDB->visitCoins([&](const Coin& c)
        {
            if (c.m_status != Coin::Status::Available &&
                c.m_status != Coin::Status::Outgoing && 
                c.m_status != Coin::Status::Maturing)
                return true;

            ECC::Point comm;
            if (m_WalletDB->get_CommitmentSafe(comm, c.m_ID))
            {
                m_Commitments.emplace(comm, c.m_maturity);
            }
            if (c.m_ID.IsBb21Possible())
            {
                CoinID cid = c.m_ID;
                cid.set_WorkaroundBb21();
                if (m_WalletDB->get_CommitmentSafe(comm, cid))
                {
                    m_Commitments.emplace(comm, c.m_maturity);
                }
            }
            return true;
        });
        m_IsCommitmentsCached = true;
    }

    void Wallet::CacheCommitment(const ECC::Point& comm, Height maturity, bool add)
    {
        if (!IsMobileNodeEnabled())
            return;

        if (add)
        {
            m_Commitments.emplace(comm, maturity);
        }
        else
        {
            m_Commitments.erase(comm);
        }
    }

    void Wallet::ResetCommitmentsCache()
    {
        m_Commitments.clear();
        m_IsCommitmentsCached = false;
    }

    bool Wallet::IsMobileNodeEnabled() const
    {
        return !m_OwnedNodesOnline
            && (m_IsBodyRequestsEnabled || storage::needToRequestBodies(*m_WalletDB))
            && !!m_WalletDB->get_MasterKdf();
    }
}
