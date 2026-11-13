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

#include "core/net/fly_client.h"
#include "utility/executor.h"

namespace beam {
namespace proto {

void FlyClient::INetwork::PostRequest(Request& r, Request::IHandler& h)
{
    assert(!r.m_pTrg);
    r.m_pTrg = &h;
    PostRequestInternal(r);
}

void FlyClient::NetworkStd::PostRequestInternal(Request& r)
{
    assert(r.m_pTrg);

    if (Request::Type::BbsMsg == r.get_Type())
    {
        auto& r2 = Cast::Up<RequestBbsMsg>(r);
        if ((Request::Type::BbsMsg == r.get_Type()) && (Rules::get().m_Consensus != Rules::Consensus::FakePoW))
        {
            m_BbsMiner.Post(r2);
            return;
        }

        r2.m_Msg.m_TimePosted = getTimestamp();
    }

    RequestNode* pNode = m_lst.Create_back();
    pNode->m_pRequest = &r;

    OnNewRequests();
}

void FlyClient::NetworkStd::OnNewRequests()
{
    for (ConnectionList::iterator it = m_Connections.begin(); m_Connections.end() != it; ++it)
    {
        Connection& c = *it;
        if (c.IsLive() && c.IsSecureOut())
        {
            c.AssignRequests();
            break;
        }
    }
}

bool FlyClient::NetworkStd::Connection::IsAtTip() const
{
    Block::SystemState::Full sTip;
    return m_This.m_Client.get_History().get_Tip(sTip) && (sTip == m_Tip);
}

void FlyClient::NetworkStd::Connection::AssignRequests()
{
    if (!(Flags::Node & m_Flags))
        return;

    // currently we have no requests that can be assigned unless at tip.
    if (!IsAtTip())
        return;

    RequestList lst;
    for (lst.swap(m_This.m_lst); !lst.empty(); )
    {
        RequestNode& n = lst.front();
        if (n.m_pRequest->m_pTrg)
        {
            lst.pop_front();
            m_lst.push_back(n);
            AssignRequest(n);
        }
        else
            lst.Delete(n);
    }

    if (m_lst.empty() && m_This.m_Cfg.m_PollPeriod_ms)
        SetTimer(m_This.m_Cfg.m_CloseConnectionDelay_ms); // this should allow to get sbbs messages
    else
        KillTimer();
}

void FlyClient::NetworkStd::Connection::AssignRequest(RequestNode& n)
{
    assert(n.m_pRequest);

    switch (n.m_pRequest->get_Type())
    {
#define THE_MACRO(type) \
    case Request::Type::type: \
        if (SendRequest(Cast::Up<Request##type>(*n.m_pRequest))) \
            return; \
        break;

    REQUEST_TYPES_All(THE_MACRO)
#undef THE_MACRO

    default: // ?!
        m_lst.Finish(n);
        return;
    }

    // could not assign the request, return it to global list
    m_lst.erase(RequestList::s_iterator_to(n));
    m_This.m_lst.push_back(n);
}

void FlyClient::NetworkStd::RequestList::Finish(RequestNode& n)
{
    assert(n.m_pRequest);
    if (n.m_pRequest->m_pTrg)
        n.m_pRequest->m_pTrg->OnComplete(*n.m_pRequest);
    Delete(n);
}

FlyClient::NetworkStd::RequestNode& FlyClient::NetworkStd::Connection::get_FirstRequest()
{
    if (m_lst.empty())
        ThrowUnexpected();
    RequestNode& n = m_lst.front();
    assert(n.m_pRequest);

    return n;
}

#define REQUEST_STD_RCV(type, msgIn) \
void FlyClient::NetworkStd::Connection::OnMsg(msgIn&& msg) \
{ \
    auto& n = get_FirstRequest(); \
    auto& r = n.m_pRequest->As<Request##type>(); \
    r.m_Res = std::move(msg); \
 \
     OnRequestData(r); \
 \
    OnDone(n); \
}


#define THE_MACRO(type, msgOut, msgIn) \
REQUEST_STD_RCV(type, msgIn) \
 \
bool FlyClient::NetworkStd::Connection::SendRequest(Request##type& req) \
{  \
    if (!IsSupported(req)) \
        return false; \
 \
    Send(req.m_Msg); \
    return true; \
}

REQUEST_TYPES_Std(THE_MACRO)
#undef THE_MACRO

void FlyClient::NetworkStd::Connection::OnRequestData(RequestUtxo& req)
{
    for (size_t i = 0; i < req.m_Res.m_Proofs.size(); i++)
        if (!m_Tip.IsValidProofUtxo(req.m_Msg.m_Utxo, req.m_Res.m_Proofs[i]))
            ThrowUnexpected();
}

void FlyClient::NetworkStd::Connection::OnRequestData(RequestKernel& req)
{
    if (!req.m_Res.m_Proof.empty())
    {
        if (!m_Tip.IsValidProofKernel(req.m_Msg.m_ID, req.m_Res.m_Proof))
            ThrowUnexpected();

        m_This.m_Client.get_History().AddStates(&req.m_Res.m_Proof.m_State, 1);
    }
}

void FlyClient::NetworkStd::Connection::OnRequestData(RequestAsset& req)
{
    if (req.m_Res.m_Info.m_Owner != Zero) // valid asset info
    {
        if (req.m_Msg.m_Owner != Zero && req.m_Msg.m_Owner != req.m_Res.m_Info.m_Owner)
            ThrowUnexpected();

        if (req.m_Msg.m_AssetID && (req.m_Msg.m_AssetID != req.m_Res.m_Info.m_ID))
            ThrowUnexpected();
    }

    if (!req.m_Res.m_Proof.empty())
        if (!m_Tip.IsValidProofAsset(req.m_Res.m_Info, req.m_Res.m_Proof))
            ThrowUnexpected();
}

bool FlyClient::NetworkStd::Connection::IsSupported(RequestEvents&)
{
    return !!(Flags::Owned & m_Flags);
}

bool FlyClient::NetworkStd::Connection::SendRequest(RequestEnsureSync& req)
{
    if (req.m_IsDependent)
    {
        if (!m_This.HasDependentSubscriptions())
        {
            // temporarily emulate subscription, to get the most recent dependent state
            {
                uint32_t nSub = 1;
                TemporarySwap ts(nSub, m_This.m_DependentSubscriptions);

                SendLoginPlus();
            }
            SendLogin();
        }

        if (Flags::DependentPending & m_Flags)
            return true;
    }

    RequestNode& n = m_lst.back(); // SendRequest is called on the most recently added request
    assert(&req == n.m_pRequest);

    OnDone(n);

    return true;
}

bool FlyClient::NetworkStd::Connection::IsSupported(RequestTransaction& req)
{
    if (!(LoginFlags::SpreadingTransactions & m_LoginFlags))
        return false;

    if (req.m_Msg.m_Context && (get_Ext() < 9))
        return false;

    return true;
}

void FlyClient::NetworkStd::Connection::OnRequestData(RequestProofShieldedInp& req)
{
    if (!req.m_Res.m_Proof.empty())
    {
        ShieldedTxo::DescriptionInp desc;
        desc.m_Height = req.m_Res.m_Height;
        desc.m_SpendPk = req.m_Msg.m_SpendPk;

        if (!m_Tip.IsValidProofShieldedInp(desc, req.m_Res.m_Proof))
            ThrowUnexpected();
    }
}

void FlyClient::NetworkStd::Connection::OnRequestData(RequestProofShieldedOutp& req)
{
    if (!req.m_Res.m_Proof.empty())
    {
        ShieldedTxo::DescriptionOutp desc;
        desc.m_ID = req.m_Res.m_ID;
        desc.m_Height = req.m_Res.m_Height;
        desc.m_SerialPub = req.m_Msg.m_SerialPub;
        desc.m_Commitment = req.m_Res.m_Commitment;

        if (!m_Tip.IsValidProofShieldedOutp(desc, req.m_Res.m_Proof))
            ThrowUnexpected();
    }
}

void FlyClient::Data::DecodedHdrPack::Decode(std::vector<Block::SystemState::Full>& v, const HdrPack& msg)
{
    v.resize(msg.m_vElements.size());
    if (v.empty())
        return;

    v.front().SetFirst(msg.m_Prefix, msg.m_vElements.back());

    for (size_t i = 1; i < msg.m_vElements.size(); i++)
        v[i].SetNext(v[i - 1], msg.m_vElements[msg.m_vElements.size() - i - 1]);
}

bool FlyClient::Data::DecodedHdrPack::DecodeAndCheck(const HdrPack& msg)
{
    if (msg.m_vElements.empty())
        return true; // this is allowed

    std::vector<Block::SystemState::Full> v;
    Decode(v, msg);

    for (size_t i = 0; i < v.size(); i++)
        if (!v[i].IsSane())
            return false;

    // PoW verification is heavy for big packs. Do it in parallel
    PoWCheck pow(Executor::s_pInstance);
    pow.Start(v);

    if (!pow.Wait())
        return false;

    m_vStates = std::move(v);
    return true;
}

struct FlyClient::Data::DecodedHdrPack::PoWCheck::MyTask
    :public Executor::TaskAsync
{
    const Block::SystemState::Full* m_pV;
    uint32_t m_Count;
    std::shared_ptr<std::atomic<bool> > m_pValid;

    void Exec(Executor::Context&) override
    {
        Verify(m_pV, m_Count, *m_pValid);
    }

    static void Verify(const Block::SystemState::Full* pV, uint32_t nCount, std::atomic<bool>& valid)
    {
        for (uint32_t i = 0; i < nCount; i++)
        {
            if (!valid)
                break; // already failed
            if (!pV[i].IsValidPoW())
            {
                valid = false;
                break;
            }
        }
    }
};

FlyClient::Data::DecodedHdrPack::PoWCheck::PoWCheck(Executor* pExec)
    :m_pExec(pExec)
    ,m_pValid(std::make_shared<std::atomic<bool> >(true))
{
}

FlyClient::Data::DecodedHdrPack::PoWCheck::~PoWCheck()
{
    try {
        Wait(); // the tasks reference the states
    }
    catch (...) {
        // ignore
    }
}

void FlyClient::Data::DecodedHdrPack::PoWCheck::Start(const std::vector<Block::SystemState::Full>& v)
{
    assert(!m_Pending);
    uint32_t nTotal = static_cast<uint32_t>(v.size());

    if (!m_pExec || !nTotal)
    {
        MyTask::Verify(v.data(), nTotal, *m_pValid);
        return;
    }

    // Several chunks per thread: the cores differ in speed (performance vs efficiency ones), and the pack is done
    // when the slowest chunk is. Not too small though, each one is a separate task.
    const uint32_t nMinChunk = 32;
    uint32_t nChunks = std::min(m_pExec->get_Threads() * 4, (nTotal + nMinChunk - 1) / nMinChunk);
    nChunks = std::max(nChunks, 1U);

    m_Pending = true;

    for (uint32_t iChunk = 0, i0 = 0; iChunk < nChunks; iChunk++)
    {
        uint32_t i1 = static_cast<uint32_t>(static_cast<uint64_t>(nTotal) * (iChunk + 1) / nChunks);

        auto pTask = std::make_unique<MyTask>();
        pTask->m_pV = v.data() + i0;
        pTask->m_Count = i1 - i0;
        pTask->m_pValid = m_pValid;
        m_pExec->Push(std::move(pTask));

        i0 = i1;
    }
}

bool FlyClient::Data::DecodedHdrPack::PoWCheck::Wait()
{
    if (m_Pending)
    {
        m_pExec->Flush(0);
        m_Pending = false;
    }
    return *m_pValid;
}

bool FlyClient::NetworkStd::Connection::SendRequest(RequestEnumHdrs& req)
{
    Send(req.m_Msg);
    return true;
}

void FlyClient::NetworkStd::Connection::OnMsg(proto::HdrPack&& msg)
{
    auto& n = get_FirstRequest();
    auto& r = n.m_pRequest->As<RequestEnumHdrs>();

    if (!r.DecodeAndCheck(msg))
        ThrowUnexpected();

    OnDone(n, r.m_vStates.empty());
}

void FlyClient::NetworkStd::Connection::OnMsg(proto::Hdr&& msg)
{
    if (!msg.m_Description.IsValid())
        ThrowUnexpected();

    m_This.m_Client.get_History().AddStates(&msg.m_Description, 1);
}

void FlyClient::NetworkStd::Connection::OnMsg(DataMissing&& msg)
{
    auto& n = get_FirstRequest();
    auto& r = *n.m_pRequest;

    switch (r.get_Type())
    {
    case Request::Type::EnumHdrs:
    case Request::Type::BodyPack:
        OnDone(n);
        break;

    default:
        ThrowUnexpected();
    }
}

bool FlyClient::NetworkStd::Connection::SendRequest(RequestKernel2& req)
{
    Send(req.m_Msg);
    return true;
}

bool FlyClient::NetworkStd::Connection::SendRequest(RequestKernel3& req)
{
    if (get_Ext() < 11)
        return false;

    Send(req.m_Msg);
    return true;
}

void FlyClient::NetworkStd::Connection::OnMsg(ProofKernel2&& msg)
{
    auto& n = get_FirstRequest();
    switch (n.m_pRequest->get_Type())
    {
    case Request::Type::Kernel2:
        {
            auto& req = Cast::Up<RequestKernel2>(*n.m_pRequest);
            req.m_Res = std::move(msg);

            if (req.m_Res.m_Proof.empty())
            {
                if (req.m_Res.m_Kernel)
                    ThrowUnexpected();
            }
            else
            {
                if (!m_Tip.IsValidProofKernel(req.m_Msg.m_ID, req.m_Res.m_Proof))
                    ThrowUnexpected();

                if (req.m_Res.m_Kernel)
                {
                    if (!req.m_Msg.m_Fetch)
                        ThrowUnexpected();

                    if (req.m_Res.m_Kernel->IsValid(req.m_Res.m_Height))
                        ThrowUnexpected();

                    if (req.m_Res.m_Kernel->get_ID() != req.m_Msg.m_ID)
                        ThrowUnexpected();
                }
                else
                {
                    if (req.m_Msg.m_Fetch)
                        ThrowUnexpected();
                }
            }
        }
        break;

    case Request::Type::Kernel3:
        {
            auto& req = Cast::Up<RequestKernel3>(*n.m_pRequest);
            req.m_Res = std::move(msg);

            if (req.m_Res.m_Kernel)
            {
                if (!req.m_Res.m_Kernel->IsValid(req.m_Res.m_Height))
                    ThrowUnexpected();

                if (req.m_Msg.m_WithProof && !m_Tip.IsValidProofKernel(req.m_Res.m_Kernel->get_ID(), req.m_Res.m_Proof))
                    ThrowUnexpected();
            }

        }
        break;

    default:
        proto::NodeConnection::ThrowUnexpected();
    }
 
    OnDone(n);
}

bool FlyClient::NetworkStd::Connection::SendTrgCtx(const std::unique_ptr<Merkle::Hash>& pCtx)
{
    if (pCtx)
    {
        if (get_Ext() < 9)
            return false;

        if (m_Dependent.m_pQuery)
        {
            if (*pCtx == *m_Dependent.m_pQuery)
                return true;
        }
        else
            m_Dependent.m_pQuery = std::make_unique<Merkle::Hash>();

        *m_Dependent.m_pQuery = *pCtx;
    }
    else
    {
        if (!m_Dependent.m_pQuery)
            return true;
        m_Dependent.m_pQuery.reset();
    }

    proto::SetDependentContext msg;
    TemporarySwap ts(msg.m_Context, m_Dependent.m_pQuery);
    Send(msg);

    return true;
}

bool FlyClient::NetworkStd::Connection::SendRequest(RequestContractVars& req)
{
    if (!SendTrgCtx(req.m_pCtx))
        return false;

    Send(req.m_Msg);
    return true;
}

REQUEST_STD_RCV(ContractVars, ContractVars)

bool FlyClient::NetworkStd::Connection::SendRequest(RequestContractLogs& req)
{
    if (!SendTrgCtx(req.m_pCtx))
        return false;

    Send(req.m_Msg);
    return true;
}

REQUEST_STD_RCV(ContractLogs, ContractLogs)

void FlyClient::NetworkStd::Connection::OnRequestData(RequestContractVar& req)
{
    if (!req.m_Res.m_Proof.empty() && !m_Tip.IsValidProofContract(req.m_Msg.m_Key, req.m_Res.m_Value, req.m_Res.m_Proof))
        ThrowUnexpected();
}

bool FlyClient::NetworkStd::Connection::SendRequest(RequestBbsMsg& req)
{
    if (!(LoginFlags::Bbs & m_LoginFlags))
        return false;

    Send(req.m_Msg);
    Send(proto::Ping());

    return true;
}

bool FlyClient::NetworkStd::Connection::SendRequest(RequestAssetsListAt& req)
{
    if (get_Ext() < 10)
        return false;

    Send(req.m_Msg);
    return true;
}

void FlyClient::NetworkStd::Connection::OnMsg(proto::Pong&&)
{
    if (!m_lst.empty())
    {
        auto& n = get_FirstRequest();
        switch (n.m_pRequest->get_Type())
        {
        case Request::Type::BbsMsg:
            OnDone(n, false);
            return;

        default: // suppress warning
            break;
        }
    }

    if (!(Flags::DependentPending & m_Flags))
        ThrowUnexpected();
    m_Flags &= ~Flags::DependentPending;

    for (auto it = m_lst.begin(); m_lst.end() != it; )
    {
        auto& n = *it++;
        if (Request::Type::EnsureSync == n.m_pRequest->get_Type())
            OnDone(n, false);
    }
}

void FlyClient::NetworkStd::Connection::OnMsg(proto::AssetsListAt&& msg)
{
    auto& n = get_FirstRequest();
    auto& r = n.m_pRequest->As<RequestAssetsListAt>();

    if (msg.m_Assets.empty())
    {
        if (msg.m_bMore)
            ThrowUnexpected();
    }
    else
    {
        Asset::ID aid0 = r.m_Msg.m_Aid0;
        for (const auto& ai : msg.m_Assets)
        {
            if (aid0 > ai.m_ID)
                ThrowUnexpected();

            aid0 = ai.m_ID + 1;
        }

        if (r.m_Res.empty())
            r.m_Res = std::move(msg.m_Assets);
        else
            r.m_Res.append_range(msg.m_Assets);


        r.m_Msg.m_Aid0 = aid0;
    }


    if (msg.m_bMore)
        Send(r.m_Msg);
    else
        OnDone(n);
}

void FlyClient::NetworkStd::Connection::OnDone(RequestNode& n, bool bMaybeRetry /* = true */)
{
    assert(n.m_pRequest);

    if (n.m_pRequest->m_pTrg)
    {
        if (bMaybeRetry && !IsAtTip())
        {
            // should retry
            m_lst.erase(RequestList::s_iterator_to(n));
            m_This.m_lst.push_back(n);
            m_This.OnNewRequests();
            return;
        }

        m_lst.Finish(n);
    }
    else
        m_lst.Delete(n); // aborted already

    if (m_lst.empty() && m_This.m_Cfg.m_PollPeriod_ms)
        SetTimer(0);
}

} // namespace proto
} // namespace beam
