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
    const char Wallet::VoucherManager::Ser::s_szVarName[] = "voucher_man";

    Wallet::VoucherManager::Request* Wallet::VoucherManager::CreateIfNew(const WalletID& trg)
    {
        Request::Target key;
        key.m_Value = trg;
        if (m_setTrg.end() != m_setTrg.find(key))
            return nullptr;

        Request* pReq = new Request;
        m_lstRequests.push_back(pReq->m_ListNode);

        pReq->m_Target.m_Value = trg;
        m_setTrg.insert(pReq->m_Target);

        return pReq;
    }

    void Wallet::VoucherManager::Listen(Request& r)
    {
        r.m_OwnAddr.m_Pk.FromSk(r.m_sk);
        r.m_OwnAddr.SetChannelFromPk();

        get_ParentObj().Listen(r.m_OwnAddr, r.m_sk);
    }

    void Wallet::VoucherManager::EnsureRequest(const WalletID& trg)
    {
        Request* pReq = CreateIfNew(trg);
        if (pReq)
        {
            pReq->m_sk.GenRandomNnz();
            Listen(*pReq);

            SendRequest(*pReq);

            if (IsFirstDue(*pReq))
                CheckTimer();
        }
    }

    bool Wallet::VoucherManager::IsFirstDue(const Request& r) const
    {
        return &m_lstRequests.front() == &r.m_ListNode;
    }

    void Wallet::VoucherManager::CheckTimer()
    {
        if (m_pTimer)
            m_pTimer->cancel();

        while (!m_lstRequests.empty())
        {
            auto& r = m_lstRequests.front().get_ParentObj();
            Timestamp dt_s = getTimestamp() - r.m_ListNode.m_ReqTime_s; // don't care if overflow (time change?), not a problem

            const uint32_t nMaxDuration_s = 10u * 3600; // 10 hours

            if (dt_s < nMaxDuration_s)
            {
                if (!m_pTimer)
                    m_pTimer = io::Timer::create(io::Reactor::get_Current());

                auto dt_ms = static_cast<uint32_t>((nMaxDuration_s - dt_s) * 1000u);
                m_pTimer->start(dt_ms, false, [this]() { CheckTimer(); });
                break;
            }

            // timed-out. Check if we still need it
            auto vTxs = get_ParentObj().FindTxWaitingForVouchers(r.m_Target.m_Value);
            if (vTxs.empty())
                Delete(r);
            else
            {
                m_lstRequests.pop_front();
                m_lstRequests.push_back(r.m_ListNode);
                SendRequest(r);
            }

        }
    }

    void Wallet::VoucherManager::Delete(Request& r)
    {
        get_ParentObj().Unlisten(r.m_OwnAddr);

        m_setTrg.erase(Request::Target::Set::s_iterator_to(r.m_Target));
        m_lstRequests.erase(Request::ListNode::List::s_iterator_to(r.m_ListNode));

        delete &r;
    }

    void Wallet::VoucherManager::DeleteAll()
    {
        while (!m_lstRequests.empty())
            Delete(m_lstRequests.front().get_ParentObj());
    }

    void Wallet::VoucherManager::SendRequest(Request& r)
    {
        assert(&m_lstRequests.back() == &r.m_ListNode);
        r.m_ListNode.m_ReqTime_s = getTimestamp();

        get_ParentObj().RequestVouchersFrom(r.m_Target.m_Value, r.m_OwnAddr, 30);
    }

    ByteBuffer Wallet::VoucherManager::SaveState() const
    {
        ByteBuffer res;
        if (!m_lstRequests.empty())
        {
            Serializer ser;

            uint32_t ver = Ser::s_Ver;
            ser
                & ver
                & m_lstRequests.size();

            for (const auto& n : m_lstRequests)
            {
                auto& r = n.get_ParentObj();
                ser
                    & n.m_ReqTime_s
                    & r.m_Target.m_Value
                    & ECC::Scalar(r.m_sk);
            }

            ser.swap_buf(res);
        }

        return res;
    }

    void Wallet::VoucherManager::LoadState(const Blob& blob)
    {
        if (!blob.n)
            return;

        try
        {
            Deserializer der;
            der.reset(blob.p, blob.n);

            uint32_t ver = 0;
            der & ver;
            if (Ser::s_Ver != ver)
                return; // incompatible ver

            size_t n = 0;
            der & n;
            while (n--)
            {
                Timestamp ts_s = 0;
                WalletID widTrg;
                ECC::NoLeak<ECC::Scalar> sk;

                der
                    & ts_s
                    & widTrg
                    & sk.V;

                Request* pReq = CreateIfNew(widTrg);
                if (pReq)
                {
                    pReq->m_sk = sk.V;
                    Listen(*pReq);

                    pReq->m_ListNode.m_ReqTime_s = ts_s;
                }
            }
        }
        catch (const std::exception& e) {
            BEAM_LOG_WARNING() << "Voucher manager state deserialize error: " << e.what();
        }
    }

    void LoadState(const Blob&);

    void Wallet::get_UniqueVoucher(const WalletID& peerID, const TxID&, boost::optional<ShieldedTxo::Voucher>& res)
    {
        auto count = m_WalletDB->getVoucherCount(peerID);
        const size_t VoucherCountThreshold = 5;

        if (count < VoucherCountThreshold)
            m_VoucherManager.EnsureRequest(peerID);

        if (count > 0)
            res = m_WalletDB->grabVoucher(peerID);
    }

    void Wallet::RequestVouchersFrom(const WalletID& peerID, const WalletID& myID, uint32_t nCount /* = 1 */)
    {
        SetTxParameter msg;
        msg.m_From = myID;
        msg.m_Type = TxType::VoucherRequest;

        msg.AddParameter((TxParameterID) 0, nCount);

        SendSpecialMsg(peerID, msg);
    }

    std::vector<BaseTransaction::Ptr> Wallet::FindTxWaitingForVouchers(const WalletID& peerID) const
    {
        std::vector<BaseTransaction::Ptr> res;
        for (auto p : m_ActiveTransactions)
        {
            ShieldedTxo::Voucher voucher;
            WalletID txPeerID;
            const auto& tx = p.second;
            if (tx->GetType() == TxType::PushTransaction
                && tx->GetParameter<WalletID>(TxParameterID::PeerAddr, txPeerID)
                && peerID == txPeerID
                && !tx->GetParameter<ShieldedTxo::Voucher>(TxParameterID::Voucher, voucher))
            {
                res.push_back(tx);
            }
        }
        return res;
    }

    void Wallet::FailTxWaitingForVouchers(const WalletID& peerID)
    {
        auto transactions = FindTxWaitingForVouchers(peerID);
        for (auto tx : transactions)
        {
            tx->SetParameter(TxParameterID::FailureReason, TxFailureReason::CannotGetVouchers);
            UpdateTransaction(tx);
        }
    }

    void Wallet::FailVoucherRequest(const WalletID& peerID, const WalletID& myID)
    {
        SetTxParameter msgOut;
        msgOut.m_Type = TxType::VoucherResponse;
        msgOut.m_From = myID;
        msgOut.AddParameter(TxParameterID::FailureReason, TxFailureReason::CannotGetVouchers);

        SendSpecialMsg(peerID, msgOut);
    }

    void Wallet::OnVouchersFrom(const WalletAddress& addr, const WalletID& ownAddr, std::vector<ShieldedTxo::Voucher>&& res)
    {
        VoucherManager::Request::Target key;
        key.m_Value = addr.m_BbsAddr;

        VoucherManager::Request::Target::Set::iterator it = m_VoucherManager.m_setTrg.find(key);
        if (m_VoucherManager.m_setTrg.end() == it)
            return;
        VoucherManager::Request& r = it->get_ParentObj();

        if (r.m_OwnAddr != ownAddr)
            return;

        bool bWasAtFront = m_VoucherManager.IsFirstDue(r);
        m_VoucherManager.Delete(r);

        for (const auto& v : res)
            m_WalletDB->saveVoucher(v, addr.m_BbsAddr);

        auto transactions = FindTxWaitingForVouchers(addr.m_BbsAddr);
        for (auto tx : transactions)
        {
            UpdateTransaction(tx);
        }

        if (bWasAtFront)
            m_VoucherManager.CheckTimer();
    }
}
