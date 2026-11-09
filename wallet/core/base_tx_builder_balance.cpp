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
    // BaseTxBuilder::Coins
    void BaseTxBuilder::Coins::AddOffset(ECC::Scalar::Native& kOffs, const Key::IKdf::Ptr& pMasterKdf) const
    {
        ECC::Scalar::Native sk;
        for (const CoinID& cid : m_Input)
        {
            CoinID::Worker(cid).Create(sk, *cid.get_ChildKdf(pMasterKdf));
            kOffs += sk;
        }

        for (const auto& si : m_InputShielded)
        {
            si.get_SkOut(sk, si.m_Fee, *pMasterKdf);
            kOffs += sk;
        }

        kOffs = -kOffs;

        for (const CoinID& cid : m_Output)
        {
            CoinID::Worker(cid).Create(sk, *cid.get_ChildKdf(pMasterKdf));
            kOffs += sk;
        }

        kOffs = -kOffs;
    }


    ///////////////////////////////////////
    // BaseTxBuilder::Balance

    BaseTxBuilder::Balance::Balance(BaseTxBuilder& b)
        :m_Builder(b)
    {
    }

    void BaseTxBuilder::Balance::Add(const Coin::ID& cid, bool bOutp)
    {
        (bOutp ? m_Builder.m_Coins.m_Output : m_Builder.m_Coins.m_Input).push_back(cid);

        Entry& x = m_Map[cid.m_AssetID];
        if (bOutp)
            x.m_Value -= cid.m_Value;
        else
            x.m_Value += cid.m_Value;
    }

    void BaseTxBuilder::Balance::Add(const IPrivateKeyKeeper2::ShieldedInput& si)
    {
        m_Builder.m_Coins.m_InputShielded.push_back(si);

        m_Map[si.m_AssetID].m_Value += si.m_Value;
        m_Map[0].m_Value -= si.m_Fee;
    }

    void BaseTxBuilder::Balance::Add(const ShieldedTxo::ID& sid)
    {
        auto& fs = Transaction::FeeSettings::get(m_Builder.m_Height.m_Min);

        IPrivateKeyKeeper2::ShieldedInput si;
        Cast::Down<ShieldedTxo::ID>(si) = sid;
        si.m_Fee = fs.m_ShieldedInputTotal; // auto-fee for shielded inputs
        Add(si);
    }

    void BaseTxBuilder::Balance::CreateOutput(Amount val, Asset::ID aid, Key::Type type)
    {
        Coin newUtxo = m_Builder.m_Tx.GetWalletDB()->generateNewCoin(val, aid);
        newUtxo.m_ID.m_Type = type;

        newUtxo.m_createTxId = m_Builder.m_Tx.GetTxID();
        m_Builder.m_Tx.GetWalletDB()->storeCoin(newUtxo);

        Add(newUtxo.m_ID, true);
    }

    void BaseTxBuilder::Balance::AddPreselected()
    {
        CoinIDList cidl;
        m_Builder.GetParameter(TxParameterID::PreselectedCoins, cidl);

        for (const auto& cid : cidl)
            Add(cid, false);
    }

    void BaseTxBuilder::Balance::CompleteBalance()
    {
        std::set<Asset::ID> setRcv; // those auto-created coins should be 'Regular' instead of 'Change'
        for (const auto& v : m_Map)
        {
            if (v.second.m_Value > 0)
                setRcv.insert(v.first);
        }

        AddPreselected();

        uint32_t nNeedInputs = 0;
        for (const auto& v : m_Map)
        {
            if (v.second.m_Value < 0)
                nNeedInputs++;
        }

        if (nNeedInputs)
        {
            if (m_Map[0].m_Value >= 0)
                nNeedInputs++; // although there's no dificiency in def asset, assume it may arise due to involuntary fees

            // go by asset type in reverse order, to reach the def asset last
            for (auto it = m_Map.rbegin(); m_Map.rend() != it; ++it)
            {
                AmountSigned& val = it->second.m_Value;
                if (val >= 0)
                    continue;

                uint32_t nShieldedMax = Rules::get().Shielded.MaxIns;
                uint32_t nShieldedInUse = static_cast<uint32_t>(m_Builder.m_Coins.m_InputShielded.size()); // preselected?

                if (nShieldedMax <= nShieldedInUse)
                    nShieldedMax = 0;
                else
                    nShieldedMax -= nShieldedInUse;

                assert(nNeedInputs);
                nShieldedMax = (nShieldedMax + nNeedInputs - 1) / nNeedInputs; // leave some reserve to other assets

                Asset::ID aid = it->first;

                std::vector<Coin> vSelStd;
                std::vector<ShieldedCoin> vSelShielded;
                m_Builder.m_Tx.GetWalletDB()->selectCoins2(m_Builder.m_Height.m_Min, -val, aid, vSelStd, vSelShielded, nShieldedMax, true);

                for (const auto& c : vSelStd)
                    Add(c.m_ID, false);

                for (const auto& c : vSelShielded)
                    Add(c.m_CoinID);

                if (val < 0)
                {
                    BEAM_LOG_ERROR()
                        << m_Builder.m_Tx.GetTxID()
                        << "[" << m_Builder.m_SubTxID << "]"
                        << " Missing "
                        << PrintableAmount(Amount(-val), false, aid);

                    throw TransactionFailedException(!m_Builder.m_Tx.IsInitiator(), TxFailureReason::NoInputs);
                }

                nNeedInputs--;
            }

        }

        for (const auto& v : m_Map)
        {
            AmountSigned val = v.second.m_Value;
            assert(val >= 0);

            if (val > 0)
            {
                Asset::ID aid = v.first;

                bool bRcv = setRcv.end() != setRcv.find(aid);
                CreateOutput(val, aid, bRcv ? Key::Type::Regular : Key::Type::Change);
            }

            assert(!v.second.m_Value);
        }
    }
}
