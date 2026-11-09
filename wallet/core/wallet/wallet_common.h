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

#pragma once

// Definitions shared by all parts of Wallet implementation (wallet*.cpp).
// Must be the first include of each part. Not intended for use outside of them.

#include "wallet.h"
#include <boost/uuid/uuid.hpp>

#include "core/crypto/ecc_native.h"
#include "core/consensus/block_crypt.h"
#include "core/consensus/shielded.h"
#include "core/consensus/treasury.h"
#include "utility/logger.h"
#include "utility/helpers.h"
#include "wallet/core/tx/simple_transaction.h"
#include "wallet/core/tx/contract_transaction.h"
#include "wallet/core/common/strings_resources.h"
#include "wallet/core/assets/assets_utils.h"
#include "wallet/core/contracts/shaders_manager.h"

#include <algorithm>
#include <random>
#include <iomanip>
#include <numeric>
#include <queue>

namespace beam::wallet
{
    using namespace std;
    using namespace ECC;

    struct Wallet::VoucherManager::Ser {
        static const uint32_t s_Ver = 1u;
        static const char s_szVarName[];
    };

    struct Wallet::WidgetRunner
    {
        static const char s_szVarName[];

        struct Entry
            :public ManagerStdInWallet
            ,public intrusive::set_base_hook<std::string>
        {
            using ManagerStdInWallet::ManagerStdInWallet;

            void WriteStream(const Blob&, uint32_t iStream) override;
            void OnDone(const std::exception* pExc) override;

            Blob get_StoreName() override { return Blob(m_Key.c_str(), static_cast<uint32_t>(m_Key.size())); }
        };

        intrusive::multiset_autoclear<Entry> m_Set;

        Entry* Get(const std::string& sName)
        {
            auto it = m_Set.find(sName, Entry::Comparator());
            return (m_Set.end() != it) ? &(*it) : nullptr;
        }

        Entry* GetOrCreate(Wallet& w, std::string&& sName)
        {
            auto pVal = Get(sName);
            if (pVal)
                return pVal;

            ByteBuffer buf;
            w.m_WalletDB->get_AppData(Blob(sName.c_str(), static_cast<uint32_t>(sName.size())), Blob(), buf);
            if (buf.empty())
                return nullptr; // no bytecode

            pVal = new Entry(w);
            pVal->m_Key = std::move(sName);
            m_Set.insert(*pVal);

            pVal->m_BodyManager = std::move(buf);

            if (w.m_pWidgetNotify)
                w.m_pWidgetNotify->OnCreate(pVal->m_Key);

            return pVal;
        }

        void Save(Wallet& w)
        {
            if (m_Set.empty())
                w.m_WalletDB->removeVarRaw(s_szVarName);
            else
            {
                Serializer ser;
                ser & m_Set.size();
                for (const auto& x : m_Set)
                    ser & x.m_Key;

                auto res = ser.buffer();

                w.m_WalletDB->setVarRaw(s_szVarName, res.first, res.second);
            }

        }

        void Load(Wallet& w)
        {
            try {

                ByteBuffer buf;
                w.m_WalletDB->getBlob(s_szVarName, buf);

                if (!buf.empty())
                {
                    Deserializer der;
                    der.reset(buf);

                    size_t n = 0;
                    der & n;

                    while (n--)
                    {
                        std::string sName;
                        der & sName;
                        if (!sName.empty())
                            GetOrCreate(w, std::move(sName));
                    }
                }

            }
            catch (const std::exception&) {
                // ignore
            }
        }
    };
}
