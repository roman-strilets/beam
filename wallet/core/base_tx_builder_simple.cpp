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
    // SimpleTxBuilder

    SimpleTxBuilder::SimpleTxBuilder(BaseTransaction& tx, SubTxID subTxID)
        : BaseTxBuilder(tx, subTxID)
        , m_Lifetime(kDefaultTxLifetime)
    {
        GetParameter(TxParameterID::Amount, m_Amount);
        GetParameter(TxParameterID::AssetID, m_AssetID);
        GetParameter(TxParameterID::Lifetime, m_Lifetime);
    }

    void SimpleTxBuilder::SignSplit()
    {
        if (Stage::None != m_Signing)
            return;

        struct MyHandler
            :public KeyKeeperHandler
        {
            using KeyKeeperHandler::KeyKeeperHandler;

            IPrivateKeyKeeper2::Method::SignSplit m_Method;

            virtual ~MyHandler() {} // auto

            void OnSuccess(BaseTxBuilder& b_) override
            {
                SimpleTxBuilder& b = Cast::Up<SimpleTxBuilder>(b_);
                b.AddOffset(m_Method.m_kOffset);
                b.AddKernel(std::move(m_Method.m_pKernel));
                b.SaveKernel();
                b.SaveKernelID();
                b.SetStatus(Status::SelfSigned);

                OnAllDone(b);
            }
        };

        KeyKeeperHandler::Ptr pHandler = std::make_shared<MyHandler>(*this, m_Signing);
        MyHandler& x = Cast::Up<MyHandler>(*pHandler);

        SetCommon(x.m_Method);
        m_Tx.get_KeyKeeperStrict()->InvokeAsync(x.m_Method, pHandler);
    }

    bool SimpleTxBuilder::SignTx()
    {
        GenerateInOuts();
        SignSplit();

        return (m_Status >= Status::SelfSigned) && !IsGeneratingInOuts();
    }

    void SimpleTxBuilder::FillUserData(Output::User::Packed* user)
    {
        BaseTxBuilder::FillUserData(user);
        user->m_Amount = m_Amount;
    }
}
