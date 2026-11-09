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
    const char Wallet::WidgetRunner::s_szVarName[] = "widget_shader";

    void Wallet::SetWidget(std::string&& sName, ByteBuffer&& x)
    {
        if (sName.empty())
            return; // name shouldn't be empty

        InitWidgetRunner();
        auto& w = *m_pWidgetRunner;

        size_t n0 = w.m_Set.size();

        auto pVal = w.Get(sName);
        if (pVal)
        {
            if (m_pWidgetNotify)
                m_pWidgetNotify->OnDelete(sName);

            w.m_Set.Delete(*pVal);
        }

        if (x.empty())
        {
            m_WalletDB->ClearAppData(Blob(sName.c_str(), static_cast<uint32_t>(sName.size())));
        }
        else
        {
            Blob buf(x);
            m_WalletDB->set_AppData(Blob(sName.c_str(), static_cast<uint32_t>(sName.size())), Blob(), &buf);

            w.GetOrCreate(*this, std::move(sName));
        }

        if (w.m_Set.size() != n0)
            w.Save(*this);
    }

    void Wallet::InitWidgetRunner()
    {
        if (!m_pWidgetRunner)
        {
            m_pWidgetRunner = std::make_unique<WidgetRunner>();
            m_pWidgetRunner->Load(*this);
        }
    }

    void Wallet::ProcessWidget()
    {
        if (!m_pWidgetRunner)
            return;

        auto& w = *m_pWidgetRunner;

        for (auto& x : w.m_Set)
        {
            x.m_InvokeData.Reset();
            x.StartRun(0);
        }
    }

    void Wallet::WidgetRunner::Entry::OnDone(const std::exception* /* pExc */)
    {
    }

    void Wallet::WidgetRunner::Entry::WriteStream(const Blob& b, uint32_t iStream)
    {
        if (m_Wallet.m_pWidgetNotify)
            m_Wallet.m_pWidgetNotify->OnWriteStream(m_Key, b, iStream);

        const char* szPrefix = "";
        switch (iStream)
        {
        case Shaders::Stream::Out: szPrefix = "out"; break;
        case Shaders::Stream::Error: szPrefix = "error"; break;
        }

        std::cout << "Widget " << m_Key << " " << szPrefix << ": ";
        std::cout.write((const char*) b.p, b.n);
        std::cout << std::endl;
    }
}
