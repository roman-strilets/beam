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

// Wallet-side shader commands.

#include "cli_internal.h"

namespace wallet_cli
{
namespace
{
    void CompileShader(ByteBuffer& res, const char* sz, bvm2::Processor::Kind kind, Wasm::Compiler::DebugInfo* pDbgInfo = nullptr)
    {
        beam::FStream fs;
        fs.Open(sz, true, true);

        res.resize(static_cast<size_t>(fs.get_Remaining()));
        if (!res.empty())
            fs.read(&res.front(), res.size());

        struct MyCompiler :public bvm2::Processor::Compiler
        {
            bool m_HaveMissing;

            void OnBindingMissing(const Wasm::Compiler::PerImport& x) override
            {
                if (!m_HaveMissing)
                {
                    m_HaveMissing = true;
                    std::cout << "Shader uses newer API, some features may not work.\n";
                }
                std::cout << "\t Missing " << x << std::endl;
            }
        };

        MyCompiler c;
        c.m_HaveMissing = false;
        c.Compile(res, res, kind, pDbgInfo);
    }
}  // namespace

    int ShaderInvoke(const po::variables_map& vm)
    {
        return DoWalletFunc(vm, [](const po::variables_map& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
            {

			    struct MyManager
				    :public ManagerStdInWallet
			    {
				    bool m_Done = false;
				    bool m_Err = false;
                    bool m_Async = false;

                    using ManagerStdInWallet::ManagerStdInWallet;

                    Wasm::Compiler::DebugInfo m_DbgInfo;

				    void OnDone(const std::exception* pExc) override
				    {
					    m_Done = true;
					    m_Err = !!pExc;

                        if (pExc)
                        {
                            std::cout << "Shader exec error: " << pExc->what() << std::endl;
                            if (m_Debug)
                                DumpCallstack(std::cout, &m_DbgInfo);
                        }
                        else
                            std::cout << "Shader output: " << m_Out.str() << std::endl;

                        if (m_Async)
                            io::Reactor::get_Current().stop();
				    }
                };

                MyManager man(*wallet);
                man.m_Debug = vm[cli::SHADER_DEBUG].as<bool>();

                auto sVal = vm[cli::SHADER_BYTECODE_APP].as<string>();
                if (sVal.empty())
                    throw std::runtime_error("shader file not specified");

                CompileShader(man.m_BodyManager, sVal.c_str(), MyManager::Kind::Manager, man.m_Debug ? &man.m_DbgInfo : nullptr);

                sVal = vm[cli::SHADER_BYTECODE_CONTRACT].as<string>();
                if (!sVal.empty())
                    CompileShader(man.m_BodyContract, sVal.c_str(), MyManager::Kind::Contract);

                sVal = vm[cli::SHADER_ARGS].as<string>(); // should be comma-separated list of name=val pairs
                if (!sVal.empty())
                    man.AddArgs(sVal);

                man.set_Privilege(vm[cli::SHADER_PRIVILEGE].as<uint32_t>());

                std::cout << "Executing shader..." << std::endl;

                auto startedEvent = io::AsyncEvent::create(io::Reactor::get_Current(),
                    [&man]()
                    {
                        man.StartRun(man.m_Args.empty() ? 0 : 1); // scheme if no args
                    });

                wallet->DoInSyncedWallet([startedEvent]()
                    {
                        startedEvent->post();
                    });

                if (!man.m_Done)
                {
                    man.m_Async = true;
                    io::Reactor::get_Current().run();

                    if (!man.m_Done)
                    {
                        // abort, propagate it
                        io::Reactor::get_Current().stop();
                        return -1;
                    }
                }

                if (man.m_Err || man.m_InvokeData.m_vec.empty())
                    return 1;

                auto invData = man.get_InvokeData();
                const auto height  = wallet->get_TipHeight();
                const auto fee     = invData.get_FullFee(height);
                const auto comment = invData.get_FullComment();

                std::cout << "Creating new contract invocation tx on behalf of the shader" << std::endl;
                if (man.m_Args["action"] == "create")
                {
                    bvm2::ShaderID sid;
                    bvm2::get_ShaderID(sid, Blob(man.m_BodyContract));
                    for (auto& invokeEntry : invData.m_vec)
                    {
                        bvm2::ContractID cid;
                        bvm2::get_CidViaSid(cid, sid, Blob(invokeEntry.m_Args));
                        std::cout << "Contract ID: " << cid.str() << std::endl;
                    }
                }
                else if(man.m_Args["action"] == "destroy" && !man.m_Args["cid"].empty())
                {
                    std::cout << "Contract ID: " << man.m_Args["cid"] << std::endl;
                }
                std::cout << "\tComment: " << comment << std::endl;

                bvm2::FundsMap fmSpend;
                bool bMaxSpend = !invData.m_SpendMax.empty();
                const auto& fm = bMaxSpend ? invData.m_SpendMax : fmSpend;
                if (!bMaxSpend)
                    fmSpend = invData.get_FullSpend();

                for (const auto& info: fm)
                {
                    auto aid = info.first;
                    auto amount = info.second;

                    bool bSpend = (amount > 0);
                    if (!bSpend)
                        amount = -amount;

                    std::cout << '\t' << (bSpend ? "Send" : "Recv") << ' ' << PrintableAmount(static_cast<Amount>(amount), false, aid);

                    if (bMaxSpend)
                        std::cout << ' ' << (bSpend ? "(at most)" : "(at least)");

                    std::cout << std::endl;
                }

                std::cout << "\tTotal fee: " << PrintableAmount(fee, false, 0) << std::endl;
                std::cout << "Proceed? (y/n)" << std::endl;

                std::string s;
                std::cin >> s;

                if (s.empty() || ('y' != std::tolower(s.front())))
                    return 1;

                ByteBuffer msg(comment.begin(), comment.end());
                currentTxID = wallet->StartTransaction(
                    CreateTransactionParameters(TxType::Contract)
                    .SetParameter(TxParameterID::ContractDataPacked, invData)
                    .SetParameter(TxParameterID::Message, msg)
                );

                return 0;
            });
    }

    int ShaderWidget(const po::variables_map& vm)
    {
        return DoWalletFunc(vm, [](const po::variables_map& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
            {
                auto sName = vm[cli::SHADER_WIDGET_NAME].as<std::string>();
                if (sName.empty())
                {
                    std::cout << "widget name should not be empty";
                    return 1;
                }
                auto sPath = vm[cli::SHADER_BYTECODE_APP].as<std::string>();

                ByteBuffer buf;
                if (sPath.empty())
                    std::cout << "Widget " << sName << " removed" << std::endl;
                else
                {
                    CompileShader(buf, sPath.c_str(), bvm2::Processor::Kind::Manager);

                    bvm2::ShaderID sid;
                    bvm2::get_ShaderID(sid, buf);

                    std::cout << "Widget " << sName << " sid: " << sid << std::endl;
                }

                wallet->SetWidget(std::move(sName), std::move(buf));
                return 0;
            });
    }
}  // namespace wallet_cli
