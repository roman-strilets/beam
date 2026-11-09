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

// Block details and recovery import commands.

#include "cli_internal.h"

namespace wallet_cli
{
    int ShowBlockDetails(const po::variables_map& vm)
    {
        auto walletDB = OpenDataBase(vm);

        auto wallet = std::make_shared<Wallet>(walletDB,
            Wallet::TxCompletedAction(),
            Wallet::UpdateCompletedAction());
        auto nnet = CreateNetwork(*wallet, vm);

        if (!nnet)
        {
            return -1;
        }

        if (vm.count(cli::BLOCK_HEIGHT) != 1)
        {
            BEAM_LOG_ERROR() << "block_height should be specified";
            return -1;
        }

        Height blockHeight = vm[cli::BLOCK_HEIGHT].as<Nonnegative<Height>>().value;

        if (!blockHeight)
        {
            BEAM_LOG_ERROR() << "Cannot get block header.";
            return -1;
        }

        proto::FlyClient::RequestEnumHdrs::Ptr pRequest(new proto::FlyClient::RequestEnumHdrs);
        proto::FlyClient::RequestEnumHdrs& request = *pRequest;

        request.m_Msg.m_Height = blockHeight;

        struct MyHandler : public proto::FlyClient::Request::IHandler
        {
            proto::FlyClient::Request* m_Request = nullptr;

            ~MyHandler() {
                m_Request->m_pTrg = nullptr;
            }

            void OnComplete(proto::FlyClient::Request&) override
            {
                m_Request->m_pTrg = nullptr;
                io::Reactor::get_Current().stop();
            }

        } myHandler;

        myHandler.m_Request = &request;
        nnet->PostRequest(request, myHandler);

        io::Reactor::get_Current().run();

        if (request.m_pTrg || request.m_vStates.size() != 1)
        {
            BEAM_LOG_ERROR() << "Cannot get block header.";
            return -1;
        }

        Block::SystemState::Full state = request.m_vStates.front();
        HeightHash blockID;
        state.get_ID(blockID);

        const Rules& r = Rules::get();
        std::string rulesHash = r.pForks[r.FindFork(blockID.m_Height)].m_Hash.str();

        std::cout << "Block Details:" << "\n"
            << "Height: " << blockID.m_Height << "\n"
            << "Hash: " << blockID.m_Hash.str() << "\n"
            << "Previous state hash: " << state.m_Prev.str() << "\n"
            << "ChainWork: " << state.m_ChainWork.str() << "\n"
            << "Kernels: " << state.m_Kernels.str() << "\n"
            << "Definition: " << state.m_Definition.str() << "\n"
            << "Timestamp: " << state.m_TimeStamp << "\n"
            << "POW: " << beam::to_hex(&state.m_PoW, sizeof(state.m_PoW)) << "\n"
            << "Difficulty: " << state.m_PoW.m_Difficulty.ToFloat() << "\n"
            << "Packed difficulty: " << state.m_PoW.m_Difficulty.m_Packed << "\n"
            << "Rules hash: " << rulesHash << "\n";

        return 0;
    }

    int ImportRecovery(const po::variables_map& vm)
    {
        if (vm[cli::IMPORT_EXPORT_PATH].defaulted())
        {
            BEAM_LOG_ERROR() << kErrorFileLocationParamReqired;
            return -1;
        }

        struct MyProgress : IWalletDB::IRecoveryProgress
        {
            bool OnProgress(uint64_t done, uint64_t total) override
            {
                size_t percent = done * 100 / total;
                std::cout << "\rImporting recovery data: " << percent << "% (" << done << "/" << total << ")";
                std::cout.flush();
                return true; 
            }
        };
        auto path = vm[cli::IMPORT_EXPORT_PATH].as<string>();
        return DoWalletFunc(vm, [&path](auto&& vm, auto&& wallet, auto&& walletDB, auto& currentTxID)
            {
                MyProgress progress;
                walletDB->ImportRecovery(path, *wallet, progress);
                return 1;
            });
    }
}  // namespace wallet_cli
