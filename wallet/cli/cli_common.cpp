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

// Helpers shared by the CLI commands: database and password handling, node connection, option parsing.

#include "cli_internal.h"

namespace wallet_cli
{
namespace
{
    struct CliNodeConnection final : public proto::FlyClient::NetworkStd
    {
    public:
        explicit CliNodeConnection(proto::FlyClient& fc) : proto::FlyClient::NetworkStd(fc) {};
        void OnConnectionFailed(const proto::NodeConnection::DisconnectReason& reason) override
        {
            BEAM_LOG_ERROR() << kErrorConnectionFailed << " reason: " << reason;
        };
    };
}  // namespace

    std::string interpretStatusCliImpl(const beam::wallet::TxDescription& tx)
    {
        return beam::wallet::interpretStatus(tx);
    }

    void ResolveWID(PeerID& res, const std::string& s)
    {
        bool bValid = true;
        ByteBuffer bb = from_hex(s, &bValid);

        if ((bb.size() != res.nBytes) || !bValid)
            throw std::runtime_error(kErrorInvalidWID);

        memcpy(res.m_pData, &bb.front(), res.nBytes);
    }

    IWalletDB::Ptr OpenDataBase(const po::variables_map& vm, const SecString& pass)
    {
        BOOST_ASSERT(vm.count(cli::WALLET_STORAGE) > 0);
        auto walletPath = vm[cli::WALLET_STORAGE].as<string>();

        if (!WalletDB::isInitialized(walletPath))
        {
            throw std::runtime_error(kErrorWalletNotInitialized);
        }

        IWalletDB::Ptr walletDB = WalletDB::open(walletPath, pass);

        BEAM_LOG_INFO() << kWalletOpenedMessage;
        return walletDB;
    }

    IWalletDB::Ptr OpenDataBase(const po::variables_map& vm)
    {
        return OpenDataBase(vm, GetPassword(vm));
    }

    SecString GetPassword(const po::variables_map& vm)
    {
        SecString pass;
        if (!beam::read_wallet_pass(pass, vm))
        {
            throw std::runtime_error(kErrorWalletPwdNotProvided);
        }
        return pass;
    }

    bool ReadWalletSeed(NoLeak<uintBig>& walletSeed, const po::variables_map& vm, bool generateNew)
    {
        SecString seed;
        WordList phrase;
        if (generateNew)
        {
            BEAM_LOG_INFO() << kSeedPhraseReadTitle;
            phrase = GeneratePhrase();
        }
        else if (vm.count(cli::SEED_PHRASE))
        {
            auto tempPhrase = vm[cli::SEED_PHRASE].as<string>();
            boost::algorithm::trim_if(tempPhrase, [](char ch) { return ch == ';'; });
            phrase = string_helpers::split(tempPhrase, ';');

            if (phrase.size() != WORD_COUNT
                || (vm.count(cli::IGNORE_DICTIONARY) == 0 && !isValidMnemonic(phrase)))
            {
                BEAM_LOG_ERROR() << boost::format(kErrorSeedPhraseInvalid) % tempPhrase;
                return false;
            }
        }
        else
        {
            BEAM_LOG_ERROR() << kErrorSeedPhraseNotProvided;
            return false;
        }

        auto buf = decodeMnemonic(phrase);
        seed.assign(buf.data(), buf.size());

        walletSeed.V = seed.hash().V;
        return true;
    }

    boost::optional<TxID> GetTxID(const po::variables_map& vm)
    {
        boost::optional<TxID> res;
        auto txIdStr = vm[cli::TX_ID].as<string>();
        if (txIdStr.empty())
        {
            BEAM_LOG_ERROR() << kErrorTxIdParamReqired;
            return res;
        }

        auto txIdVec = from_hex(txIdStr);

        if (txIdVec.size() >= 16)
        {
            res.emplace();
            std::copy_n(txIdVec.begin(), 16, res->begin());
        }
        else
        {
            BEAM_LOG_ERROR() << boost::format(kErrorTxIdParamInvalid) % txIdStr;
        }
        return res;
    }

    CoinIDList GetPreselectedCoinIDs(const po::variables_map& vm)
    {
        CoinIDList coinIDs;
        if (vm.count(cli::UTXO))
        {
            auto tempCoins = vm[cli::UTXO].as<vector<string>>();
            for (const auto& s : tempCoins)
            {
                auto csv = string_helpers::split(s, ',');
                for (const auto& v : csv)
                {
                    auto coinID = Coin::FromString(v);
                    if (coinID)
                    {
                        coinIDs.push_back(*coinID);
                    }
                }
            }
        }
        return coinIDs;
    }

    proto::FlyClient::NetworkStd::Ptr CreateNetwork(
        proto::FlyClient& fc, const po::variables_map& vm)
    {
        if (vm.count(cli::NODE_ADDR) == 0)
        {
            BEAM_LOG_ERROR() << kErrorNodeAddrNotSpecified;
            return nullptr;
        }

        string nodeURI = vm[cli::NODE_ADDR].as<string>();
        io::Address nodeAddress;
        if (!nodeAddress.resolve(nodeURI.c_str()))
        {
            BEAM_LOG_ERROR() << boost::format(kErrorNodeAddrUnresolved) % nodeURI;
            return nullptr;
        }

        auto nnet = make_shared<CliNodeConnection>(fc);
        nnet->m_Cfg.m_PollPeriod_ms =
            vm[cli::NODE_POLL_PERIOD].as<Nonnegative<uint32_t>>().value;
        if (nnet->m_Cfg.m_PollPeriod_ms)
        {
            BEAM_LOG_INFO() << boost::format(kNodePoolPeriod)
                       % nnet->m_Cfg.m_PollPeriod_ms;
            uint32_t timeout_ms =
                std::max(Rules::get().DA.Target_ms,
                         nnet->m_Cfg.m_PollPeriod_ms);
            if (timeout_ms != nnet->m_Cfg.m_PollPeriod_ms)
            {
                BEAM_LOG_INFO() << boost::format(kNodePoolPeriodRounded)
                           % timeout_ms;
            }
        }

        if (vm.count(cli::MINE_ONLINE))
            nnet->m_Cfg.m_PreferOnlineMining = vm[cli::MINE_ONLINE].as<bool>();

        uint32_t responceTime_ms =
            Rules::get().DA.Target_ms * wallet::kDefaultTxResponseTime;
        if (nnet->m_Cfg.m_PollPeriod_ms >= responceTime_ms)
        {
            BEAM_LOG_WARNING() << boost::format(kErrorNodePoolPeriodTooMuch)
                          % uint32_t(responceTime_ms / 3'600'000);
        }
        nnet->m_Cfg.m_vNodes.push_back(nodeAddress);
        nnet->m_Cfg.m_UseProxy = vm[cli::PROXY_USE].as<bool>();
        if (nnet->m_Cfg.m_UseProxy)
        {
            string proxyURI = vm[cli::PROXY_ADDRESS].as<string>();
            io::Address proxyAddr;
            if (!proxyAddr.resolve(proxyURI.c_str()))
            {
                BEAM_LOG_ERROR() << boost::format(kErrorNodeAddrUnresolved) % proxyURI;
                return nullptr;
            }
            nnet->m_Cfg.m_ProxyAddr = proxyAddr;
        }
        nnet->Connect();

        return nnet;
    }
}  // namespace wallet_cli
