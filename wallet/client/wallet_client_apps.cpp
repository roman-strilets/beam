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

// Apps list and shader calls.

#include "wallet_client_internal.h"

namespace beam::wallet
{
    IShadersManager::Ptr WalletClient::IWThread_createAppShaders(const std::string& appid, const std::string& appname, uint32_t privilegeLvl)
    {
        auto wallet = m_wallet.lock();
        auto network = m_nodeNetwork.lock();
        assert(wallet && network);
        return IShadersManager::CreateInstance(*wallet, appid, appname, privilegeLvl);
    }

    namespace
    {
        const char* getAppsUrl()
        {
            switch (Rules::get().m_Network)
            {
            case Rules::Network::testnet: return "https://apps-testnet.beam.mw/appslist.json";
            case Rules::Network::mainnet: return "https://apps.beam.mw/appslist.json";
            case Rules::Network::dappnet: return "https://apps-dappnet.beam.mw/app/appslist.json";
            case Rules::Network::masternet: return "http://3.19.32.148/app/appslist.json"; // none of the above
            default:
                break; // suppress warning
            }
            return "";
        }
    }

    void WalletClient::getAppsList(AppsListCallback&& callback)
    {
        io::Address address;

        auto url = getAppsUrl();

        static std::regex exrp("^(?:(http[s]?)://)?([^/]+)((/?.*/?)/(.*))$");
        std::smatch groups;
        std::string host;
        std::string path;
        std::string scheme;
        std::string myUrl(url);
        if (std::regex_match(myUrl, groups, exrp))
        {
            host.assign(groups[2].first, groups[2].second);
            path.assign(groups[3].first, groups[3].second);
            scheme.assign(groups[1].first, groups[1].second);
        }

        if (!address.resolve(host.c_str()))
        {
            BEAM_LOG_ERROR() << "Unable to resolve address: " << host;

            postFunctionToClientContext([cb = std::move(callback)]()
            {
                cb(false, "");
            });
            return;
        }
        if (address.port() == 0)
        {
            if (scheme == "http")
                address.port(80);
            else if (scheme == "https")
                address.port(443);
        }

        std::vector<HeaderPair> headers;
        headers.push_back({ "Content-Type", "application/json" });
        headers.push_back({ "Host", host.c_str() });

        HttpClient::Request request;

        request.address(address)
            //.connectTimeoutMsec(2000)
            .pathAndQuery(path.c_str())
            .headers(&headers.front())
            .numHeaders(headers.size())
            .method("GET");

        request.callback([this, cb = std::move(callback)](uint64_t id, const HttpMsgReader::Message& msg) -> bool
        {
            bool isOk = false;
            std::string response;
            if (msg.what == HttpMsgReader::http_message)
            {
                size_t sz = 0;
                const void* body = msg.msg->get_body(sz);
                isOk = sz > 0 && body;
                if (isOk)
                {
                    response = std::string(static_cast<const char*>(body), sz);
                }
            }
            else if (msg.what == HttpMsgReader::connection_error)
            {
                BEAM_LOG_ERROR() << "Failed to load application list: conection error(" << msg.connectionError << ")";
            }
            else if (msg.what == HttpMsgReader::message_corrupted)
            {
                BEAM_LOG_ERROR() << "Failed to load application list: corrupted message";
            }
            else
            {
                BEAM_LOG_ERROR() << "Failed to load application list reason: " << msg.what;
            }

            postFunctionToClientContext([isOk, res = std::move(response), callback = std::move(cb)]()
            {
                callback(isOk, res);
            });
            return false;
        });

        if (!m_httpClient)
        {
            m_httpClient = std::make_unique<HttpClient>(*m_reactor);
        }

        m_httpClient->send_request(request, scheme == "https");
    }

    void WalletClient::makeIWTCall(std::function<boost::any()>&& function, AsyncCallback<const boost::any&>&& resultCallback)
    {
        postFunctionToClientContext([result = function(), cb = std::move(resultCallback)]()
        {
            cb(result);
        });
    }

    void WalletClient::callShaderAndStartTx(beam::ByteBuffer&& shader, std::string&& args, CallShaderAndStartTxCallback&& cback)
    {
        auto smgr = _clientShaders.lock();
        if (!smgr)
        {
            assert(false);
            postFunctionToClientContext([cb = std::move(cback)]() {
                cb("unexpected: m_wallet is null", "", TxID());
            });
            return;
        }

        smgr->CallShaderAndStartTx(std::move(shader), std::move(args), args.empty() ? 0 : 1, 0, 0,
            [this, cb = std::move(cback), shaders = _clientShaders]
            (const boost::optional<TxID>& txid, boost::optional<std::string>&& result, boost::optional<std::string>&& error) {
                auto smgr = _clientShaders.lock();
                if (!smgr)
                {
                    BEAM_LOG_WARNING () << "onShaderDone but empty manager. This can happen if node changed.";
                    return;
                }

                if (!cb)
                {
                    assert(false);
                    BEAM_LOG_ERROR() << "onShaderDone but empty callback";
                    return;
                }

                postFunctionToClientContext(
                    [txid, res = std::move(result), err = std::move(error), cb = std::move(cb)] () {
                        cb(err ? *err : "", res ? *res : "", txid ? *txid: TxID());
                    });
        });
    }

    void WalletClient::callShaderAndStartTx(std::string&& shaderFile, std::string&& args, CallShaderAndStartTxCallback&& cback)
    {
        try
        {
            callShaderAndStartTx(fsutils::fread(shaderFile), std::move(args), std::move(cback));
        }
        catch (std::runtime_error& err)
        {
            postFunctionToClientContext([errorMsg = std::string(err.what()), cb = std::move(cback)]() {
                cb(errorMsg, "", TxID());
            });
        }
    }

    void WalletClient::callShader(beam::ByteBuffer&& shader, std::string&& args, CallShaderCallback&& cback)
    {
        auto smgr = _clientShaders.lock();
        if (!smgr)
        {
            assert(false);
            postFunctionToClientContext([cb = std::move(cback)]() {
                cb("unexpected: m_wallet is null", "", beam::ByteBuffer());
            });
            return;
        }

        smgr->CallShader(std::move(shader), std::move(args), args.empty() ? 0 : 1, 0, 0,
            [this, cb = std::move(cback), shaders = _clientShaders]
            (boost::optional<ByteBuffer>&& data, boost::optional<std::string>&& output, boost::optional<std::string>&& error) {
                auto smgr = _clientShaders.lock();
                if (!smgr)
                {
                    BEAM_LOG_WARNING() << "onShaderDone but empty manager. This can happen if node changed.";
                    return;
                }

                if (!cb)
                {
                    assert(false);
                    BEAM_LOG_ERROR() << "onShaderDone but empty callback";
                    return;
                }

                postFunctionToClientContext(
                    [data = std::move(data), res = std::move(output), err = std::move(error), cb = std::move(cb)] () {
                        cb(err ? *err : "", res ? *res : "", data ? *data : beam::ByteBuffer());
                    });
        });
    }

    void WalletClient::callShader(std::string&& shaderFile, std::string&& args, CallShaderCallback&& cback)
    {
        try
        {
            callShader(fsutils::fread(shaderFile), std::move(args), std::move(cback));
        }
        catch (std::runtime_error& err)
        {
            postFunctionToClientContext([errorMsg = std::string(err.what()), cb = std::move(cback)]() {
                cb(errorMsg, "", {});
            });
        }
    }

    void WalletClient::processShaderTxData(beam::ByteBuffer&& data, ProcessShaderTxDataCallback&& cback)
    {
        auto smgr = _clientShaders.lock();
        if (!smgr)
        {
            assert(false);
            postFunctionToClientContext([cb = std::move(cback)]() {
                cb("unexpected: m_wallet is null", TxID());
            });
            return;
        }

        smgr->ProcessTxData(data,
            [this, cb = std::move(cback), shaders = _clientShaders] (const boost::optional<TxID>& txid, boost::optional<std::string>&& error) {
                auto smgr = _clientShaders.lock();
                if (!smgr)
                {
                    BEAM_LOG_WARNING() << "onShaderDone but empty manager. This can happen if node changed.";
                    return;
                }

                if (!cb)
                {
                    assert(false);
                    BEAM_LOG_ERROR() << "onShaderDone but empty callback";
                    return;
                }

                postFunctionToClientContext(
                    [txid, err = std::move(error), cb = std::move(cb)] () {
                        cb(err ? *err : "", txid ? *txid : TxID());
                    });
        });
    }
}
