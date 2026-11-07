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
#include "v7_0_api.h"
#include "version.h"
#include "bvm/bvm2.h"

namespace beam::wallet
{
    namespace
    {
        struct MyProcessor : bvm2::ProcessorManager
        {
            using ProcessorManager::DeriveKeyPreimage;
        };

        void GetMessageHash(ECC::Hash::Value& hv, const std::string& message)
        {
            std::stringstream ss;
            ss << "beam.signed.message"
                << message.size()
                << message;

            ECC::Hash::Processor()
                << ss.str()
                >> hv;
        }
    }

    void V70Api::onHandleSignMessage(const JsonRpcId& id, SignMessage&& req)
    {
        SignMessage::Response resp;
        ECC::Hash::Value hv;
        MyProcessor::DeriveKeyPreimage(hv, Blob(req.keyMaterial));
        auto db = getWalletDB();
        auto pKdf = db->get_MasterKdf();
        ECC::Scalar::Native sk;
        pKdf->DeriveKey(sk, hv);

        GetMessageHash(hv, req.message);

        ECC::Signature sig;
        sig.Sign(hv, sk);

        Serializer s;
        s & sig;

        resp.signature = to_hex(s.buffer().first, s.buffer().second);
        doResponse(id, resp);
    }

    void V70Api::onHandleVerifySignature(const JsonRpcId& id, VerifySignature&& req)
    {
        VerifySignature::Response resp;
        ECC::Hash::Value hv;
        GetMessageHash(hv, req.message);

        Deserializer d;
        ECC::Signature sig;
        d.reset(req.signature);
        d& sig;
        resp.result = sig.IsValid(hv, req.publicKey);
        doResponse(id, resp);
    }
}
