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

// Transaction details, payment proofs, key export and wallet data import/export.

#include "cli_internal.h"

namespace wallet_cli
{
namespace
{
    std::string TxDetailsInfo(const IWalletDB::Ptr& walletDB, const TxID& txID)
    {
        auto tx = walletDB->getTx(txID);
        if (!tx)
        {
            BEAM_LOG_WARNING() << "Can't get transaction details";
            return "";
        }
        const TxDescription& desc = *tx;
        TxAddressType addressType = GetAddressType(desc);

        if (addressType == TxAddressType::AtomicSwap)
        {
            return "";
        }

        bool hasNoPeerId = desc.m_sender && (addressType == TxAddressType::PublicOffline || addressType == TxAddressType::MaxPrivacy);

        auto senderIdentity = desc.getSenderEndpoint();
        auto receiverIdentity = desc.getReceiverEndpoint();
        bool showIdentity = !senderIdentity.empty() && !receiverIdentity.empty();

        auto sender = desc.getSender();
        auto receiver = desc.getReceiver();
        if (desc.m_txType == wallet::TxType::PushTransaction && !desc.m_sender)
        {
            sender = "shielded pool";
        }
        else if (desc.m_txType == wallet::TxType::Contract)
        {
            sender = receiver = "n/a";
        }

        std::ostringstream s;
        s << "Type:              " << desc.getTxTypeString() << '\n';
        s << "Sender:            " << sender << std::endl;
        if (showIdentity)
        {
            s << "Sender wallet's signature:   " << senderIdentity << std::endl;
        }

        s << "Receiver:          " << (hasNoPeerId ? desc.getToken() : receiver) << std::endl;
        if (showIdentity)
        {
            s << "Receiver wallet's signature: " << receiverIdentity << std::endl;
        }

        if (desc.m_assetId == Asset::s_BeamID)
        {
            s << "Amount:            " << PrintableAmount(desc.m_amount) << std::endl;
        }
        else
        {
            const auto info = walletDB->findAsset(desc.m_assetId);
            std::string unitName = kAmountASSET;
            std::string nthName  = kAmountAGROTH;
            if (info.is_initialized())
            {
                const auto &meta = WalletAssetMeta(*info);
                unitName = meta.GetUnitName();
                trimAssetName(unitName, 28);
                nthName  = meta.GetNthUnitName();
                trimAssetName(nthName, 28);
            }
            s << "Amount:            "
              << PrintableAmount(desc.m_amount, false, desc.m_assetId, unitName, nthName) << std::endl;
        }

        s << "KernelID:          " << beam::to_string(desc.m_kernelID) << std::endl;

        return s.str();

    }

    bool LoadDataToImport(const std::string& path, ByteBuffer& data)
    {
        FStream f;
        if (f.Open(path.c_str(), true))
        {
            size_t size = static_cast<size_t>(f.get_Remaining());
            if (size > 0)
            {
                data.resize(size);
                return f.read(data.data(), data.size()) == size;
            }
        }
        return false;
    }

    bool SaveExportedData(const ByteBuffer& data, const std::string& path)
    {
        const auto timestampedPath = TimestampFile(path);

        FStream f;
        if (f.Open(timestampedPath.c_str(), false) && f.write(data.data(), data.size()) == data.size())
        {
            BEAM_LOG_INFO() << kDataExportedMessage;
            return true;
        }

        BEAM_LOG_ERROR() << kErrorExportDataFail;
        return false;
    }
}  // namespace

    int TxDetails(const po::variables_map& vm)
    {
        auto txId = GetTxID(vm);
        if (!txId)
        {
            return -1;
        }
        auto walletDB = OpenDataBase(vm);
        auto tx = walletDB->getTx(*txId);
        if (!tx)
        {
            BEAM_LOG_ERROR() << boost::format(kErrorTxWithIdNotFound) % vm[cli::TX_ID].as<string>();
            return -1;
        }

        const auto txdetails = TxDetailsInfo(walletDB, *txId);
        if (txdetails.empty()) {
            // TxDetailsInfo already printed an error
            return -1;
        }

        const auto token = tx->getToken();

        cout
            << "\n"
            << boost::format(kTxDetailsFormat) % txdetails % interpretStatusCliImpl(*tx)
            << (tx->m_status == TxStatus::Failed ? boost::format(kTxDetailsFailReason) % GetFailureMessage(tx->m_failureReason) : boost::format(""))
            << (!token.empty() ? "\nAddress:           " : "") << token;

        if (vm.count(cli::UTXO_LIST))
        {
            cout << "\n\n";
            ShowAssetCoins(walletDB, Zero, txId);
        }

        return 0;
    }

    int ExportPaymentProof(const po::variables_map& vm)
    {
        auto txId = GetTxID(vm);
        if (!txId)
        {
            return -1;
        }

        auto walletDB = OpenDataBase(vm);
        auto tx = walletDB->getTx(*txId);
        if (!tx)
        {
            BEAM_LOG_ERROR() << kErrorPpExportFailed;
            return -1;
        }
        if (!tx->m_sender || tx->m_selfTx)
        {
            BEAM_LOG_ERROR() << kErrorPpCannotExportForReceiver;
            return -1;
        }
        if (tx->m_status != TxStatus::Completed)
        {
            BEAM_LOG_ERROR() << kErrorPpExportFailedTxNotCompleted;
            return -1;
        }

        auto res = storage::ExportPaymentProof(*walletDB, *txId);
        if (!res.empty())
        {
            std::string sTxt;
            sTxt.resize(res.size() * 2);

            beam::to_hex(&sTxt.front(), res.data(), res.size());
            std::cout << boost::format(kPpExportedFrom) % sTxt;
        }

        return 0;
    }

    int ExportSlatepack(const po::variables_map& vm)
    {
        auto txId = GetTxID(vm);
        if (!txId)
        {
            return -1;
        }

        auto walletDB = OpenDataBase(vm);
        if (!walletDB->getTx(*txId))
        {
            BEAM_LOG_ERROR() << boost::format(kErrorTxWithIdNotFound) % vm[cli::TX_ID].as<string>();
            return -1;
        }

        // Read the armored Slatepack the send/reply step stored on the tx (see SlatepackEndpoint).
        std::string armored;
        if (!storage::getTxParameter(*walletDB, *txId, TxParameterID::SlatepackOutgoing, armored) || armored.empty())
        {
            BEAM_LOG_ERROR() << "No stored Slatepack for this transaction (only manually-transported sends keep one, and it is dropped on cancel).";
            return -1;
        }

        OutputSlatepack(vm, *txId, armored);
        return 0;
    }

    int VerifyPaymentProof(const po::variables_map& vm)
    {
        const auto& pprofData = vm[cli::PAYMENT_PROOF_DATA];
        if (pprofData.empty())
        {
            throw std::runtime_error(kErrorPpNotProvided);
        }
        ByteBuffer buf = from_hex(pprofData.as<string>());

        bool isValid = false;
        auto walletDB = OpenDataBase(vm);

        try
        {
            isValid = storage::VerifyPaymentProof(buf, *walletDB);
        }
        catch (const std::runtime_error & e)
        {
            BEAM_LOG_ERROR() << e.what();
            throw std::runtime_error(kErrorPpInvalid);
        }

        if (!isValid)
            throw std::runtime_error(kErrorPpInvalid);

        return 0;
    }

    int ExportMinerKey(const po::variables_map& vm)
    {
        if (vm.count(cli::KEY_SUBKEY) == 0)
        {
            cout << kErrorSubkeyNotSpecified << endl;
            return -1;
        }
        auto pass = GetPassword(vm);
        auto walletDB = OpenDataBase(vm, pass);

        uint32_t subKey = vm[cli::KEY_SUBKEY].as<Positive<uint32_t>>().value;

        Key::IKdf::Ptr pMaster = walletDB->get_MasterKdf();
        if (!pMaster)
        {
            cout << "Miner key not accessible" << endl;
            return -1;
        }

        Key::IKdf::Ptr pKey = MasterKey::get_Child(*pMaster, subKey);

        KeyString ks;
        ks.SetPassword(Blob(pass.data(), static_cast<uint32_t>(pass.size())));
        ks.m_sMeta = std::to_string(subKey);

        ks.ExportS(*pKey);
        cout << boost::format(kSubKeyInfo) % subKey % ks.m_sRes << std::endl;

        if (Rules::Consensus::Pbft == Rules::get().m_Consensus)
        {
            ECC::Scalar::Native sk;
            PeerID addr;
            Block::Pbft::DeriveValidatorAddress(*pKey, addr, sk);

            std::cout << "Validator address: " << addr.str() << std::endl;
        }

        return 0;
    }

    int ExportOwnerKey(const po::variables_map& vm)
    {
        auto pass = GetPassword(vm);
        auto walletDB = OpenDataBase(vm, pass);

        Key::IPKdf::Ptr pKey = walletDB->get_OwnerKdf();

        KeyString ks;
        ks.SetPassword(Blob(pass.data(), static_cast<uint32_t>(pass.size())));
        ks.m_sMeta = std::to_string(0);

        ks.ExportP(*pKey);
        cout << boost::format(kOwnerKeyInfo) % ks.m_sRes << std::endl;

        return 0;
    }

    int ExportWalletData(const po::variables_map& vm)
    {
        auto walletDB = OpenDataBase(vm);
        auto s = storage::ExportDataToJson(*walletDB);
        return SaveExportedData(ByteBuffer(s.begin(), s.end()), vm[cli::IMPORT_EXPORT_PATH].as<string>()) ? 0 : -1;
    }

    int ImportWalletData(const po::variables_map& vm)
    {
        ByteBuffer buffer;
        if (vm[cli::IMPORT_EXPORT_PATH].defaulted())
        {
            BEAM_LOG_ERROR() << kErrorFileLocationParamReqired;
            return -1;
        }

        auto path = vm[cli::IMPORT_EXPORT_PATH].as<string>();
        if (path.empty() || !LoadDataToImport(path, buffer))
        {
            BEAM_LOG_ERROR() << boost::format(kErrorImportPathInvalid)
                        % path;
            return -1;
        }

        auto walletDB = OpenDataBase(vm);
        const char* p = (char*)(&buffer[0]);
        return storage::ImportDataFromJson(*walletDB, p, buffer.size()) ? 0 : -1;
    }
}  // namespace wallet_cli
