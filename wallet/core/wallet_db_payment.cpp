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

#include "wallet_db_common.h"

namespace beam::wallet
{
    namespace storage
    {
        std::pair<std::string, std::string> getAssetNames(beam::Asset::ID assetID, const IWalletDB& wdb)
        {
            std::string uname, nthname;

            if (assetID)
            {
                uname = kAmountASSET;
                nthname = kAmountAGROTH;

                if (auto asset = wdb.findAsset(assetID))
                {
                    WalletAssetMeta meta(*asset);
                    if (meta.isStd_v5_0())
                    {
                        uname = meta.GetUnitName();
                        nthname = meta.GetNthUnitName();
                    }
                }
            }

            return std::make_pair(uname, nthname);
        }

        PaymentInfo::PaymentInfo()
        {
            Reset();
        }

        bool PaymentInfo::IsValid() const
        {
            wallet::PaymentConfirmation pc;
            pc.m_Value = m_Amount;
            pc.m_KernelID = m_KernelID;
            pc.m_Signature = m_Signature;
            pc.m_Sender = m_Sender;
            pc.m_AssetID = m_AssetID;
            return pc.IsValid(m_Receiver);
        }

        std::string PaymentInfo::to_string(const IWalletDB& wdb) const
        {
            const auto names = getAssetNames(m_AssetID, wdb);

            std::ostringstream s;
            s
                << "Sender:            " << beam::to_string(m_Sender) << "\n"
                << "Receiver:          " << beam::to_string(m_Receiver) << "\n"
                << "Amount:            " << PrintableAmount(m_Amount, false, m_AssetID, names.first, names.second) << "\n"
                << "KernelID:          " << beam::to_string(m_KernelID) << "\n";
            return s.str();
        }

        void PaymentInfo::Reset()
        {
            ZeroObject(*this);
        }

        PaymentInfo PaymentInfo::FromByteBuffer(const ByteBuffer& data)
        {
            PaymentInfo pi;
            if (!data.empty())
            {
                Deserializer der;
                der.reset(data);
                der & pi;
                if (der.bytes_left() > 0)
                {
                    throw std::runtime_error("Invalid data buffer");
                }
            }
            return pi;
        }

        bool ShieldedPaymentInfo::IsValid() const
        {
            ShieldedTxo::Voucher voucher;
            voucher.m_SharedSecret = m_VoucherSharedSecret;
            voucher.m_Signature = m_VoucherSignature;
            voucher.m_Ticket = m_TxoTicket;

            if (!voucher.IsValid(m_Receiver))
            {
                return false;
            }

            return true;
        }

        std::string ShieldedPaymentInfo::to_string(const IWalletDB& wdb) const
        {
            const auto names = getAssetNames(m_AssetID, wdb);

            std::ostringstream s;
            s
                << "Sender:            " << beam::to_string(m_Sender) << "\n"
                << "Receiver:          " << beam::to_string(m_Receiver) << "\n"
                << "Amount:            " << PrintableAmount(m_Amount, false, m_AssetID, names.first, names.second) << "\n"
                << "KernelID:          " << beam::to_string(m_KernelID) << "\n";
            return s.str();
        }

        ShieldedPaymentInfo ShieldedPaymentInfo::FromByteBuffer(const ByteBuffer& data)
        {
            ShieldedPaymentInfo pi;
            if (!data.empty())
            {
                Deserializer der;
                der.reset(data);
                der & pi;
                if (der.bytes_left() > 0)
                {
                    throw std::runtime_error("Invalid data buffer");
                }
                pi.RestoreKernelID();
            }
            return pi;
        }

        void ShieldedPaymentInfo::RestoreKernelID()
        {
            // restore kernels
            TxKernelStd kernel;
            kernel.m_Fee = m_Fee;
            kernel.m_Height = m_Height;
            kernel.m_CanEmbed = false;
            kernel.m_Commitment = m_Commitment;

            auto nestedKernel = std::make_unique<TxKernelShieldedOutput>();
            nestedKernel->m_CanEmbed = true;
            nestedKernel->m_Txo.m_Ticket = m_TxoTicket;

            ECC::Oracle oracle;
            oracle << nestedKernel->get_Msg();
            ShieldedTxo::Data::OutputParams outputParams;
            outputParams.m_Value = m_Amount;
            outputParams.m_AssetID = m_AssetID;
            outputParams.m_User.m_Sender = m_Sender;
            outputParams.m_User.m_pMessage[0] = m_pMessage[0];
            outputParams.m_User.m_pMessage[1] = m_pMessage[1];
            outputParams.Restore_kG(m_VoucherSharedSecret);

            {
                Asset::Proof::Params::Override po(Asset::Proof::Params::Make(0, m_HideAssetAlways)); // KernelID should not depend on specific Asset::Proof structure, in particular the selected window.
                // We should only know if it's present
                
                outputParams.Generate(nestedKernel->m_Txo, m_VoucherSharedSecret, m_Height.m_Min, oracle);
            }

            kernel.m_vNested.push_back(std::move(nestedKernel));

            m_KernelID = kernel.get_ID();
        }

        namespace
        {
            ByteBuffer ExportSimplePaymentProof(const IWalletDB& walletDB, const TxID& txID)
            {
                PaymentInfo pi;
                uint64_t nAddrOwnID = 0;

                bool bSuccess =
                    walletDB.get_EffectiveEndpointPeer(txID, kDefaultSubTxID, pi.m_Receiver) &&
                    walletDB.get_EffectiveEndpointMy(txID, kDefaultSubTxID, pi.m_Sender) &&
                    storage::getTxParameter(walletDB, txID, TxParameterID::KernelID, pi.m_KernelID) &&
                    storage::getTxParameter(walletDB, txID, TxParameterID::Amount, pi.m_Amount) &&
                    storage::getTxParameter(walletDB, txID, TxParameterID::PaymentConfirmation, pi.m_Signature) &&
                    storage::getTxParameter(walletDB, txID, TxParameterID::MyAddressID, nAddrOwnID);

                // There might be old transactions without asset id
                if (!storage::getTxParameter(walletDB, txID, TxParameterID::AssetID, pi.m_AssetID))
                {
                    pi.m_AssetID = Asset::s_BeamID;
                    BEAM_LOG_DEBUG() << "ExportPaymentProof, transaction " << txID << " is without assetId, defaulting to 0 (BEAM)";
                }

                if (bSuccess)
                {
                    std::cout << "Payment tx details:\n" << pi.to_string(walletDB) << "\n";
                    std::cout << "Sender address own ID: " << nAddrOwnID << std::endl;

                    Serializer ser;
                    ser & pi;

                    auto res = ser.buffer();
                    return ByteBuffer(res.first, res.first + res.second);
                }
                else
                {
                    BEAM_LOG_WARNING() << "No payment confirmation for the specified transaction.";
                }

                return ByteBuffer();
            }

            ByteBuffer ExportShieldedPaymentProof(const IWalletDB& walletDB, const TxID& txID)
            {
                ShieldedPaymentInfo pi;
                TxKernel::Ptr rootKernel;
                ShieldedTxo::Voucher voucher;
                bool bSuccess =
                    storage::getTxParameter(walletDB, txID, TxParameterID::PeerEndpoint, pi.m_Receiver) &&
                    storage::getTxParameter(walletDB, txID, TxParameterID::MyEndpoint, pi.m_Sender) &&
                    storage::getTxParameter(walletDB, txID, TxParameterID::Voucher, voucher) &&
                    storage::getTxParameter(walletDB, txID, TxParameterID::Kernel, rootKernel) &&
                    storage::getTxParameter(walletDB, txID, TxParameterID::Amount, pi.m_Amount) &&
                    rootKernel->get_Subtype() == TxKernel::Subtype::Std &&
                    rootKernel->m_vNested.size() == 1 &&
                    rootKernel->m_vNested[0]->get_Subtype() == TxKernel::Subtype::ShieldedOutput;

                storage::getTxParameter(walletDB, txID, TxParameterID::AssetID, pi.m_AssetID);

                if (bSuccess)
                {
                    auto& stdKernel = rootKernel->CastTo_Std();

                    pi.m_Commitment = stdKernel.m_Commitment;
                    pi.m_Fee = stdKernel.m_Fee;
                    pi.m_Height = stdKernel.m_Height;
                    pi.m_Signature = stdKernel.m_Signature;

                    auto& nestedKernel = rootKernel->m_vNested[0]->CastTo_ShieldedOutput();
                    pi.m_TxoTicket = nestedKernel.m_Txo.m_Ticket;

                    pi.m_VoucherSharedSecret = voucher.m_SharedSecret;
                    pi.m_VoucherSignature = voucher.m_Signature;
                    {
                        ECC::Oracle oracle;
                        oracle << nestedKernel.get_Msg();
                        ShieldedTxo::Data::OutputParams outputParams;
                        outputParams.Recover(nestedKernel.m_Txo, voucher.m_SharedSecret, rootKernel->m_Height.m_Min, oracle);
                        pi.m_pMessage[0] = outputParams.m_User.m_pMessage[0];
                        pi.m_pMessage[1] = outputParams.m_User.m_pMessage[1];
                    }
                    pi.m_HideAssetAlways = ((pi.m_AssetID == Asset::s_InvalidID) && nestedKernel.m_Txo.m_pAsset);
                    pi.RestoreKernelID();
                    std::cout << "Payment tx details:\n" << pi.to_string(walletDB) << std::endl;

                    Serializer ser;
                    ser & pi;

                    auto res = ser.buffer();
                    return ByteBuffer(res.first, res.first + res.second);
                }
                else
                {
                    BEAM_LOG_WARNING() << "No payment confirmation for the specified transaction.";
                }

                return ByteBuffer();
            }

            template<typename T>
            bool VerifyPaymentProofImpl(const ByteBuffer& data, const IWalletDB& wdb)
            {
                try
                {
                    T pi = T::FromByteBuffer(data);

                    if (!pi.IsValid())
                    {
                        return false;
                    }

                    std::cout << "Payment tx details:\n" << pi.to_string(wdb) << "Verified." << std::endl;

                    return true;
                }
                catch (...)
                {

                }
                return false;
            }
        }

        ByteBuffer ExportPaymentProof(const IWalletDB& walletDB, const TxID& txID)
        {
            TxType txType = TxType::Simple;
            if (!storage::getTxParameter(walletDB, txID, TxParameterID::TransactionType, txType))
            {
                BEAM_LOG_ERROR() << "There is no transaction with given ID.";
                return {};
            }
            switch (txType)
            {
            case TxType::Simple:
                return ExportSimplePaymentProof(walletDB, txID);
            case TxType::PushTransaction:
                return ExportShieldedPaymentProof(walletDB, txID);
            default:
                BEAM_LOG_WARNING() << "Cannot provide payment proof for this transaction type: " << (int)txType;
            }
            
            return ByteBuffer();
        }

        bool VerifyPaymentProof(const ByteBuffer& data, const IWalletDB& wdb)
        {
            return VerifyPaymentProofImpl<PaymentInfo>(data, wdb)
                || VerifyPaymentProofImpl<ShieldedPaymentInfo>(data, wdb);
        }
    }
}
