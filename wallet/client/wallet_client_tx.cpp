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

// Transactions, coin selection and slatepacks.

#include "wallet_client_internal.h"

namespace beam::wallet
{
    namespace
    {
        constexpr int kOneTimeLoadTxCount = 100;
    }

    void WalletClient::sendMoney(const WalletID& receiver, const std::string& comment, Amount amount, Amount fee)
    {
        sendMoneyInternal(nullptr, receiver, comment, amount, fee);
    }

    void WalletClient::sendMoney(const WalletID& sender, const WalletID& receiver, const std::string& comment, Amount amount, Amount fee)
    {
        sendMoneyInternal(&sender, receiver, comment, amount, fee);
    }

    void WalletClient::sendMoneyInternal(const WalletID* pSender, const WalletID& receiver, const std::string& comment, Amount amount, Amount fee)
    {
        try
        {
            assert(!m_wallet.expired());
            auto s = m_wallet.lock();
            if (s)
            {
                ByteBuffer message(comment.begin(), comment.end());
                TxParameters txParameters = CreateSimpleTransactionParameters()
                    .SetParameter(TxParameterID::PeerAddr, receiver)
                    .SetParameter(TxParameterID::Amount, amount)
                    .SetParameter(TxParameterID::Fee, fee)
                    .SetParameter(TxParameterID::Message, message);
                
                if (pSender)
                    txParameters.SetParameter(TxParameterID::MyAddr, *pSender);

                s->StartTransaction(txParameters);
            }

            onSendMoneyVerified();
        }
        catch (const CannotGenerateSecretException&)
        {
            onNewAddressFailed();
            return;
        }
        catch (const ReceiverAddressExpiredException&)
        {
            onCantSendToExpired();
            return;
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
        catch (...) {
            BEAM_LOG_UNHANDLED_EXCEPTION();
        }
    }

    void WalletClient::startTransaction(TxParameters&& parameters)
    {
        try
        {
            assert(!m_wallet.expired());
            auto s = m_wallet.lock();
            if (s)
                s->StartTransaction(parameters);

            onSendMoneyVerified();
        }
        catch (const CannotGenerateSecretException&)
        {
            onNewAddressFailed();
            return;
        }
        catch (const ReceiverAddressExpiredException&)
        {
            onCantSendToExpired();
            return;
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
        catch (...) {
            BEAM_LOG_UNHANDLED_EXCEPTION();
        }
    }

    void WalletClient::calcChange(Amount amount, Amount fee, Asset::ID assetId)
    {
        CoinsSelectionInfo csi;
        csi.m_requestedSum = amount;
        csi.m_assetID = assetId;
        csi.m_explicitFee = fee;
        csi.Calculate(m_currentHeight, m_walletDB, false);
        onChangeCalculated(csi.m_changeAsset, csi.m_changeBeam, assetId);
    }

    void WalletClient::selectCoins(Amount requested, Amount beforehandMinFee, Asset::ID assetId, bool isShielded /* = false */)
    {
        CoinsSelectionInfo csi;
        csi.m_requestedSum = requested;
        csi.m_assetID = assetId;
        csi.m_explicitFee = beforehandMinFee;
        csi.Calculate(m_currentHeight, m_walletDB, isShielded);
        onCoinsSelected(csi);
    }

     void WalletClient::selectCoins(Amount amount, Amount beforehandMinFee, Asset::ID assetId, bool isShielded, AsyncCallback<const CoinsSelectionInfo&>&& callback)
     {
        CoinsSelectionInfo csi;
        csi.m_requestedSum = amount;
        csi.m_assetID = assetId;
        csi.m_explicitFee = beforehandMinFee;
        csi.Calculate(m_currentHeight, m_walletDB, isShielded);
        postFunctionToClientContext([csi, cback = std::move(callback)]()
        {
            cback(csi);
        });
     }

    void WalletClient::getTransactions()
    {
        onTransactionChanged(ChangeAction::Reset, m_walletDB->getTxHistory(wallet::TxType::ALL));
    }

    void WalletClient::getTransactions(AsyncCallback<const std::vector<TxDescription>&>&& callback)
    {
        postFunctionToClientContext([res = m_walletDB->getTxHistory(wallet::TxType::ALL), cb = std::move(callback)]()
        {
            cb(res);
        });
    }

    void WalletClient::getTransactionsSmoothly()
    {
        //auto txCount = m_walletDB->getTxCount(wallet::TxType::ALL);
        //if (txCount > kOneTimeLoadTxCount)
        //{
        //    onTransactionChanged(ChangeAction::Reset, vector<wallet::TxDescription>());

        //    auto iterationsCount = txCount / kOneTimeLoadTxCount + (txCount % kOneTimeLoadTxCount ? 1 : 0);
        //    for(int i = 0; i < iterationsCount; ++i) 
        //        onTransactionChanged(
        //            ChangeAction::Added, 
        //            m_walletDB->getTxHistory(wallet::TxType::ALL,
        //                                     i * kOneTimeLoadTxCount,
        //                                     i * kOneTimeLoadTxCount + kOneTimeLoadTxCount));
        //} 
        //else
        //{
            getTransactions();
        //}
    }

    void WalletClient::getAllUtxosStatus()
    {
        onCoinsChanged(ChangeAction::Reset, m_walletDB->getAllNormalCoins());
        onShieldedCoinsChanged(ChangeAction::Reset, m_walletDB->getAllShieldedCoins());
    }

    void WalletClient::cancelTx(const TxID& id)
    {
        auto w = m_wallet.lock();
        if (w)
        {
            w->CancelTransaction(id);
        }
    }

    void WalletClient::deleteTx(const TxID& id)
    {
        auto w = m_wallet.lock();
        if (w)
        {
            w->DeleteTransaction(id);
        }
    }

    void WalletClient::getCoinsByTx(const TxID& id)
    {
        onCoinsByTx(m_walletDB->getCoinsByTx(id));
    }

    void WalletClient::exportPaymentProof(const TxID& id)
    {
        onPaymentProofExported(id, storage::ExportPaymentProof(*m_walletDB, id));
    }

    void WalletClient::importSlatepack(const std::string& text)
    {
        // Runs on the wallet thread; decrypts a pasted Slatepack and previews it. The tx does
        // not proceed until the user confirms via commitSlatepack.
        slatepack::Error error = slatepack::Error::None;
        SlatepackEndpoint::ImportInfo info;
        auto ep = m_slatepackEndpoint.lock();
        const bool ok = ep && ep->Preview(text, error, info);
        if (!ep)
            error = slatepack::Error::ReadOnlyWallet; // no manual transport in this session
        onSlatepackImportResult(ok, error, info);
    }

    void WalletClient::commitSlatepack(const std::string& txId)
    {
        slatepack::Error error = slatepack::Error::None;
        if (auto ep = m_slatepackEndpoint.lock(); ep && !ep->Commit(txId, error))
            BEAM_LOG_WARNING() << "Slatepack commit failed: " << slatepack::ErrorToString(error);
    }

    void WalletClient::cancelSlatepack(const std::string& txId)
    {
        if (auto ep = m_slatepackEndpoint.lock())
            ep->CancelPending(txId);
    }
}
