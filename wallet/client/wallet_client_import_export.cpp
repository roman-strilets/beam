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

// Import and export of wallet data.

#include "wallet_client_internal.h"

namespace beam::wallet
{
    std::string WalletClient::exportOwnerKey(const beam::SecString& pass) const
    {
        // TODO: remove this, it is not thread safe
        Key::IPKdf::Ptr pOwner = m_walletDB->get_OwnerKdf();

        KeyString ks;
        ks.SetPassword(Blob(pass.data(), static_cast<uint32_t>(pass.size())));
        ks.m_sMeta = std::to_string(0);

        ks.ExportP(*pOwner);

        return ks.m_sRes;
    }

    void WalletClient::importRecovery(const std::string& path)
    {
        try
        {
            if (auto w = getWallet())
            {
                if (!m_walletDB->ImportRecovery(path, *w, *this))
                {
                    onWalletError(ErrorType::ImportRecoveryError);
                }
            }
            return;
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
        catch (...) 
        {
            BEAM_LOG_UNHANDLED_EXCEPTION();
        }
        onWalletError(ErrorType::ImportRecoveryError);
    }

    void WalletClient::importDataFromJson(const std::string& data)
    {
        auto isOk = storage::ImportDataFromJson(*m_walletDB, data.data(), data.size());

        onImportDataFromJson(isOk);
    }

    void WalletClient::exportDataToJson()
    {
        auto data = storage::ExportDataToJson(*m_walletDB);

        onExportDataToJson(data);
    }

    void WalletClient::exportTxHistoryToCsv()
    {
        auto data = ExportTxHistoryToCsv(*m_walletDB);
        onExportTxHistoryToCsv(data);

#ifdef BEAM_ASSET_SWAP_SUPPORT
        auto dataAssetsSwap = ExportAssetsSwapTxHistoryToCsv(*m_walletDB);
        onExportAssetsSwapTxHistoryToCsv(dataAssetsSwap);
#endif  // BEAM_ASSET_SWAP_SUPPORT
        auto dataContracts = ExportContractTxHistoryToCsv(*m_walletDB);
        onExportContractTxHistoryToCsv(dataContracts);
    }

    bool WalletClient::OnProgress(uint64_t done, uint64_t total)
    {
        onImportRecoveryProgress(done, total);
        return true;
    }
}
