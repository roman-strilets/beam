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
        using nlohmann::json;

        namespace
        {
            namespace Fields
            {
                const string OwnAddresses = "OwnAddresses";
                const string Contacts = "Contacts";
                const string TransactionParameters = "TransactionParameters";
                const string Category = "Category";
                const string BbsAddr = "WalletID";
                const string EndpointHex = "Identity";
                const string Index = "Index";
                const string Label = "Label";
                const string CreationTime = "CreationTime";
                const string Duration = "Duration";
                const string TransactionId = "TransactionId";
                const string SubTransactionId = "SubTransactionId";
                const string ParameterId = "ParameterId";
                const string Value = "Value";
                const string Address = "Address";
            }
            
            bool ImportAddressesFromJson(IWalletDB& db, const json& obj, const string& nodeName)
            {
                if (obj.find(nodeName) == obj.end())
                {
                    return true;
                }

                for (const auto& jsonAddress : obj[nodeName])
                {
                    WalletAddress address;
                    if (address.m_BbsAddr.FromHex(jsonAddress[Fields::BbsAddr]))
                    {
                        address.m_OwnID = jsonAddress[Fields::Index];
                        if (!address.isOwn() || db.ValidateSbbsWalletID(address.m_BbsAddr, address.m_OwnID))
                        {
                            //{ "SubIndex", 0 },
                            address.m_label = jsonAddress[Fields::Label].get<std::string>();
                            auto creationTime = jsonAddress[Fields::CreationTime];
                            auto currentTime = beam::getTimestamp();
                            if (currentTime >= creationTime)
                            {
                                address.m_createTime = creationTime;
                                address.m_duration = jsonAddress[Fields::Duration];
                            }
                            else
                            {
                                address.m_createTime = currentTime;
                                address.m_duration = WalletAddress::AddressExpirationAuto;
                            }
                            if (auto it = jsonAddress.find(Fields::Category); it != jsonAddress.end()) // for compatibility with older export
                            {
                                address.m_category = it->get<std::string>();
                            }
                            if (auto it = jsonAddress.find(Fields::EndpointHex); it != jsonAddress.end())
                            {
                                bool isValid = false;
                                auto buf = from_hex(*it, &isValid);
                                if (isValid)
                                {
                                    address.m_Endpoint = Blob(buf);
                                }
                            }
                            if (auto it = jsonAddress.find(Fields::Address); it != jsonAddress.end())
                            {
                                address.m_Token = it->get<std::string>();
                            }
                            db.saveAddress(address);

                            BEAM_LOG_INFO() << "The address [" << jsonAddress[Fields::BbsAddr] << "] has been successfully imported.";
                            continue;
                        }
                    }

                    BEAM_LOG_INFO() << "The address [" << jsonAddress[Fields::BbsAddr] << "] has NOT been imported. Wrong address.";
                    return false;
                }
                return true;
            }

            bool ImportAddressesFromJson(IWalletDB& db, const json& obj)
            {
                return ImportAddressesFromJson(db, obj, Fields::OwnAddresses);
            }

            bool ImportContactsFromJson(IWalletDB& db, const json& obj)
            {
                return ImportAddressesFromJson(db, obj, Fields::Contacts);
            }

            bool ImportTransactionsFromJson(IWalletDB& db, const json& obj)
            {
                if (obj.find(Fields::TransactionParameters) == obj.end())
                {
                    return true;
                }

                std::unordered_map<
                    TxID,
                    std::unordered_map<
                    TxParameterID,
                    TxParameter>,
                    ArrayHash
                > importedTransactionsMap;
                for (const auto& jsonTxParameter : obj[Fields::TransactionParameters])
                {
                    TxParameter txParameter;
                    txParameter.m_txID = jsonTxParameter[Fields::TransactionId];
                    txParameter.m_subTxID = jsonTxParameter[Fields::SubTransactionId];
                    txParameter.m_paramID = jsonTxParameter[Fields::ParameterId];
                    for (const auto& v : jsonTxParameter[Fields::Value])
                    {
                        txParameter.m_value.push_back(v);
                    }
                    importedTransactionsMap[txParameter.m_txID].emplace(static_cast<TxParameterID>(txParameter.m_paramID), txParameter);
                }

                uint32_t errorsCount = 0;
                for (const auto& txPair : importedTransactionsMap)
                {
                    const auto& paramsMap = txPair.second;

                    auto itype = paramsMap.find(TxParameterID::TransactionType);
                    if(itype == paramsMap.end())
                    {
                        BEAM_LOG_ERROR() << "Transaction " << txPair.first << " was not imported. No txtype parameter";
                        ++errorsCount;
                        continue;
                    }

                    TxType txtype = TxType::Simple;
                    if (!fromByteBuffer(itype->second.m_value, txtype))
                    {
                        BEAM_LOG_ERROR() << "Transaction " << txPair.first << " was not imported. Failed to read txtype parameter";
                        ++errorsCount;
                        continue;
                    }

                    WalletID wid(Zero);
                    if (auto idIt = paramsMap.find(TxParameterID::MyAddr); idIt != paramsMap.end() && !wid.FromBuf(idIt->second.m_value))
                    {
                        BEAM_LOG_ERROR() << "Transaction " << txPair.first << " was not imported. Invalid myID parameter";
                        ++errorsCount;
                        continue;
                    }

                    if(txtype == TxType::AssetConsume ||
                       txtype == TxType::AssetIssue ||
                       txtype == TxType::AssetReg ||
                       txtype == TxType::AssetUnreg ||
                       txtype == TxType::AssetInfo )
                    {
                        // Should be Zero for assets issue & consume
                        if (wid != Zero)
                        {
                            BEAM_LOG_ERROR() << "Transaction " << txPair.first << " was not imported. Nonzero MyID for asset issue/consume/lelanutus";
                            ++errorsCount;
                            continue;
                        }
                    }
                    else
                    {
                        if (wid != Zero && !wid.IsValid())
                        {
                            BEAM_LOG_ERROR() << "Transaction " << txPair.first << " was not imported. Invalid myID parameter";
                            ++errorsCount;
                            continue;
                        }

                        auto waddr = db.getAddress(wid);
                        if (waddr && (!waddr->isOwn() || !db.ValidateSbbsWalletID(wid, waddr->m_OwnID)))
                        {
                            BEAM_LOG_ERROR() << "Transaction " << txPair.first << " was not imported. Invalid address parameter";
                            ++errorsCount;
                            continue;
                        }

                        uint64_t myAddrId = 0;
                        auto addressIt = paramsMap.find(TxParameterID::MyAddressID);
                        if (addressIt != paramsMap.end() && (!fromByteBuffer(addressIt->second.m_value, myAddrId) ||
                            !db.ValidateSbbsWalletID(wid, myAddrId)))
                        {
                            BEAM_LOG_ERROR() << "Transaction " << txPair.first << " was not imported. Invalid MyAddressID parameter";
                            ++errorsCount;
                            continue;
                        }

                        if (!waddr && addressIt == paramsMap.end())
                        {
                            BEAM_LOG_WARNING() << "Transaction " << txPair.first << ". Cannot check imported address";
                        }
                    }
                    
                    for (const auto& paramPair : paramsMap)
                    {
                        const auto& p = paramPair.second;
                        db.setTxParameter(p.m_txID,
                            static_cast<SubTxID>(p.m_subTxID),
                            paramPair.first,
                            p.m_value,
                            false);
                    }
                    BEAM_LOG_INFO() << "Transaction " << txPair.first << " was imported.";
                }
                return errorsCount == 0;
            }

            json ExportAddressesToJson(const IWalletDB& db, bool own)
            {
                json addresses = json::array();
                for (const auto& address : db.getAddresses(own))
                {
                    addresses.emplace_back(
                        json
                        {
                            {Fields::Index, address.m_OwnID},
                            {"SubIndex", 0},
                            {Fields::BbsAddr, to_string(address.m_BbsAddr)},
                            {Fields::Label, address.m_label},
                            {Fields::CreationTime, address.m_createTime},
                            {Fields::Duration, address.m_duration},
                            {Fields::Category, address.m_category},
                            {Fields::Address,  address.m_Token}
                        }
                    );
                    if (address.m_Endpoint != Zero)
                    {
                        addresses.back().push_back({ Fields::EndpointHex, to_string(address.m_Endpoint) });
                    }
                }
                return addresses;
            }


            json ExportTransactionsToJson(const IWalletDB& db)
            {
                json txParams = json::array();
                map<TxID, map<SubTxID, map<TxParameterID, ByteBuffer>>> exportedParams;
                set<TxID> txIDs;
                for (const auto& p : db.getAllTxParameters())
                {
                    exportedParams[p.m_txID][(SubTxID)p.m_subTxID].emplace((TxParameterID)p.m_paramID, p.m_value);
                    txIDs.insert(p.m_txID);
                }

                array<TxParameterID, 5> mandatoryTxParams = { 
                        TxParameterID::TransactionType,
                        TxParameterID::CreateTime
                };

                for (const auto& tx : txIDs)
                {
                    const auto& params = exportedParams[tx][(int)kDefaultSubTxID];

                    if (params.size() == 1 && params.begin()->first == TxParameterID::TransactionType) // we do not export deleted transactions
                    {
                        continue;
                    }

                    bool canExport = true;
                    for (const auto& mp : mandatoryTxParams)
                    {
                        if (params.find(mp) == params.end())
                        {
                            BEAM_LOG_WARNING() << "Transaction " << tx << " doesn't have mandatory parameters" << (int)mp << ". Skipping it";
                            canExport = false;
                            break;
                        }
                    }
                    if (!canExport)
                    {
                        continue;
                    }

                    for (const auto& subTx : exportedParams[tx])
                    {
                        for (const auto& p : subTx.second)
                        {
                            txParams.push_back(
                                json
                                {
                                    {Fields::TransactionId, tx},
                                    {Fields::SubTransactionId, subTx.first},
                                    {Fields::ParameterId, p.first},
                                    {Fields::Value, p.second}
                                }
                            );
                        }
                    }
                }
                
                return txParams;
            }
        }

        string ExportDataToJson(const IWalletDB& db)
        {
            auto res = json
            {
                {Fields::OwnAddresses, ExportAddressesToJson(db, true)},
                {Fields::Contacts, ExportAddressesToJson(db, false)},
                {Fields::TransactionParameters, ExportTransactionsToJson(db)}
            };
            return res.dump();
        }

        bool ImportDataFromJson(IWalletDB& db, const char* data, size_t size)
        {
            try
            {
                json obj = json::parse(data, data + size);
                return ImportAddressesFromJson(db, obj) 
                    && ImportContactsFromJson(db, obj)
                    && ImportTransactionsFromJson(db, obj);
            }
            catch (const nlohmann::detail::exception& e)
            {
                BEAM_LOG_ERROR() << "json parse: " << e.what() << "\n" << std::string(data, data + (size > 1024 ? 1024 : size));
            }
            return false;
        }
    }
}
