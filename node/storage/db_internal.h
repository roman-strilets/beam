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

// Internal header shared by the db*.cpp sources of NodeDB: the include list,
// the table and column names used in the SQL statements, and BlobGuard.
#pragma once

#include "db.h"
#include <algorithm> // sort
#include "core/net/peer_manager.h"
#include "utility/logger.h"
#include "utility/byteorder.h"
#include <algorithm>

namespace beam {

// Literal constants
#define TblParams				"Params"
#define TblParams_ID			"ID"
#define TblParams_Int			"ParamInt"
#define TblParams_Blob			"ParamBlob"

#define TblStates				"States"
#define TblStates_Number		"Height"
#define TblStates_Hash			"Hash"
#define TblStates_HashPrev		"HashPrev"
#define TblStates_Timestamp		"Timestamp"
#define TblStates_Kernels		"Kernels"
#define TblStates_Definition	"Definition"
#define TblStates_Flags			"Flags"
#define TblStates_RowPrev		"RowPrev"
#define TblStates_CountNext		"CountNext"
#define TblStates_CountNextF	"CountNextFunctional"
#define TblStates_PoW			"PoW"
#define TblStates_Rollback		"Mmr" // For historical reasons it was used for states MMR. Not it's a rollback data
#define TblStates_BodyP			"Perishable"
#define TblStates_BodyE			"Ethernal"
#define TblStates_Peer			"Peer"
#define TblStates_ChainWork		"ChainWork"
#define TblStates_Txos			"Txos"
#define TblStates_Extra			"Extra"
#define TblStates_Inputs		"Inputs"

#define TblTips					"Tips"
#define TblTipsReachable		"TipsReachable"
#define TblTips_Number			"Height"
#define TblTips_State			"State"
#define TblTips_ChainWork		"ChainWork"

#define TblKernels				"Kernels"
#define TblKernels_Key			"Commitment"
#define TblKernels_Height		"Height"
#define IdxKernels				"Idx" TblKernels
#define IdxKernels_Create		"CREATE INDEX IF NOT EXISTS [" IdxKernels "] ON [" TblKernels "] ([" TblKernels_Key "],[" TblKernels_Height "] DESC);"

#define TblEvents				"Events"
#define TblEvents_Account		"Account"
#define TblEvents_Pos			"Pos"
#define TblEvents_Body			"Body"
#define TblEvents_Key			"Key"

#define TblAccounts				"Accounts"
#define TblAccounts_Index		"iIdx"
#define TblAccounts_OwnerID		"OwnerID"
#define TblAccounts_Serif		"Serif"
#define TblAccounts_TxoHi		"HeightHi"

#define TblPeer					"Peers"
#define TblPeer_Key				"Key"
#define TblPeer_Rating			"Rating"
#define TblPeer_Addr			"Address"
#define TblPeer_LastSeen		"LastSeen"

#define TblBbs					"Bbs"
#define TblBbs_ID				"ID"
#define TblBbs_Key				"Key"
#define TblBbs_Channel			"Channel"
#define TblBbs_Time				"Time"
#define TblBbs_Msg				"Message"
#define TblBbs_Nonce			"Nonce"

#define TblDummy				"Dummies"
#define TblDummy_ID				"ID"
#define TblDummy_SpendHeight	"SpendHeight"

#define TblTxo					"Txo"
#define TblTxo_ID				"ID"
#define TblTxo_Value			"Value"
#define TblTxo_SpendHeight		"SpendHeight"

#define TblStreams				"Streams"
#define TblStream_ID			"ID"
#define TblStream_Value			"Value"

#define TblUnique				"UniqueStorage"
#define TblUnique_Key			"Key"
#define TblUnique_Value			"Value"

#define TblBridge				"Bridge"
#define TblBridge_Pos			"Pos"
#define TblBridge_Key			"Key"
#define TblBridge_Value			"Value"

#define TblAssets				"Assets"
#define TblAssets_ID			"ID"
#define TblAssets_Owner			"Owner"
#define TblAssets_Cid			"Cid"
#define TblAssets_Value			"Value"
#define TblAssets_Data			"MetaData"
#define TblAssets_Deposit		"Deposit"
#define TblAssets_LockHeight	"LockHeight"

#define TblAssetEvts			"AssetsEvents"
#define TblAssetEvts_ID			"ID"
#define TblAssetEvts_Height		"Height"
#define TblAssetEvts_Index		"Seq"
#define TblAssetEvts_Data		"Data"

#define TblShieldedStatistic			"ShieldedStatistic"
#define TblShieldedStatistic_Height		"Height"
#define TblShieldedStatistic_OutCount	"OutCount"

#define TblContracts			"Contracts"
#define TblContracts_Key		"Key"
#define TblContracts_Value		"Value"

#define TblContractLogs			"ContractLogs"
#define TblContractLogs_Pos		"Pos"
#define TblContractLogs_Key		"Key"
#define TblContractLogs_Data	"Data"

#define TblCache				"Cache"
#define TblCache_Key			"Key"
#define TblCache_Data			"Data"
#define TblCache_LastHit		"Hit"

#define TblKrnInfo				"KrnInfo"
#define TblKrnInfo_Pos			"Pos"
#define TblKrnInfo_Key			"Key"
#define TblKrnInfo_Data			"Data"

struct NodeDB::BlobGuard
{
	sqlite3_blob* m_pPtr = nullptr;

	~BlobGuard()
	{
		if (m_pPtr)
			BEAM_VERIFY(SQLITE_OK == sqlite3_blob_close(m_pPtr));
	}
};

} // namespace beam
