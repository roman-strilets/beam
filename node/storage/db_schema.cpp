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

#include "db_internal.h"

namespace beam {

void NodeDB::DisableMemorySecurity()
{
	// The pragma sets a global flag, so a temporary in-memory connection is enough
	sqlite3* pDb = nullptr;
	if (SQLITE_OK == sqlite3_open_v2(":memory:", &pDb, SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX, nullptr))
		sqlite3_exec(pDb, "PRAGMA cipher_memory_security = OFF", nullptr, nullptr, nullptr);
	sqlite3_close(pDb);
}

void NodeDB::Open(const char* szPath)
{
	TestRet(sqlite3_open_v2(szPath, &m_pDb, SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_CREATE, NULL));
	// Attempt to fix the "busy" error when PC goes to sleep and then awakes. Try the busy handler with non-zero timeout (maybe a single retry would be enough)
	sqlite3_busy_timeout(m_pDb, 5000);

	ExecTextOut("PRAGMA locking_mode = EXCLUSIVE");
	ExecTextOut("PRAGMA journal_size_limit=1048576"); // limit journal file, otherwise it may remain huge even after tx commit, until the app is closed
	ExecTextOut("PRAGMA cache_size=-524288"); // 512 MiB page cache (negative value is in KiB). The default ~2 MiB makes block import spend most of its time in pread

	bool bCreate;
	{
		Recordset rs(*this, Query::Scheme, "SELECT name FROM sqlite_master WHERE type='table' AND name=?");
		rs.put(0, TblParams);
		bCreate = !rs.Step();
	}

	const uint64_t nVersionTop = 38;


	Transaction t(*this);

	if (bCreate)
	{
		Create();
		ParamIntSet(ParamID::DbVer, nVersionTop);
	}
	else
	{
		uint64_t nVer = ParamIntGetDef(ParamID::DbVer);
		switch (nVer)
		{
		case 19: // before Shielded shards
			// ignore
			// no break;

		case 20: // Deprecated Shielded table created.
			CreateTables20();

			BEAM_LOG_INFO() << "DB migrate from " << 20;
			MigrateFrom20();
			// no break;

		case 21:
			CreateTables21();
			// no break;

		case 22:
			CreateTables22();
			// no break;

		case 23:
			CreateTables23();
			// no break;

		case 24:
		case 25:
			ParamDelSafe(ParamID::RichContractInfo); // former SyncTarget
			ParamDelSafe(ParamID::RichContractParser); // former Deprecated_2
			ParamDelSafe(ParamID::Deprecated_3);
			// no break;

		case 26: // ShieldedState stream added
			// no break;

		case 27:
			CreateTables27();
			// no break;

		case 28:
			CreateTables28();
			// no break;

		case 29: // Block interpretation nKrnIdx fixed to match KrnWalker's
			// no break;

		case 30: // Block interpretation nKrnIdx fixed to match KrnWalker's
			ExecQuick("DROP TABLE IF EXISTS " TblKrnInfo);
			CreateTables30();

			// no break;

		case 31:
		case 32:
			ExecQuick("DROP TABLE IF EXISTS " TblAssets);
			CreateTables31();
			// no break;

		case 33: // fix asset evt table, after previous Rebuild non-std
			// no break;

		case 34:

			ParamDelSafe(ParamID::Deprecated_EventsOwnerID);
			ParamDelSafe(ParamID::Deprecated_EventsSerif);

			// no break;

		case 35: // changed format of contract invoke info

			ParamIntSet(ParamID::Flags1, ParamIntGetDef(ParamID::Flags1) | Flags1::PendingRebuildNonStd);
			// no break;

		case 36: // changed Events table

			ExecQuick("DROP INDEX IF EXISTS [Idx" TblEvents "]");
			ExecQuick("DROP INDEX IF EXISTS [Idx" TblEvents TblEvents_Key "]");
			ExecQuick("DROP TABLE IF EXISTS " TblEvents);
			ExecQuick("DROP TABLE IF EXISTS " TblAccounts);

			CreateTables36();
			// no break;

		case 37: // bridge
			CreateTables37();
			// no break;

			ParamIntSet(ParamID::DbVer, nVersionTop);
			// no break;

		case nVersionTop:
			break;

		default:
			if (nVer < nVersionTop)
				throw NodeDBUpgradeException("Node upgrade is not supported. Please, remove node.db and tempmb files");

			if (nVer > nVersionTop)
				throw NodeDBUpgradeException("Unsupported db version");
		}
	}

	t.Commit();
}

void NodeDB::CheckIntegrity()
{
	std::string s = ExecTextOut("PRAGMA integrity_check");
	if (s != "ok")
		ThrowError(("sqlite integrity: " + s).c_str());
}

void NodeDB::Create()
{
	// create tables

	ExecQuick("CREATE TABLE [" TblParams "] ("
		"[" TblParams_ID	"] INTEGER NOT NULL PRIMARY KEY,"
		"[" TblParams_Int	"] INTEGER,"
		"[" TblParams_Blob	"] BLOB)");

	ExecQuick("CREATE TABLE [" TblStates "] ("
		"[" TblStates_Number		"] INTEGER NOT NULL,"
		"[" TblStates_Hash			"] BLOB NOT NULL,"
		"[" TblStates_HashPrev		"] BLOB NOT NULL,"
		"[" TblStates_Timestamp		"] INTEGER NOT NULL,"
		"[" TblStates_Kernels		"] BLOB NOT NULL,"
		"[" TblStates_Definition	"] BLOB NOT NULL,"
		"[" TblStates_Flags			"] INTEGER NOT NULL,"
		"[" TblStates_RowPrev		"] INTEGER,"
		"[" TblStates_CountNext		"] INTEGER NOT NULL,"
		"[" TblStates_CountNextF	"] INTEGER NOT NULL,"
		"[" TblStates_PoW			"] BLOB,"
		"[" TblStates_Rollback		"] BLOB,"
		"[" TblStates_BodyP			"] BLOB,"
		"[" TblStates_BodyE			"] BLOB,"
		"[" TblStates_Peer			"] BLOB,"
		"[" TblStates_ChainWork		"] BLOB,"
		"[" TblStates_Txos			"] INTEGER,"
		"[" TblStates_Extra			"] BLOB,"
		"[" TblStates_Inputs		"] BLOB,"
		"PRIMARY KEY (" TblStates_Number "," TblStates_Hash "),"
		"FOREIGN KEY (" TblStates_RowPrev ") REFERENCES " TblStates "(OID))");

	ExecQuick("CREATE INDEX [Idx" TblStates "Wrk] ON [" TblStates "] ([" TblStates_ChainWork "]);");
	ExecQuick("CREATE INDEX [Idx" TblStates TblStates_Txos "] ON [" TblStates "] ([" TblStates_Txos "]);");

	ExecQuick("CREATE TABLE [" TblTips "] ("
		"[" TblTips_Number	"] INTEGER NOT NULL,"
		"[" TblTips_State	"] INTEGER NOT NULL,"
		"PRIMARY KEY (" TblTips_Number "," TblTips_State "),"
		"FOREIGN KEY (" TblTips_State ") REFERENCES " TblStates "(OID))");

	ExecQuick("CREATE TABLE [" TblTipsReachable "] ("
		"[" TblTips_State		"] INTEGER NOT NULL,"
		"[" TblTips_ChainWork	"] BLOB NOT NULL,"
		"PRIMARY KEY (" TblTips_State "),"
		"FOREIGN KEY (" TblTips_State ") REFERENCES " TblStates "(OID),"
		"FOREIGN KEY (" TblTips_ChainWork ") REFERENCES " TblStates "(" TblStates_ChainWork "))");

	ExecQuick("CREATE INDEX [Idx" TblTipsReachable "Wrk] ON [" TblTipsReachable "] ([" TblTips_ChainWork "]);");

	ExecQuick("CREATE TABLE [" TblKernels "] ("
		"[" TblKernels_Key		"] BLOB NOT NULL,"
		"[" TblKernels_Height	"] INTEGER NOT NULL)");

	ExecQuick("CREATE INDEX [Idx" TblKernels "] ON [" TblKernels "] ([" TblKernels_Key "],[" TblKernels_Height "]  DESC);");

	ExecQuick("CREATE TABLE [" TblPeer "] ("
		"[" TblPeer_Key			"] BLOB NOT NULL,"
		"[" TblPeer_Rating		"] INTEGER NOT NULL,"
		"[" TblPeer_Addr		"] INTEGER NOT NULL,"
		"[" TblPeer_LastSeen	"] INTEGER NOT NULL)");

	ExecQuick("CREATE TABLE [" TblBbs "] ("
		"[" TblBbs_ID		"] INTEGER PRIMARY KEY AUTOINCREMENT,"
		"[" TblBbs_Key		"] BLOB NOT NULL,"
		"[" TblBbs_Channel	"] INTEGER NOT NULL,"
		"[" TblBbs_Time		"] INTEGER NOT NULL,"
		"[" TblBbs_Msg		"] BLOB NOT NULL,"
		"[" TblBbs_Nonce	"] INTEGER)");

	ExecQuick("CREATE INDEX [Idx" TblBbs "CSeq] ON [" TblBbs "] ([" TblBbs_Channel "],[" TblBbs_ID "]);");
	ExecQuick("CREATE INDEX [Idx" TblBbs "TSeq] ON [" TblBbs "] ([" TblBbs_Time "],[" TblBbs_ID "]);");
	ExecQuick("CREATE INDEX [Idx" TblBbs "Key] ON [" TblBbs "] ([" TblBbs_Key "]);");

	ExecQuick("CREATE TABLE [" TblDummy "] ("
		"[" TblDummy_ID				"] BLOB NOT NULL PRIMARY KEY,"
		"[" TblDummy_SpendHeight	"] INTEGER NOT NULL)");

	ExecQuick("CREATE INDEX [Idx" TblDummy "H] ON [" TblDummy "] ([" TblDummy_SpendHeight "])");

	ExecQuick("CREATE TABLE [" TblTxo "] ("
		"[" TblTxo_ID				"] INTEGER NOT NULL PRIMARY KEY,"
		"[" TblTxo_Value			"] BLOB NOT NULL,"
		"[" TblTxo_SpendHeight		"] INTEGER)");

	CreateTables20();
	CreateTables21();
	CreateTables22();
	CreateTables23();
	CreateTables27();
	CreateTables28();
	CreateTables30();
	CreateTables31();
	CreateTables36();
	CreateTables37();
}

void NodeDB::CreateTables20()
{
	ExecQuick("CREATE TABLE [" TblStreams "] ("
		"[" TblStream_ID			"] INTEGER NOT NULL PRIMARY KEY,"
		"[" TblStream_Value			"] BLOB NOT NULL)");

	ExecQuick("CREATE TABLE [" TblUnique "] ("
		"[" TblUnique_Key			"] BLOB NOT NULL PRIMARY KEY,"
		"[" TblUnique_Value			"] BLOB) WITHOUT ROWID");
}

void NodeDB::CreateTables21()
{
	ExecQuick("CREATE TABLE [" TblAssetEvts "] ("
		"[" TblAssetEvts_ID			"] INTEGER NOT NULL,"
		"[" TblAssetEvts_Height		"] INTEGER NOT NULL,"
		"[" TblAssetEvts_Index		"] INTEGER NOT NULL,"
		"[" TblAssetEvts_Data		"] BLOB)");

	ExecQuick("CREATE INDEX [Idx" TblAssetEvts "_1" "] ON [" TblAssetEvts "] ([" TblAssetEvts_ID "],[" TblAssetEvts_Height  "],[" TblAssetEvts_Index "]);");
	ExecQuick("CREATE INDEX [Idx" TblAssetEvts "_2" "] ON [" TblAssetEvts "] ([" TblAssetEvts_Height  "],[" TblAssetEvts_Index "]);");
}

void NodeDB::CreateTables22()
{
	ExecQuick("CREATE TABLE [" TblShieldedStatistic "] ("
		"[" TblShieldedStatistic_Height"] INTEGER NOT NULL PRIMARY KEY,"
		"[" TblShieldedStatistic_OutCount"] INTEGER NOT NULL)");
}

void NodeDB::CreateTables23()
{
	ExecQuick("CREATE TABLE [" TblContracts "] ("
		"[" TblContracts_Key		"] BLOB NOT NULL PRIMARY KEY,"
		"[" TblContracts_Value		"] BLOB NOT NULL)");
}

void NodeDB::CreateTables27()
{
	ExecQuick("CREATE TABLE [" TblContractLogs "] ("
		"[" TblContractLogs_Pos			"] BLOB NOT NULL PRIMARY KEY,"
		"[" TblContractLogs_Key			"] BLOB NOT NULL,"
		"[" TblContractLogs_Data		"] BLOB NOT NULL"
		") WITHOUT ROWID");

	ExecQuick("CREATE INDEX [Idx" TblContractLogs "_Key" "] ON [" TblContractLogs "] ([" TblContractLogs_Key "],[" TblContractLogs_Pos "]);");
}

void NodeDB::CreateTables28()
{
	ExecQuick("CREATE TABLE [" TblCache "] ("
		"[" TblCache_Key		"] BLOB NOT NULL,"
		"[" TblCache_Data		"] BLOB NOT NULL,"
		"[" TblCache_LastHit	"] INTEGER NOT NULL)");

	ExecQuick("CREATE INDEX [Idx" TblCache "_Key" "] ON [" TblCache "] ([" TblCache_Key "]);");
	ExecQuick("CREATE INDEX [Idx" TblCache "_Hit" "] ON [" TblCache "] ([" TblCache_LastHit "]);");
}

void NodeDB::CreateTables30()
{
	ExecQuick("CREATE TABLE [" TblKrnInfo "] ("
		"[" TblKrnInfo_Pos		"] BLOB NOT NULL PRIMARY KEY,"
		"[" TblKrnInfo_Key		"] BLOB NOT NULL,"
		"[" TblKrnInfo_Data		"] BLOB NOT NULL"
		") WITHOUT ROWID");

	ExecQuick("CREATE INDEX [Idx" TblKrnInfo "_Key" "] ON [" TblKrnInfo "] ([" TblKrnInfo_Key "],[" TblKrnInfo_Pos "]);");
}

void NodeDB::CreateTables31()
{
	ExecQuick("CREATE TABLE [" TblAssets "] ("
		"[" TblAssets_ID			"] INTEGER NOT NULL PRIMARY KEY,"
		"[" TblAssets_Owner			"] BLOB,"
		"[" TblAssets_Cid			"] BLOB,"
		"[" TblAssets_Data			"] BLOB,"
		"[" TblAssets_Deposit		"] INTEGER,"
		"[" TblAssets_LockHeight	"] INTEGER,"
		"[" TblAssets_Value			"] BLOB)");

	ExecQuick("CREATE INDEX [Idx" TblAssets "Own] ON [" TblAssets "] ([" TblAssets_Owner "])");
}

void NodeDB::CreateTables36()
{
	ExecQuick("CREATE TABLE [" TblEvents "] ("
		"[" TblEvents_Account	"] INTEGER NOT NULL,"
		"[" TblEvents_Pos		"] BLOB NOT NULL,"
		"[" TblEvents_Body		"] BLOB NOT NULL,"
		"[" TblEvents_Key		"] BLOB NOT NULL)");

	ExecQuick("CREATE INDEX [Idx" TblEvents "] ON [" TblEvents "] ([" TblEvents_Account "],[" TblEvents_Pos "]);");
	ExecQuick("CREATE INDEX [Idx" TblEvents TblEvents_Key "] ON [" TblEvents "] ([" TblEvents_Account "],["  TblEvents_Key "],[" TblEvents_Pos "]);");

	ExecQuick("CREATE TABLE [" TblAccounts "] ("
		"[" TblAccounts_Index	"] INTEGER NOT NULL PRIMARY KEY,"
		"[" TblAccounts_OwnerID	"] BLOB NOT NULL,"
		"[" TblAccounts_Serif	"] BLOB NOT NULL,"
		"[" TblAccounts_TxoHi	"] INTEGER NOT NULL)");
}

void NodeDB::CreateTables37()
{
	ExecQuick("CREATE TABLE [" TblBridge "] ("
		"[" TblBridge_Pos		"] BLOB NOT NULL PRIMARY KEY,"
		"[" TblBridge_Key		"] BLOB,"
		"[" TblBridge_Value		"] BLOB) WITHOUT ROWID");

	ExecQuick("CREATE INDEX [Idx" TblBridge "] ON [" TblBridge "] ([" TblBridge_Key "],[" TblBridge_Pos "]);");
}


void NodeDB::MigrateFrom20()
{
	StateID sid;
	get_Cursor(sid);

	LongAction la("Rebuilding states MMR...", sid.m_Number.v);

	ExecQuick("UPDATE " TblStates " SET " TblStates_Rollback "=NULL"); // was used for states MMR. Prepare it for the new use


	StatesMmr smmr(*this);
	for (Block::Number num(1); num.v < sid.m_Number.v; num.v++)
	{
		Merkle::Hash hv;
		smmr.LoadStateHash(hv, num); // there's a more effective way to select hashes of all active states. But it's just a migration.
		smmr.Append(hv);

		la.OnProgress(num.v);
	}
}

} // namespace beam
