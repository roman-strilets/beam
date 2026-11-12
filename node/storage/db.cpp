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

NodeDB::NodeDB()
	:m_pDb(nullptr)
{

}

NodeDB::~NodeDB()
{
	Close();
}

void NodeDB::TestRet(int ret)
{
	if (SQLITE_OK != ret)
		ThrowSqliteError(ret);
}

void NodeDB::ThrowSqliteError(int ret)
{
	char sz[0x1000];
	snprintf(sz, _countof(sz), "sqlite err %d, %s", ret, sqlite3_errmsg(m_pDb));
	ThrowError(sz);
}

void NodeDB::ThrowError(const char* sz)
{
	// Currently all DB errors are defined as corruption
	CorruptionException exc;
	exc.m_sErr = sz;
	throw exc;
}

void NodeDB::ThrowInconsistent()
{
	ThrowError("data inconcistent");
}

void NodeDB::Statement::Close()
{
	if (m_pStmt)
	{
		sqlite3_finalize(m_pStmt); // don't care about retval
		m_pStmt = nullptr;
	}
}

void NodeDB::Close()
{
	if (m_pDb)
	{
		for (size_t i = 0; i < _countof(m_pPrep); i++)
			m_pPrep[i].Close();

        BEAM_VERIFY(SQLITE_OK == sqlite3_close(m_pDb));
		m_pDb = NULL;

		m_KrnWnd.Clear();
		m_KrnWnd.m_dh = 0;
		m_KrnWnd.m_hLo = 0;
	}
}

NodeDB::Recordset::Recordset()
	:m_ppCache(nullptr)
	,m_pStmt(nullptr)
	,m_pDB(nullptr)
{
}

NodeDB::Recordset::Recordset(NodeDB& db, Query::Enum val, const char* sql)
	:m_ppCache(nullptr)
	,m_pStmt(nullptr)
{
	InitInternal(db, val, sql);
}

void NodeDB::Recordset::InitInternal(NodeDB& db, Query::Enum val, const char* sql)
{
	m_pDB = &db;

	auto& s = db.get_Statement(val, sql);
	m_ppCache = &s.m_pStmt;

	// take ownership
	assert(!m_pStmt && s.m_pStmt);
	std::swap(m_pStmt, s.m_pStmt);
}

NodeDB::Recordset::~Recordset()
{
	Reset();
}

void NodeDB::Recordset::Reset()
{
	if (m_pStmt)
	{
		assert(m_ppCache);
		if (*m_ppCache)
			sqlite3_finalize(m_pStmt); // don't care about retval
		else
		{
			// return to cache
			*m_ppCache = m_pStmt;

			sqlite3_reset(m_pStmt); // don't care about retval
			sqlite3_clear_bindings(m_pStmt);
		}

		m_pStmt = nullptr;
	}
}

void NodeDB::Recordset::Reset(NodeDB& db, Query::Enum val, const char* sql)
{
	Reset();
	InitInternal(db, val, sql);
}

bool NodeDB::Recordset::Step()
{
	return m_pDB->ExecStep(m_pStmt);
}

void NodeDB::Recordset::StepStrict()
{
	if (!Step())
		ThrowError("not found");
}

bool NodeDB::Recordset::StepModifySafe()
{
	int nVal = m_pDB->ExecStepRaw(m_pStmt);
	switch (nVal)
	{

	default:
		m_pDB->ThrowSqliteError(nVal);
		// no break

	case SQLITE_DONE:
		return true;

	case SQLITE_CONSTRAINT:
		return false;
	}
}

bool NodeDB::Recordset::IsNull(int col)
{
	return SQLITE_NULL == sqlite3_column_type(m_pStmt, col);
}

void NodeDB::Recordset::putNull(int col)
{
	m_pDB->TestRet(sqlite3_bind_null(m_pStmt, col+1));
}

void NodeDB::Recordset::put(int col, uint32_t x)
{
	m_pDB->TestRet(sqlite3_bind_int(m_pStmt, col+1, x));
}

void NodeDB::Recordset::put(int col, uint64_t x)
{
	m_pDB->TestRet(sqlite3_bind_int64(m_pStmt, col+1, x));
}

void NodeDB::Recordset::put(int col, const Blob& x)
{
	// According to our convention empty blob is NOT NULL, it should be an empty BLOB field.
	// During initialization from buffer, if the buffer size is 0 - the x.p is left uninitialized.
	//
	// In sqlite code if x.p is NULL - it would treat the field as NULL, rather than an empty blob.
	// And if the uninitialized x.p is occasionally NULL - we get wrong behavior.
	//
	// Hence - we work this around, use `this`, as an arbitrary non-NULL pointer
	const void* pPtr = x.n ? x.p : this;
	m_pDB->TestRet(sqlite3_bind_blob(m_pStmt, col+1, pPtr, x.n, NULL));
}

void NodeDB::Recordset::put(int col, const char* sz)
{
	m_pDB->TestRet(sqlite3_bind_text(m_pStmt, col+1, sz, -1, NULL));
}

void NodeDB::Recordset::put(int col, const Key::ID& kid, Key::ID::Packed& p)
{
	p = kid;
	put(col, Blob(&p, sizeof(p)));
}

void NodeDB::Recordset::putZeroBlob(int col, uint32_t nSize)
{
	m_pDB->TestRet(sqlite3_bind_zeroblob(m_pStmt, col + 1, nSize));
}

void NodeDB::Recordset::get(int col, uint32_t& x)
{
	x = sqlite3_column_int(m_pStmt, col);
}

void NodeDB::Recordset::get(int col, uint64_t& x)
{
	x = sqlite3_column_int64(m_pStmt, col);
}

void NodeDB::Recordset::get(int col, Blob& x)
{
	x.p = sqlite3_column_blob(m_pStmt, col);
	x.n = sqlite3_column_bytes(m_pStmt, col);
}

void NodeDB::Recordset::get(int col, ByteBuffer& x)
{
	Blob b;
	get(col, b);
	b.Export(x);
}

void NodeDB::Recordset::get(int col, Key::ID& kid)
{
	Key::ID::Packed p;
	get_As(col, p);
	kid = p;
}

const void* NodeDB::Recordset::get_BlobStrict(int col, uint32_t n)
{
	Blob x;
	get(col, x);

	if (x.n != n)
	{
		char sz[0x80];
		snprintf(sz, sizeof(sz), "Blob size expected=%u, actual=%u", n, x.n);
		ThrowError(sz);
	}

	return x.p;
}


void NodeDB::Vacuum()
{
	ExecQuick("VACUUM");
}

void NodeDB::ExecQuick(const char* szSql)
{
	int n = sqlite3_total_changes(m_pDb);
	TestRet(sqlite3_exec(m_pDb, szSql, NULL, NULL, NULL));

	if (sqlite3_total_changes(m_pDb) != n)
		OnModified();
}

std::string NodeDB::ExecTextOut(const char* szSql)
{
	int n = sqlite3_total_changes(m_pDb);
	TestRet(sqlite3_exec(m_pDb, szSql, NULL, NULL, NULL));

	Statement s;
	Prepare(s, szSql);

	std::string sRes;

	if (ExecStep(s.m_pStmt))
		sRes = (const char*) sqlite3_column_text(s.m_pStmt, 0);

	if (sqlite3_total_changes(m_pDb) != n)
		OnModified();

	return sRes;
}

int NodeDB::ExecStepRaw(sqlite3_stmt* pStmt)
{
	int n = sqlite3_total_changes(m_pDb);

	int nVal = sqlite3_step(pStmt);

	if (sqlite3_total_changes(m_pDb) != n)
		OnModified();

	return nVal;
}

bool NodeDB::ExecStep(sqlite3_stmt* pStmt)
{
	int nVal = ExecStepRaw(pStmt);
	switch (nVal)
	{

	default:
		ThrowSqliteError(nVal);
		// no break

	case SQLITE_DONE:
		return false;

	case SQLITE_ROW:
		//{
		//	int nCount = sqlite3_column_count(pStmt);
		//	for (int ii = 0; ii < nCount; ii++)
		//	{
		//		const char* sz = sqlite3_column_name(pStmt, ii);
		//		sz = sz;
		//	}
		//}
		return true;
	}
}

bool NodeDB::ExecStep(Query::Enum val, const char* sql)
{
	return ExecStep(get_Statement(val, sql).m_pStmt);

}

void NodeDB::Prepare(Statement& s, const char* szSql)
{
	assert(!s.m_pStmt);

	const char* szTail;
	int nRet = sqlite3_prepare_v2(m_pDb, szSql, -1, &s.m_pStmt, &szTail);
	TestRet(nRet);
	assert(s.m_pStmt);
}

NodeDB::Statement& NodeDB::get_Statement(Query::Enum val, const char* sql)
{
	assert(val < _countof(m_pPrep));
	Statement& s = m_pPrep[val];

	if (!s.m_pStmt)
		Prepare(s, sql);

	return s;
}


int NodeDB::get_RowsChanged() const
{
	return sqlite3_changes(m_pDb);
}

uint64_t NodeDB::get_LastInsertRowID() const
{
	return sqlite3_last_insert_rowid(m_pDb);
}

void NodeDB::TestChanged1Row()
{
	if (1 != get_RowsChanged())
		ThrowError("1row change failed");
}

void NodeDB::ParamSet(uint32_t ID, const uint64_t* p0, const Blob* p1)
{
	Recordset rs(*this, Query::ParamSet, "INSERT OR REPLACE INTO " TblParams " (" TblParams_ID "," TblParams_Int "," TblParams_Blob ") VALUES(?,?,?)");
	rs.put(0, ID);
	if (p0)
		rs.put(1, *p0);
	if (p1)
		rs.put(2, *p1);
	rs.Step();
}

void NodeDB::ParamIntSet(uint32_t ID, uint64_t val)
{
	ParamSet(ID, &val, nullptr);
}

bool NodeDB::ParamGet(uint32_t ID, uint64_t* p0, Blob* p1, ByteBuffer* p2 /* = NULL */)
{
	Recordset rs(*this, Query::ParamGet, "SELECT " TblParams_Int "," TblParams_Blob " FROM " TblParams " WHERE " TblParams_ID "=?");
	rs.put(0, ID);

	if (!rs.Step())
		return false;

	if (p0)
		rs.get(0, *p0);
	if (p1)
	{
		if (rs.IsNull(1))
			return false;

		Blob x;
		rs.get(1, x);
		if (x.n != p1->n)
			return false;

		memcpy(Cast::NotConst(p1->p), x.p, x.n);
	}
	if (p2)
		rs.get(1, *p2);

	return true;
}

uint64_t NodeDB::ParamIntGetDef(uint32_t ID, uint64_t def /* = 0 */)
{
	ParamGet(ID, &def, NULL);
	return def;
}

bool NodeDB::ParamDelSafe(uint32_t ID)
{
	Recordset rs(*this, Query::ParamDel, "DELETE FROM " TblParams " WHERE " TblParams_ID "=?");
	rs.put(0, ID);
	rs.Step();

	return !!get_RowsChanged();
}

NodeDB::Transaction::Transaction(NodeDB* pDB)
	:m_pDB(NULL)
{
	if (pDB)
		Start(*pDB);
}

NodeDB::Transaction::~Transaction()
{
	if (std::uncaught_exceptions())
	{
		try {
			Rollback();
		}
		catch (...) {
			// ignore
		}
	}
	else
		Rollback();
}

void NodeDB::Transaction::Start(NodeDB& db)
{
	assert(!m_pDB);
	db.ExecStep(Query::Begin, "BEGIN");
	m_pDB = &db;
}

void NodeDB::Transaction::Commit()
{
	assert(m_pDB);
	m_pDB->ExecStep(Query::Commit, "COMMIT");
	m_pDB = NULL;
}

void NodeDB::Transaction::Rollback()
{
	if (m_pDB)
	{
		m_pDB->ExecStep(Query::Rollback, "ROLLBACK");
		m_pDB->OnTxRolledBack();
		m_pDB = nullptr;
	}
}

NodeDB::Savepoint::Savepoint(NodeDB& db)
	:m_pDB(nullptr)
{
	db.ExecStep(Query::SavepointSet, "SAVEPOINT sp");
	m_pDB = &db;
}

NodeDB::Savepoint::~Savepoint()
{
	if (!m_pDB)
		return;

	try {
		// ROLLBACK TO leaves the savepoint on the stack, it must be released too
		m_pDB->ExecStep(Query::SavepointRollback, "ROLLBACK TO sp");
		m_pDB->ExecStep(Query::SavepointRelease, "RELEASE sp");
		m_pDB->OnTxRolledBack();
	}
	catch (...) {
		// ignore
	}
}

void NodeDB::Savepoint::Release()
{
	assert(m_pDB);
	m_pDB->ExecStep(Query::SavepointRelease, "RELEASE sp");
	m_pDB = nullptr;
}

} // namespace beam
