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

bool NodeDB::UniqueInsertSafe(const Blob& key, const Blob* pVal)
{
	Recordset rs(*this, Query::UniqueIns, "INSERT INTO " TblUnique " (" TblUnique_Key "," TblUnique_Value ") VALUES(?,?)");
	rs.put(0, key);
	if (pVal)
		rs.put(1, *pVal);

	return rs.StepModifySafe();
}

bool NodeDB::UniqueFind(const Blob& key, Recordset& rs)
{
	rs.Reset(*this, Query::UniqueFind, "SELECT " TblUnique_Value " FROM " TblUnique " WHERE " TblUnique_Key "=?");
	rs.put(0, key);
	return rs.Step();
}

void NodeDB::UniqueDeleteStrict(const Blob& key)
{
	Recordset rs(*this, Query::UniqueDel, "DELETE FROM " TblUnique " WHERE " TblUnique_Key "=?");
	rs.put(0, key);

	rs.Step();
	TestChanged1Row();
}

void NodeDB::UniqueDeleteAll()
{
	Recordset rs(*this, Query::UniqueDelAll, "DELETE FROM " TblUnique);
	rs.Step();
}

bool NodeDB::BridgeInsertSafe(const HeightPos& pos, const Blob& key, const Blob* pVal)
{
	Recordset rs(*this, Query::BridgeIns, "INSERT INTO " TblBridge " (" TblBridge_Pos "," TblBridge_Key "," TblBridge_Value ") VALUES(?,?,?)");

	HeightPosPacked buf;
	buf.put(rs, 0, pos);

	rs.put(1, key);
	if (pVal)
		rs.put(2, *pVal);

	return rs.StepModifySafe();
}

HeightPos NodeDB::BridgeFind(const Blob& key, Blob& val, Recordset& rs)
{
	rs.Reset(*this, Query::BridgeFind, "SELECT " TblBridge_Pos "," TblBridge_Value " FROM " TblBridge " WHERE " TblBridge_Key "=? ORDER BY " TblBridge_Pos " ASC LIMIT 1");
	rs.put(0, key);

	HeightPos pos;

	if (rs.Step())
	{
		HeightPosPacked::get(rs, 0, pos);
		rs.get(1, val);
	}
	else
		ZeroObject(val);

	return pos;
}

void NodeDB::BridgeDeleteFrom(const HeightPos& pos)
{
	Recordset rs(*this, Query::BridgeDelFrom, "DELETE FROM " TblBridge " WHERE " TblBridge_Pos ">=?");

	HeightPosPacked buf;
	buf.put(rs, 0, pos);
	rs.Step();
}

HeightPos NodeDB::BridgeGetLastPos()
{
	HeightPos pos;

	Recordset rs(*this, Query::BridgeMaxPos, "SELECT MAX(" TblBridge_Pos ") FROM " TblBridge);
	if (rs.Step() && !rs.IsNull(0))
		HeightPosPacked::get(rs, 0, pos);

	return pos;
}

void NodeDB::get_CacheState(CacheState& cs)
{
	Blob blob(&cs, sizeof(cs));
	if (ParamGet(ParamID::CacheState, nullptr, &blob))
	{
		cs.m_HitCounter = ByteOrder::from_le(cs.m_HitCounter);
		cs.m_SizeMax = ByteOrder::from_le(cs.m_SizeMax);
		cs.m_SizeCurrent = ByteOrder::from_le(cs.m_SizeCurrent);
	}
	else
	{
		ZeroObject(cs);
		cs.m_SizeMax = 512U * 1024U * 1024U; // default cache size is 512MB
	}
}

void NodeDB::set_CacheState(CacheState& cs)
{
	if (cs.m_SizeCurrent > cs.m_SizeMax)
	{
		for (Recordset rs(*this, Query::CacheEnumByHit, "SELECT rowid,LENGTH(" TblCache_Data ") FROM " TblCache " ORDER BY " TblCache_LastHit); rs.Step(); )
		{
			uint64_t rowid, nSize;
			rs.get(0, rowid);
			rs.get(1, nSize);

			if (nSize > cs.m_SizeCurrent)
				ThrowInconsistent();

			Recordset rs2(*this, Query::CacheDel, "DELETE FROM " TblCache " WHERE rowid=?");
			rs2.put(0, rowid);
			rs2.Step();
			TestChanged1Row();

			cs.m_SizeCurrent -= nSize;
			if (cs.m_SizeCurrent <= cs.m_SizeMax)
				break;
		}

	}

	cs.m_HitCounter = ByteOrder::to_le(cs.m_HitCounter);
	cs.m_SizeMax = ByteOrder::to_le(cs.m_SizeMax);
	cs.m_SizeCurrent = ByteOrder::to_le(cs.m_SizeCurrent);

	Blob blob(&cs, sizeof(cs));
	ParamSet(ParamID::CacheState, nullptr, &blob);
}

void NodeDB::CacheSetMaxSize(uint64_t nSize)
{
	CacheState cs;
	get_CacheState(cs);

	cs.m_SizeMax = nSize;
	set_CacheState(cs);
}


void NodeDB::CacheInsert(const Blob& key, const Blob& data)
{
	CacheState cs;
	get_CacheState(cs);
	if (data.n > cs.m_SizeMax)
		return;

	Recordset rs(*this, Query::CacheIns, "INSERT INTO " TblCache " (" TblCache_Key "," TblCache_Data "," TblCache_LastHit ") VALUES(?,?,?)");
	rs.put(0, key);
	rs.put(1, data);
	rs.put(2, ++cs.m_HitCounter);

	rs.Step();
	TestChanged1Row();

	cs.m_SizeCurrent += data.n;
	set_CacheState(cs);
}

bool NodeDB::CacheFind(const Blob& key, ByteBuffer& res)
{
	Recordset rs(*this, Query::CacheFind, "SELECT rowid FROM " TblCache " WHERE " TblCache_Key "=?");
	rs.put(0, key);

	if (!rs.Step())
		return false;

	uint64_t rowid;
	rs.get(0, rowid);

	CacheState cs;
	get_CacheState(cs);

	rs.Reset(*this, Query::CacheUpdateHit, "UPDATE " TblCache " SET " TblCache_LastHit "=? WHERE rowid=?");
	rs.put(0, ++cs.m_HitCounter);
	rs.put(1, rowid);

	rs.Step();
	TestChanged1Row();

	BlobGuard blob;
	OpenBlob(blob, TblCache, TblCache_Data, rowid, false);

	uint32_t nSize = sqlite3_blob_bytes(blob.m_pPtr);
	res.resize(nSize);

	if (nSize)
		TestRet(sqlite3_blob_read(blob.m_pPtr, &res.front(), nSize, 0));

	set_CacheState(cs);
	return true;
}

} // namespace beam
