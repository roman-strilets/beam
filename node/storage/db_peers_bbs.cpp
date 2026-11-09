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

void NodeDB::EnumPeers(WalkerPeer& x)
{
	x.m_Rs.Reset(*this, Query::PeerEnum, "SELECT " TblPeer_Key "," TblPeer_Rating "," TblPeer_Addr "," TblPeer_LastSeen " FROM " TblPeer);
}

bool NodeDB::WalkerPeer::MoveNext()
{
	if (!m_Rs.Step())
		return false;
	m_Rs.get(0, m_Data.m_ID);
	m_Rs.get(1, m_Data.m_Rating);
	m_Rs.get(2, m_Data.m_Address);
	m_Rs.get(3, m_Data.m_LastSeen);
	return true;
}

void NodeDB::PeersDel()
{
	Recordset rs(*this, Query::PeerDel, "DELETE FROM " TblPeer);
	rs.Step();
}

void NodeDB::PeerIns(const WalkerPeer::Data& d)
{
	Recordset rs(*this, Query::PeerAdd, "INSERT INTO " TblPeer "(" TblPeer_Key "," TblPeer_Rating "," TblPeer_Addr "," TblPeer_LastSeen ") VALUES(?,?,?,?)");
	rs.put(0, d.m_ID);
	rs.put(1, d.m_Rating);
	rs.put(2, d.m_Address);
	rs.put(3, d.m_LastSeen);
	rs.Step();
	TestChanged1Row();
}

bool NodeDB::EnumBbs(IBbsHistogram& x)
{
	Recordset rs(*this, Query::BbsHistogram, "SELECT " TblBbs_Channel ",COUNT(*) FROM " TblBbs " GROUP BY " TblBbs_Channel " ORDER BY " TblBbs_Channel);
	while (rs.Step())
	{
		BbsChannel ch;
		uint64_t nCount;

		rs.get(0, ch);
		rs.get(1, nCount);

		if (!x.OnChannel(ch, nCount))
			return false;
	}

	return true;
}

void NodeDB::EnumAllBbsSeq(WalkerBbsLite& x)
{
	x.m_Rs.Reset(*this, Query::BbsEnumAllSeq, "SELECT " TblBbs_ID "," TblBbs_Key ",LENGTH(" TblBbs_Msg ") FROM " TblBbs " WHERE " TblBbs_ID ">? ORDER BY " TblBbs_ID);
	x.m_Rs.put(0, x.m_ID);
}

bool NodeDB::WalkerBbsLite::MoveNext()
{
	if (!m_Rs.Step())
		return false;
	m_Rs.get(0, m_ID);
	m_Rs.get(1, m_Key);
	m_Rs.get(2, m_Size);
	return true;
}

void NodeDB::EnumAllBbs(WalkerBbsTimeLen& x)
{
	x.m_Rs.Reset(*this, Query::BbsEnumAll, "SELECT " TblBbs_ID "," TblBbs_Time ",LENGTH(" TblBbs_Msg ") FROM " TblBbs " ORDER BY " TblBbs_ID);
}

bool NodeDB::WalkerBbsTimeLen::MoveNext()
{
	if (!m_Rs.Step())
		return false;
	m_Rs.get(0, m_ID);
	m_Rs.get(1, m_Time);
	m_Rs.get(2, m_Size);
	return true;
}

void NodeDB::get_BbsTotals(BbsTotals& x)
{
	Recordset rs(*this, Query::BbsTotals, "SELECT COUNT(*), SUM(Length(" TblBbs_Msg ")) FROM " TblBbs);
	rs.StepStrict();

	rs.get(0, x.m_Count);
	rs.get(1, x.m_Size);
}

#define TblBbs_InsFieldsListed TblBbs_Key "," TblBbs_Channel "," TblBbs_Time "," TblBbs_Msg "," TblBbs_Nonce
#define TblBbs_AllFieldsListed TblBbs_ID "," TblBbs_InsFieldsListed

void NodeDB::EnumBbsCSeq(WalkerBbs& x)
{
	x.m_Rs.Reset(*this, Query::BbsEnumCSeq, "SELECT " TblBbs_AllFieldsListed " FROM " TblBbs " WHERE " TblBbs_Channel "=? AND " TblBbs_ID ">? ORDER BY " TblBbs_ID);

	x.m_Rs.put(0, x.m_Data.m_Channel);
	x.m_Rs.put(1, x.m_ID);
}

uint64_t NodeDB::get_AutoincrementID(const char* szTable)
{
	Recordset rs(*this, Query::AutoincrementID, "SELECT seq FROM sqlite_sequence WHERE name=?");
	rs.put(0, szTable);
	if (!rs.Step())
		return 0;

	if (rs.IsNull(0))
		return 0;

	uint64_t id;
	rs.get(0, id);
	return id;
}

uint64_t NodeDB::get_BbsLastID()
{
	return get_AutoincrementID(TblBbs);
}

bool NodeDB::WalkerBbs::MoveNext()
{
	if (!m_Rs.Step())
		return false;
	m_Rs.get(0, m_ID);
	m_Rs.get(1, m_Data.m_Key);
	m_Rs.get(2, m_Data.m_Channel);
	m_Rs.get(3, m_Data.m_TimePosted);
	m_Rs.get(4, m_Data.m_Message);
	m_Rs.get(5, m_Data.m_Nonce); // don't care if NULL, would be 0

	return true;
}

bool NodeDB::BbsFind(WalkerBbs& x)
{
	x.m_Rs.Reset(*this, Query::BbsFind, "SELECT " TblBbs_AllFieldsListed " FROM " TblBbs " WHERE " TblBbs_Key "=?");

	x.m_Rs.put(0, x.m_Data.m_Key);
	return x.MoveNext();
}

uint64_t NodeDB::BbsFind(const WalkerBbs::Key& key)
{
	Recordset rs(*this,Query::BbsFindRaw, "SELECT rowid FROM " TblBbs " WHERE " TblBbs_Key "=?");
	rs.put(0, key);
	if (!rs.Step())
		return 0;

	uint64_t id;
	rs.get(0, id);
	return id;
}

void NodeDB::BbsDel(uint64_t id)
{
	Recordset rs(*this, Query::BbsDel, "DELETE FROM " TblBbs " WHERE " TblBbs_ID "=?");
	rs.put(0, id);
	rs.Step();
	TestChanged1Row();
}

uint64_t NodeDB::BbsIns(const WalkerBbs::Data& d)
{
	Recordset rs(*this, Query::BbsIns, "INSERT INTO " TblBbs "(" TblBbs_InsFieldsListed ") VALUES(?,?,?,?,?)");
	rs.put(0, d.m_Key);
	rs.put(1, d.m_Channel);
	rs.put(2, d.m_TimePosted);
	rs.put(3, d.m_Message);
	rs.put(4, d.m_Nonce);

	rs.Step();
	TestChanged1Row();

	return sqlite3_last_insert_rowid(m_pDb);
}

uint64_t NodeDB::BbsFindCursor(Timestamp t)
{
	Recordset rs(*this, Query::BbsFindCursor, "SELECT " TblBbs_ID " FROM " TblBbs " WHERE " TblBbs_Time ">=? ORDER BY " TblBbs_ID " ASC LIMIT 1");
	rs.put(0, t);

	if (!rs.Step())
		return get_BbsLastID() + 1;

	if (rs.IsNull(0))
		return 0;

	uint64_t id;
	rs.get(0, id);
	return id;
}

Timestamp NodeDB::get_BbsMaxTime()
{
	Recordset rs(*this, Query::BbsMaxTime, "SELECT MAX(" TblBbs_Time ") FROM " TblBbs);
	if (!rs.Step() || rs.IsNull(0))
		return 0;

	Timestamp ret;
	rs.get(0, ret); // to call, NULL is converted to numeric 0
	return ret;
}

} // namespace beam
