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

void NodeDB::InsertEvent(AccountIndex iAccount, const HeightPos& pos, const Blob& b, const Blob& key)
{
	Recordset rs(*this, Query::EventIns, "INSERT INTO " TblEvents "(" TblEvents_Pos "," TblEvents_Body "," TblEvents_Key "," TblEvents_Account ") VALUES (?,?,?,?)");

	HeightPosPacked buf;
	buf.put(rs, 0, pos);
	rs.put(1, b);
	rs.put(2, key);
	rs.put(3, iAccount);
	rs.Step();
	TestChanged1Row();
}

void NodeDB::DeleteEventsFrom(AccountIndex iAccount, Height h)
{
	Recordset rs(*this, Query::EventDelByHeight, "DELETE FROM " TblEvents " WHERE " TblEvents_Account "=? AND " TblEvents_Pos ">=?");
	rs.put(0, iAccount);

	HeightPosPacked buf;
	buf.put(rs, 1, HeightPos(h));

	rs.Step();
}

void NodeDB::EnumEvents(WalkerEvent& x, AccountIndex iAccount, Height hMin)
{
	x.m_Rs.Reset(*this, Query::EventEnum, "SELECT " TblEvents_Pos "," TblEvents_Body "," TblEvents_Key " FROM " TblEvents " WHERE " TblEvents_Account "=? AND " TblEvents_Pos ">=? ORDER BY " TblEvents_Pos " ASC");
	x.m_Rs.put(0, iAccount);

	x.m_bufPos.put(x.m_Rs, 1, HeightPos(hMin));
}

void NodeDB::FindEvents(WalkerEvent& x, AccountIndex iAccount, const Blob& key)
{
	x.m_Rs.Reset(*this, Query::EventFind, "SELECT " TblEvents_Pos "," TblEvents_Body "," TblEvents_Key " FROM " TblEvents " WHERE " TblEvents_Account "=? AND " TblEvents_Key "=? ORDER BY " TblEvents_Pos " DESC");
	x.m_Rs.put(0, iAccount);
	x.m_Rs.put(1, key);
}

bool NodeDB::WalkerEvent::MoveNext()
{
	if (!m_Rs.Step())
		return false;
	HeightPosPacked::get(m_Rs, 0, m_Pos);
	m_Rs.get(1, m_Body);
	if (m_Rs.IsNull(2))
		ZeroObject(m_Key);
	else
		m_Rs.get(2, m_Key);

	return true;
}

void NodeDB::InsertAccount(const WalkerAccount::DataPlus& d)
{
	Recordset rs(*this, Query::AccountIns, "INSERT INTO " TblAccounts "(" TblAccounts_Index "," TblAccounts_OwnerID "," TblAccounts_Serif "," TblAccounts_TxoHi ") VALUES(?,?,?,?)");
	rs.put(0, d.m_iAccount);
	rs.put(1, d.m_Owner);
	rs.put(2, d.m_Serif);
	rs.put(3, d.m_hTxoHi);
	rs.Step();
	TestChanged1Row();
}

void NodeDB::DeleteAccountOnly(AccountIndex iAccount)
{
	Recordset rs(*this, Query::AccountDel, "DELETE FROM " TblAccounts " WHERE " TblAccounts_Index "=?");
	rs.put(0, iAccount);
	rs.Step();
	TestChanged1Row();
}

void NodeDB::DeleteAccountWithEvents(AccountIndex iAccount)
{
	DeleteAccountOnly(iAccount);

	Recordset rs(*this, Query::EventDelByAccount, "DELETE FROM " TblEvents " WHERE " TblEvents_Account "=?");
	rs.put(0, iAccount);
	rs.Step();
}

void NodeDB::EnumAccounts(WalkerAccount& wlk)
{
	wlk.m_Rs.Reset(*this, Query::AccountEnum, "SELECT " TblAccounts_Index "," TblAccounts_OwnerID "," TblAccounts_Serif "," TblAccounts_TxoHi " FROM " TblAccounts " ORDER BY " TblAccounts_Index);
}

void NodeDB::SetAccountTxoHi(WalkerAccount::Data& d)
{
	Recordset rs(*this, Query::AccountUpdTxoHi, "UPDATE " TblAccounts " SET " TblAccounts_TxoHi "=? WHERE " TblAccounts_Index "=?");
	rs.put(0, d.m_hTxoHi);
	rs.put(1, d.m_iAccount);

	rs.Step();
	TestChanged1Row();
}

bool NodeDB::WalkerAccount::MoveNext()
{
	if (!m_Rs.Step())
		return false;

	m_Rs.get(0, m_Data.m_iAccount);
	m_Rs.get(1, m_Data.m_Owner);
	m_Rs.get(2, m_Data.m_Serif);
	m_Rs.get(3, m_Data.m_hTxoHi);
	return true;
}

} // namespace beam
