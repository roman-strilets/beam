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

bool NodeDB::ContractDataFind(const Blob& key, Blob& data, Recordset& rs)
{
	rs.Reset(*this, Query::ContractDataFind, "SELECT " TblContracts_Value " FROM " TblContracts " WHERE " TblContracts_Key "=?");
	rs.put(0, key);
	if (!rs.Step())
		return false;

	rs.get(0, data);
	return true;
}

bool NodeDB::ContractDataFindNext(Blob& key, Recordset& rs)
{
	rs.Reset(*this, Query::ContractDataFindNext, "SELECT " TblContracts_Key " FROM " TblContracts " WHERE " TblContracts_Key ">? ORDER BY " TblContracts_Key " ASC LIMIT 1");
	rs.put(0, key);
	if (!rs.Step())
		return false;

	rs.get(0, key);
	return true;
}

bool NodeDB::ContractDataFindPrev(Blob& key, Recordset& rs)
{
	rs.Reset(*this, Query::ContractDataFindPrev, "SELECT " TblContracts_Key " FROM " TblContracts " WHERE " TblContracts_Key "<? ORDER BY " TblContracts_Key " DESC LIMIT 1");
	rs.put(0, key);
	if (!rs.Step())
		return false;

	rs.get(0, key);
	return true;
}

void NodeDB::ContractDataInsert(const Blob& key, const Blob& data)
{
	Recordset rs(*this, Query::ContractDataInsert, "INSERT INTO " TblContracts " (" TblContracts_Key "," TblContracts_Value ") VALUES(?,?)");
	rs.put(0, key);
	rs.put(1, data);

	rs.Step();
	TestChanged1Row();
}

void NodeDB::ContractDataUpdate(const Blob& key, const Blob& data)
{
	Recordset rs(*this, Query::ContractDataUpdate, "UPDATE " TblContracts " SET " TblContracts_Value "=? WHERE " TblContracts_Key "=?");
	rs.put(0, data);
	rs.put(1, key);

	rs.Step();
	TestChanged1Row();
}

void NodeDB::ContractDataDel(const Blob& key)
{
	Recordset rs(*this, Query::ContractDataDel, "DELETE FROM " TblContracts " WHERE " TblContracts_Key "=?");
	rs.put(0, key);

	rs.Step();
	TestChanged1Row();
}

void NodeDB::ContractDataDelAll()
{
	Recordset rs(*this, Query::ContractDataDelAll, "DELETE FROM " TblContracts);
	rs.Step();
}

void NodeDB::ContractDataEnum(WalkerContractData& wlk, const Blob& keyMin, const Blob& keyMax)
{
	wlk.m_Rs.Reset(*this, Query::ContractDataEnum, "SELECT " TblContracts_Key "," TblContracts_Value " FROM " TblContracts " WHERE " TblContracts_Key ">=? AND " TblContracts_Key "<=? ORDER BY " TblContracts_Key);
	wlk.m_Rs.put(0, keyMin);
	wlk.m_Rs.put(1, keyMax);
}

void NodeDB::ContractDataEnum(WalkerContractData& wlk)
{
	wlk.m_Rs.Reset(*this, Query::ContractDataEnumAll, "SELECT " TblContracts_Key "," TblContracts_Value " FROM " TblContracts " ORDER BY " TblContracts_Key);
}

bool NodeDB::WalkerContractData::MoveNext()
{
	if (!m_Rs.Step())
		return false;

	m_Rs.get(0, m_Key);
	m_Rs.get(1, m_Val);
	return true;
}

void NodeDB::HeightPosPacked::put(NodeDB::Recordset& rs, int iCol, const HeightPos& pos)
{
	m_Height = pos.m_Height;
	m_Idx = pos.m_Pos;
	rs.put(iCol, Blob(this, sizeof(*this)));
}

void NodeDB::HeightPosPacked::get(NodeDB::Recordset& rs, int iCol, HeightPos& pos)
{
	const auto& x = rs.get_As<HeightPosPacked>(iCol);
	x.m_Height.Export(pos.m_Height);
	x.m_Idx.Export(pos.m_Pos);
}

void NodeDB::ContractLogInsert(const ContractLog::Entry& x)
{
	Recordset rs(*this, Query::ContractLogInsert, "INSERT INTO " TblContractLogs " (" TblContractLogs_Pos "," TblContractLogs_Key "," TblContractLogs_Data ") VALUES(?,?,?)");

	HeightPosPacked buf;
	buf.put(rs, 0, x.m_Pos);

	rs.put(1, x.m_Key);
	rs.put(2, x.m_Val);

	rs.Step();
	TestChanged1Row();
}

void NodeDB::ContractLogDel(const HeightPos& posMin, const HeightPos& posMax)
{
	Recordset rs(*this, Query::ContractLogDel, "DELETE FROM " TblContractLogs " WHERE " TblContractLogs_Pos " BETWEEN ? AND ?");

	HeightPosPacked bufMin, bufMax;
	bufMin.put(rs, 0, posMin);
	bufMax.put(rs, 1, posMax);

	rs.Step();
}

void NodeDB::ContractLogEnum(ContractLog::Walker& wlk, const HeightPos& posMin, const HeightPos& posMax)
{
	wlk.m_Rs.Reset(*this, Query::ContractLogEnum, "SELECT * FROM " TblContractLogs " WHERE " TblContractLogs_Pos " BETWEEN ? AND ? ORDER BY " TblContractLogs_Pos);

	wlk.m_bufMin.put(wlk.m_Rs, 0, posMin);
	wlk.m_bufMax.put(wlk.m_Rs, 1, posMax);
}

void NodeDB::ContractLogEnum(ContractLog::Walker& wlk, const Blob& keyMin, const Blob& keyMax, const HeightPos& posMin, const HeightPos& posMax)
{
	wlk.m_Rs.Reset(*this, Query::ContractLogEnumCid, "SELECT * FROM " TblContractLogs
		" WHERE (" TblContractLogs_Key " BETWEEN ? AND ?) AND (" TblContractLogs_Pos " BETWEEN ? AND ?) ORDER BY " TblContractLogs_Key "," TblContractLogs_Pos);
	wlk.m_Rs.put(0, keyMin);
	wlk.m_Rs.put(1, keyMax);

	wlk.m_bufMin.put(wlk.m_Rs, 2, posMin);
	wlk.m_bufMax.put(wlk.m_Rs, 3, posMax);
}

bool NodeDB::ContractLog::Walker::MoveNext()
{
	if (!m_Rs.Step())
		return false;

	HeightPosPacked::get(m_Rs, 0, m_Entry.m_Pos);
	m_Rs.get(1, m_Entry.m_Key);
	m_Rs.get(2, m_Entry.m_Val);
	return true;
}

void NodeDB::KrnInfoInsert(const KrnInfo::Entry& x)
{
	Recordset rs(*this, Query::KrnInfoInsert, "INSERT INTO " TblKrnInfo " (" TblKrnInfo_Pos "," TblKrnInfo_Key "," TblKrnInfo_Data ") VALUES(?,?,?)");

	HeightPosPacked buf;
	buf.put(rs, 0, Cast::Reinterpret<HeightPos>(x.m_Pos));

	rs.put(1, x.m_Cid);
	rs.put(2, x.m_Val);

	rs.Step();
	TestChanged1Row();
}

void NodeDB::KrnInfoDelFrom(Height h)
{
	Recordset rs(*this, Query::KrnInfoDel, "DELETE FROM " TblKrnInfo " WHERE " TblKrnInfo_Pos ">=?");

	HeightPos posMin(h);
	HeightPosPacked bufMin;
	bufMin.put(rs, 0, posMin);

	rs.Step();
}

void NodeDB::KrnInfoEnum(KrnInfo::Walker& wlk, Height h)
{
	HeightPos pos0(h);
	HeightPos pos1(h, static_cast<uint32_t>(-1));

	KrnInfoEnum(wlk, pos0, pos1);
}

void NodeDB::KrnInfoEnum(KrnInfo::Walker& wlk, const HeightPos& posMin, const HeightPos& posMax)
{
	wlk.m_Rs.Reset(*this, Query::KrnInfoEnumN, "SELECT * FROM " TblKrnInfo " WHERE " TblKrnInfo_Pos " BETWEEN ? AND ? ORDER BY " TblKrnInfo_Pos);

	wlk.m_bufMin.put(wlk.m_Rs, 0, posMin);
	wlk.m_bufMax.put(wlk.m_Rs, 1, posMax);
}

void NodeDB::KrnInfoEnum(KrnInfo::Walker& wlk, const KrnInfo::Cid& cid, Height hMax)
{
	wlk.m_Rs.Reset(*this, Query::KrnInfoEnumCid, "SELECT * FROM " TblKrnInfo
		" WHERE (" TblKrnInfo_Key "=?) AND (" TblKrnInfo_Pos "<=?) ORDER BY " TblContractLogs_Pos " DESC");
	wlk.m_Rs.put(0, cid);
	wlk.m_bufMax.put(wlk.m_Rs, 1, HeightPos(hMax, static_cast<uint32_t>(-1)));
}

bool NodeDB::KrnInfo::Walker::MoveNext()
{
	if (!m_Rs.Step())
		return false;

	HeightPosPacked::get(m_Rs, 0, Cast::Reinterpret<HeightPos>(m_Entry.m_Pos));
	m_Rs.get_As(1, m_Entry.m_Cid);
	m_Rs.get(2, m_Entry.m_Val);
	return true;
}

} // namespace beam
