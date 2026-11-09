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

void NodeDB::InsertDummy(Height h, const Key::ID& kid)
{
	Recordset rs(*this, Query::DummyIns, "INSERT INTO " TblDummy "(" TblDummy_ID "," TblDummy_SpendHeight ") VALUES(?,?)");
	Key::ID::Packed p;
	rs.put(0, kid, p);
	rs.put(1, h);
	rs.Step();
	TestChanged1Row();
}

Height NodeDB::GetLowestDummy(Key::ID& kid)
{
	Recordset rs(*this, Query::DummyFindLowest, "SELECT " TblDummy_ID "," TblDummy_SpendHeight " FROM " TblDummy " ORDER BY " TblDummy_SpendHeight " ASC LIMIT 1");
	if (!rs.Step())
		return MaxHeight;

	Height h;
	rs.get(0, kid);
	rs.get(1, h);

	return h;
}

Height NodeDB::GetDummyHeight(const Key::ID& kid)
{
	Recordset rs(*this, Query::DummyFind, "SELECT " TblDummy_SpendHeight " FROM " TblDummy " WHERE " TblDummy_ID "=?");
	Key::ID::Packed p;
	rs.put(0, kid, p);
	if (!rs.Step())
		return MaxHeight;

	Height h;
	rs.get(0, h);
	return h;
}

void NodeDB::DeleteDummy(const Key::ID& kid)
{
	Recordset rs(*this, Query::DummyDel, "DELETE FROM " TblDummy " WHERE " TblDummy_ID "=?");
	Key::ID::Packed p;
	rs.put(0, kid, p);
	rs.Step();
	TestChanged1Row();
}

void NodeDB::SetDummyHeight(const Key::ID& kid, Height h)
{
	Recordset rs(*this, Query::DummyUpdHeight, "UPDATE " TblDummy " SET " TblDummy_SpendHeight "=? WHERE " TblDummy_ID "=?");
	rs.put(0, h);
	Key::ID::Packed p;
	rs.put(1, kid, p);
	rs.Step();
	TestChanged1Row();
}

void NodeDB::InsertKernel(const Blob& key, Height h)
{
	assert(h);

	Recordset rs(*this, Query::KernelIns, "INSERT INTO " TblKernels "(" TblKernels_Key "," TblKernels_Height ") VALUES(?,?)");
	rs.put(0, key);
	rs.put(1, h);
	rs.Step();
	TestChanged1Row();
}

void NodeDB::DeleteKernel(const Blob& key, Height h)
{
	assert(h);

	Recordset rs(*this, Query::KernelDel, "DELETE FROM " TblKernels " WHERE " TblKernels_Key "=? AND " TblKernels_Height "=?");
	rs.put(0, key);
	rs.put(1, h);
	rs.Step();

	uint32_t nRows = get_RowsChanged();
	if (!nRows)
		ThrowError("no krn");
	else
		// in the *very* unlikely case of kernel duplicate at the same height (!!!) - just re-insert it
		while (--nRows)
			InsertKernel(key, h);
}

Height NodeDB::FindKernel(const Blob& key)
{
	Recordset rs(*this, Query::KernelFind, "SELECT " TblKernels_Height " FROM " TblKernels " WHERE " TblKernels_Key "=? ORDER BY " TblKernels_Height " DESC LIMIT 1");
	rs.put(0, key);
	if (!rs.Step())
		return 0;

	Height h;
	rs.get(0, h);

	assert(h);
	return h;
}

void NodeDB::TxoAdd(TxoID id, const Blob& b)
{
	Recordset rs(*this, Query::TxoAdd, "INSERT INTO " TblTxo "(" TblTxo_ID "," TblTxo_Value ") VALUES(?,?)");
	rs.put(0, id);
	rs.put(1, b);
	rs.Step();
}

void NodeDB::TxoDel(TxoID id)
{
	Recordset rs(*this, Query::TxoDel, "DELETE FROM " TblTxo " WHERE " TblTxo_ID "=?");
	rs.put(0, id);
	rs.Step();
	TestChanged1Row();
}

void NodeDB::TxoDelFrom(TxoID id)
{
	Recordset rs(*this, Query::TxoDelFrom, "DELETE FROM " TblTxo " WHERE " TblTxo_ID ">=?");
	rs.put(0, id);
	rs.Step();
}

void NodeDB::TxoSetSpent(TxoID id, Height h)
{
	Recordset rs(*this, Query::TxoSetSpent, "UPDATE " TblTxo " SET " TblTxo_SpendHeight "=? WHERE " TblTxo_ID "=?");
	if (MaxHeight != h)
		rs.put(0, h);
	rs.put(1, id);

	rs.Step();
	TestChanged1Row();
}

void NodeDB::EnumTxos(WalkerTxo& wlk, TxoID id0)
{
	wlk.m_Rs.Reset(*this, Query::TxoEnum, "SELECT " TblTxo_ID "," TblTxo_Value "," TblTxo_SpendHeight " FROM " TblTxo " WHERE " TblTxo_ID ">=? ORDER BY " TblTxo_ID);
	wlk.m_Rs.put(0, id0);
}

bool NodeDB::WalkerTxo::MoveNext()
{
	if (!m_Rs.Step())
		return false;

	m_Rs.get(0, m_ID);
	m_Rs.get(1, m_Value);

	if (m_Rs.IsNull(2))
		m_SpendHeight = MaxHeight;
	else
		m_Rs.get(2, m_SpendHeight);

	return true;
}

void NodeDB::TxoSetValue(TxoID id, const Blob& v)
{
	Recordset rs(*this, Query::TxoSetValue, "UPDATE " TblTxo " SET " TblTxo_Value "=? WHERE " TblTxo_ID "=?");
	rs.put(0, v);
	rs.put(1, id);
	rs.Step();
	TestChanged1Row();
}

void NodeDB::TxoGetValue(WalkerTxo& wlk, TxoID id0)
{
	wlk.m_Rs.Reset(*this, Query::TxoGetValue, "SELECT " TblTxo_Value " FROM " TblTxo " WHERE " TblTxo_ID "=?");
	wlk.m_Rs.put(0, id0);

	wlk.m_Rs.StepStrict();
	wlk.m_Rs.get(0, wlk.m_Value);
}

void NodeDB::ShieldedOutpSet(Height h, uint64_t count)
{
	Recordset rs(*this, Query::ShieldedStatisticIns, "INSERT INTO " TblShieldedStatistic " (" TblShieldedStatistic_Height "," TblShieldedStatistic_OutCount ") VALUES(?,?)");
	rs.put(0, h);
	rs.put(1, count);
	rs.Step();
}

uint64_t NodeDB::ShieldedOutpGet(Height h)
{
	Recordset rs(*this, Query::ShieldedStatisticSel, "SELECT " TblShieldedStatistic_OutCount " FROM " TblShieldedStatistic " WHERE " TblShieldedStatistic_Height "<=? ORDER BY " TblShieldedStatistic_Height " DESC LIMIT 1");
	rs.put(0, h);

	if (!rs.Step())
		return 0;

	uint64_t ret;
	rs.get(0, ret);
	return ret;
}

void NodeDB::ShieldedOutpDelFrom(Height h)
{
	Recordset rs(*this, Query::ShieldedStatisticDel, "DELETE FROM " TblShieldedStatistic " WHERE " TblShieldedStatistic_Height ">=?");
	rs.put(0, h);
	rs.Step();
}

} // namespace beam
