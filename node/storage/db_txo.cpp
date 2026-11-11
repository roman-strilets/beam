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

	bool bWnd = KrnWndEnsure(); // before the insert, the reload (if needed) must not see the new row

	Recordset rs(*this, Query::KernelIns, "INSERT INTO " TblKernels "(" TblKernels_Key "," TblKernels_Height ") VALUES(?,?)");
	rs.put(0, key);
	rs.put(1, h);
	rs.Step();
	TestChanged1Row();

	if (bWnd)
	{
		m_KrnWnd.Add(KrnWndKey(key), h, get_LastInsertRowID());
		if (h > m_KrnWnd.m_dh)
			m_KrnWnd.Raise(h - m_KrnWnd.m_dh);
	}
}

void NodeDB::DeleteKernel(const Blob& key, Height h)
{
	assert(h);

	if (KrnWndEnsure())
	{
		auto r = m_KrnWnd.m_mapKey.equal_range(KrnWndKey(key));
		for (auto it = r.first; r.second != it; ++it)
		{
			if (it->second.m_Height != h)
				continue;

			Recordset rs(*this, Query::KernelDelByRowID, "DELETE FROM " TblKernels " WHERE rowid=?");
			rs.put(0, it->second.m_RowID);
			rs.Step();
			TestChanged1Row();

			m_KrnWnd.Erase(it);
			return;
		}

		if (h >= m_KrnWnd.m_hLo)
			ThrowError("no krn");

		// deep rollback, below the window. Would be a table scan per kernel without the index
		BEAM_LOG_INFO() << "Kernel deleted below the in-memory window (" << h << " < " << m_KrnWnd.m_hLo << ")";
		KernelIndexRestore();
	}

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

Height NodeDB::FindKernel(const Blob& key, Height hMin /* = 0 */)
{
	if (KrnWndEnsure())
	{
		Height hRet = 0;

		auto r = m_KrnWnd.m_mapKey.equal_range(KrnWndKey(key));
		for (auto it = r.first; r.second != it; ++it)
			setmax(hRet, it->second.m_Height);

		if (hRet)
			return hRet; // the window has all the kernels above its bottom, so this is the largest one

		if (hMin >= m_KrnWnd.m_hLo)
			return 0; // older kernels are of no interest

		m_KrnWnd.m_nScans++; // no index, the query below scans the whole table
	}

	Recordset rs(*this, Query::KernelFind, "SELECT " TblKernels_Height " FROM " TblKernels " WHERE " TblKernels_Key "=? ORDER BY " TblKernels_Height " DESC LIMIT 1");
	rs.put(0, key);
	if (!rs.Step())
		return 0;

	Height h;
	rs.get(0, h);

	assert(h);
	return h;
}

bool NodeDB::IsKernelIndexed()
{
	Recordset rs(*this, Query::KernelIndexExists, "SELECT 1 FROM sqlite_master WHERE type='index' AND name='" IdxKernels "'");
	return rs.Step();
}

void NodeDB::KernelIndexSuspend(Height hLo, Height dh)
{
	if (IsKernelIndexed())
	{
		BEAM_LOG_INFO() << "Dropping kernel index";
		ExecQuick("DROP INDEX [" IdxKernels "]");
	}

	m_KrnWnd.Clear();
	m_KrnWnd.m_hLo = hLo;
	m_KrnWnd.m_dh = dh;
	m_KrnWnd.m_nScans = 0;

	KrnWndLoad();
}

void NodeDB::KernelIndexRestore()
{
	m_KrnWnd.Clear();

	if (IsKernelIndexed())
		return;

	BEAM_LOG_INFO() << "Building kernel index... (lookups without the index: " << m_KrnWnd.m_nScans << ")";
	auto t0 = std::chrono::steady_clock::now();

	ExecQuick(IdxKernels_Create);

	auto dt = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0);
	BEAM_LOG_INFO() << "Kernel index built in " << dt.count() << " ms";
}

void NodeDB::KrnWndLoad()
{
	// full table scan, done once on start (or after tx rollback)
	m_KrnWnd.m_mapKey.clear();
	m_KrnWnd.m_mapHeight.clear();

	Recordset rs(*this, Query::KernelEnumFrom, "SELECT rowid," TblKernels_Key "," TblKernels_Height " FROM " TblKernels " WHERE " TblKernels_Height ">=?");
	rs.put(0, m_KrnWnd.m_hLo);

	while (rs.Step())
	{
		uint64_t rowid;
		Merkle::Hash hv;
		Height h;

		rs.get(0, rowid);
		rs.get(1, hv);
		rs.get(2, h);

		m_KrnWnd.Add(hv, h, rowid);
	}

	m_KrnWnd.m_On = true;
	m_KrnWnd.m_Stale = false;

	BEAM_LOG_INFO() << "Kernel index suspended, kernels in memory: " << m_KrnWnd.m_mapKey.size() << ", from height " << m_KrnWnd.m_hLo;
}

bool NodeDB::KrnWndEnsure()
{
	if (m_KrnWnd.m_Stale)
	{
		m_KrnWnd.m_Stale = false;

		if (IsKernelIndexed())
			m_KrnWnd.Clear();
		else
			KrnWndLoad();
	}

	return m_KrnWnd.m_On;
}

void NodeDB::OnTxRolledBack()
{
	// the window may contain the rolled-back changes (or the suspend/restore itself may be rolled back)
	if (m_KrnWnd.m_dh)
		m_KrnWnd.m_Stale = true;
}

const Merkle::Hash& NodeDB::KrnWndKey(const Blob& key)
{
	if (sizeof(Merkle::Hash) != key.n)
		ThrowError("krn key size");

	return *reinterpret_cast<const Merkle::Hash*>(key.p);
}

void NodeDB::KrnWnd::Clear()
{
	m_mapHeight.clear();
	m_mapKey.clear();
	m_On = false;
	m_Stale = false;
}

void NodeDB::KrnWnd::Add(const Merkle::Hash& key, Height h, uint64_t rowid)
{
	if (h < m_hLo)
		return; // the window invariant is only about the kernels at hLo and above

	auto itK = m_mapKey.emplace(key, Item{ h, rowid });
	m_mapHeight.emplace(std::make_pair(h, rowid), itK);
}

void NodeDB::KrnWnd::Erase(MapKey::iterator itK)
{
	m_mapHeight.erase(std::make_pair(itK->second.m_Height, itK->second.m_RowID));
	m_mapKey.erase(itK);
}

void NodeDB::KrnWnd::Raise(Height hLo)
{
	if (hLo <= m_hLo)
		return;

	m_hLo = hLo;

	while (!m_mapHeight.empty())
	{
		auto it = m_mapHeight.begin();
		if (it->first.first >= hLo)
			break;

		m_mapKey.erase(it->second);
		m_mapHeight.erase(it);
	}
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
