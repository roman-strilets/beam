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

#define StateCvt_Fields(macro, sep) \
	macro(Number,		m_Number.v) sep \
	macro(HashPrev,		m_Prev) sep \
	macro(Timestamp,	m_TimeStamp) sep \
	macro(PoW,			m_PoW) sep \
	macro(ChainWork,	m_ChainWork) sep \
	macro(Kernels,		m_Kernels) sep \
	macro(Definition,	m_Definition)

#define THE_MACRO_NOP0
#define THE_MACRO_COMMA_S ","

void NodeDB::get_State(uint64_t rowid, Block::SystemState::Full& out)
{
#define THE_MACRO_1(dbname, extname) TblStates_##dbname
	Recordset rs(*this, Query::StateGet, "SELECT " StateCvt_Fields(THE_MACRO_1, THE_MACRO_COMMA_S) " FROM " TblStates " WHERE rowid=?");
#undef THE_MACRO_1

	rs.put(0, rowid);

	rs.StepStrict();

	int iCol = 0;

#define THE_MACRO_1(dbname, extname) rs.get(iCol++, out.extname);
	StateCvt_Fields(THE_MACRO_1, THE_MACRO_NOP0)
#undef THE_MACRO_1
}

uint64_t NodeDB::InsertState(const Block::SystemState::Full& s, const PeerID& peer)
{
	assert(s.m_Number.v);

	// Is there a prev? Is it a tip currently?
	Difficulty::Raw wrk = s.m_ChainWork - s.m_PoW.m_Difficulty;

	Recordset rs(*this, Query::StateFind2, "SELECT rowid," TblStates_CountNext " FROM " TblStates " WHERE " TblStates_Number "=? AND " TblStates_Hash "=? AND " TblStates_ChainWork "=?");
	rs.put(0, s.m_Number.v - 1);
	rs.put(1, s.m_Prev);
	rs.put(2, wrk);

	uint32_t nPrevCountNext = 0; // initialized to suppress warning, not really needed
	uint64_t rowPrev;

	if (rs.Step())
	{
		rs.get(0, rowPrev);
		rs.get(1, nPrevCountNext);
	}
	else
		rowPrev = 0;

	Merkle::Hash hash;
	s.get_Hash(hash);

	// Insert row

#define THE_MACRO_1(dbname, extname) TblStates_##dbname ","
#define THE_MACRO_2(dbname, extname) "?,"

	rs.Reset(*this, Query::StateIns, "INSERT INTO " TblStates
		" (" TblStates_Hash "," StateCvt_Fields(THE_MACRO_1, THE_MACRO_NOP0) TblStates_Flags "," TblStates_CountNext "," TblStates_CountNextF "," TblStates_RowPrev "," TblStates_Peer ")"
		" VALUES(?," StateCvt_Fields(THE_MACRO_2, THE_MACRO_NOP0) "0,0,?,?,?)");

#undef THE_MACRO_1
#undef THE_MACRO_2

	int iCol = 0;
	rs.put(iCol++, hash);

#define THE_MACRO_1(dbname, extname) rs.put(iCol++, s.extname);
	StateCvt_Fields(THE_MACRO_1, THE_MACRO_NOP0)
#undef THE_MACRO_1

	rs.put(iCol++, 0u); // next functional, update it later if needed
	if (rowPrev)
		rs.put(iCol, rowPrev); // otherwise it'd be NULL
	iCol++;

	rs.put(iCol, peer);

	rs.Step();
	TestChanged1Row();

	uint64_t rowid = get_LastInsertRowID();
	assert(rowid);

	if (rowPrev)
	{
		SetNextCount(rowPrev, nPrevCountNext + 1);

		if (!nPrevCountNext)
			TipDel(rowPrev, Block::Number(s.m_Number.v - 1));
	}

	// Ancestors
	uint32_t nCountAncestors = 0, nCountNextF = 0;
	rs.Reset(*this, Query::StateGetNextOf, "SELECT rowid," TblStates_ChainWork "," TblStates_PoW "," TblStates_Flags " FROM " TblStates " WHERE " TblStates_Number "=? AND " TblStates_HashPrev "=?");
	rs.put(0, s.m_Number.v + 1);
	rs.put(1, hash);

	while (rs.Step())
	{
		auto& wrk1 = rs.get_As<Difficulty::Raw>(1);
		auto& pow1 = rs.get_As<Block::PoW>(2);

		if (s.m_ChainWork + pow1.m_Difficulty != wrk1)
			continue; // no match

		uint64_t rowNext;
		rs.get(0, rowNext);

		{
			Recordset rs2(*this, Query::StateUpdPrevRow, "UPDATE " TblStates " SET " TblStates_RowPrev "=? WHERE rowid=?");
			rs2.put(0, rowid);
			rs2.put(1, rowNext);
			rs2.Step();
			TestChanged1Row();
		}
		nCountAncestors++;

		uint32_t nFlags;
		rs.get(3, nFlags);

		if (StateFlags::Functional & nFlags)
			nCountNextF++;
	}
	
	if (nCountAncestors)
		SetNextCount(rowid, nCountAncestors);
	else
		TipAdd(rowid, s.m_Number);

	if (nCountNextF)
		SetNextCountFunctional(rowid, nCountNextF);

	return rowid;
}

void NodeDB::get_StateHash(uint64_t rowid, Merkle::Hash& hv)
{
	Recordset rs(*this, Query::StateGetHash, "SELECT " TblStates_Hash " FROM " TblStates " WHERE rowid=?");
	rs.put(0, rowid);

	rs.StepStrict();

	rs.get(0, hv);
}

bool NodeDB::DeleteState(uint64_t rowid, uint64_t& rowPrev)
{
	Recordset rs(*this, Query::StateGetNumberAndPrev, "SELECT "
		TblStates "." TblStates_Number ","
		TblStates "." TblStates_RowPrev ","
		TblStates "." TblStates_CountNext ","
		"prv." TblStates_CountNext ","
		TblStates "." TblStates_Flags ","
		"prv." TblStates_CountNextF
		" FROM " TblStates " LEFT JOIN " TblStates " prv ON " TblStates "." TblStates_RowPrev "=prv.rowid" " WHERE " TblStates ".rowid=?");

	rs.put(0, rowid);
	rs.StepStrict();

	if (rs.IsNull(1))
		rowPrev = 0;
	else
		rs.get(1, rowPrev);

	uint32_t nCountNext, nFlags, nCountPrevF;
	rs.get(2, nCountNext);
	if (nCountNext)
		return false;

	rs.get(4, nFlags);
	if (StateFlags::Active & nFlags)
		ThrowError("attempt to delete an active state");

	Block::Number num;
	rs.get(0, num.v);

	if (!rs.IsNull(1))
	{
		rs.get(3, nCountNext);
		if (!nCountNext)
			ThrowInconsistent();

		nCountNext--;

		SetNextCount(rowPrev, nCountNext);

		if (!nCountNext)
			TipAdd(rowPrev, Block::Number(num.v - 1));

		if (StateFlags::Functional & nFlags)
		{
			rs.get(5, nCountPrevF);

			if (!nCountPrevF)
				ThrowInconsistent();

			nCountPrevF--;
			SetNextCountFunctional(rowPrev, nCountPrevF);

			if (!nCountPrevF && (StateFlags::Reachable & nFlags))
				TipReachableAdd(rowPrev);

		}
	}

	TipDel(rowid, num);

	if (StateFlags::Reachable & nFlags)
		TipReachableDel(rowid);

	rs.Reset(*this, Query::StateDel, "DELETE FROM " TblStates " WHERE rowid=?");
	rs.put(0, rowid);

	rs.Step();
	TestChanged1Row();

	return true;
}

uint64_t NodeDB::StateFindSafe(const Block::SystemState::ID& k)
{
	Recordset rs(*this, Query::StateFind, "SELECT rowid FROM " TblStates " WHERE " TblStates_Number "=? AND " TblStates_Hash "=?");
	rs.put(0, k.m_Number.v);
	rs.put(1, k.m_Hash);
	if (!rs.Step())
		return 0;

	uint64_t rowid;
	rs.get(0, rowid);
	assert(rowid);
	return rowid;
}

uint64_t NodeDB::FindActiveStateStrict(Block::Number num)
{
	Recordset rs(*this, Query::StateFindWithFlag, "SELECT rowid FROM " TblStates " WHERE " TblStates_Number "=? AND (" TblStates_Flags " & ?)");
	rs.put(0, num.v);
	rs.put(1, StateFlags::Active);
	rs.StepStrict();

	uint64_t rowid;
	rs.get(0, rowid);
	assert(rowid);
	return rowid;
}

void NodeDB::FindActiveStateStrictLowBound(StateID& sid, const Difficulty::Raw& d)
{
	Recordset rs(*this, Query::StateFindWithFlagCWLB, "SELECT rowid," TblStates_Number " FROM " TblStates " WHERE " TblStates_ChainWork ">=? AND (" TblStates_Flags " & ?) LIMIT 1");
	rs.put(0, d);
	rs.put(1, StateFlags::Active);
	rs.StepStrict();

	rs.get(0, sid.m_Row);
	rs.get(1, sid.m_Number.v);
	assert(sid.m_Row);
}

void NodeDB::SetNextCount(uint64_t rowid, uint32_t n)
{
	Recordset rs(*this, Query::StateSetNextCount, "UPDATE " TblStates " SET " TblStates_CountNext "=? WHERE rowid=?");
	rs.put(0, n);
	rs.put(1, rowid);

	rs.Step();
	TestChanged1Row();
}

void NodeDB::SetNextCountFunctional(uint64_t rowid, uint32_t n)
{
	Recordset rs(*this, Query::StateSetNextCountF, "UPDATE " TblStates " SET " TblStates_CountNextF "=? WHERE rowid=?");
	rs.put(0, n);
	rs.put(1, rowid);

	rs.Step();
	TestChanged1Row();
}

void NodeDB::EnumSystemStatesBkwd(WalkerSystemState& x, const StateID& sid)
{
#define THE_MACRO_1(dbname, extname) TblStates_##dbname
	x.m_Rs.Reset(*this, Query::EnumSystemStatesBkwd,
		"SELECT rowid," TblStates_RowPrev "," StateCvt_Fields(THE_MACRO_1, THE_MACRO_COMMA_S)
		" FROM " TblStates " WHERE " TblStates_Number "<=? ORDER BY " TblStates_Number " DESC");
#undef THE_MACRO_1

	x.m_RowTrg = sid.m_Row;
	x.m_Rs.put(0, sid.m_Number.v);
}

bool NodeDB::WalkerSystemState::MoveNext()
{
	while (true)
	{
		if (!m_Rs.Step())
			return false;

		uint64_t rowID;
		m_Rs.get(0, rowID);

		if (m_RowTrg == rowID)
			break;
	}

	m_Rs.get(1, m_RowTrg);

	int iCol = 2;

#define THE_MACRO_1(dbname, extname) m_Rs.get(iCol++, m_State.extname);
	StateCvt_Fields(THE_MACRO_1, THE_MACRO_NOP0)
#undef THE_MACRO_1

	return true;
}

void NodeDB::TipAdd(uint64_t rowid, Block::Number num)
{
	Recordset rs(*this, Query::TipAdd, "INSERT INTO " TblTips " VALUES(?,?)");
	rs.put(0, num.v);
	rs.put(1, rowid);

	rs.Step();
}

void NodeDB::TipDel(uint64_t rowid, Block::Number num)
{
	Recordset rs(*this, Query::TipDel, "DELETE FROM " TblTips " WHERE " TblTips_Number "=? AND " TblTips_State "=?");
	rs.put(0, num.v);
	rs.put(1, rowid);

	rs.Step();
	TestChanged1Row();
}

void NodeDB::TipReachableAdd(uint64_t rowid)
{
	Difficulty::Raw wrk;
	get_ChainWork(rowid, wrk);

	Recordset rs(*this, Query::TipReachableAdd, "INSERT INTO " TblTipsReachable " VALUES(?,?)");
	rs.put(0, rowid);
	rs.put(1, wrk);

	rs.Step();
}

void NodeDB::TipReachableDel(uint64_t rowid)
{
	Recordset rs(*this, Query::TipReachableDel, "DELETE FROM " TblTipsReachable " WHERE " TblTips_State "=?");
	rs.put(0, rowid);

	rs.Step();
	TestChanged1Row();
}

void NodeDB::SetStateFunctional(uint64_t rowid)
{
	Recordset rs(*this, Query::StateGetNumberAndAux, "SELECT "
		TblStates "." TblStates_Number ","
		TblStates "." TblStates_RowPrev ","
		TblStates "." TblStates_Flags ","
		"prv." TblStates_Flags ","
		"prv." TblStates_CountNextF
		" FROM " TblStates " LEFT JOIN " TblStates " prv ON " TblStates "." TblStates_RowPrev "=prv.rowid" " WHERE " TblStates ".rowid=?");

	rs.put(0, rowid);
	rs.StepStrict();

	uint32_t nFlags, nFlagsPrev, nCountPrevF;
	rs.get(2, nFlags);
	if (StateFlags::Functional & nFlags)
		return; // ?!

	nFlags |= StateFlags::Functional;

	Block::Number num;
	rs.get(0, num.v);
	assert(num.v);

	uint64_t rowPrev = 0;

	if (num.v > 1)
	{
		if (!rs.IsNull(1))
		{
			rs.get(1, rowPrev);
			rs.get(3, nFlagsPrev);
			rs.get(4, nCountPrevF);

			SetNextCountFunctional(rowPrev, nCountPrevF + 1);

			if (StateFlags::Reachable & nFlagsPrev)
			{
				nFlags |= StateFlags::Reachable;

				if (!nCountPrevF)
					TipReachableDel(rowPrev);
			}
		}

	} else
	{
		assert(rs.IsNull(1));
		nFlags |= StateFlags::Reachable;
	}

	SetFlags(rowid, nFlags);

	if (StateFlags::Reachable & nFlags)
		OnStateReachable(rowid, rowPrev, num, true);
}

void NodeDB::SetStateNotFunctional(uint64_t rowid)
{
	Recordset rs(*this, Query::StateGetFlags1, "SELECT "
		TblStates "." TblStates_Number ","
		TblStates "." TblStates_RowPrev ","
		TblStates "." TblStates_Flags ","
		"prv." TblStates_CountNextF
		" FROM " TblStates " LEFT JOIN " TblStates " prv ON " TblStates "." TblStates_RowPrev "=prv.rowid" " WHERE " TblStates ".rowid=?");

	rs.put(0, rowid);
	rs.StepStrict();

	uint32_t nFlags, nCountPrevF;
	rs.get(2, nFlags);

	if (!(StateFlags::Functional & nFlags))
		return; // ?!
	nFlags &= ~StateFlags::Functional;

	Block::Number num;
	rs.get(0, num.v);
	assert(num.v);

	uint64_t rowPrev = 0;

	bool bReachable = (StateFlags::Reachable & nFlags) != 0;
	if (bReachable)
		nFlags &= ~StateFlags::Reachable;

	if (num.v > 1)
	{
		if (rs.IsNull(1))
			assert(!bReachable); // orphan
		else
		{
			rs.get(1, rowPrev);
			rs.get(3, nCountPrevF);

			if (!nCountPrevF)
				ThrowInconsistent();

			nCountPrevF--;
			SetNextCountFunctional(rowPrev, nCountPrevF);

			if (!nCountPrevF && bReachable)
				TipReachableAdd(rowPrev);
		}
	} else
		assert(rs.IsNull(1) && bReachable);

	SetFlags(rowid, nFlags);

	if (bReachable)
		OnStateReachable(rowid, rowPrev, num, false);
}

void NodeDB::OnStateReachable(uint64_t rowid, uint64_t rowPrev, Block::Number num, bool b)
{
	typedef std::pair<uint64_t, uint32_t> RowAndFlags;
	std::vector<RowAndFlags> rows;

	while (true)
	{
		rowPrev = rowid;

		{
			Recordset rs(*this, Query::StateGetNextFunctional, "SELECT rowid," TblStates_Flags " FROM " TblStates " WHERE " TblStates_Number "=? AND " TblStates_RowPrev "=? AND (" TblStates_Flags " & ?)");
			rs.put(0, num.v + 1);
			rs.put(1, rowid);
			rs.put(2, StateFlags::Functional);

			while (rs.Step())
			{
				rs.get(0, rowid);
				uint32_t nFlags;
				rs.get(1, nFlags);
				assert(StateFlags::Functional & nFlags);
				assert(!(StateFlags::Reachable & nFlags) == b);
				rows.emplace_back(rowid, nFlags);
			}
		}

		if (rows.empty())
		{
			if (b)
				TipReachableAdd(rowid);
			else
				TipReachableDel(rowid);

			break;
		}

		for (size_t i = 0; i < rows.size(); i++)
			SetFlags(rows[i].first, rows[i].second ^ StateFlags::Reachable);

		rowid = rows[0].first;
		num.v++;

		for (size_t i = 1; i < rows.size(); i++)
			OnStateReachable(rows[i].first, rowPrev, num, b);

		rows.clear();
	}
}

void NodeDB::assert_valid()
{
	[[maybe_unused]] uint32_t nTips = 0, nTipsReachable = 0;

	Recordset rs(*this, Query::Dbg0, "SELECT "
		TblStates ".rowid,"
		TblStates "." TblStates_Number ","
		TblStates "." TblStates_Flags ","
		TblStates "." TblStates_RowPrev ","
		TblStates "." TblStates_CountNext ","
		TblStates "." TblStates_CountNextF ","
		"prv.rowid,"
		"prv." TblStates_Flags
		" FROM " TblStates " LEFT JOIN " TblStates " prv ON (" TblStates "." TblStates_Number "=prv." TblStates_Number "+1) AND (" TblStates "." TblStates_HashPrev "=prv." TblStates_Hash ")");

	while (rs.Step())
	{
		uint64_t rowid, rowPrev, rowPrev2;
		uint32_t nFlags, nFlagsPrev, nNext, nNextF;
		Block::Number num;

		rs.get(0, rowid);
		rs.get(1, num.v);
		rs.get(2, nFlags);
		rs.get(4, nNext);
		rs.get(5, nNextF);

		if (StateFlags::Reachable & nFlags)
			assert(StateFlags::Functional & nFlags);

		assert(rs.IsNull(3) == rs.IsNull(6));
		if (!rs.IsNull(3))
		{
			rs.get(3, rowPrev);
			rs.get(6, rowPrev2);
			rs.get(7, nFlagsPrev);
			assert(rowPrev == rowPrev2);

			if (StateFlags::Reachable & nFlags)
				assert(StateFlags::Reachable & nFlagsPrev);
			else
				if (StateFlags::Functional & nFlags)
					assert(!(StateFlags::Reachable & nFlagsPrev));


		} else
		{
			if (StateFlags::Reachable & nFlags)
				assert(num.v == 1u);
		}

		assert(nNext >= nNextF);

		if (!nNext)
			nTips++;

		if (!nNextF && (StateFlags::Reachable & nFlags))
			nTipsReachable++;
	}
	
	rs.Reset(*this, Query::Dbg1, "SELECT "
		TblTips "." TblTips_Number ","
		TblStates "." TblStates_Number ","
		TblStates "." TblStates_CountNext
		" FROM " TblTips " LEFT JOIN " TblStates " ON " TblTips "." TblTips_State "=" TblStates ".rowid");

	for (; rs.Step(); nTips--)
	{
		Block::Number num0, num1;
		rs.get(0, num0.v);
		rs.get(1, num1.v);
		assert(num0.v == num1.v);

		uint32_t nNext;
		rs.get(2, nNext);
		assert(!nNext);
	}

	assert(!nTips);

	rs.Reset(*this, Query::Dbg2, "SELECT "
		TblStates "." TblStates_CountNextF ","
		TblStates "." TblStates_Flags
		" FROM " TblTipsReachable " LEFT JOIN " TblStates " ON " TblTipsReachable "." TblTips_State "=" TblStates ".rowid");

	for (; rs.Step(); nTipsReachable--)
	{
		uint32_t nNextF, nFlags;
		rs.get(0, nNextF);
		rs.get(1, nFlags);
		assert(!nNextF);
		assert(StateFlags::Reachable & nFlags);
	}

	assert(!nTipsReachable);

	rs.Reset(*this, Query::Dbg3, "SELECT "
		TblStates ".rowid," TblStates "." TblStates_CountNext ",COUNT(nxt.rowid) FROM " TblStates
		" LEFT JOIN " TblStates " nxt ON (" TblStates "." TblStates_Number "=nxt." TblStates_Number "-1) AND (" TblStates "." TblStates_Hash "=nxt." TblStates_HashPrev ")"
		"GROUP BY " TblStates ".rowid");

	while (rs.Step())
	{
		uint64_t rowid;
		uint32_t n0, n1;
		rs.get(0, rowid);
		rs.get(1, n0);
		rs.get(2, n1);
		assert(n0 == n1);
	}

	rs.Reset(*this, Query::Dbg4, "SELECT "
		TblStates ".rowid," TblStates "." TblStates_CountNextF ",COUNT(nxt.rowid) FROM " TblStates
		" LEFT JOIN " TblStates " nxt ON (" TblStates "." TblStates_Number "=nxt." TblStates_Number "-1) AND (" TblStates "." TblStates_Hash "=nxt." TblStates_HashPrev ") AND (nxt." TblStates_Flags " & 1) "
		"GROUP BY " TblStates ".rowid");

	while (rs.Step())
	{
		uint64_t rowid;
		uint32_t n0, n1;
		rs.get(0, rowid);
		rs.get(1, n0);
		rs.get(2, n1);
		assert(n0 == n1);
	}
}

void NodeDB::EnumTips(WalkerState& x)
{
	x.m_Rs.Reset(*this, Query::EnumTips, "SELECT " TblTips_Number "," TblTips_State " FROM " TblTips " ORDER BY "  TblTips_Number " ASC," TblTips_State " ASC");
}

void NodeDB::EnumFunctionalTips(WalkerState& x)
{
	x.m_Rs.Reset(*this, Query::EnumFunctionalTips, "SELECT "
		TblStates "." TblStates_Number ","
		TblStates ".rowid"
		" FROM " TblTipsReachable
		" LEFT JOIN " TblStates " ON (" TblTipsReachable "." TblTips_State "=" TblStates ".rowid) "
		" ORDER BY "  TblTipsReachable "." TblTips_ChainWork " DESC");
}

void NodeDB::EnumStatesAt(WalkerState& x, Block::Number num)
{
	x.m_Rs.Reset(*this, Query::EnumAtNumber, "SELECT " TblStates_Number ",rowid FROM " TblStates " WHERE " TblStates_Number "=? ORDER BY " TblStates_Hash);
	x.m_Rs.put(0, num.v);
}

bool NodeDB::WalkerState::MoveNext()
{
	if (!m_Rs.Step())
		return false;
	m_Rs.get(0, m_Sid.m_Number.v);
	m_Rs.get(1, m_Sid.m_Row);
	return true;
}

bool NodeDB::get_Prev(uint64_t& rowid)
{
	assert(rowid);
	Recordset rs(*this, Query::StateGetPrev, "SELECT " TblStates_RowPrev " FROM " TblStates " WHERE rowid=?");
	rs.put(0, rowid);

	rs.StepStrict();

	if (rs.IsNull(0))
		return false;

	rs.get(0, rowid);
	return true;
}

bool NodeDB::get_Prev(StateID& sid)
{
	if (!get_Prev(sid.m_Row))
		return false;

	sid.m_Number.v--;
	return true;
}

bool NodeDB::get_Cursor(StateID& sid)
{
	sid.m_Row = ParamIntGetDef(ParamID::CursorRow);
	if (!sid.m_Row)
	{
		sid.m_Number.v = 0;
		return false;
	}

	sid.m_Number.v = ParamIntGetDef(ParamID::CursorNumber);
	assert(sid.m_Number.v);
	return true;
}

void NodeDB::put_Cursor(const StateID& sid)
{
	ParamIntSet(ParamID::CursorRow, sid.m_Row);
	ParamIntSet(ParamID::CursorNumber, sid.m_Number.v);
}

void NodeDB::StateID::SetNull()
{
	m_Row = 0;
	m_Number.v = 0;
}

void NodeDB::MoveBack(StateID& sid)
{
	Recordset rs(*this, Query::Unactivate, "UPDATE " TblStates " SET " TblStates_Flags "=" TblStates_Flags " & ? WHERE rowid=?");
	rs.put(0, ~uint32_t(StateFlags::Active));
	rs.put(1, sid.m_Row);
	rs.Step();
	TestChanged1Row();

	if (!get_Prev(sid))
		sid.SetNull();

	put_Cursor(sid);
}

void NodeDB::MoveFwd(const StateID& sid)
{
	Recordset rs(*this, Query::Activate, "UPDATE " TblStates " SET " TblStates_Flags "=" TblStates_Flags " | ? WHERE rowid=?");
	rs.put(0, StateFlags::Active);
	rs.put(1, sid.m_Row);
	rs.Step();
	TestChanged1Row();

	put_Cursor(sid);
}

uint64_t NodeDB::FindStateWorkGreater(const Difficulty::Raw& d)
{
	Recordset rs(*this, Query::StateFindWorkGreater, "SELECT rowid FROM " TblStates " WHERE " TblStates_ChainWork ">? AND " TblStates_Flags "& ? != 0 ORDER BY " TblStates_ChainWork " ASC LIMIT 1");
	rs.put_As(0, d);
	rs.put(1, StateFlags::Active);

	rs.StepStrict();

	uint64_t res;
	rs.get(0, res);
	return res;
}

} // namespace beam
