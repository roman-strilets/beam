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

void NodeDB::set_Peer(uint64_t rowid, const PeerID* pPeer)
{
	Recordset rs(*this, Query::StateSetPeer, "UPDATE " TblStates " SET " TblStates_Peer "=? WHERE rowid=?");
	if (pPeer)
		rs.put_As(0, *pPeer);
	rs.put(1, rowid);
	rs.Step();
	TestChanged1Row();
}

bool NodeDB::get_Peer(uint64_t rowid, PeerID& peer)
{
	Recordset rs(*this, Query::StateGetPeer, "SELECT " TblStates_Peer " FROM " TblStates " WHERE rowid=?");
	rs.put(0, rowid);
	rs.StepStrict();

	if (rs.IsNull(0))
		return false;

	rs.get_As(0, peer);

	return true;
}

uint32_t NodeDB::get_StateExtra(uint64_t rowid, void* pOut, uint32_t nSize)
{
	Recordset rs(*this, Query::StateGetExtra, "SELECT " TblStates_Extra " FROM " TblStates " WHERE rowid=?");
	rs.put(0, rowid);
	rs.StepStrict();

	memset0(pOut, nSize);

	if (rs.IsNull(0))
		return 0;

	// the actual data size may be less than requested
	Blob b;
	rs.get(0, b);

	memcpy(pOut, b.p, std::min(b.n, nSize));
	return b.n;
}

void NodeDB::set_StateInputs(uint64_t rowid, StateInput* p, size_t n)
{
	Recordset rs(*this, Query::StateSetInputs, "UPDATE " TblStates " SET " TblStates_Inputs "=? WHERE rowid=?");
	if (n)
		rs.put(0, Blob(p, static_cast<uint32_t>(sizeof(StateInput) * n)));
	rs.put(1, rowid);
	rs.Step();
	TestChanged1Row();
}


bool NodeDB::get_StateInputs(uint64_t rowid, std::vector<StateInput>& v)
{
	Recordset rs(*this, Query::StateGetInputs, "SELECT " TblStates_Inputs " FROM " TblStates " WHERE rowid=?");
	rs.put(0, rowid);
	rs.StepStrict();

	Blob blob;
	rs.get(0, blob); // if NULL empty blob will be returned

	get_StateInputs(blob, v);
	return !v.empty();
}

void NodeDB::get_StateInputs(const Blob& blob, std::vector<StateInput>& v)
{
	v.resize(blob.n / sizeof(StateInput));
	if (!v.empty())
		memcpy(&v.front(), blob.p, v.size() * sizeof(StateInput)); // don't use blob size, it may be bigger if blob.n isn't multiple of sizeof(StateInput)
}

void NodeDB::StateInput::Set(TxoID id, const ECC::Point& pt)
{
	Set(id, pt.m_X, pt.m_Y);
}

void NodeDB::StateInput::Set(TxoID id, const ECC::uintBig& x, uint8_t y)
{
	m_Txo_AndY = id;
	m_CommX = x;
	if (y)
		m_Txo_AndY |= s_Y;
}

TxoID NodeDB::StateInput::get_ID() const
{
	return m_Txo_AndY & ~s_Y;
}

void NodeDB::StateInput::Get(ECC::Point& pt) const
{
	pt.m_X = m_CommX;
	pt.m_Y = (s_Y & m_Txo_AndY) ? 1 : 0;
}

bool NodeDB::StateInput::IsLess(const StateInput& x1, const StateInput& x2)
{
	ECC::Point pt1, pt2;
	x1.Get(pt1);
	x2.Get(pt2);
	return pt1 < pt2;
}

void NodeDB::set_StateTxosAndExtra(uint64_t rowid, const TxoID* pId, const Blob* pExtra, const Blob* pRB)
{
	Recordset rs(*this, Query::StateSetTxosAndExtra, "UPDATE " TblStates " SET " TblStates_Txos "=?," TblStates_Extra "=?," TblStates_Rollback "=? WHERE rowid=?");
	if (pId)
		rs.put(0, *pId);
	if (pExtra)
		rs.put(1, *pExtra);
	if (pRB)
		rs.put(2, *pRB);
	rs.put(3, rowid);
	rs.Step();
	TestChanged1Row();
}

void NodeDB::set_StateRB(uint64_t rowid, const Blob& rb)
{
	Recordset rs(*this, Query::StateSetRB, "UPDATE " TblStates " SET " TblStates_Rollback "=? WHERE rowid=?");
	rs.put(0, rb);
	rs.put(1, rowid);
	rs.Step();
	TestChanged1Row();
}

void NodeDB::set_StateExtra(uint64_t rowid, const Blob* pExtra)
{
	Recordset rs(*this, Query::StateSetExtra, "UPDATE " TblStates " SET " TblStates_Extra "=? WHERE rowid=?");
	if (pExtra)
		rs.put(0, *pExtra);
	rs.put(1, rowid);
	rs.Step();
	TestChanged1Row();
}

TxoID NodeDB::get_StateTxos(uint64_t rowid)
{
	Recordset rs(*this, Query::StateGetTxos, "SELECT " TblStates_Txos " FROM " TblStates " WHERE rowid=?");
	rs.put(0, rowid);
	rs.StepStrict();

	if (rs.IsNull(0))
		return MaxHeight;

	TxoID id;
	rs.get(0, id);
	return id;
}

TxoID NodeDB::FindStateByTxoID(StateID& sid, TxoID id0)
{
	Recordset rs(*this, Query::StateFindByTxos, "SELECT rowid," TblStates_Number "," TblStates_Txos " FROM " TblStates
		" WHERE " TblStates_Txos ">? AND " TblStates_Flags "& ? != 0  ORDER BY " TblStates_Txos " ASC LIMIT 1");
	rs.put(0, id0);
	rs.put(1, StateFlags::Active);
	rs.StepStrict();

	rs.get(0, sid.m_Row);
	rs.get(1, sid.m_Number.v);
	rs.get(2, id0);

	return id0;
}

void NodeDB::SetStateBlock(uint64_t rowid, const Blob& bodyP, const Blob& bodyE, const PeerID& peer)
{
	Recordset rs(*this, Query::StateSetBlock, "UPDATE " TblStates " SET " TblStates_BodyP "=?," TblStates_BodyE "=?," TblStates_Peer "=? WHERE rowid=?");
	if (bodyP.n)
		rs.put(0, bodyP);
	if (bodyE.n)
		rs.put(1, bodyE);
	rs.put(2, peer);
	rs.put(3, rowid);

	rs.Step();
	TestChanged1Row();
}

void NodeDB::GetStateBlock(uint64_t rowid, ByteBuffer* pP, ByteBuffer* pE, ByteBuffer* pRB)
{
	Recordset rs(*this, Query::StateGetBlock, "SELECT " TblStates_BodyP "," TblStates_BodyE "," TblStates_Rollback " FROM " TblStates " WHERE rowid=?");
	rs.put(0, rowid);
	rs.StepStrict();

	if (pP && !rs.IsNull(0))
		rs.get(0, *pP);
	if (pE && !rs.IsNull(1))
		rs.get(1, *pE);
	if (pRB && !rs.IsNull(2))
		rs.get(2, *pRB);
}

void NodeDB::DelStateBlockPP(uint64_t rowid)
{
	Recordset rs(*this, Query::StateDelBlockPP, "UPDATE " TblStates " SET " TblStates_BodyP "=NULL," TblStates_Peer "=NULL WHERE rowid=?");
	rs.put(0, rowid);
	rs.Step();
	TestChanged1Row();
}

void NodeDB::DelStateBlockPPR(uint64_t rowid)
{
	Recordset rs(*this, Query::StateDelBlockPPR, "UPDATE " TblStates " SET " TblStates_BodyP "=NULL," TblStates_Rollback "=NULL," TblStates_Peer "=NULL WHERE rowid=?");
	rs.put(0, rowid);
	rs.Step();
	TestChanged1Row();
}

void NodeDB::DelStateBlockAll(uint64_t rowid)
{
	Recordset rs(*this, Query::StateDelBlockAll, "UPDATE " TblStates
		" SET " TblStates_BodyP "=NULL," TblStates_BodyE "=NULL," TblStates_Rollback "=NULL," TblStates_Peer "=NULL," TblStates_Extra "=NULL," TblStates_Txos "=NULL WHERE rowid=?");
	rs.put(0, rowid);
	rs.Step();
	TestChanged1Row();
}

void NodeDB::SetFlags(uint64_t rowid, uint32_t n)
{
	Recordset rs(*this, Query::StateSetFlags, "UPDATE " TblStates " SET " TblStates_Flags "=? WHERE rowid=?");
	rs.put(0, n);
	rs.put(1, rowid);

	rs.Step();
	TestChanged1Row();
}

uint32_t NodeDB::GetStateFlags(uint64_t rowid)
{
	Recordset rs(*this, Query::StateGetFlags0, "SELECT " TblStates_Flags " FROM " TblStates " WHERE rowid=?");
	rs.put(0, rowid);

	rs.StepStrict();
	
	uint32_t nFlags;
	rs.get(0, nFlags);
	return nFlags;
}

void NodeDB::get_ChainWork(uint64_t rowid, Difficulty::Raw& wrk)
{
	Recordset rs(*this, Query::StateGetChainWork, "SELECT " TblStates_ChainWork " FROM " TblStates " WHERE rowid=?");
	rs.put(0, rowid);

	rs.StepStrict();

	rs.get_As(0, wrk);
}

} // namespace beam
