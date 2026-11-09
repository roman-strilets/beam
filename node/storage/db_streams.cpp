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

NodeDB::StreamMmr::StreamMmr(NodeDB& db, StreamType::Enum eType, bool bStoreH0)
	:m_hStoreFrom(!bStoreH0)
	,m_eType(eType)
	,m_DB(db)
{
	for (size_t i = 0; i < _countof(m_pCache); i++)
		m_pCache[i].m_X = static_cast<uint64_t>(-1);

	m_LastOut.m_Pos.H = Merkle::Position::HMax;
	m_LastOut.m_Pos.X = static_cast<uint64_t>(-1);
}

void NodeDB::StreamMmr::Append(const Merkle::Hash& hv)
{
	uint64_t n = m_Count;
	ResizeTo(n + 1);
	Mmr::Replace(n, hv);
}

void NodeDB::StreamMmr::ShrinkTo(uint64_t nCount)
{
	assert(m_Count >= nCount);
	ResizeTo(nCount);
}

void NodeDB::StreamMmr::ResizeTo(uint64_t nCount)
{
	m_DB.StreamResize(m_eType, get_TotalHashes(nCount, m_hStoreFrom) * sizeof(Merkle::Hash), get_TotalHashes(m_Count, m_hStoreFrom) * sizeof(Merkle::Hash));
	m_Count = nCount;
}

void NodeDB::StreamMmr::LoadElement(Merkle::Hash& hv, const Merkle::Position& pos) const
{
	if (CacheFind(hv, pos))
		return;

	m_DB.StreamIO(m_eType, Pos2Idx(pos, m_hStoreFrom) * sizeof(Merkle::Hash), hv.m_pData, hv.nBytes, false);
	Cast::NotConst(this)->CacheAdd(hv, pos);
}

void NodeDB::StreamMmr::SaveElement(const Merkle::Hash& hv, const Merkle::Position& pos)
{
	m_DB.StreamIO(m_eType, Pos2Idx(pos, m_hStoreFrom) * sizeof(Merkle::Hash), Cast::NotConst(hv.m_pData), hv.nBytes, true);
	CacheAdd(hv, pos);
}

bool NodeDB::StreamMmr::CacheFind(Merkle::Hash& hv, const Merkle::Position& pos) const
{
	// Note: ALWAYS test the main cache BEFORE m_LastOut, coz that element could already be overwritten
	if (pos.H < _countof(m_pCache)) // 'if' is needed only if we decide to reduce the cache size
	{
		const CacheEntry& ce = m_pCache[pos.H];
		if (ce.m_X == pos.X)
		{
			hv = ce.m_Value;
			return true;
		}
	}

	if ((m_LastOut.m_Pos.H == pos.H) && (m_LastOut.m_Pos.X == pos.X))
	{
		hv = m_LastOut.m_Value;
		return true;
	}

	return false;
}

void NodeDB::StreamMmr::CacheAdd(const Merkle::Hash& hv, const Merkle::Position& pos)
{
	if (pos.H < _countof(m_pCache)) // 'if' is needed only if we decide to reduce the cache size
	{
		CacheEntry& ce = m_pCache[pos.H];

		if ((ce.m_X != pos.X) && (ce.m_X != static_cast<uint64_t>(-1)))
		{
			m_LastOut.m_Pos.X = ce.m_X;
			m_LastOut.m_Pos.H = pos.H;
			m_LastOut.m_Value = ce.m_Value;
		}

		ce.m_Value = hv;
		ce.m_X = pos.X;
	}
}

NodeDB::StatesMmr::StatesMmr(NodeDB& db)
	:StreamMmr(db, StreamType::StatesMmr, false)
{
}

uint64_t NodeDB::StatesMmr::N2I(Block::Number num)
{
	return num.v ? (num.v - 1) : 0;
}

void NodeDB::StatesMmr::LoadElement(Merkle::Hash& hv, const Merkle::Position& pos) const
{
	if (pos.H)
		StreamMmr::LoadElement(hv, pos);
	else
	{
		if (CacheFind(hv, pos))
			return;

		LoadStateHash(hv, Block::Number(pos.X + 1));
		Cast::NotConst(this)->CacheAdd(hv, pos);
	}
}

void NodeDB::StatesMmr::LoadStateHash(Merkle::Hash& hv, Block::Number num) const
{
	uint64_t row = m_DB.FindActiveStateStrict(num);
	m_DB.get_StateHash(row, hv);
}

void NodeDB::StatesMmr::SaveElement(const Merkle::Hash& hv, const Merkle::Position& pos)
{
	if (pos.H)
		StreamMmr::SaveElement(hv, pos);
	else
		CacheAdd(hv, pos);
}

const uint32_t NodeDB::s_StreamBlob = 1024*1024; // arbitrary, but should not be changed after DB is created

uint64_t NodeDB::StreamType::Key(uint64_t idx, Enum eType)
{
	return idx | (static_cast<uint64_t>(eType) << 32);
}


void NodeDB::StreamResize(StreamType::Enum eType, uint64_t n, uint64_t n0)
{
	uint64_t nBlobs0 = (n0 + s_StreamBlob - 1) / s_StreamBlob;
	uint64_t nBlobs1 = (n + s_StreamBlob - 1) / s_StreamBlob;

	for (; nBlobs0 < nBlobs1; nBlobs0++)
	{
		Recordset rs(*this, Query::StreamIns, "INSERT INTO " TblStreams "(" TblStream_ID "," TblStream_Value ") VALUES (?,?)");
		rs.put(0, StreamType::Key(nBlobs0, eType));
		rs.putZeroBlob(1, s_StreamBlob);
		rs.Step();
		TestChanged1Row();
	}

	if (nBlobs0 > nBlobs1)
	{
		StreamShrinkInternal(StreamType::Key(nBlobs1, eType), StreamType::Key(nBlobs0, eType));

		uint64_t ret = get_RowsChanged();
		if (ret != nBlobs0 - nBlobs1)
			ThrowInconsistent();
	}
}

void NodeDB::StreamShrinkInternal(uint64_t k0, uint64_t k1)
{
	Recordset rs(*this, Query::StreamDel, "DELETE FROM " TblStreams " WHERE " TblStream_ID ">=? AND " TblStream_ID "<?");
	rs.put(0, k0);
	rs.put(1, k1);
	rs.Step();
}

void NodeDB::StreamsDelAll(StreamType::Enum t0, StreamType::Enum t1)
{
	StreamShrinkInternal(StreamType::Key(0, t0), StreamType::Key(std::numeric_limits<uint32_t>::max(), t1));
}

void NodeDB::OpenBlob(BlobGuard& blob, const char* szTable, const char* szColumn, uint64_t rowid, bool bRW)
{
	TestRet(sqlite3_blob_open(m_pDb, "main", szTable, szColumn, rowid, bRW ? 1 : 0, &blob.m_pPtr));
}

void NodeDB::StreamIO(StreamType::Enum eType, uint64_t pos, uint8_t* p, uint64_t nCount, bool bWrite)
{
	uint64_t nBlob0 = pos / s_StreamBlob;
	uint32_t nOffs = static_cast<uint32_t>(pos % s_StreamBlob);

	while (nCount)
	{
		BlobGuard blob;
		OpenBlob(blob, TblStreams, TblStream_Value, StreamType::Key(nBlob0, eType), bWrite);

		uint32_t nPortion = s_StreamBlob - nOffs;
		if (nPortion > nCount)
			nPortion = static_cast<uint32_t>(nCount);

		int nRes = bWrite ?
			sqlite3_blob_write(blob.m_pPtr, p, nPortion, nOffs) :
			sqlite3_blob_read(blob.m_pPtr, p, nPortion, nOffs);

		TestRet(nRes);

		nCount -= nPortion;
		p += nPortion;
		nOffs = 0;
		nBlob0++;
	}
}

} // namespace beam
