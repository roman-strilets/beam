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

const Asset::ID NodeDB::s_AssetEmpty0 = Asset::s_MaxCount;

Asset::ID NodeDB::AssetFindByOwner(const PeerID& owner)
{
	Recordset rs(*this, Query::AssetFindOwner, "SELECT " TblAssets_ID " FROM " TblAssets " WHERE " TblAssets_Owner "=?");
	rs.put_As(0, owner);
	if (!rs.Step())
		return false;

	Asset::ID ret;
	rs.get(0, ret);
	return ret;
}

void NodeDB::AssetDeleteRaw(Asset::ID id)
{
	Recordset rs(*this, Query::AssetDel, "DELETE FROM " TblAssets " WHERE " TblAssets_ID "=?");
	rs.put(0, id);
	rs.Step();
	TestChanged1Row();
}

void NodeDB::AssetInsertRaw(Asset::ID id, const Asset::Full* pAi)
{
	Recordset rs(*this, Query::AssetAdd, "INSERT INTO " TblAssets "(" TblAssets_ID "," TblAssets_Owner "," TblAssets_Cid "," TblAssets_Data "," TblAssets_Deposit "," TblAssets_Value "," TblAssets_LockHeight ") VALUES(?,?,?,?,?,?,?)");
	rs.put(0, id);

	if (pAi)
	{
		rs.put(1, pAi->m_Owner);
		if (pAi->m_Cid != Zero)
			rs.put(2, pAi->m_Cid);
		rs.put(3, Blob(pAi->m_Metadata.m_Value));
		rs.put(4, pAi->m_Deposit);
		rs.put_As(5, pAi->m_Value);
		rs.put(6, pAi->m_LockHeight);
	}

	rs.Step();
	TestChanged1Row();
}

Asset::ID NodeDB::AssetFindMinFree(Asset::ID nMin)
{
	// find free index
	Recordset rs(*this, Query::AssetFindMin, "SELECT " TblAssets_ID " FROM " TblAssets " WHERE " TblAssets_ID ">=? ORDER BY " TblAssets_ID " ASC LIMIT 1");
	rs.put(0, nMin);

	if (!rs.Step())
		return 0;

	Asset::ID ret;
	rs.get(0, ret);
	return ret;
}

bool NodeDB::AssetAdd(Asset::Full& ai)
{
	Asset::ID aidMin = ai.m_ID;
	if (aidMin >= s_AssetEmpty0)
		return false;

	// find freed index to reuse
	ai.m_ID = AssetFindMinFree(ai.m_ID + s_AssetEmpty0);
	if (ai.m_ID)
	{
		assert(ai.m_ID > s_AssetEmpty0);
		assert(ai.m_ID >= aidMin);

		AssetDeleteRaw(ai.m_ID);
		ai.m_ID -= s_AssetEmpty0;
	}
	else
	{
		// extend the used range
		ai.m_ID = static_cast<Asset::ID>(ParamIntGetDef(ParamID::AidMax) + 1);
		if (ai.m_ID >= s_AssetEmpty0)
			return false; // overflow

		for ( ; ai.m_ID < aidMin; ai.m_ID++)
			AssetInsertRaw(ai.m_ID + s_AssetEmpty0, nullptr);

		ParamIntSet(ParamID::AidMax, ai.m_ID);
	}

	AssetInsertRaw(ai.m_ID, &ai);

	ParamIntSet(ParamID::AssetsActive, ParamIntGetDef(ParamID::AssetsActive) + 1);
	return true;
}

Asset::ID NodeDB::AssetDelete(Asset::ID id)
{
	assert(id < s_AssetEmpty0);
	AssetDeleteRaw(id);

	Asset::ID aidMax = static_cast<Asset::ID>(ParamIntGetDef(ParamID::AidMax));
	if (aidMax == id)
	{
		// last erased.
		while (--aidMax)
		{
			id = aidMax + s_AssetEmpty0;
			if (!AssetFindMinFree(id))
				break;

			AssetDeleteRaw(id);
		}

		ParamIntSet(ParamID::AidMax, aidMax);
	}
	else
		AssetInsertRaw(id + s_AssetEmpty0, nullptr);

	ParamIntSet(ParamID::AssetsActive, ParamIntGetDef(ParamID::AssetsActive) - 1);

	return aidMax;
}

void NodeDB::AssetsDelAll(Asset::ID aid0)
{
	Recordset rs(*this, Query::AssetsDelAll, "DELETE FROM " TblAssets);
	rs.Step();

	ParamDelSafe(ParamID::AssetsActive);
	ParamIntSet(ParamID::AidMax, aid0);
}

bool NodeDB::AssetGetSafe(Asset::Full& ai)
{
	if (ai.m_ID >= s_AssetEmpty0)
		return false;

	Recordset rs(*this, Query::AssetGet, "SELECT " TblAssets_Value "," TblAssets_Owner "," TblAssets_Cid "," TblAssets_Data "," TblAssets_Deposit "," TblAssets_LockHeight " FROM " TblAssets " WHERE " TblAssets_ID "=?");
	rs.put(0, ai.m_ID);
	if (!rs.Step())
		return false;

	rs.get_As(0, ai.m_Value);
	rs.get_As(1, ai.m_Owner);

	if (rs.IsNull(2))
		ai.m_Cid = Zero;
	else
		rs.get_As(2, ai.m_Cid);

	rs.get(3, ai.m_Metadata.m_Value);
	rs.get(4, ai.m_Deposit);
	rs.get(5, ai.m_LockHeight);

	ai.m_Metadata.UpdateHash();

	return true;
}

bool NodeDB::AssetGetNext(Asset::Full& ai)
{
	assert(ai.m_ID < Asset::s_MaxCount);

	ai.m_ID = AssetFindMinFree(ai.m_ID + 1);
	if (ai.m_ID > Asset::s_MaxCount)
		return false;

	return AssetGetSafe(ai);
}

void NodeDB::AssetSetValue(Asset::ID id, const AmountBig::Type& val, Height hLockHeight)
{
	Recordset rs(*this, Query::AssetSetVal, "UPDATE " TblAssets " SET " TblAssets_Value "=?," TblAssets_LockHeight "=? WHERE " TblAssets_ID "=?");
	rs.put_As(0, val);
	rs.put(1, hLockHeight);
	rs.put(2, id);
	rs.Step();
	TestChanged1Row();
}

bool NodeDB::WalkerAssetEvt::MoveNext()
{
	if (!m_Rs.Step())
		return false;

	m_Rs.get(0, m_ID);
	m_Rs.get(1, m_Height);
	m_Rs.get(2, m_Index);
	m_Rs.get(3, m_Body);

	return true;
}

void NodeDB::AssetEvtsInsert(const AssetEvt& x)
{
	Recordset rs(*this, Query::AssetEvtsInsert, "INSERT INTO " TblAssetEvts " (" TblAssetEvts_ID "," TblAssetEvts_Height "," TblAssetEvts_Index "," TblAssetEvts_Data ") VALUES(?,?,?,?)");
	rs.put(0, x.m_ID);
	rs.put(1, x.m_Height);
	rs.put(2, x.m_Index);
	rs.put(3, x.m_Body);
	rs.Step();
}

void NodeDB::AssetEvtsEnumBwd(WalkerAssetEvt& wlk, Asset::ID id, Height h)
{
	wlk.m_Rs.Reset(*this, Query::AssetEvtsEnumBwd, "SELECT * FROM " TblAssetEvts " WHERE " TblAssetEvts_ID "=? AND " TblAssetEvts_Height "<=? ORDER BY " TblAssetEvts_Height " DESC," TblAssetEvts_Index " DESC");
	wlk.m_Rs.put(0, id);
	wlk.m_Rs.put(1, h);
}

void NodeDB::AssetEvtsEnumBwd2(WalkerAssetEvt& wlk, Asset::ID id, Height h)
{
	wlk.m_Rs.Reset(*this, Query::AssetEvtsEnumBwd2, "SELECT * FROM " TblAssetEvts " WHERE " TblAssetEvts_ID ">=? AND " TblAssetEvts_Height "<=? ORDER BY " TblAssetEvts_ID " ASC," TblAssetEvts_Height " DESC," TblAssetEvts_Index " DESC");
	wlk.m_Rs.put(0, id);
	wlk.m_Rs.put(1, h);
}

void NodeDB::AssetEvtsGetStrict(WalkerAssetEvt& wlk, Height h, uint64_t nIdx)
{
	wlk.m_Rs.Reset(*this, Query::AssetEvtsGet, "SELECT * FROM " TblAssetEvts " WHERE " TblAssetEvts_Height "=? AND " TblAssetEvts_Index "=?");
	wlk.m_Rs.put(0, h);
	wlk.m_Rs.put(1, nIdx);
	if (!wlk.MoveNext())
		ThrowInconsistent();
}

void NodeDB::AssetEvtsDeleteFrom(Height h)
{
	Recordset rs(*this, Query::AssetEvtsDeleteFrom, "DELETE FROM " TblAssetEvts " WHERE " TblAssetEvts_Height ">=?");
	rs.put(0, h);
	rs.Step();
}

} // namespace beam
