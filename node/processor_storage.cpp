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

#include "processor_common.h"

namespace beam {

bool NodeProcessor::Mapped::Contract::IsStored(const Blob& key)
{
	if (key.n > bvm2::ContractID::nBytes)
	{
		uint8_t nTag = reinterpret_cast<const uint8_t*>(key.p)[bvm2::ContractID::nBytes];
		if (Shaders::KeyTag::InternalStealth == nTag)
			return false;
	}

	return true;
}

void NodeProcessor::Mapped::Contract::Toggle(const Blob& key, const Blob& data, bool bAdd)
{
	if (!IsStored(key))
		return;

	Merkle::Hash hv;
	Block::get_HashContractVar(hv, key, data);

	if (bAdd)
		EnsureReserve();

	bool bCreate = true;
	RadixHashOnlyTree::Cursor cu;
	Find(cu, hv, bCreate);

	if (!bAdd)
		Delete(cu);

	if (bAdd != bCreate)
	{
		Exc::CheckpointTxt cp("SaveVar collision");

		if (!bAdd)
			OnCorrupted();

		Exc::Fail();
	}
}

NodeProcessor::RecentStates::Entry& NodeProcessor::RecentStates::get_FromTail(size_t x) const
{
	assert((x < m_Count) && (m_Count <= m_vec.size()));
	return Cast::NotConst(m_vec[(m_i0 + m_Count - x - 1) % m_vec.size()]);
}

const NodeProcessor::RecentStates::Entry* NodeProcessor::RecentStates::Get(Block::Number num) const
{
	if (!m_Count)
		return nullptr;

	const Entry& e = get_FromTail(0);
	if (num.v > e.m_State.m_Number.v)
		return nullptr;

	auto dn = e.m_State.m_Number.v - num.v;
	if (dn >= m_Count)
		return nullptr;

	const Entry& e2 = get_FromTail(static_cast<size_t>(dn));
	assert(e2.m_State.m_Number.v == num.v);
	return &e2;
}

void NodeProcessor::RecentStates::RollbackTo(Block::Number num)
{
	for (; m_Count; m_Count--)
	{
		const Entry& e = get_FromTail(0);
		if (e.m_State.m_Number.v == num.v)
			break;
	}
}

void NodeProcessor::RecentStates::Push(uint64_t rowID, const Block::SystemState::Full& s)
{
	if (m_vec.empty())
	{
		// we use this cache mainly to improve difficulty calculation. Hence the cache size is appropriate
		const Rules& r = Rules::get();
	
		const size_t n = std::max(r.DA.WindowWork + r.DA.WindowMedian1, r.DA.WindowMedian0) + 5;
		m_vec.resize(n);
	}
	else
	{
		// ensure we don't have out-of-order entries
		RollbackTo(Block::Number(s.m_Number.v - 1));
	}

	if (m_Count < m_vec.size())
		m_Count++;
	else
		m_i0++;

	Entry& e = get_FromTail(0);
	e.m_RowID = rowID;
	e.m_State = s;
}

void NodeProcessor::ValidatedCache::ShrinkTo(uint32_t n)
{
	while (m_Mru.size() > n)
		Delete(m_Mru.back().get_ParentObj());
}

void NodeProcessor::ValidatedCache::OnShLo(const Entry::ShLo::Type& nShLo)
{
	while (true)
	{
		ShLoSet::reverse_iterator it = m_ShLo.rbegin();
		if (m_ShLo.rend() == it)
			break;
		Entry::ShLo& x = *it;

		if (x.m_End <= nShLo)
			break;

		Delete(x.get_ParentObj());
	}
}

void NodeProcessor::ValidatedCache::RemoveRaw(Entry& x)
{
	m_Keys.erase(KeySet::s_iterator_to(x.m_Key));
	m_ShLo.erase(ShLoSet::s_iterator_to(x.m_ShLo));
	m_Mru.erase(MruList::s_iterator_to(x.m_Mru));
}

void NodeProcessor::ValidatedCache::Delete(Entry& x)
{
	RemoveRaw(x);
	delete &x;
}

void NodeProcessor::ValidatedCache::MoveToFront(Entry& x)
{
	m_Mru.erase(MruList::s_iterator_to(x.m_Mru));
	m_Mru.push_front(x.m_Mru);
}

bool NodeProcessor::ValidatedCache::Find(const Entry::Key::Type& val)
{
	Entry::Key key;
	key.m_Value = val;

	KeySet::iterator it = m_Keys.find(key);
	if (m_Keys.end() == it)
		return false;

	MoveToFront(it->get_ParentObj());
	return true;
}

void NodeProcessor::ValidatedCache::Insert(const Entry::Key::Type& val, const Entry::ShLo::Type& nShLo)
{
	Entry* pEntry(new Entry);
	pEntry->m_Key.m_Value = val;
	pEntry->m_ShLo.m_End = nShLo;

	InsertRaw(*pEntry);
}

void NodeProcessor::ValidatedCache::InsertRaw(Entry& x)
{
	m_Keys.insert(x.m_Key);
	m_ShLo.insert(x.m_ShLo);
	m_Mru.push_front(x.m_Mru);
}

void NodeProcessor::ValidatedCache::MoveInto(ValidatedCache& dst)
{
	while (!m_Mru.empty())
	{
		Entry& x = m_Mru.back().get_ParentObj();
		RemoveRaw(x);
		dst.InsertRaw(x);
	}
}

struct NodeProcessor::Mapped::Type {
	enum Enum {

		UtxoLeaf,
		HashJoint,
		UtxoQueue,
		UtxoNode,
		HashLeaf,

		count
	};
};

bool NodeProcessor::Mapped::Open(const char* sz, const Stamp& s)
{
	// change this when format changes
	static const uint8_t s_pSig[] = {
		0xFB, 0x6A, 0x15, 0x54,
		0x41, 0x7C, 0x4C, 0x3D,
		0x81, 0xD5, 0x9C, 0xD9,
		0x17, 0xCE, 0xA4, 0x92
	};

	MappedFile::Defs d;
	d.m_pSig = s_pSig;
	d.m_nSizeSig = sizeof(s_pSig);
	d.m_nBanks = Type::count;
	d.m_nFixedHdr = sizeof(Hdr);

	m_Mapping.Open(sz, d);

	Hdr& h = get_Hdr();
	if (!h.m_Dirty && (h.m_Stamp == s))
	{
		m_Utxo.m_RootOffset = h.m_RootUtxo;
		m_Contract.m_RootOffset = h.m_RootContract;
		return true;
	}

	m_Mapping.Open(sz, d, true); // reset
	return false;
}

void NodeProcessor::Mapped::Close()
{
	m_Utxo.m_RootOffset = 0; // prevent cleanup
	m_Contract.m_RootOffset = 0;
	m_Mapping.Close();
}

NodeProcessor::Mapped::Hdr& NodeProcessor::Mapped::get_Hdr()
{
	return *static_cast<Hdr*>(m_Mapping.get_FixedHdr());
}

void NodeProcessor::Mapped::FlushStrict(const Stamp& s)
{
	Hdr& h = get_Hdr();
	assert(h.m_Dirty);

	h.m_Dirty = 0;
	h.m_RootUtxo = m_Utxo.m_RootOffset;
	h.m_RootContract = m_Contract.m_RootOffset;
	// TODO: flush

	h.m_Stamp = s;
}

void NodeProcessor::Mapped::Utxo::EnsureReserve()
{
	try
	{
		get_ParentObj().m_Mapping.EnsureReserve(Type::UtxoLeaf, sizeof(MyLeaf), 1);
		get_ParentObj().m_Mapping.EnsureReserve(Type::HashJoint, sizeof(MyJoint), 1);
		get_ParentObj().m_Mapping.EnsureReserve(Type::UtxoQueue, sizeof(MyLeaf::IDQueue), 1);
		get_ParentObj().m_Mapping.EnsureReserve(Type::UtxoNode, sizeof(MyLeaf::IDNode), 1);
	}
	catch (const std::exception& e)
	{
		// promote it
		CorruptionException exc;
		exc.m_sErr = e.what();
		throw exc;
	}
}

void NodeProcessor::Mapped::OnDirty()
{
	get_Hdr().m_Dirty = 1;
}

intptr_t NodeProcessor::Mapped::Utxo::get_Base() const
{
	return reinterpret_cast<intptr_t>(get_ParentObj().m_Mapping.get_Base());
}

RadixTree::Leaf* NodeProcessor::Mapped::Utxo::CreateLeaf()
{
	return get_ParentObj().Allocate<MyLeaf>(Type::UtxoLeaf);
}

void NodeProcessor::Mapped::Utxo::DeleteEmptyLeaf(Leaf* p)
{
	get_ParentObj().m_Mapping.Free(Type::UtxoLeaf, p);
}

RadixTree::Joint* NodeProcessor::Mapped::Utxo::CreateJoint()
{
	return get_ParentObj().Allocate<MyJoint>(Type::HashJoint);
}

void NodeProcessor::Mapped::Utxo::DeleteJoint(Joint* p)
{
	get_ParentObj().m_Mapping.Free(Type::HashJoint, p);
}

UtxoTree::MyLeaf::IDQueue* NodeProcessor::Mapped::Utxo::CreateIDQueue()
{
	return get_ParentObj().Allocate<MyLeaf::IDQueue>(Type::UtxoQueue);
}

void NodeProcessor::Mapped::Utxo::DeleteIDQueue(MyLeaf::IDQueue* p)
{
	get_ParentObj().m_Mapping.Free(Type::UtxoQueue, p);
}

UtxoTree::MyLeaf::IDNode* NodeProcessor::Mapped::Utxo::CreateIDNode()
{
	return get_ParentObj().Allocate<MyLeaf::IDNode>(Type::UtxoNode);
}

void NodeProcessor::Mapped::Utxo::DeleteIDNode(MyLeaf::IDNode* p)
{
	get_ParentObj().m_Mapping.Free(Type::UtxoNode, p);
}

intptr_t NodeProcessor::Mapped::Contract::get_Base() const
{
	return reinterpret_cast<intptr_t>(get_ParentObj().m_Mapping.get_Base());
}

void NodeProcessor::Mapped::Contract::EnsureReserve()
{
	try
	{
		get_ParentObj().m_Mapping.EnsureReserve(Type::HashJoint, sizeof(MyJoint), 1);
		get_ParentObj().m_Mapping.EnsureReserve(Type::HashLeaf, sizeof(MyLeaf), 1);
	}
	catch (const std::exception& e)
	{
		// promote it
		CorruptionException exc;
		exc.m_sErr = e.what();
		throw exc;
	}
}

RadixTree::Leaf* NodeProcessor::Mapped::Contract::CreateLeaf()
{
	return get_ParentObj().Allocate<MyLeaf>(Type::HashLeaf);
}

void NodeProcessor::Mapped::Contract::DeleteLeaf(Leaf* p)
{
	get_ParentObj().m_Mapping.Free(Type::HashLeaf, p);
}

RadixTree::Joint* NodeProcessor::Mapped::Contract::CreateJoint()
{
    static_assert(sizeof(MyJoint) == sizeof(UtxoTree::MyJoint));
	return get_ParentObj().Allocate<MyJoint>(Type::HashJoint);
}

void NodeProcessor::Mapped::Contract::DeleteJoint(Joint* p)
{
	get_ParentObj().m_Mapping.Free(Type::HashJoint, p);
}

} // namespace beam
