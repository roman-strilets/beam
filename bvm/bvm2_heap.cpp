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
#include "bvm2_internal.h"

namespace beam {
namespace bvm2 {

	bool Processor::HeapAllocEx(uint32_t& res, uint32_t size)
	{
		DischargeUnits(Limits::Cost::HeapOp);
		DischargeMemOp(size);

		if (m_Heap.Alloc(res, size))
			return true;

		uint32_t nSizeOld = m_LinearMem.n;
		uint32_t nReserve = m_Heap.get_UnusedAtEnd(nSizeOld);
		assert(nReserve < size);
		uint32_t nSizeNew = nSizeOld + size - nReserve;

		uint32_t nHeapMax = get_HeapLimit();
		if (nHeapMax < nSizeNew)
			return false;

		// grow
		beam::setmax(nSizeNew, 0x1000U); // 4K 
		beam::setmax(nSizeNew, static_cast<uint32_t>(m_Heap.m_vMem.size()) * 2);
		beam::setmin(nSizeNew, nHeapMax);

		HeapReserveStrict(nSizeNew);

		return m_Heap.Alloc(res, size);
	}

	void Processor::HeapReserveStrict(uint32_t n)
	{
		uint32_t nOld = m_LinearMem.n;
		assert(nOld < n);

		m_Heap.m_vMem.resize(n, 0); // zero-init new mem
		m_LinearMem = m_Heap.m_vMem;

		m_Heap.OnGrow(nOld, n);
	}

	void Processor::HeapFreeEx(uint32_t res)
	{
		DischargeUnits(Limits::Cost::HeapOp);
		m_Heap.Free(res);
	}

	BVM_METHOD(Heap_Alloc)
	{
		size = Stack::AlignUp(size); // use the same alignment for heap too

		uint32_t val;
		if (!HeapAllocEx(val, size))
			return 0;

		return Wasm::MemoryType::Global | val;
	}
	BVM_METHOD_HOST(Heap_Alloc)
	{
		uint32_t val;
		if (!HeapAllocEx(val, size))
			return nullptr;

		return reinterpret_cast<uint8_t*>(Cast::NotConst(m_LinearMem.p)) + val;
	}

	BVM_METHOD(Heap_Free)
	{
		HeapFreeEx(pPtr ^ Wasm::MemoryType::Global);
	}
	BVM_METHOD_HOST(Heap_Free)
	{
		auto val =
			reinterpret_cast<const uint8_t*>(pPtr) -
			reinterpret_cast<const uint8_t*>(m_LinearMem.p);

		HeapFreeEx(static_cast<uint32_t>(val));
	}

	bool Processor::Heap::Alloc(uint32_t& retVal, uint32_t size)
	{
		auto it = m_mapSize.lower_bound(size, Entry::Size::Comparator());
		if (m_mapSize.end() == it)
			return false;

		auto& e = it->get_ParentObj();
		assert(e.m_Size.m_Key >= size);
		retVal = e.m_Pos.m_Key;

		if (e.m_Size.m_Key == size)
		{
			Remove(e, true);
			Insert(e, false);
		}
		else
		{
			// partition
			Create(e.m_Pos.m_Key, size, false);

			e.m_Pos.m_Key += size; // no need to remove and re-insert, order should not change
			UpdateSizeFree(e, e.m_Size.m_Key - size);
		}

		return true;
	}

	Processor::Heap::Entry* Processor::Heap::Create(uint32_t nPos, uint32_t nSize, bool bFree)
	{
		auto* p = new Heap::Entry;
		p->m_Pos.m_Key = nPos;
		p->m_Size.m_Key = nSize;
		Insert(*p, bFree);
		return p;
	}

	void Processor::Heap::Free(uint32_t ptr)
	{
		auto it = m_mapAllocated.find(ptr, Entry::Pos::Comparator());
		Exc::Test(m_mapAllocated.end() != it);

		auto& e = it->get_ParentObj();
		Remove(e, false);
		Insert(e, true);

		TryMerge(e);

		it = MapPos::s_iterator_to(e.m_Pos);
		if (m_mapFree.begin() != it)
		{
			--it;
			TryMerge(it->get_ParentObj());
		}
	}

	void Processor::Heap::Test(uint32_t ptr, uint32_t size)
	{
		auto it = m_mapAllocated.upper_bound(ptr, Entry::Pos::Comparator());
		Exc::Test(m_mapAllocated.begin() != it);
		--it;

		auto& e = it->get_ParentObj();
		assert(ptr >= e.m_Pos.m_Key);
		
		ptr += size;
		Exc::Test((ptr >= size) && (ptr <= e.m_Pos.m_Key + e.m_Size.m_Key));
	}

	void Processor::Heap::UpdateSizeFree(Entry& e, uint32_t newVal)
	{
		m_mapSize.erase(MapSize::s_iterator_to(e.m_Size));
		e.m_Size.m_Key = newVal;
		m_mapSize.insert(e.m_Size);
	}

	void Processor::Heap::TryMerge(Entry& e)
	{
		auto it = m_mapFree.find(e.m_Pos.m_Key + e.m_Size.m_Key, Entry::Pos::Comparator());
		if (m_mapFree.end() != it)
		{
			auto& e2 = it->get_ParentObj();
			UpdateSizeFree(e, e.m_Size.m_Key + e2.m_Size.m_Key);
			Delete(e2, true);
		}
	}

	void Processor::Heap::Insert(Entry& e, bool bFree)
	{
		if (bFree)
		{
			m_mapFree.insert(e.m_Pos);
			m_mapSize.insert(e.m_Size);
		}
		else
			m_mapAllocated.insert(e.m_Pos);
	}

	void Processor::Heap::Remove(Entry& e, bool bFree)
	{
		if (bFree)
		{
			m_mapFree.erase(MapPos::s_iterator_to(e.m_Pos));
			m_mapSize.erase(MapSize::s_iterator_to(e.m_Size));
		}
		else
			m_mapAllocated.erase(MapPos::s_iterator_to(e.m_Pos));
	}

	void Processor::Heap::Delete(Entry& e, bool bFree)
	{
		Remove(e, bFree);
		delete &e;
	}

	void Processor::Heap::Clear()
	{
		while (!m_mapFree.empty())
			Delete(m_mapFree.begin()->get_ParentObj(), true);

		while (!m_mapAllocated.empty())
			Delete(m_mapAllocated.begin()->get_ParentObj(), false);

		m_vMem.clear();
	}

	uint32_t Processor::Heap::get_UnusedAtEnd(uint32_t nEnd) const
	{
		if (!m_mapFree.empty())
		{
			auto& e = m_mapFree.rbegin()->get_ParentObj();
			if (e.m_Pos.m_Key + e.m_Size.m_Key == nEnd)
				return e.m_Size.m_Key;
		}
		return 0;
	}

	void Processor::Heap::OnGrow(uint32_t nOld, uint32_t nNew)
	{
		assert(nOld < nNew);

		if (!m_mapFree.empty())
		{
			auto& e = m_mapFree.rbegin()->get_ParentObj();
			assert(e.m_Pos.m_Key + e.m_Size.m_Key <= nOld);

			if (e.m_Pos.m_Key + e.m_Size.m_Key == nOld)
			{
				UpdateSizeFree(e, nNew - e.m_Pos.m_Key);
				return;
			}
		}

		Create(nOld, nNew - nOld, true);
	}

	void Processor::Heap::swap(Heap& x)
	{
		m_mapAllocated.swap(x.m_mapAllocated);
		m_mapFree.swap(x.m_mapFree);
		m_mapSize.swap(x.m_mapSize);
		m_vMem.swap(x.m_vMem);
	}

} // namespace bvm2
} // namespace beam
