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

#include "core/consensus/transaction.h"
#include "core/consensus/rules.h"
#include "core/consensus/impl_helpers.h"
#include <algorithm>

namespace beam
{
	/////////////
	// FeeSettings

	struct FeeSettingsGlobal
	{
		Transaction::FeeSettings m_BeforeHF3;
		Transaction::FeeSettings m_AfterHF3;

		FeeSettingsGlobal()
		{
			ZeroObject(*this);

			m_BeforeHF3.m_Output = 10;
			m_BeforeHF3.m_Kernel = 10;
			m_BeforeHF3.m_Default = 100;

			m_BeforeHF3.m_ShieldedInputTotal = Rules::Coin / 100;
			m_BeforeHF3.m_ShieldedOutputTotal = Rules::Coin / 100;

			m_AfterHF3.m_Output = 18000;
			m_AfterHF3.m_Kernel = 10000;
			m_AfterHF3.m_Default = 100000; // exactly covers 5 outputs + 1 kernel

			m_AfterHF3.m_ShieldedOutputTotal = Rules::Coin / 100;

			m_AfterHF3.m_Bvm.m_ChargeUnitPrice = 10; // 10 groth
			m_AfterHF3.m_Bvm.m_Minimum = 1000000; // 0.01 beam. This pays for 100K charge

			// the following is to mitigate spamming by excessive contract argumets + data size. Make it comparable to the spamming price by UTXOs
			// for UTXO the price is approximately 25 groth/byte
			m_AfterHF3.m_Bvm.m_ExtraSizeFree = 32768; // up to 32K arguments (+ bytecode for creation) are included in minimal fee. This is nearly 30 groth/byte
			m_AfterHF3.m_Bvm.m_ExtraBytePrice = 50;
		}

	} g_FeeSettingsGlobal;

	const Transaction::FeeSettings& Transaction::FeeSettings::get(Height h)
	{
		return Rules::get().IsPastFork_<3>(h) ?
			g_FeeSettingsGlobal.m_AfterHF3 :
			g_FeeSettingsGlobal.m_BeforeHF3;
	}

	Amount Transaction::FeeSettings::Calculate(const Transaction& t) const
	{
		TxStats s;
		t.get_Reader().AddStats(s);
		return Calculate(s);
	}

	Amount Transaction::FeeSettings::Calculate(const TxStats& s) const
	{
		Amount val =
			m_Output * s.m_Outputs +
			m_Kernel * (s.m_Kernels - s.m_InputsShielded - s.m_OutputsShielded) +
			m_ShieldedInputTotal * s.m_InputsShielded +
			m_ShieldedOutputTotal * s.m_OutputsShielded;

		uint32_t nContractSizeFree = m_Bvm.m_ExtraSizeFree * s.m_Contract;
		if (s.m_ContractSizeExtra > nContractSizeFree)
			val += m_Bvm.m_ExtraBytePrice *(s.m_ContractSizeExtra - nContractSizeFree);

		return val;
	}

	Amount Transaction::FeeSettings::CalculateForBvm(const TxStats& s, uint32_t nBvmCharge) const
	{
		Amount fee = m_Bvm.m_ChargeUnitPrice * nBvmCharge;
		Amount feeMin = m_Bvm.m_Minimum * s.m_Contract;

		return std::max(fee, feeMin);
	}

	Amount Transaction::FeeSettings::get_DefaultStd() const
	{
		return m_Default;
	}

	Amount Transaction::FeeSettings::get_DefaultShieldedOut(uint32_t nNumShieldedOutputs) const
	{
		return m_Default + m_ShieldedOutputTotal * nNumShieldedOutputs;
	}

	/////////////
	// Transaction
	template <class T>
	void RebuildVectorWithoutNulls(std::vector<T>& v, size_t nDel)
	{
		std::vector<T> vSrc;
		vSrc.swap(v);
		v.reserve(vSrc.size() - nDel);

		for (size_t i = 0; i < vSrc.size(); i++)
			if (vSrc[i])
				v.push_back(std::move(vSrc[i]));
	}

	int TxBase::CmpInOut(const Input& in, const Output& out)
	{
		return Cast::Down<TxElement>(in).cmp(out);
	}

	size_t TxVectors::Perishable::NormalizeP()
	{
		std::sort(m_vInputs.begin(), m_vInputs.end());
		std::sort(m_vOutputs.begin(), m_vOutputs.end());

		size_t nDel = 0;

		size_t i1 = 0;
		for (size_t i0 = 0; i0 < m_vInputs.size(); i0++)
		{
			Input::Ptr& pInp = m_vInputs[i0];

			for (; i1 < m_vOutputs.size(); i1++)
			{
				Output::Ptr& pOut = m_vOutputs[i1];

				int n = TxBase::CmpInOut(*pInp, *pOut);
				if (n <= 0)
				{
					if (!n)
					{
						pInp.reset();
						pOut.reset();
						nDel++;
						i1++;
					}
					break;
				}
			}
		}

		if (nDel)
		{
			RebuildVectorWithoutNulls(m_vInputs, nDel);
			RebuildVectorWithoutNulls(m_vOutputs, nDel);
		}

		return nDel;
	}

	void TxVectors::Eternal::NormalizeE()
	{
		// NOTE
		// By convention we should not reorder non-standard kernels, we only need to move the standard ones and sort them.
		// However std::sort is *NOT* guaranteed to keep the order of 'equal' elements. Means - it may reorder the non-standard kernels as well!
		//
		// It's possible to use std::stable_sort. But we prefer to manually split std/non-std, and then sort std normally.
		//std::stable_sort(m_vKernels.begin(), m_vKernels.end());

		size_t nStd = m_vKernels.size();
		for (size_t i = nStd; i--; )
		{
			auto& pKrn = m_vKernels[i];
			if (pKrn->HasNonStd())
			{
				nStd--;
				if (i != nStd)
					std::swap(pKrn, m_vKernels[nStd]);
			}
		}

		std::sort(m_vKernels.begin(), m_vKernels.begin() + nStd);
	}

	size_t TxVectors::Full::Normalize()
	{
		NormalizeE();
		return NormalizeP();
	}

	bool TxVectors::Full::IsEmpty() const
	{
		return
			m_vInputs.empty() &&
			m_vOutputs.empty() &&
			m_vKernels.empty();
	}

	template <typename T>
	void MoveIntoVec(std::vector<T>& trg, std::vector<T>& src)
	{
		if (trg.empty())
			trg = std::move(src);
		else
		{
			trg.reserve(trg.size() + src.size());

			for (size_t i = 0; i < src.size(); i++)
				trg.push_back(std::move(src[i]));

			src.clear();
		}
	}

	void TxVectors::Full::MoveInto(Full& trg)
	{
		MoveIntoVec(trg.m_vInputs, m_vInputs);
		MoveIntoVec(trg.m_vOutputs, m_vOutputs);
		MoveIntoVec(trg.m_vKernels, m_vKernels);
	}

	template <typename T>
	int CmpPtrVectors(const std::vector<T>& a, const std::vector<T>& b)
	{
		CMP_SIMPLE(a.size(), b.size())

		for (size_t i = 0; i < a.size(); i++)
		{
			CMP_PTRS(a[i], b[i])
		}

		return 0;
	}

	void Transaction::TestValid(Context& ctx) const
	{
		assert(Kind::Tx == ctx.m_Params.m_Kind);
		ctx.ValidateAndSummarizeStrict(*this, get_Reader());
		ctx.TestSigma();
	}

	bool Transaction::IsValid(Context& ctx, std::string* psErr /* = nullptr */) const
	{
		try {
			TestValid(ctx);
		}
		catch (const std::exception& e) {
			if (psErr)
				(*psErr) = e.what();
			return false;
		}

		return true;
	}

	void Transaction::get_Key(KeyType& key) const
	{
		// proper transactions must contain non-trivial offset, and this should be enough to identify it with sufficient probability
		// In case it's not specified - just ignore the collisions (means, part of those txs would not propagate)
		key = m_Offset.m_Value;
	}

	template <typename T>
	const T* get_FromVector(const std::vector<std::unique_ptr<T> >& v, size_t idx)
	{
		return (idx >= v.size()) ? NULL : v[idx].get();
	}

	void TxBase::IReader::Compare(IReader&& rOther, bool& bICover, bool& bOtherCovers)
	{
		bICover = bOtherCovers = true;
		Reset();
		rOther.Reset();

#define COMPARE_TYPE(var, fnNext) \
		while (var) \
		{ \
			if (!rOther.var) \
			{ \
				bOtherCovers = false; \
				break; \
			} \
 \
			int n = var->cmp(*rOther.var); \
			if (n < 0) \
				bOtherCovers = false; \
			if (n > 0) \
				bICover = false; \
			if (n <= 0) \
				fnNext(); \
			if (n >= 0) \
				rOther.fnNext(); \
		} \
 \
		if (rOther.var) \
			bICover = false;

		COMPARE_TYPE(m_pUtxoIn, NextUtxoIn)
		COMPARE_TYPE(m_pUtxoOut, NextUtxoOut)
		COMPARE_TYPE(m_pKernel, NextKernel)
	}

	void TxBase::IReader::AddStats(TxStats& s)
	{
		Reset();

		for (; m_pUtxoIn; NextUtxoIn())
			m_pUtxoIn->AddStats(s);

		for (; m_pUtxoOut; NextUtxoOut())
			m_pUtxoOut->AddStats(s);

		for (; m_pKernel; NextKernel())
			m_pKernel->AddStats(s);
	}

	void TxVectors::Reader::Clone(Ptr& pOut)
	{
		pOut.reset(new Reader(m_P, m_E));
	}

	void TxVectors::Reader::Reset()
	{
		ZeroObject(m_pIdx);

		m_pUtxoIn = get_FromVector(m_P.m_vInputs, 0);
		m_pUtxoOut = get_FromVector(m_P.m_vOutputs, 0);
		m_pKernel = get_FromVector(m_E.m_vKernels, 0);
	}

	void TxVectors::Reader::NextUtxoIn()
	{
		m_pUtxoIn = get_FromVector(m_P.m_vInputs, ++m_pIdx[0]);
	}

	void TxVectors::Reader::NextUtxoOut()
	{
		m_pUtxoOut = get_FromVector(m_P.m_vOutputs, ++m_pIdx[1]);
	}

	void TxVectors::Reader::NextKernel()
	{
		m_pKernel = get_FromVector(m_E.m_vKernels, ++m_pIdx[2]);
	}

	void TxVectors::Writer::Write(const Input& v)
	{
		PushVectorPtr(m_P.m_vInputs, v);
	}

	void TxVectors::Writer::Write(const Output& v)
	{
		PushVectorPtr(m_P.m_vOutputs, v);
	}

	void TxVectors::Writer::Write(const TxKernel& v)
	{
		m_E.m_vKernels.emplace_back();
		v.Clone(m_E.m_vKernels.back());
	}

} // namespace beam
