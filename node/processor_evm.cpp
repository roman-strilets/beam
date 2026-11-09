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

#include "processor_bic.h"

namespace beam {

bool NodeProcessor::BlockInterpretCtx::HandleKernelType(const TxKernelEvmInvoke& krn)
{
	if (m_Fwd)
	{
		BlockInterpretCtx::MyEvmProcessor proc(*this);

		try
		{
			proc.InvokeGuarded(krn);
			return true;
		}
		catch (const std::exception& e)
		{
			m_TxStatus = proto::TxStatus::ContractFailFirst;

			if (m_pTxErrorInfo)
				*m_pTxErrorInfo << e.what();
		}
		
		m_Storage.UndoVars();
		return false;
	}
	else
		m_Storage.UndoVars();

	return true;
}

void NodeProcessor::BlockInterpretCtx::MyEvmProcessor::InvokeGuarded(const TxKernelEvmInvoke& krn)
{
	const auto& r = Rules::get();
	if (!r.Evm.Groth2Wei || !r.Evm.BaseGasPrice)
		Exc::Fail("Evm disabled");

	auto& acc = LoadAccountInternal(krn.m_From);
	if (acc.m_pContractCode)
		Exc::Fail("not EOA");
	if (acc.m_Nonce != krn.m_Nonce)
		Exc::Fail("nonce gap");

	m_Top.m_pAccount = &acc;
	m_Top.EnsureAccountCreated(acc);

	bool bAdd;
	Amount valUns = SplitAmountSigned(krn.m_Subsidy, bAdd);
	if (valUns)
	{
		Evm::Word wSubsidy;
		wSubsidy.FromNumber(MultiWord::From(valUns) * MultiWord::From(r.Evm.Groth2Wei)); // won't overflow

		if (bAdd)
		{
			wSubsidy += acc.m_Balance.m_Value;
			if (wSubsidy < acc.m_Balance.m_Value)
				Exc::Fail("balance overflow");

		}
		else
		{
			if (wSubsidy > acc.m_Balance.m_Value)
				Exc::Fail("balance underflow");

			wSubsidy.Negate();
			wSubsidy += acc.m_Balance.m_Value;
		}

		m_Top.UpdateBalance(acc, wSubsidy);
	}

	// gas parameters. Gas is payed from the tx kernel fee, not sender balance
	auto nGasLimit = m_Bic.m_ChargePerBlock;

	auto numGas = (MultiWord::From(krn.m_Fee) * MultiWord::From(r.Evm.Groth2Wei)) / MultiWord::From(r.Evm.BaseGasPrice);

	if (numGas.get_ConstSlice().get_clz_Words() >= numGas.nWords - MultiWord::NumberForType<uint32_t>::Type::nWords)
	{
		numGas.get_Element(nGasLimit);
		beam::setmin(nGasLimit, m_Bic.m_ChargePerBlock);
	}

	if (nGasLimit < r.Evm.MinTxGasUnits)
		Exc::Fail("gas below min");

	m_Top.m_Gas = nGasLimit - r.Evm.MinTxGasUnits;

	Args args;
	args.m_Buf = krn.m_Args;
	args.m_CallValue = krn.m_CallValue;

	Call(krn.m_To, args, krn.m_Nonce);

	while (!m_lstFrames.empty())
		RunOnce();

	acc.m_Nonce++;
	acc.m_Balance.m_Modified++; // to ensure it'll be saved

	// everything is fine. Commit the changes, and create recovery info
	uint32_t nGasConsumed = nGasLimit - static_cast<uint32_t>(m_Top.m_Gas);
	assert(nGasConsumed <= m_Bic.m_ChargePerBlock);

	UpdChargeWithRecovery(m_Bic.m_ChargePerBlock - nGasConsumed);

	CommitChanges();
}

void NodeProcessor::BlockInterpretCtx::MyEvmProcessor::CommitChanges()
{
	for (auto& ad_ : m_Accounts)
	{
		auto& ad = Cast::Up<AccountWrap>(ad_);
		assert(ad.m_pEntry);

		bool bChanged =
			ad.m_Balance.m_Modified ||
			ad.m_Code.m_Modified ||
			ad.m_Exists.m_Modified;

		bool bIsContract = ad.m_pContractCode || ad.m_Code.m_Modified;

		if (bChanged)
		{
			if (ad.m_Exists.m_Value)
			{
				if (bIsContract)
				{
					EvmAccount::Contract ca;
					ca.m_Balance_Wei = ad.m_Balance.m_Value;

					if (ad.m_Code.m_Modified)
					{
						if (ad.m_pContractCode)
							m_Bic.m_Storage.DataSaveWithRecovery(*ad.m_pContractCode, Blob(nullptr, 0));

						HashOf(ca.m_CodeHash, ad.m_Code.m_Value);

						AccountKey ak;
						ak.m_Addr = ad.m_Key;
						ak.m_Var = ca.m_CodeHash;

						auto& e = m_Bic.m_Storage.get_Var(Blob(&ak, sizeof(ak)));
						m_Bic.m_Storage.DataSaveWithRecovery(e, ad.m_Code.m_Value);
					}
					else
					{
						// use previous value
						if (ad.m_pEntry)
							ca.m_CodeHash = reinterpret_cast<EvmAccount::Contract*>(&ad.m_pEntry->m_Data.front())->m_CodeHash;
						else
							ca.m_CodeHash = Zero;
					}

					m_Bic.m_Storage.DataSaveWithRecovery(*ad.m_pEntry, Blob(&ca, sizeof(ca)));
				}
				else
				{
					EvmAccount::User ua;
					ua.m_Balance_Wei = ad.m_Balance.m_Value;
					ua.m_Nonce = ad.m_Nonce;

					m_Bic.m_Storage.DataSaveWithRecovery(*ad.m_pEntry, Blob(&ua, sizeof(ua)));
				}

			}
			else
			{
				// account was erased
				m_Bic.m_Storage.DataSaveWithRecovery(*ad.m_pEntry, Blob(nullptr, 0));

				if (ad.m_pContractCode)
					m_Bic.m_Storage.DataSaveWithRecovery(*ad.m_pContractCode, Blob(nullptr, 0));
			}
		}

		// vars
		for (auto& sd_ : ad.m_Slots)
		{
			auto& sd = Cast::Up<SlotWrap>(sd_);
			assert(sd.m_pEntry);

			if (sd.m_Data.m_Modified)
			{
				Blob newVal = sd.m_Data.m_Value;
				if (sd.m_Data.m_Value == Zero)
					newVal.n = 0;

				m_Bic.m_Storage.DataSaveWithRecovery(*sd.m_pEntry, newVal);
			}
		}
		
	}

}

Evm::Processor::Account& NodeProcessor::BlockInterpretCtx::MyEvmProcessor::LoadAccount(const Evm::Address& addr)
{
	return LoadAccountInternal(addr);
}

NodeProcessor::BlockInterpretCtx::MyEvmProcessor::AccountWrap& NodeProcessor::BlockInterpretCtx::MyEvmProcessor::LoadAccountInternal(const Evm::Address& addr)
{
	AccountWrap* p = new AccountWrap;
	p->m_Key = addr;
	m_Accounts.insert(*p);

	auto& e = m_Bic.m_Storage.get_Var(addr);
	p->m_pEntry = &e;

	p->m_pContractCode = nullptr;
	p->m_Nonce = 0;
	p->m_Code.m_Value.n = 0;

	if (e.m_Data.empty())
	{
		p->m_Balance.m_Value = Zero;
		p->m_Exists.m_Value = false;
	}
	else
	{
		static_assert(sizeof(EvmAccount::User) != sizeof(EvmAccount::Contract));

		p->m_Exists.m_Value = true;

		if (e.m_Data.size() == sizeof(EvmAccount::User))
		{
			const auto& x = *reinterpret_cast<EvmAccount::User*>(&e.m_Data.front());
			p->m_Balance.m_Value = x.m_Balance_Wei;
			x.m_Nonce.Export(p->m_Nonce);
		}
		else
		{
			if (e.m_Data.size() != sizeof(EvmAccount::Contract))
				OnCorrupted();
			const auto& x = *reinterpret_cast<EvmAccount::Contract*>(&e.m_Data.front());
			p->m_Balance.m_Value = x.m_Balance_Wei;

			AccountKey ak;
			ak.m_Addr = addr;
			ak.m_Var = x.m_CodeHash;

			p->m_pContractCode = &m_Bic.m_Storage.get_Var(Blob(&ak, sizeof(ak)));

			p->m_Code.m_Value = p->m_pContractCode->m_Data;
		}
	}

	return *p;
}

Evm::Processor::Account::Slot& NodeProcessor::BlockInterpretCtx::MyEvmProcessor::LoadSlot(Evm::Processor::Account& acc, const Evm::Word& key)
{
	SlotWrap* p = new SlotWrap;
	p->m_Key = key;
	acc.m_Slots.insert(*p);

	AccountKey ak;
	ak.m_Addr = acc.m_Key;
	ak.m_Var = key;

	auto& e = m_Bic.m_Storage.get_Var(Blob(&ak, sizeof(ak)));
	p->m_pEntry = &e;

	if (e.m_Data.empty())
		p->m_Data.m_Value = Zero;
	else
	{
		if (sizeof(Evm::Word) != e.m_Data.size())
			OnCorrupted();

		p->m_Data.m_Value = *reinterpret_cast<Evm::Word*>(&e.m_Data.front());
	}

	return *p;
}

Height NodeProcessor::BlockInterpretCtx::MyEvmProcessor::get_Height()
{
	return m_Bic.m_Height - 1;
}

bool NodeProcessor::BlockInterpretCtx::MyEvmProcessor::get_BlockHeader(BlockHeader& res, Height h)
{
	Block::SystemState::Full s;
	if (!m_Bic.m_Proc.get_HdrAt(s, h))
		return false;

	s.get_Hash(res.m_Hash);
	s.m_PoW.m_Difficulty.Unpack(res.m_Difficulty);
	res.m_GasLimit = Zero;
	Cast::Down<Evm::Address::Base>(res.m_Coinbase) = Zero;
	res.m_Timestamp = s.m_TimeStamp;

	return true;
}

} // namespace beam
