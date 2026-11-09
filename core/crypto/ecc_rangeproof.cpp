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

#include "core/crypto/ecc_internal.h"

namespace ECC {

	/////////////////////
	// RangeProof
	namespace RangeProof
	{
		void get_PtMinusVal(Point::Native& out, const Point::Native& comm, Amount val, const Point::Native* pHGen)
		{
			out = comm;

			Point::Native ptAmount;
			Tag::AddValue(ptAmount, pHGen, val);

			ptAmount = -ptAmount;
			out += ptAmount;
		}

		// Public
		bool Public::IsValid(const Point::Native& comm, Oracle& oracle, const Point::Native* pHGen /* = nullptr */) const
		{
			Mode::Scope scope(Mode::Fast);

			if (m_Value < s_MinimumValue)
				return false;

			Point::Native pk;
			get_PtMinusVal(pk, comm, m_Value, pHGen);

			Hash::Value hv;
			get_Msg(hv, oracle);

			return m_Signature.IsValid(hv, pk);
		}

		void Public::Create(const Scalar::Native& sk, const Params::Base& cp, Oracle& oracle)
		{
			m_Value = cp.m_Value;
			assert(m_Value >= s_MinimumValue);

			cp.BlobSave(reinterpret_cast<uint8_t*>(&m_Recovery.m_Kid), sizeof(m_Recovery.m_Kid));
			XCryptKid(m_Recovery.m_Kid, cp, m_Recovery.m_Checksum);

			Hash::Value hv;
			get_Msg(hv, oracle);

			m_Signature.Sign(hv, sk);
		}

		bool Public::Recover(Params::Base& cp) const
		{
			Key::ID::Packed kid = m_Recovery.m_Kid;
			Hash::Value hvChecksum;
			XCryptKid(kid, cp, hvChecksum);

			if (!(m_Recovery.m_Checksum == hvChecksum))
				return false;

			if (!cp.BlobRecover(reinterpret_cast<const uint8_t*>(&kid), sizeof(kid)))
				return false;

			cp.m_Value = m_Value;
			return true;
		}

		int Public::cmp(const Public& x) const
		{
			int n = m_Signature.cmp(x.m_Signature);
			if (n)
				return n;

			n = memcmp(&m_Recovery, &x.m_Recovery, sizeof(m_Recovery));
			if (n)
				return n;

			if (m_Value < x.m_Value)
				return -1;
			if (m_Value > x.m_Value)
				return 1;

			return 0;
		}

		void Public::XCryptKid(Key::ID::Packed& kid, const Params::Base& cp, Hash::Value& hvChecksum)
		{
			NonceGenerator nonceGen("beam-psig");
			nonceGen << cp.m_Seed.V;

			const Hash::Value& okm = nonceGen.get_Okm();
			static_assert(sizeof(okm) >= sizeof(kid), "");
			memxor(reinterpret_cast<uint8_t*>(&kid), okm.m_pData, sizeof(kid));

			nonceGen >> hvChecksum;
		}

		void Public::get_Msg(Hash::Value& hv, Oracle& oracle) const
		{
			oracle
				<< m_Value
				<< beam::Blob(&m_Recovery, sizeof(m_Recovery))
				>> hv;
		}

	} // namespace RangeProof

} // namespace ECC
