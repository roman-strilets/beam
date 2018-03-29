#pragma once

#include "common.h"
#include <vector>
#include <array>
#include <cstdint>
#include <functional>

namespace equi
{
using uint256_t = std::array<uint8_t, 32>;
using Solution = std::vector<uint8_t>;
using Input = std::vector<uint8_t>;
using Cancel = std::function<bool()>;

struct Proof
{
    uint256_t nonce;
    Solution solution;
    Proof() = default;
    Proof(Proof &&);
};

Proof get_solution(const Input &input, const uint256_t &initial_nonce, const Cancel = []{ return false; });
bool is_valid_proof(const Input &input, const Proof &proof);
}
