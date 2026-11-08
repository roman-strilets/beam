cmake_minimum_required(VERSION 3.13)

set(CORE_SRC
    base/uintBig.cpp
    base/numeric_utils.cpp
    crypto/ecc.cpp
    crypto/ecc_bulletproof.cpp
    crypto/aes.cpp
    crypto/base58.cpp
    consensus/chain_types.cpp
    consensus/asset.cpp
    consensus/rules.cpp
    consensus/txo.cpp
    consensus/tx_kernel.cpp
    consensus/transaction.cpp
    consensus/block.cpp
    storage/block_rw.cpp
    consensus/block_validation.cpp
    consensus/difficulty.cpp
    consensus/chainwork.cpp
    base/mapped_file.cpp
    storage/radixtree.cpp
    consensus/merkle.cpp
    crypto/keccak.cpp
    channels/negotiator.cpp
    channels/lightning.cpp
    crypto/lelantus.cpp
    net/proto.cpp
    net/peer_manager.cpp
    net/fly_client.cpp
    consensus/treasury.cpp
    consensus/shielded.cpp
# ~etc
)

add_library(core STATIC ${CORE_SRC})
target_link_libraries(core 
    PUBLIC
        Boost::boost
        p2p
        utility
        keccak
    PRIVATE
        pow
        pbkdf
        secp256k1
)

configure_file("${PROJECT_SOURCE_DIR}/version.h.in" "${CMAKE_CURRENT_BINARY_DIR}/version.h")
target_include_directories(core INTERFACE ${CMAKE_CURRENT_BINARY_DIR})

if(BEAM_TESTS_ENABLED)
    add_subdirectory(unittest)
endif()
