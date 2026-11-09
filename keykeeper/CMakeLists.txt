project(keykeeper)
cmake_minimum_required(VERSION 3.13)

set(TARGET_NAME wasm-key-keeper)

if(EMSCRIPTEN)
    set(CMAKE_CXX_STANDARD 20)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()

add_library(keykeeper STATIC)

target_sources(keykeeper
    PRIVATE
        local_private_key_keeper.cpp
)

if(EMSCRIPTEN AND NOT BEAM_WALLET_CLIENT_LIBRARY)
    target_compile_definitions(keykeeper PUBLIC EMSCRIPTEN)
    target_link_libraries(keykeeper PRIVATE secp256k1)
    target_include_directories(keykeeper PUBLIC 
        "${PROJECT_SOURCE_DIR}/.." 
        "${PROJECT_SOURCE_DIR}/../3rdparty" 
        "${PROJECT_SOURCE_DIR}/../3rdparty/libuv/include"
        $ENV{BOOST_ROOT})
    
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-value")

    add_executable(${TARGET_NAME} wasm_key_keeper.cpp)
    target_link_libraries(${TARGET_NAME} PRIVATE keykeeper)

    add_subdirectory(${PROJECT_SOURCE_DIR}/../3rdparty/secp256k1 secp256k1)

    target_sources(keykeeper
        PRIVATE
            ${PROJECT_SOURCE_DIR}/../core/base/uintBig.cpp
            ${PROJECT_SOURCE_DIR}/../core/crypto/ecc.cpp
            ${PROJECT_SOURCE_DIR}/../core/crypto/ecc_bulletproof.cpp
            ${PROJECT_SOURCE_DIR}/../core/consensus/chain_types.cpp
            ${PROJECT_SOURCE_DIR}/../core/consensus/asset.cpp
            ${PROJECT_SOURCE_DIR}/../core/consensus/rules.cpp
            ${PROJECT_SOURCE_DIR}/../core/consensus/txo.cpp
            ${PROJECT_SOURCE_DIR}/../core/consensus/tx_kernel.cpp
            ${PROJECT_SOURCE_DIR}/../core/consensus/transaction.cpp
            ${PROJECT_SOURCE_DIR}/../core/consensus/block.cpp
            ${PROJECT_SOURCE_DIR}/../core/storage/block_rw.cpp
            ${PROJECT_SOURCE_DIR}/../core/consensus/merkle.cpp
            ${PROJECT_SOURCE_DIR}/../core/consensus/block_validation.cpp
            ${PROJECT_SOURCE_DIR}/../core/crypto/aes.cpp
            ${PROJECT_SOURCE_DIR}/../utility/common.cpp
            ${PROJECT_SOURCE_DIR}/../utility/hex.cpp

            ${PROJECT_SOURCE_DIR}/../mnemonic/dictionary.cpp
            ${PROJECT_SOURCE_DIR}/../mnemonic/mnemonic.cpp
            ${PROJECT_SOURCE_DIR}/../wallet/core/keys/private_key_keeper.cpp
            ${PROJECT_SOURCE_DIR}/../wallet/core/common/common.cpp
            ${PROJECT_SOURCE_DIR}/../wallet/core/common/common_to_string.cpp
            ${PROJECT_SOURCE_DIR}/../wallet/core/common/common_token_json.cpp
            ${PROJECT_SOURCE_DIR}/../wallet/core/common/common_tx_description.cpp
            ${PROJECT_SOURCE_DIR}/../wallet/core/common/common_tx_parameters.cpp
            ${PROJECT_SOURCE_DIR}/../wallet/core/common/common_tx_status.cpp
            ${PROJECT_SOURCE_DIR}/../wallet/core/common/common_version.cpp
            ${PROJECT_SOURCE_DIR}/../wallet/core/common/base58.cpp

            ${PROJECT_SOURCE_DIR}/../3rdparty/hmac_sha512.c
            ${PROJECT_SOURCE_DIR}/../3rdparty/pkcs5_pbkdf2.c
            ${PROJECT_SOURCE_DIR}/../3rdparty/sha512.c
            ${PROJECT_SOURCE_DIR}/../3rdparty/zeroize.c
            ${PROJECT_SOURCE_DIR}/../3rdparty/utilstrencodings.cpp
    )

    set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS "--bind -s WASM=1 -s ERROR_ON_UNDEFINED_SYMBOLS=0 -s MODULARIZE=1 -s USE_BOOST_HEADERS=1")
else()

    target_link_libraries(keykeeper PRIVATE Boost::boost utility)
    
endif()
