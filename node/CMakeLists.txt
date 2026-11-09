cmake_minimum_required(VERSION 3.13)

set(NODE_SRC
    node.cpp
    node_bbs.cpp
    node_diag.cpp
    node_discovery.cpp
    node_miner.cpp
    node_pbft.cpp
    node_pbft_validators.cpp
    node_peer.cpp
    node_peer_query.cpp
    node_peer_sync.cpp
    node_processor.cpp
    node_tx.cpp
    db.cpp
    processor.cpp
    processor_assets.cpp
    processor_block.cpp
    processor_chain.cpp
    processor_contracts.cpp
    processor_evm.cpp
    processor_generate.cpp
    processor_interpret.cpp
    processor_maintenance.cpp
    processor_multiblock.cpp
    processor_query.cpp
    processor_recognizer.cpp
    processor_storage.cpp
    txpool.cpp
    bridge.cpp
    node_client.cpp
)

if(ANDROID)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-tautological-constant-out-of-range-compare")
endif()

add_library(node STATIC ${NODE_SRC})

target_link_libraries(node
    PUBLIC
        core
        bvm
    PRIVATE
        sqlite
)
if(BEAM_TESTS_ENABLED)
    add_subdirectory(unittests)
endif()
if (NOT BEAM_WALLET_CLIENT_LIBRARY)
    add_subdirectory(utils)
endif()
