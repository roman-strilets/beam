cmake_minimum_required(VERSION 3.13)

set(NODE_SRC
    net/node.cpp
    net/node_bbs.cpp
    net/node_diag.cpp
    net/node_discovery.cpp
    net/node_miner.cpp
    pbft/node_pbft.cpp
    pbft/node_pbft_validators.cpp
    net/node_peer.cpp
    net/node_peer_query.cpp
    net/node_peer_sync.cpp
    net/node_processor.cpp
    net/node_tx.cpp
    storage/db.cpp
    processor/processor.cpp
    processor/processor_assets.cpp
    processor/processor_block.cpp
    processor/processor_chain.cpp
    processor/processor_contracts.cpp
    processor/processor_evm.cpp
    processor/processor_generate.cpp
    processor/processor_interpret.cpp
    processor/processor_maintenance.cpp
    processor/processor_multiblock.cpp
    processor/processor_query.cpp
    processor/processor_recognizer.cpp
    processor/processor_storage.cpp
    storage/txpool.cpp
    bridge/bridge.cpp
    client/node_client.cpp
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
