cmake_minimum_required(VERSION 3.13)

set(BVM_SRC
    wasm_interpreter.cpp
    bvm2.cpp
    bvm2_compiler.cpp
    bvm2_contract.cpp
    bvm2_methods_common.cpp
    bvm2_heap.cpp
    bvm2_methods_contract.cpp
    bvm2_hash.cpp
    bvm2_secp.cpp
    bvm2_manager.cpp
    bvm2_manager_enum.cpp
    bvm2_manager_keys.cpp
    bvm2_manager_doc.cpp
    bvm2_manager_kernel.cpp
    invoke_data.cpp
    ManagerStd.cpp
    evm.cpp
# ~etc
)

add_library(bvm STATIC ${BVM_SRC})
target_link_libraries(bvm 
    PUBLIC
        core
        ethash
)

target_include_directories(bvm PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

if (NOT BEAM_WALLET_CLIENT_LIBRARY)
    add_subdirectory(ethash_service)
endif()

if(NOT ANDROID AND NOT IOS AND NOT EMSCRIPTEN)
##if (NOT BEAM_WALLET_CLIENT_LIBRARY OR BEAM_BVM_ONLY)
    add_subdirectory(sid_generator)
#endif()
endif()

if(BEAM_TESTS_ENABLED)
    add_subdirectory(unittest)
endif()
