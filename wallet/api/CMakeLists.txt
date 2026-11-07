cmake_minimum_required(VERSION 3.13)

set(SOURCES
    base/api_base.cpp
    base/api_errors_imp.cpp
    i_wallet_api.cpp
    v6_0/v6_api.cpp
    v6_0/v6_api_handle.cpp
    v6_0/v6_api_parse.cpp
    v6_1/v6_1_api.cpp
    v6_1/v6_1_api_handle.cpp
    v6_1/v6_1_api_parse.cpp
    v6_1/v6_1_api_notify.cpp
    v7_0/v7_0_api.cpp
    v7_0/v7_0_api_handle.cpp
    v7_0/v7_0_api_parse.cpp
    v7_1/v7_1_api.cpp
    v7_1/v7_1_api_handle.cpp
    v7_1/v7_1_api_parse.cpp
    v7_2/v7_2_api.cpp
    v7_2/v7_2_api_handle.cpp
    v7_2/v7_2_api_parse.cpp
    v7_3/v7_3_api.cpp
    v7_3/v7_3_api_handle.cpp
    v7_3/v7_3_api_parse.cpp
    v7_4/v7_4_api.cpp
    v7_4/v7_4_api_handle.cpp
    v7_4/v7_4_api_parse.cpp
)

add_library(wallet_api STATIC
    ${SOURCES}
)

target_link_libraries(wallet_api
    PUBLIC
        wallet
        wallet_client
        utility
)

if(BEAM_ASSET_SWAP_SUPPORT)
    target_link_libraries(wallet_api PUBLIC client_extensions)
endif()

if (NOT BEAM_WALLET_CLIENT_LIBRARY)
    set(WALLET_API_NAME wallet-api)

    add_executable(${WALLET_API_NAME} cli/api_cli.cpp)

    target_link_libraries(${WALLET_API_NAME}
        PRIVATE
            wallet_api
            http
            cli
    )

    if(LINUX)
        target_link_libraries(${WALLET_API_NAME} 
            PRIVATE
                -static-libstdc++
                -static-libgcc)
    endif()

    if (BEAM_SIGN_PACKAGE AND WIN32)
        add_custom_command(
            TARGET ${WALLET_API_NAME} POST_BUILD
            COMMAND ${SIGNTOOL_EXECUTABLE} sign ${SIGNTOOL_OPTIONS} "${CMAKE_CURRENT_BINARY_DIR}\\${WALLET_API_NAME}.exe"
        )
    endif()
endif()