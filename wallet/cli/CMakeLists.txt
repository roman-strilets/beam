cmake_minimum_required(VERSION 3.13)

set(TARGET_NAME beam-wallet)


set(CLI_SRC
    cli.cpp
    cli_address.cpp
    cli_assets.cpp
    cli_blocks.cpp
    cli_common.cpp
    cli_export.cpp
    cli_info.cpp
    cli_init.cpp
    cli_shader.cpp
    cli_transactions.cpp
    cli_treasury.cpp
    utils.cpp
)
if(BEAM_LASER_SUPPORT)
    set(CLI_SRC
        ${CLI_SRC}
        laser.cpp
    )
endif()

if(BEAM_ASSET_SWAP_SUPPORT)
    set(CLI_SRC
        ${CLI_SRC}
        assets_swap.cpp
        cli_assets_swap.cpp
    )
endif()

add_executable(${TARGET_NAME} ${CLI_SRC})
target_compile_definitions(${TARGET_NAME} PRIVATE APP_NAME=\"${TARGET_NAME}\")

if(LINUX)
    target_link_libraries(${TARGET_NAME}
        PRIVATE 
            -static-libstdc++ 
            -static-libgcc)
endif()

target_link_libraries(${TARGET_NAME}
    PRIVATE
        wallet
        cli
        mnemonic
)

if(BEAM_LASER_SUPPORT)
    target_link_libraries(${TARGET_NAME} PRIVATE laser)
endif()

if(BEAM_ASSET_SWAP_SUPPORT)
    target_link_libraries(${TARGET_NAME} PRIVATE broadcast_gateway dex_board)
endif()

if (BEAM_SIGN_PACKAGE AND WIN32)
    add_custom_command(
        TARGET ${TARGET_NAME} POST_BUILD
        COMMAND ${SIGNTOOL_EXECUTABLE} sign ${SIGNTOOL_OPTIONS} "${CMAKE_CURRENT_BINARY_DIR}\\{TARGET_NAME}.exe"
    )
endif()
