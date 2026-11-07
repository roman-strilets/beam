cmake_minimum_required(VERSION 3.13)

add_definitions( -DSQLITE_HAS_CODEC )

add_library(wallet_test_node STATIC wallet_test_node.cpp)
target_link_libraries(wallet_test_node PUBLIC node wallet)


add_test_snippet(wallet_test wallet node mnemonic wallet_client wallet_api wallet_test_node)
add_test_snippet(wallet_db_test wallet_core)
add_test_snippet(slatepack_test wallet_core)
add_test_snippet(slatepack_exchange_test wallet node mnemonic wallet_client wallet_api wallet_test_node)
add_test_snippet(wallet_api_test wallet_api)
add_test_snippet(wallet_assets_test core node wallet_core pow assets wallet_test_node)
add_test_snippet(news_channels_test wallet_client node)
add_test_snippet(broadcasting_test broadcast_gateway node)

if (BEAM_ASSET_SWAP_SUPPORT)
    add_test_snippet(dex_board_test dex_board)
endif()

if (BEAM_LASER_SUPPORT)
#    add_test_snippet(laser_test_open node laser wallet_test_node)
#    add_test_snippet(laser_test_open_fail node laser wallet_test_node)
#    add_test_snippet(laser_test_close node laser wallet_test_node)
#    add_test_snippet(laser_test_drop node laser wallet_test_node)
#    add_test_snippet(laser_test_send node laser wallet_test_node)
#    add_test_snippet(laser_test_send_fail node laser wallet_test_node)
#    add_test_snippet(laser_test_listen_first node laser wallet_test_node)
#    add_test_snippet(laser_test_listen_second node laser wallet_test_node)
#    add_test_snippet(laser_test_auto_close node laser wallet_test_node)
#    add_test_snippet(laser_test_delete node laser wallet_test_node)
endif()

if (BEAM_LELANTUS_SUPPORT)
    add_test_snippet(lelantus_test lelantus wallet_core node wallet_test_node)
endif()

#add_executable(offline offline.cpp)
#target_link_libraries(offline node wallet_core)
