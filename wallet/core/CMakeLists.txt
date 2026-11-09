cmake_minimum_required(VERSION 3.13)

add_library(wallet_core STATIC)

target_sources(wallet_core
    PRIVATE
        common.cpp
        common_utils.cpp
        default_peers.cpp
        wallet.cpp
        base_transaction.cpp
        base_tx_builder.cpp
        simple_transaction.cpp
        contract_transaction.cpp
        strings_resources.cpp
        wallet_network.cpp
        node_network.cpp
        wallet_db.cpp
        wallet_db_addresses.cpp
        wallet_db_assets.cpp
        wallet_db_coins.cpp
        wallet_db_exchange.cpp
        wallet_db_laser.cpp
        wallet_db_messages.cpp
        wallet_db_payment.cpp
        wallet_db_recovery.cpp
        wallet_db_schema.cpp
        wallet_db_shielded.cpp
        wallet_db_state.cpp
        wallet_db_storage.cpp
        wallet_db_storage_json.cpp
        wallet_db_storage_restore.cpp
        wallet_db_tokens.cpp
        wallet_db_tx.cpp
        wallet_db_types.cpp
        base58.cpp
        slatepack.cpp
        slatepack_endpoint.cpp
        version.cpp
        exchange_rate.cpp
        currency.cpp
        assets_utils.cpp
        dex.cpp
        private_key_keeper.cpp
        contracts/shaders_manager.cpp
    PUBLIC
        common.h
        default_peers.h
        wallet.h
        wallet_db.h
        wallet_network.h
        simple_transaction.h
        base_transaction.h
        private_key_keeper.h
        slatepack.h
        slatepack_endpoint.h
)

target_link_libraries(wallet_core
    PUBLIC
        core
        keykeeper
        bvm
        node
    PRIVATE
        sqlite
)

target_compile_features(wallet_core PUBLIC cxx_std_20)


if (NOT ANDROID)

    if(BEAM_LASER_SUPPORT)
        target_compile_definitions(wallet_core PUBLIC BEAM_LASER_SUPPORT)
    endif()

    if(BEAM_ASSET_SWAP_SUPPORT)
        target_compile_definitions(wallet_core PUBLIC BEAM_ASSET_SWAP_SUPPORT)
    endif()
endif()
