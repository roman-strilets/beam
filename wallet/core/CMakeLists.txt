cmake_minimum_required(VERSION 3.13)

add_library(wallet_core STATIC)

target_sources(wallet_core
    PRIVATE
        common/common.cpp
        common/common_to_string.cpp
        common/common_token_json.cpp
        common/common_tx_description.cpp
        common/common_tx_parameters.cpp
        common/common_tx_status.cpp
        common/common_version.cpp
        common/common_utils.cpp
        net/default_peers.cpp
        wallet/wallet.cpp
        wallet/wallet_bodies.cpp
        wallet/wallet_events.cpp
        wallet/wallet_messages.cpp
        wallet/wallet_requests.cpp
        wallet/wallet_sync.cpp
        wallet/wallet_transactions.cpp
        wallet/wallet_vouchers.cpp
        wallet/wallet_widgets.cpp
        tx/base_transaction.cpp
        tx/base_tx_builder.cpp
        tx/base_tx_builder_balance.cpp
        tx/base_tx_builder_inouts.cpp
        tx/base_tx_builder_mutual.cpp
        tx/base_tx_builder_simple.cpp
        tx/simple_transaction.cpp
        tx/contract_transaction.cpp
        common/strings_resources.cpp
        net/wallet_network.cpp
        net/node_network.cpp
        db/wallet_db.cpp
        db/wallet_db_addresses.cpp
        db/wallet_db_assets.cpp
        db/wallet_db_coins.cpp
        db/wallet_db_exchange.cpp
        db/wallet_db_laser.cpp
        db/wallet_db_messages.cpp
        db/wallet_db_payment.cpp
        db/wallet_db_recovery.cpp
        db/wallet_db_schema.cpp
        db/wallet_db_shielded.cpp
        db/wallet_db_state.cpp
        db/wallet_db_storage.cpp
        db/wallet_db_storage_json.cpp
        db/wallet_db_storage_restore.cpp
        db/wallet_db_tokens.cpp
        db/wallet_db_tx.cpp
        db/wallet_db_types.cpp
        common/base58.cpp
        slatepack/slatepack.cpp
        slatepack/slatepack_endpoint.cpp
        common/version.cpp
        assets/exchange_rate.cpp
        assets/currency.cpp
        assets/assets_utils.cpp
        assets/dex.cpp
        keys/private_key_keeper.cpp
        contracts/shaders_manager.cpp
    PUBLIC
        common/common.h
        net/default_peers.h
        wallet/wallet.h
        db/wallet_db.h
        net/wallet_network.h
        tx/simple_transaction.h
        tx/base_transaction.h
        keys/private_key_keeper.h
        slatepack/slatepack.h
        slatepack/slatepack_endpoint.h
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

target_compile_features(wallet_core PUBLIC cxx_std_23)


if (NOT ANDROID)

    if(BEAM_LASER_SUPPORT)
        target_compile_definitions(wallet_core PUBLIC BEAM_LASER_SUPPORT)
    endif()

    if(BEAM_ASSET_SWAP_SUPPORT)
        target_compile_definitions(wallet_core PUBLIC BEAM_ASSET_SWAP_SUPPORT)
    endif()
endif()
