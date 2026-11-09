cmake_minimum_required(VERSION 3.13)

set(SOURCES_PRIVATE
    wallet_client.cpp
    wallet_client_address.cpp
    wallet_client_apps.cpp
    wallet_client_assets.cpp
    wallet_client_import_export.cpp
    wallet_client_notifications.cpp
    wallet_client_settings.cpp
    wallet_client_status.cpp
    wallet_client_tx.cpp
    filter.cpp
    apps_api/apps_api.cpp
    apps_api/apps_utils.cpp
)

set(SOURCES_PUBLIC
    wallet_client.h
    filter.h
)

add_library(wallet_client STATIC ${SOURCES_PRIVATE})

target_sources(wallet_client
    PUBLIC  ${SOURCES_PUBLIC}
)

target_link_libraries(wallet_client 
    PUBLIC wallet
    PRIVATE http
)

target_compile_features(wallet_client PUBLIC cxx_std_20)

add_subdirectory(extensions)

target_link_libraries(wallet_client PUBLIC client_extensions)
