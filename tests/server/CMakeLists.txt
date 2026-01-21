# ServerStats tests

add_executable(server_stats_test
  server_stats_test.cpp
)

target_link_libraries(server_stats_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid server stats state conflicts
gtest_discover_tests(server_stats_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_state
)

# ResponseFormatter tests

add_executable(response_formatter_test
  response_formatter_test.cpp
)

target_link_libraries(response_formatter_test
  mygramdb_server  # Includes all dependencies transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid formatter state conflicts
gtest_discover_tests(response_formatter_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_state
)

# ServerLifecycleManager tests

add_executable(server_lifecycle_manager_test
  server_lifecycle_manager_test.cpp
)

target_link_libraries(server_lifecycle_manager_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(server_lifecycle_manager_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# TCP Server tests (split into focused files)

add_executable(tcp_server_basic_test
  tcp_server_basic_test.cpp
)

target_link_libraries(tcp_server_basic_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(tcp_server_basic_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

add_executable(tcp_server_search_test
  tcp_server_search_test.cpp
)

target_link_libraries(tcp_server_search_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(tcp_server_search_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

add_executable(tcp_server_commands_test
  tcp_server_commands_test.cpp
)

target_link_libraries(tcp_server_commands_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(tcp_server_commands_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

add_executable(tcp_server_advanced_test
  tcp_server_advanced_test.cpp
)

target_link_libraries(tcp_server_advanced_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(tcp_server_advanced_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# Thread Pool tests

add_executable(thread_pool_test
  thread_pool_test.cpp
)

target_link_libraries(thread_pool_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid thread resource contention
gtest_discover_tests(thread_pool_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK thread_resources
)

# Thread Pool shutdown tests

add_executable(thread_pool_shutdown_test
  thread_pool_shutdown_test.cpp
)

target_link_libraries(thread_pool_shutdown_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid thread resource contention
# On macOS, run serially to avoid scheduler timing issues
if(APPLE)
  gtest_discover_tests(thread_pool_shutdown_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
    PROPERTIES
      RESOURCE_LOCK thread_resources
      RUN_SERIAL TRUE
  )
else()
  gtest_discover_tests(thread_pool_shutdown_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK thread_resources
  )
endif()

# HTTP Server tests (split into focused files)

add_executable(http_server_basic_test
  http_server_basic_test.cpp
)

target_link_libraries(http_server_basic_test
  mygramdb_server
  httplib::httplib
  nlohmann_json::nlohmann_json
  GTest::gtest_main
)

target_include_directories(http_server_basic_test PRIVATE ${CMAKE_BINARY_DIR}/src)

gtest_discover_tests(http_server_basic_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

add_executable(http_server_search_test
  http_server_search_test.cpp
)

target_link_libraries(http_server_search_test
  mygramdb_server
  httplib::httplib
  nlohmann_json::nlohmann_json
  GTest::gtest_main
)

target_include_directories(http_server_search_test PRIVATE ${CMAKE_BINARY_DIR}/src)

gtest_discover_tests(http_server_search_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

add_executable(http_server_document_test
  http_server_document_test.cpp
)

target_link_libraries(http_server_document_test
  mygramdb_server
  httplib::httplib
  nlohmann_json::nlohmann_json
  GTest::gtest_main
)

target_include_directories(http_server_document_test PRIVATE ${CMAKE_BINARY_DIR}/src)

gtest_discover_tests(http_server_document_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

add_executable(http_server_features_test
  http_server_features_test.cpp
)

target_link_libraries(http_server_features_test
  mygramdb_server
  httplib::httplib
  nlohmann_json::nlohmann_json
  GTest::gtest_main
)

target_include_directories(http_server_features_test PRIVATE ${CMAKE_BINARY_DIR}/src)

gtest_discover_tests(http_server_features_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# DumpHandler tests

add_executable(dump_handler_test
  dump_handler_test.cpp
)

target_link_libraries(dump_handler_test
  mygramdb_server  # Includes all dependencies transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts (dump files)
gtest_discover_tests(dump_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK storage_io
)

# Dump Auto-Save tests

add_executable(dump_auto_save_test
  dump_auto_save_test.cpp
)

target_link_libraries(dump_auto_save_test
  mygramdb_server  # Includes all dependencies transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts (dump files)
gtest_discover_tests(dump_auto_save_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK storage_io
)

# SyncHandler tests

add_executable(sync_handler_test
  sync_handler_test.cpp
)

target_link_libraries(sync_handler_test
  mygramdb_server  # Includes all dependencies transitively
  GTest::gtest_main
  GTest::gmock
)

# RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts
gtest_discover_tests(sync_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK mysql_connection
)

# Config Handler tests

add_executable(config_handler_test
  config_handler_test.cpp
)

target_link_libraries(config_handler_test
  mygramdb_server  # Includes all dependencies transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid server state conflicts
gtest_discover_tests(config_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# ConnectionIOHandler tests

add_executable(connection_io_handler_test
  connection_io_handler_test.cpp
)

target_link_libraries(connection_io_handler_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid I/O handler conflicts
gtest_discover_tests(connection_io_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# RequestDispatcher tests

add_executable(request_dispatcher_test
  request_dispatcher_test.cpp
)

target_link_libraries(request_dispatcher_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid request dispatcher conflicts
gtest_discover_tests(request_dispatcher_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# SyncOperationManager Deadlock Prevention tests

add_executable(sync_operation_manager_deadlock_test
  sync_operation_manager_deadlock_test.cpp
)

target_link_libraries(sync_operation_manager_deadlock_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid threading conflicts
gtest_discover_tests(sync_operation_manager_deadlock_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK thread_resources
)

# RateLimiter tests

add_executable(rate_limiter_test
  rate_limiter_test.cpp
)

target_link_libraries(rate_limiter_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid threading conflicts
gtest_discover_tests(rate_limiter_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK thread_resources
)

# RateLimiter cleanup tests

add_executable(rate_limiter_cleanup_test
  rate_limiter_cleanup_test.cpp
)

target_link_libraries(rate_limiter_cleanup_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid threading conflicts
gtest_discover_tests(rate_limiter_cleanup_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK thread_resources
)

# ConnectionLimit tests (SECURITY: Resource exhaustion prevention)

add_executable(connection_limit_test
  connection_limit_test.cpp
)

target_link_libraries(connection_limit_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding conflicts
gtest_discover_tests(connection_limit_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# Health Endpoint tests

add_executable(health_endpoint_test
  health_endpoint_test.cpp
)

target_link_libraries(health_endpoint_test
  mygramdb_server
  httplib::httplib
  nlohmann_json::nlohmann_json
  GTest::gtest_main
)

target_include_directories(health_endpoint_test PRIVATE ${CMAKE_BINARY_DIR}/src)

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding conflicts
# On macOS, run serially to avoid network stack resource contention
if(APPLE)
  gtest_discover_tests(health_endpoint_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
    PROPERTIES
      RESOURCE_LOCK server_port
      RUN_SERIAL TRUE
  )
else()
  gtest_discover_tests(health_endpoint_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK server_port
  )
endif()

# ReplicationHandler tests (regression tests for replication bugs)

add_executable(replication_handler_test
  replication_handler_test.cpp
)

target_link_libraries(replication_handler_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts
gtest_discover_tests(replication_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK mysql_connection
)

# DebugHandler tests (DEBUG and OPTIMIZE commands)

add_executable(debug_handler_test
  debug_handler_test.cpp
)

target_link_libraries(debug_handler_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid server state conflicts
gtest_discover_tests(debug_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_state
)

# VariableHandler tests (SET/SHOW VARIABLES commands)

add_executable(variable_handler_test
  variable_handler_test.cpp
)

target_link_libraries(variable_handler_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid server state conflicts
gtest_discover_tests(variable_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_state
)

# TableCatalog tests

add_executable(table_catalog_test
  table_catalog_test.cpp
)

target_link_libraries(table_catalog_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(table_catalog_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# SnapshotScheduler tests

add_executable(snapshot_scheduler_test
  snapshot_scheduler_test.cpp
)

target_link_libraries(snapshot_scheduler_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts
gtest_discover_tests(snapshot_scheduler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK storage_io
)

# SearchHandler bug fix tests (TDD)

add_executable(search_handler_bug_fixes_test
  search_handler_bug_fixes_test.cpp
)

target_link_libraries(search_handler_bug_fixes_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(search_handler_bug_fixes_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_state
)

# SyncInstancePreservation tests (Bug fix: replication events not reflected in search)

add_executable(sync_instance_preservation_test
  sync_instance_preservation_test.cpp
)

target_link_libraries(sync_instance_preservation_test
  mygramdb_index
  mygramdb_storage
  GTest::gtest_main
)

gtest_discover_tests(sync_instance_preservation_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

