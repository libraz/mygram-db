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
  PROPERTIES RESOURCE_LOCK server_state
)

# TCP Server tests

add_executable(tcp_server_test
  tcp_server_test.cpp
)

target_link_libraries(tcp_server_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding conflicts
gtest_discover_tests(tcp_server_test
  PROPERTIES RESOURCE_LOCK server_port
)

# TCP Server Multi-Table tests

add_executable(tcp_server_multi_table_test
  tcp_server_multi_table_test.cpp
)

target_link_libraries(tcp_server_multi_table_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding conflicts
gtest_discover_tests(tcp_server_multi_table_test
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
  PROPERTIES RESOURCE_LOCK thread_resources
)

# HTTP Server tests

add_executable(http_server_test
  http_server_test.cpp
)

target_link_libraries(http_server_test
  mygramdb_server
  httplib::httplib
  nlohmann_json::nlohmann_json
  GTest::gtest_main
)

target_include_directories(http_server_test PRIVATE ${CMAKE_BINARY_DIR}/src)

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding conflicts
gtest_discover_tests(http_server_test
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
  PROPERTIES RESOURCE_LOCK storage_io
)

# GTID Dump Integration tests

add_executable(gtid_dump_integration_test
  gtid_dump_integration_test.cpp
)

target_link_libraries(gtid_dump_integration_test
  mygramdb_server  # Includes all dependencies transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts (dump files)
gtest_discover_tests(gtid_dump_integration_test
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
  PROPERTIES RESOURCE_LOCK server_port
)

# Integration: Cache Pagination tests
# Tests for SEARCH query caching with OFFSET/LIMIT

add_executable(integration_cache_pagination_test
  integration_cache_pagination_test.cpp
)

target_link_libraries(integration_cache_pagination_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding and cache state conflicts
gtest_discover_tests(integration_cache_pagination_test
  PROPERTIES RESOURCE_LOCK server_port
)

# Integration: Cache COUNT tests
# Tests for COUNT query caching

add_executable(integration_cache_count_test
  integration_cache_count_test.cpp
)

target_link_libraries(integration_cache_count_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding and cache state conflicts
gtest_discover_tests(integration_cache_count_test
  PROPERTIES RESOURCE_LOCK server_port
)
