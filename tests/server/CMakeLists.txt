# ServerStats tests

add_executable(server_stats_test
  server_stats_test.cpp
)

target_link_libraries(server_stats_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(server_stats_test)

# ResponseFormatter tests

add_executable(response_formatter_test
  response_formatter_test.cpp
)

target_link_libraries(response_formatter_test
  mygramdb_server  # Includes all dependencies transitively
  mygramdb_mysql
  GTest::gtest_main
)

gtest_discover_tests(response_formatter_test)

# TCP Server tests

add_executable(tcp_server_test
  tcp_server_test.cpp
)

target_link_libraries(tcp_server_test
  mygramdb_server
  mygramdb_mysql
  GTest::gtest_main
)

gtest_discover_tests(tcp_server_test)

# TCP Server Multi-Table tests

add_executable(tcp_server_multi_table_test
  tcp_server_multi_table_test.cpp
)

target_link_libraries(tcp_server_multi_table_test
  mygramdb_server
  mygramdb_mysql
  GTest::gtest_main
)

gtest_discover_tests(tcp_server_multi_table_test)

# Thread Pool tests

add_executable(thread_pool_test
  thread_pool_test.cpp
)

target_link_libraries(thread_pool_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(thread_pool_test)

# HTTP Server tests

add_executable(http_server_test
  http_server_test.cpp
)

target_link_libraries(http_server_test
  mygramdb_server
  mygramdb_mysql
  httplib::httplib
  nlohmann_json::nlohmann_json
  GTest::gtest_main
)

target_include_directories(http_server_test PRIVATE ${CMAKE_BINARY_DIR}/src)

gtest_discover_tests(http_server_test)

# DumpHandler tests

add_executable(dump_handler_test
  dump_handler_test.cpp
)

target_link_libraries(dump_handler_test
  mygramdb_server  # Includes all dependencies transitively
  mygramdb_mysql
  GTest::gtest_main
)

gtest_discover_tests(dump_handler_test)

# GTID Dump Integration tests

add_executable(gtid_dump_integration_test
  gtid_dump_integration_test.cpp
)

target_link_libraries(gtid_dump_integration_test
  mygramdb_server  # Includes all dependencies transitively
  mygramdb_mysql
  GTest::gtest_main
)

gtest_discover_tests(gtid_dump_integration_test)

# Dump Auto-Save tests

add_executable(dump_auto_save_test
  dump_auto_save_test.cpp
)

target_link_libraries(dump_auto_save_test
  mygramdb_server  # Includes all dependencies transitively
  mygramdb_mysql
  GTest::gtest_main
)

gtest_discover_tests(dump_auto_save_test)

# SyncHandler tests

add_executable(sync_handler_test
  sync_handler_test.cpp
)

target_link_libraries(sync_handler_test
  mygramdb_server  # Includes all dependencies transitively
  mygramdb_mysql
  GTest::gtest_main
)

gtest_discover_tests(sync_handler_test)
