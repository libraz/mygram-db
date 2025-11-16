# Server Integration Tests

# GTID Dump Integration Tests
add_executable(gtid_dump_integration_test
  gtid_dump_test.cpp
)

target_link_libraries(gtid_dump_integration_test
  mygramdb_server  # Includes all dependencies transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts (dump files)
gtest_discover_tests(gtid_dump_integration_test
  PROPERTIES RESOURCE_LOCK storage_io
)

# Multi-Table Integration Tests
add_executable(multi_table_integration_test
  multi_table_test.cpp
)

target_link_libraries(multi_table_integration_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding conflicts
gtest_discover_tests(multi_table_integration_test
  PROPERTIES RESOURCE_LOCK server_port
)
