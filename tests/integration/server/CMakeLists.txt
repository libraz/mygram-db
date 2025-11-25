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
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
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
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# End-to-End Integration Tests
add_executable(end_to_end_integration_test
  end_to_end_test.cpp
)

target_link_libraries(end_to_end_integration_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding conflicts
gtest_discover_tests(end_to_end_integration_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# Stress Tests
add_executable(stress_test
  stress_test.cpp
)

target_link_libraries(stress_test
  mygramdb_storage  # Includes mygramdb_index transitively
  GTest::gtest_main
)

# Note: Stress tests involve large-scale concurrent operations
# and can take 20-110 seconds in CI environments
gtest_discover_tests(stress_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "SLOW"
    TIMEOUT 300  # 5 minutes timeout
)

# Variable Handler Integration Tests
add_executable(variable_handler_integration_test
  variable_handler_test.cpp
)

target_link_libraries(variable_handler_integration_test
  mygramdb_server  # Includes mygramdb_config transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding conflicts
gtest_discover_tests(variable_handler_integration_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)
