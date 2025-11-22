# MySQL Connection Integration Tests
# Requires running MySQL server (disabled by default)

add_executable(mysql_connection_integration_test
  connection_test.cpp
)

target_link_libraries(mysql_connection_integration_test
  mygramdb_mysql
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts
gtest_discover_tests(mysql_connection_integration_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK mysql_connection
)

# MySQL Failover Integration Tests
# Tests runtime variable MySQL reconnection functionality
add_executable(mysql_failover_integration_test
  failover_test.cpp
)

target_link_libraries(mysql_failover_integration_test
  mygramdb_mysql  # Includes mygramdb_storage, mygramdb_config transitively
  mygramdb_app
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts
gtest_discover_tests(mysql_failover_integration_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK mysql_connection
)
