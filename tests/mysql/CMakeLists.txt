# MySQL connection tests

if(USE_MYSQL AND MYSQL_FOUND)
  add_executable(mysql_connection_test
    connection_test.cpp
  )

  target_link_libraries(mysql_connection_test
    mygramdb_mysql
    GTest::gtest_main
  )

  # RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts and race conditions
  gtest_discover_tests(mysql_connection_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  # Binlog reader tests (split into focused files)

  add_executable(binlog_reader_core_test
    binlog_reader_core_test.cpp
  )

  target_link_libraries(binlog_reader_core_test
    mygramdb_server
    GTest::gtest_main
    GTest::gmock
  )

  gtest_discover_tests(binlog_reader_core_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  add_executable(binlog_reader_events_test
    binlog_reader_events_test.cpp
  )

  target_link_libraries(binlog_reader_events_test
    mygramdb_server
    GTest::gtest_main
    GTest::gmock
  )

  gtest_discover_tests(binlog_reader_events_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  add_executable(binlog_reader_multitable_test
    binlog_reader_multitable_test.cpp
  )

  target_link_libraries(binlog_reader_multitable_test
    mygramdb_server
    GTest::gtest_main
    GTest::gmock
  )

  gtest_discover_tests(binlog_reader_multitable_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  # Rows parser tests
  add_executable(rows_parser_test
    rows_parser_test.cpp
  )

  target_link_libraries(rows_parser_test
    mygramdb_mysql
    GTest::gtest_main
  )

  # RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts and race conditions
  gtest_discover_tests(rows_parser_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  # Connection validation tests
  add_executable(connection_validate_test
    connection_validate_test.cpp
  )

  target_link_libraries(connection_validate_test
    mygramdb_mysql
    GTest::gtest_main
    GTest::gmock
  )

  # RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts and race conditions
  gtest_discover_tests(connection_validate_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  # Connection validator tests (failover detection)
  add_executable(connection_validator_test
    connection_validator_test.cpp
  )

  target_link_libraries(connection_validator_test
    mygramdb_mysql
    GTest::gtest_main
    GTest::gmock
  )

  # RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts and race conditions
  gtest_discover_tests(connection_validator_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  # Binlog parsing tests (MySQL 8.0 compatibility)
  add_executable(binlog_parsing_test
    binlog_parsing_test.cpp
  )

  target_link_libraries(binlog_parsing_test
    mygramdb_mysql
    GTest::gtest_main
  )

  # RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts and race conditions
  gtest_discover_tests(binlog_parsing_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  # Binlog filter validation tests (SECURITY: Input validation)
  add_executable(binlog_filter_validation_test
    binlog_filter_validation_test.cpp
  )

  target_link_libraries(binlog_filter_validation_test
    mygramdb_server
    GTest::gtest_main
  )

  # RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts
  gtest_discover_tests(binlog_filter_validation_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  # Binlog reader resource management tests
  add_executable(binlog_reader_resource_test
    binlog_reader_resource_test.cpp
  )

  target_link_libraries(binlog_reader_resource_test
    mygramdb_server
    GTest::gtest_main
  )

  # RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts
  gtest_discover_tests(binlog_reader_resource_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )
endif()
