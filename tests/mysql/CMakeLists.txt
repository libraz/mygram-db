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
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  # Binlog reader tests
  add_executable(mysql_binlog_reader_test
    binlog_reader_test.cpp
  )

  target_link_libraries(mysql_binlog_reader_test
    mygramdb_server
    GTest::gtest_main
    GTest::gmock
  )

  # RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts and race conditions
  gtest_discover_tests(mysql_binlog_reader_test
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
    PROPERTIES RESOURCE_LOCK mysql_connection
  )
endif()
