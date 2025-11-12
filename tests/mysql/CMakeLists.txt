# MySQL connection tests

if(USE_MYSQL AND MYSQL_FOUND)
  add_executable(mysql_connection_test
    connection_test.cpp
  )

  target_link_libraries(mysql_connection_test
    mygramdb_mysql
    GTest::gtest_main
  )

  gtest_discover_tests(mysql_connection_test)

  # Binlog reader tests
  add_executable(mysql_binlog_reader_test
    binlog_reader_test.cpp
  )

  target_link_libraries(mysql_binlog_reader_test
    mygramdb_mysql
    mygramdb_server
    GTest::gtest_main
  )

  gtest_discover_tests(mysql_binlog_reader_test)

  # Rows parser tests
  add_executable(rows_parser_test
    rows_parser_test.cpp
  )

  target_link_libraries(rows_parser_test
    mygramdb_mysql
    GTest::gtest_main
  )

  gtest_discover_tests(rows_parser_test)

  # Binlog parsing tests (MySQL 8.0 compatibility)
  add_executable(binlog_parsing_test
    binlog_parsing_test.cpp
  )

  target_link_libraries(binlog_parsing_test
    mygramdb_mysql
    GTest::gtest_main
  )

  gtest_discover_tests(binlog_parsing_test)

  # Integration tests (disabled by default, require MySQL server)
  add_executable(mysql_connection_integration_test
    connection_integration_test.cpp
  )

  target_link_libraries(mysql_connection_integration_test
    mygramdb_mysql
    GTest::gtest_main
  )

  # Integration tests are not added to CTest by default
  # Run manually with: ENABLE_MYSQL_INTEGRATION_TESTS=1 ./bin/mysql_connection_integration_test
  # To add to CTest: gtest_discover_tests(mysql_connection_integration_test)
endif()
