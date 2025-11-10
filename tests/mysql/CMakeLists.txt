# MySQL connection tests

if(USE_MYSQL AND MYSQL_FOUND)
  add_executable(mysql_connection_test
    connection_test.cpp
  )

  target_link_libraries(mysql_connection_test
    mygramdb_mysql
    GTest::gtest
    GTest::gtest_main
  )

  gtest_discover_tests(mysql_connection_test)

  # Binlog reader tests
  add_executable(mysql_binlog_reader_test
    binlog_reader_test.cpp
  )

  target_link_libraries(mysql_binlog_reader_test
    mygramdb_mysql
    mygramdb_index
    mygramdb_storage
    GTest::gtest
    GTest::gtest_main
  )

  gtest_discover_tests(mysql_binlog_reader_test)
endif()
