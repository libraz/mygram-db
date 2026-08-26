# Loader module tests

if(USE_MYSQL AND MYSQL_FOUND)
  # Initial loader query logic tests
  add_executable(initial_loader_query_test initial_loader_query_test.cpp)
  target_link_libraries(
    initial_loader_query_test PRIVATE GTest::gtest_main mygramdb_loader
  )
  target_include_directories(initial_loader_query_test PRIVATE ${CMAKE_SOURCE_DIR}/tests/mysql)
  gtest_discover_tests(initial_loader_query_test DISCOVERY_TIMEOUT 30)

  # Agreement between the snapshot rendering and the binlog row image
  add_executable(column_value_agreement_test column_value_agreement_test.cpp)
  target_link_libraries(
    column_value_agreement_test PRIVATE GTest::gtest_main mygramdb_loader
  )
  gtest_discover_tests(column_value_agreement_test DISCOVERY_TIMEOUT 30)

  # Agreement between the initial-load WHERE clause and the binlog predicate
  add_executable(required_filter_predicate_test required_filter_predicate_test.cpp)
  target_link_libraries(
    required_filter_predicate_test PRIVATE GTest::gtest_main mygramdb_loader
  )
  gtest_discover_tests(required_filter_predicate_test DISCOVERY_TIMEOUT 30)
endif()
