# Query parser tests

add_executable(query_parser_test
  query_parser_test.cpp
)

target_link_libraries(query_parser_test
  mygramdb_query
  GTest::gtest
  GTest::gtest_main
)

gtest_discover_tests(query_parser_test)
