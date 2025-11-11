# Query parser tests

add_executable(query_parser_test
  query_parser_test.cpp
)

target_link_libraries(query_parser_test
  mygramdb_query
  GTest::gtest_main
)

gtest_discover_tests(query_parser_test)

# Query AST tests

add_executable(query_ast_test
  query_ast_test.cpp
)

target_link_libraries(query_ast_test
  mygramdb_query
  GTest::gtest_main
)

gtest_discover_tests(query_ast_test)

# Result sorter tests

add_executable(result_sorter_test
  result_sorter_test.cpp
)

target_link_libraries(result_sorter_test
  mygramdb_query
  mygramdb_storage
  GTest::gtest_main
)

gtest_discover_tests(result_sorter_test)
