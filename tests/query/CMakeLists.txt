# Query parser tests

add_executable(query_parser_test
  query_parser_test.cpp
)

target_link_libraries(query_parser_test
  mygramdb_query
  GTest::gtest_main
)

# Non-performance tests run normally
gtest_discover_tests(query_parser_test
  TEST_FILTER "-*Performance*"
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Performance tests: labeled SLOW (coverage instrumentation makes timing unreliable)
gtest_discover_tests(query_parser_test
  TEST_PREFIX "QueryParserPerf."
  TEST_FILTER "*Performance*"
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "SLOW"
)

# Query AST tests

add_executable(query_ast_test
  query_ast_test.cpp
)

target_link_libraries(query_ast_test
  mygramdb_query
  GTest::gtest_main
)

gtest_discover_tests(query_ast_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Result sorter tests

add_executable(result_sorter_test
  result_sorter_test.cpp
)

target_link_libraries(result_sorter_test
  mygramdb_query
  GTest::gtest_main
)

gtest_discover_tests(result_sorter_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Result sorter ASC/DESC bug reproduction test

add_executable(result_sorter_asc_desc_test
  result_sorter_asc_desc_test.cpp
)

target_link_libraries(result_sorter_asc_desc_test
  mygramdb_query
  GTest::gtest_main
)

gtest_discover_tests(result_sorter_asc_desc_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)
