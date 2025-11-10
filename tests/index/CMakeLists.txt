# Index tests

add_executable(index_test
  index_test.cpp
)

target_link_libraries(index_test
  mygramdb_index
  mygramdb_utils
  GTest::gtest
  GTest::gtest_main
)

gtest_discover_tests(index_test)
