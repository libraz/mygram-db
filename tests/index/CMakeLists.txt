# Index tests

add_executable(index_test
  index_test.cpp
)

target_link_libraries(index_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(index_test)

# Index concurrent tests
add_executable(index_concurrent_test
  index_concurrent_test.cpp
)

target_link_libraries(index_concurrent_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(index_concurrent_test)
