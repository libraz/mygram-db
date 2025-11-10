add_executable(string_utils_test
  string_utils_test.cpp
)

target_link_libraries(string_utils_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(string_utils_test)
