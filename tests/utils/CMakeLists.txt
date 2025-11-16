add_executable(string_utils_test
  string_utils_test.cpp
)

target_link_libraries(string_utils_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(string_utils_test)

add_executable(network_utils_test
  network_utils_test.cpp
)

target_link_libraries(network_utils_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(network_utils_test)

add_executable(memory_utils_test
  memory_utils_test.cpp
)

target_link_libraries(memory_utils_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(memory_utils_test)

add_executable(file_logging_test
  file_logging_test.cpp
)

target_link_libraries(file_logging_test PRIVATE
  spdlog::spdlog
  GTest::gtest_main
)

gtest_discover_tests(file_logging_test)

add_executable(daemon_utils_test
  daemon_utils_test.cpp
)

target_link_libraries(daemon_utils_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(daemon_utils_test)
