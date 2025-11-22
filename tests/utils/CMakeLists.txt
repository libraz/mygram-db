add_executable(string_utils_test
  string_utils_test.cpp
)

target_link_libraries(string_utils_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(string_utils_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(network_utils_test
  network_utils_test.cpp
)

target_link_libraries(network_utils_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(network_utils_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(memory_utils_test
  memory_utils_test.cpp
)

target_link_libraries(memory_utils_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(memory_utils_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(file_logging_test
  file_logging_test.cpp
)

target_link_libraries(file_logging_test PRIVATE
  spdlog::spdlog
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid logging file I/O conflicts
gtest_discover_tests(file_logging_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK logging_file_io
)

add_executable(daemon_utils_test
  daemon_utils_test.cpp
)

target_link_libraries(daemon_utils_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(daemon_utils_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(network_acl_security_test
  network_acl_security_test.cpp
)

target_link_libraries(network_acl_security_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(network_acl_security_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(expected_test
  expected_test.cpp
)

target_link_libraries(expected_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(expected_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(error_test
  error_test.cpp
)

target_link_libraries(error_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(error_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(structured_log_test
  structured_log_test.cpp
)

target_include_directories(structured_log_test PRIVATE
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(structured_log_test PRIVATE
  spdlog::spdlog
  GTest::gtest_main
)

gtest_discover_tests(structured_log_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(datetime_converter_test
  datetime_converter_test.cpp
)

target_link_libraries(datetime_converter_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(datetime_converter_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)
