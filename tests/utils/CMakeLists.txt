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

add_executable(fd_guard_test
  fd_guard_test.cpp
)

target_link_libraries(fd_guard_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(fd_guard_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(flag_guard_test
  flag_guard_test.cpp
)

target_link_libraries(flag_guard_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(flag_guard_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(binary_io_test
  binary_io_test.cpp
)

target_include_directories(binary_io_test PRIVATE
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(binary_io_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(binary_io_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(crc32_test
  crc32_test.cpp
)

target_link_libraries(crc32_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(crc32_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(hash_utils_test
  hash_utils_test.cpp
)

target_include_directories(hash_utils_test PRIVATE
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(hash_utils_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(hash_utils_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(sql_utils_test
  sql_utils_test.cpp
)

target_link_libraries(sql_utils_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(sql_utils_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(endian_utils_test
  endian_utils_test.cpp
)

target_include_directories(endian_utils_test PRIVATE
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(endian_utils_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(endian_utils_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# AtomicFileWriter tests

add_executable(comparison_utils_test
  comparison_utils_test.cpp
)

target_link_libraries(comparison_utils_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(comparison_utils_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(numeric_parse_test
  numeric_parse_test.cpp
)

target_link_libraries(numeric_parse_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(numeric_parse_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(edit_distance_test
  edit_distance_test.cpp
)

target_link_libraries(edit_distance_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(edit_distance_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(atomic_file_writer_test
  atomic_file_writer_test.cpp
)

target_link_libraries(atomic_file_writer_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts
gtest_discover_tests(atomic_file_writer_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK storage_io
)

add_executable(safe_path_test
  safe_path_test.cpp
)

target_link_libraries(safe_path_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(safe_path_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(roaring_bitmap_ptr_test
  roaring_bitmap_ptr_test.cpp
)

target_link_libraries(roaring_bitmap_ptr_test PRIVATE
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(roaring_bitmap_ptr_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)
