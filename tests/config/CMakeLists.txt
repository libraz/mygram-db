add_executable(config_test
  config_test.cpp
)

target_link_libraries(config_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

gtest_discover_tests(config_test)

# Required filters test
add_executable(required_filters_test
  required_filters_test.cpp
)

target_link_libraries(required_filters_test PRIVATE
  mygramdb_config
  mygramdb_storage
  GTest::gtest_main
)

gtest_discover_tests(required_filters_test)

# Required filters formatting test
add_executable(required_filters_formatting_test
  required_filters_formatting_test.cpp
)

target_link_libraries(required_filters_formatting_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

gtest_discover_tests(required_filters_formatting_test)

# Copy test config file to build directory
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/test_config.yaml
  ${CMAKE_CURRENT_BINARY_DIR}/test_config.yaml
  COPYONLY
)
