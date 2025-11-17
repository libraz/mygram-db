add_executable(config_test
  config_test.cpp
)

target_link_libraries(config_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

gtest_discover_tests(config_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Required filters test
add_executable(required_filters_test
  required_filters_test.cpp
)

target_link_libraries(required_filters_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

gtest_discover_tests(required_filters_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Required filters formatting test
add_executable(required_filters_formatting_test
  required_filters_formatting_test.cpp
)

target_link_libraries(required_filters_formatting_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

gtest_discover_tests(required_filters_formatting_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Config help test
add_executable(config_help_test
  config_help_test.cpp
)

target_link_libraries(config_help_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

gtest_discover_tests(config_help_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Logging config test
add_executable(logging_config_test
  logging_config_test.cpp
)

target_link_libraries(logging_config_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

gtest_discover_tests(logging_config_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Rate limiting config test
add_executable(config_rate_limiting_test
  config_rate_limiting_test.cpp
)

target_link_libraries(config_rate_limiting_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

gtest_discover_tests(config_rate_limiting_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    RESOURCE_LOCK config_rate_limiting_lock
)

# Copy test config file to build directory
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/test_config.yaml
  ${CMAKE_CURRENT_BINARY_DIR}/test_config.yaml
  COPYONLY
)
