add_executable(config_test
  config_test.cpp
)

target_link_libraries(config_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid config file I/O conflicts
gtest_discover_tests(config_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK config_file_io
)

# Required filters test
add_executable(required_filters_test
  required_filters_test.cpp
)

target_link_libraries(required_filters_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid config file I/O conflicts
gtest_discover_tests(required_filters_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK config_file_io
)

# Required filters formatting test
add_executable(required_filters_formatting_test
  required_filters_formatting_test.cpp
)

target_link_libraries(required_filters_formatting_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid config file I/O conflicts
gtest_discover_tests(required_filters_formatting_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK config_file_io
)

# Config help test
add_executable(config_help_test
  config_help_test.cpp
)

target_link_libraries(config_help_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid config file I/O conflicts
gtest_discover_tests(config_help_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK config_file_io
)

# Logging config test
add_executable(logging_config_test
  logging_config_test.cpp
)

target_link_libraries(logging_config_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid config file I/O conflicts
gtest_discover_tests(logging_config_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK config_file_io
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

# Runtime variable manager test
add_executable(runtime_variable_manager_test
  runtime_variable_manager_test.cpp
)

target_link_libraries(runtime_variable_manager_test PRIVATE
  mygramdb_runtime_config
  GTest::gtest_main
)

gtest_discover_tests(runtime_variable_manager_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    RESOURCE_LOCK runtime_variable_manager_lock
)

# Bind address validation test
add_executable(bind_address_validation_test
  bind_address_validation_test.cpp
)

target_link_libraries(bind_address_validation_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid config file I/O conflicts
gtest_discover_tests(bind_address_validation_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK config_file_io
)

# Config validation test
add_executable(config_validation_test
  config_validation_test.cpp
)

target_link_libraries(config_validation_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid config file I/O conflicts
gtest_discover_tests(config_validation_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK config_file_io
)

# Config error handling test
add_executable(config_error_handling_test
  config_error_handling_test.cpp
)

target_link_libraries(config_error_handling_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid config file I/O conflicts
gtest_discover_tests(config_error_handling_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK config_file_io
)

# Config security test
add_executable(config_security_test
  config_security_test.cpp
)

target_link_libraries(config_security_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid config file I/O conflicts
gtest_discover_tests(config_security_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK config_file_io
)

file(GLOB MYGRAMDB_EXAMPLE_YAML_CONFIGS
  "${PROJECT_SOURCE_DIR}/examples/*.yaml"
)

foreach(example_config IN LISTS MYGRAMDB_EXAMPLE_YAML_CONFIGS)
  get_filename_component(example_name "${example_config}" NAME_WE)
  add_test(
    NAME example_yaml_config_${example_name}
    COMMAND $<TARGET_FILE:mygramdb> -t "${example_config}"
  )
  set_tests_properties(example_yaml_config_${example_name}
    PROPERTIES RESOURCE_LOCK config_file_io
  )
endforeach()

# Copy test config file to build directory
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/test_config.yaml
  ${CMAKE_CURRENT_BINARY_DIR}/test_config.yaml
  COPYONLY
)
