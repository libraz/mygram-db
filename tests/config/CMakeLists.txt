add_executable(config_test
  config_test.cpp
)

target_link_libraries(config_test PRIVATE
  mygramdb_config
  GTest::gtest_main
)

gtest_discover_tests(config_test)

# Copy test config file to build directory
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/test_config.yaml
  ${CMAKE_CURRENT_BINARY_DIR}/test_config.yaml
  COPYONLY
)
