# ConfigurationManager tests
add_executable(configuration_manager_test configuration_manager_test.cpp)
target_link_libraries(configuration_manager_test
    PRIVATE
    mygramdb_app
    mygramdb_config
    mygramdb_utils
    spdlog::spdlog
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(configuration_manager_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# CommandLineParser tests
add_executable(command_line_parser_test command_line_parser_test.cpp)
target_link_libraries(command_line_parser_test
    PRIVATE
    mygramdb_app
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(command_line_parser_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# SignalManager tests (SIGUSR1 log rotation)
add_executable(signal_manager_test signal_manager_test.cpp)
target_link_libraries(signal_manager_test
    PRIVATE
    mygramdb_app
    mygramdb_utils
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(signal_manager_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)
