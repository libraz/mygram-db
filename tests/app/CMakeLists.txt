# ConfigurationManager tests
add_executable(configuration_manager_test configuration_manager_test.cpp)
target_link_libraries(configuration_manager_test
    PRIVATE
    mygramdb_app
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
    GTest::gtest_main
)

gtest_discover_tests(command_line_parser_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Application tests (dump directory validation)
add_executable(application_test application_test.cpp)
target_link_libraries(application_test
    PRIVATE
    mygramdb_app
    GTest::gtest_main
)

gtest_discover_tests(application_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# SignalManager tests (SIGUSR1 log rotation)
add_executable(signal_manager_test signal_manager_test.cpp)
target_link_libraries(signal_manager_test
    PRIVATE
    mygramdb_app
    GTest::gtest_main
)

gtest_discover_tests(signal_manager_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Surface snapshot test (golden file pinning the static external surface)
add_executable(surface_snapshot_test surface_snapshot_test.cpp)
target_link_libraries(surface_snapshot_test
    PRIVATE
    mygramdb_app
    GTest::gtest_main
)

target_compile_definitions(surface_snapshot_test
    PRIVATE
    MYGRAMDB_SPEC_DIR="${CMAKE_SOURCE_DIR}/spec"
)

gtest_discover_tests(surface_snapshot_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)
