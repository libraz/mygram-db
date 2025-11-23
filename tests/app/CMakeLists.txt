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

gtest_discover_tests(configuration_manager_test DISCOVERY_TIMEOUT 5)
