# Tests

include(GoogleTest)

# Signal handler safety tests
add_executable(signal_handler_test signal_handler_test.cpp)
target_link_libraries(signal_handler_test PRIVATE GTest::gtest_main)
gtest_discover_tests(signal_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Unit test subdirectories
add_subdirectory(utils)
add_subdirectory(config)
add_subdirectory(index)
add_subdirectory(storage)
add_subdirectory(loader)
add_subdirectory(query)
add_subdirectory(cache)
add_subdirectory(mysql)
add_subdirectory(server)
add_subdirectory(client)

# Integration tests
add_subdirectory(integration)
