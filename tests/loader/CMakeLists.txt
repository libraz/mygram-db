# Loader module tests

if(USE_MYSQL AND MYSQL_FOUND)
  # Initial loader query logic tests
  add_executable(initial_loader_query_test initial_loader_query_test.cpp)
  target_link_libraries(
    initial_loader_query_test PRIVATE GTest::gtest GTest::gtest_main mygramdb_config
  )
  gtest_discover_tests(initial_loader_query_test DISCOVERY_TIMEOUT 30)

  # Initial loader bug fixes tests
  add_executable(initial_loader_bug_fixes_test initial_loader_bug_fixes_test.cpp)
  target_link_libraries(
    initial_loader_bug_fixes_test PRIVATE GTest::gtest GTest::gtest_main
    mygramdb_storage mygramdb_index
  )
  gtest_discover_tests(initial_loader_bug_fixes_test DISCOVERY_TIMEOUT 30)
endif()
