# Cache Integration Tests

# Cache Pagination Integration Tests
# Tests for SEARCH query caching with OFFSET/LIMIT
add_executable(cache_pagination_integration_test
  pagination_test.cpp
)

target_link_libraries(cache_pagination_integration_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding and cache state conflicts
gtest_discover_tests(cache_pagination_integration_test
  PROPERTIES RESOURCE_LOCK server_port
)

# Cache COUNT Integration Tests
# Tests for COUNT query caching
add_executable(cache_count_integration_test
  count_test.cpp
)

target_link_libraries(cache_count_integration_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding and cache state conflicts
gtest_discover_tests(cache_count_integration_test
  PROPERTIES RESOURCE_LOCK server_port
)
