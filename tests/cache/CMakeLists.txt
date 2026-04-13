# Cache module tests

# MD5 test
add_executable(md5_test
  md5_test.cpp
)

target_link_libraries(md5_test PRIVATE
  mygramdb_query  # md5 moved to query module
  GTest::gtest_main
)

gtest_discover_tests(md5_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# CacheKey test
add_executable(cache_key_test
  cache_key_test.cpp
)

target_link_libraries(cache_key_test PRIVATE
  mygramdb_query  # cache_key moved to query module
  GTest::gtest_main
)

gtest_discover_tests(cache_key_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# QueryNormalizer test
add_executable(query_normalizer_test
  query_normalizer_test.cpp
)

target_link_libraries(query_normalizer_test PRIVATE
  mygramdb_cache  # Includes mygramdb_query transitively
  GTest::gtest_main
)

gtest_discover_tests(query_normalizer_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# ResultCompressor test
add_executable(result_compressor_test
  result_compressor_test.cpp
)

target_link_libraries(result_compressor_test PRIVATE
  mygramdb_cache
  GTest::gtest_main
)

gtest_discover_tests(result_compressor_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# InvalidationManager test
add_executable(invalidation_manager_test
  invalidation_manager_test.cpp
)

target_link_libraries(invalidation_manager_test PRIVATE
  mygramdb_cache
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid invalidation manager state conflicts
gtest_discover_tests(invalidation_manager_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK cache_state
)

# QueryCache test
add_executable(query_cache_test
  query_cache_test.cpp
)

target_link_libraries(query_cache_test PRIVATE
  mygramdb_cache
  GTest::gtest_main
)

# Fast query_cache_test (non-TTL tests run quickly)
gtest_discover_tests(query_cache_test
  TEST_PREFIX "QueryCache."
  TEST_FILTER "-*TTL*"
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK cache_state
)

# TTL tests use sleep-based timing (2-3 seconds each)
gtest_discover_tests(query_cache_test
  TEST_PREFIX "QueryCacheSlow."
  TEST_FILTER "*TTL*"
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "SLOW"
    RESOURCE_LOCK cache_state
    TIMEOUT 30
)

# CacheManager test
add_executable(cache_manager_test
  cache_manager_test.cpp
)

target_link_libraries(cache_manager_test PRIVATE
  mygramdb_cache  # Includes mygramdb_query, mygramdb_storage, mygramdb_config transitively
  GTest::gtest_main
)

# CacheManager tests include sleep-based enable/disable tests
gtest_discover_tests(cache_manager_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "SLOW"
    RESOURCE_LOCK cache_state
    TIMEOUT 30
)

# InvalidationQueue test
add_executable(invalidation_queue_test
  invalidation_queue_test.cpp
)

target_link_libraries(invalidation_queue_test PRIVATE
  mygramdb_cache
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid invalidation queue conflicts
gtest_discover_tests(invalidation_queue_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK cache_state
)

# CacheThreadSafety test
add_executable(cache_thread_safety_test
  cache_thread_safety_test.cpp
)

target_link_libraries(cache_thread_safety_test PRIVATE
  mygramdb_cache
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid cache state conflicts
gtest_discover_tests(cache_thread_safety_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK cache_state
)

# CacheMetrics test (validates performance monitoring)
add_executable(cache_metrics_test
  cache_metrics_test.cpp
)

target_link_libraries(cache_metrics_test PRIVATE
  mygramdb_server  # Includes mygramdb_cache transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid cache state conflicts
gtest_discover_tests(cache_metrics_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK cache_state TIMEOUT 60
)
