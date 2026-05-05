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
#
# SLOW filter rationale: tests that use std::this_thread::sleep_for() with
# durations >50ms must be in the SLOW suite so they are excluded from the
# fast (PR-CI) test pass. The patterns below match every test that sleeps:
#   *TTL*               -- TTL expiration tests (1-3 second sleeps)
#   *LRU*               -- LRU eviction/refresh tests (background-thread sleeps)
#   ConcurrentLookup*   -- concurrent access stress tests with 100ms run windows
#   ConcurrentSetMin*   -- runtime setter stress test with 200ms run window
#   *Bug0070_Approx*    -- LRU approximation regression (200ms sleep)
#   *Bug0070_Access*    -- access count regression (200ms sleep)
# Keep this list in sync with sleep_for occurrences. Run:
#   grep -nE "sleep_for" tests/cache/query_cache_test.cpp
# whenever adding a new test that sleeps.
gtest_discover_tests(query_cache_test
  TEST_PREFIX "QueryCache."
  # gtest --gtest_filter syntax: <positive_patterns>-<negative_patterns>,
  # with multiple patterns colon-separated. To exclude N patterns, the
  # filter takes the form "*-pat1:pat2:...:patN" (positive list is the
  # explicit "*" wildcard, then a single "-" introduces the negative list).
  # Patterns match the FULL TestSuite.TestName, so each pattern needs a
  # leading "*" if it should match by test-name suffix only.
  TEST_FILTER "*-*TTL*:*LRU*:*ConcurrentLookup*:*ConcurrentSetMin*:*Bug0070_Approx*:*Bug0070_Access*"
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK cache_state
)

# Sleep-based tests (TTL expiration, LRU refresh, concurrent stress).
# Each can sleep from 100ms (concurrent stress) to 3s (TTL expiration).
gtest_discover_tests(query_cache_test
  TEST_PREFIX "QueryCacheSlow."
  TEST_FILTER "*TTL*:*LRU*:*ConcurrentLookup*:*ConcurrentSetMin*:*Bug0070_Approx*:*Bug0070_Access*"
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
