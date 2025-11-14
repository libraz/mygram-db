# Cache module tests

# MD5 test
add_executable(md5_test
  md5_test.cpp
)

target_link_libraries(md5_test PRIVATE
  mygramdb_cache
  GTest::gtest_main
)

gtest_discover_tests(md5_test)

# CacheKey test
add_executable(cache_key_test
  cache_key_test.cpp
)

target_link_libraries(cache_key_test PRIVATE
  mygramdb_cache
  GTest::gtest_main
)

gtest_discover_tests(cache_key_test)

# QueryNormalizer test
add_executable(query_normalizer_test
  query_normalizer_test.cpp
)

target_link_libraries(query_normalizer_test PRIVATE
  mygramdb_cache
  mygramdb_query
  GTest::gtest_main
)

gtest_discover_tests(query_normalizer_test)

# ResultCompressor test
add_executable(result_compressor_test
  result_compressor_test.cpp
)

target_link_libraries(result_compressor_test PRIVATE
  mygramdb_cache
  GTest::gtest_main
)

gtest_discover_tests(result_compressor_test)

# InvalidationManager test
add_executable(invalidation_manager_test
  invalidation_manager_test.cpp
)

target_link_libraries(invalidation_manager_test PRIVATE
  mygramdb_cache
  GTest::gtest_main
)

gtest_discover_tests(invalidation_manager_test)

# QueryCache test
add_executable(query_cache_test
  query_cache_test.cpp
)

target_link_libraries(query_cache_test PRIVATE
  mygramdb_cache
  GTest::gtest_main
)

gtest_discover_tests(query_cache_test)

# CacheManager test
add_executable(cache_manager_test
  cache_manager_test.cpp
)

target_link_libraries(cache_manager_test PRIVATE
  mygramdb_cache
  mygramdb_query
  mygramdb_config
  GTest::gtest_main
)

gtest_discover_tests(cache_manager_test)

# InvalidationQueue test
add_executable(invalidation_queue_test
  invalidation_queue_test.cpp
)

target_link_libraries(invalidation_queue_test PRIVATE
  mygramdb_cache
  GTest::gtest_main
)

gtest_discover_tests(invalidation_queue_test)
