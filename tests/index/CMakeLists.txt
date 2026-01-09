# Index tests (split into focused files)

add_executable(index_basic_test
  index_basic_test.cpp
)

target_link_libraries(index_basic_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(index_basic_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(index_search_test
  index_search_test.cpp
)

target_link_libraries(index_search_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(index_search_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(index_batch_test
  index_batch_test.cpp
)

target_link_libraries(index_batch_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(index_batch_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(index_advanced_test
  index_advanced_test.cpp
)

target_link_libraries(index_advanced_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(index_advanced_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Index concurrent tests
add_executable(index_concurrent_test
  index_concurrent_test.cpp
)

target_link_libraries(index_concurrent_test
  mygramdb_index
  GTest::gtest_main
)

# Note: Concurrent tests involve multi-threaded operations, taking 10-25 seconds in CI
gtest_discover_tests(index_concurrent_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "SLOW"
    RESOURCE_LOCK index_concurrent_lock
    TIMEOUT 60
)

# Index GetTopN tests
add_executable(index_gettopn_test
  index_gettopn_test.cpp
)

target_link_libraries(index_gettopn_test
  mygramdb_index
  GTest::gtest_main
)

# Note: Some tests (e.g., MergeJoinSelectivityThreshold, BatchBlockSearchInsufficientResults)
# process 20,000+ documents and can take over 100 seconds in CI environments
# RUN_SERIAL prevents parallel execution conflicts and timeout issues
gtest_discover_tests(index_gettopn_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "SLOW"
    RUN_SERIAL TRUE
    TIMEOUT 180
)

# PostingList unit tests
add_executable(posting_list_test
  posting_list_test.cpp
)

target_link_libraries(posting_list_test
  mygramdb_index
  GTest::gtest_main
)

# Note: ContainsPerformanceBenchmark can fail under parallel execution due to resource contention
gtest_discover_tests(posting_list_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    RESOURCE_LOCK posting_list_lock
)

# PostingList thread safety tests
add_executable(posting_list_thread_safety_test
  posting_list_thread_safety_test.cpp
)

target_link_libraries(posting_list_thread_safety_test
  mygramdb_index
  GTest::gtest_main
)

# Note: Thread safety tests involve multi-threaded operations, taking 6-15 seconds in CI
gtest_discover_tests(posting_list_thread_safety_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 60
  PROPERTIES
    LABELS "SLOW"
    RESOURCE_LOCK posting_list_concurrent_lock
    TIMEOUT 60
)

# Index optimization concurrency tests
add_executable(optimize_concurrency_test
  optimize_concurrency_test.cpp
)

target_link_libraries(optimize_concurrency_test
  mygramdb_index
  GTest::gtest_main
)

# Note: Tests involve 10,000 documents with concurrent optimization/search/write
# operations, taking 45-90 seconds in CI environments
# On macOS, run serially to avoid scheduler timing issues
if(APPLE)
  gtest_discover_tests(optimize_concurrency_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
    PROPERTIES
      LABELS "SLOW"
      RESOURCE_LOCK optimize_concurrent_lock
      RUN_SERIAL TRUE
      TIMEOUT 120
  )
else()
  gtest_discover_tests(optimize_concurrency_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
    PROPERTIES
      LABELS "SLOW"
      RESOURCE_LOCK optimize_concurrent_lock
      TIMEOUT 120
  )
endif()

# Index bug fix tests (TDD)
add_executable(index_bug_fixes_test
  index_bug_fixes_test.cpp
)

target_link_libraries(index_bug_fixes_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(index_bug_fixes_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)
