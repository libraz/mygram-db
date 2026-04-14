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

# GetTopN tests are load tests that require 10K+ documents for threshold-dependent
# optimization paths. They cannot be reduced without changing test semantics.
gtest_discover_tests(index_gettopn_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "LOAD"
    RUN_SERIAL TRUE
    TIMEOUT 180
)

# PostingList serialization tests
add_executable(posting_list_serialization_test
  posting_list_serialization_test.cpp
)

target_link_libraries(posting_list_serialization_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(posting_list_serialization_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# PostingList unit tests
add_executable(posting_list_test
  posting_list_test.cpp
)

target_link_libraries(posting_list_test
  mygramdb_index
  GTest::gtest_main
)

# Discover non-performance tests normally
gtest_discover_tests(posting_list_test
  TEST_PREFIX "PostingList."
  TEST_FILTER "-*Performance*"
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    RESOURCE_LOCK posting_list_lock
)

# Performance tests: labeled SLOW so they are excluded from coverage builds
# (coverage instrumentation makes performance assertions unreliable)
gtest_discover_tests(posting_list_test
  TEST_PREFIX "PostingListPerf."
  TEST_FILTER "*Performance*"
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "SLOW"
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

# Concurrency unit tests (5-15 seconds each)
# On macOS, run serially to avoid scheduler timing issues
if(APPLE)
  gtest_discover_tests(optimize_concurrency_test
    TEST_FILTER "-*RemoveDuring*:*PerformanceComparison*"
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
    TEST_FILTER "-*RemoveDuring*:*PerformanceComparison*"
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
    PROPERTIES
      LABELS "SLOW"
      RESOURCE_LOCK optimize_concurrent_lock
      TIMEOUT 120
  )
endif()

# Load tests: RemoveDuring* tests (80+ seconds each) and PerformanceComparison (benchmark)
gtest_discover_tests(optimize_concurrency_test
  TEST_PREFIX "OptimizeConcurrencyLoad."
  TEST_FILTER "*RemoveDuring*:*PerformanceComparison*"
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "LOAD"
    RESOURCE_LOCK optimize_concurrent_lock
    RUN_SERIAL TRUE
    TIMEOUT 300
)

# Empty document return value tests
add_executable(index_empty_doc_test
  index_empty_doc_test.cpp
)

target_link_libraries(index_empty_doc_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(index_empty_doc_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# BM25 scorer tests
add_executable(bm25_scorer_test
  bm25_scorer_test.cpp
)

target_link_libraries(bm25_scorer_test
  mygramdb_index
  mygramdb_storage
  GTest::gtest_main
)

gtest_discover_tests(bm25_scorer_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Empty document and SearchOr correctness tests
add_executable(index_empty_doc_and_search_test
  index_empty_doc_and_search_test.cpp
)

target_link_libraries(index_empty_doc_and_search_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(index_empty_doc_and_search_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# SearchByThreshold tests
add_executable(search_by_threshold_test
  search_by_threshold_test.cpp
)

target_link_libraries(search_by_threshold_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(search_by_threshold_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)
