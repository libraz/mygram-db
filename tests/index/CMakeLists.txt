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

gtest_discover_tests(index_concurrent_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    RESOURCE_LOCK index_concurrent_lock
)

# Index GetTopN tests
add_executable(index_gettopn_test
  index_gettopn_test.cpp
)

target_link_libraries(index_gettopn_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(index_gettopn_test
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

gtest_discover_tests(posting_list_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Index optimization concurrency tests
add_executable(optimize_concurrency_test
  optimize_concurrency_test.cpp
)

target_link_libraries(optimize_concurrency_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(optimize_concurrency_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    RESOURCE_LOCK optimize_concurrent_lock
)
