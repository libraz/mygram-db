# Index tests (split into focused files)

add_executable(index_basic_test
  index_basic_test.cpp
)

target_link_libraries(index_basic_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(index_basic_test)

add_executable(index_search_test
  index_search_test.cpp
)

target_link_libraries(index_search_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(index_search_test)

add_executable(index_batch_test
  index_batch_test.cpp
)

target_link_libraries(index_batch_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(index_batch_test)

add_executable(index_advanced_test
  index_advanced_test.cpp
)

target_link_libraries(index_advanced_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(index_advanced_test)

# Index concurrent tests
add_executable(index_concurrent_test
  index_concurrent_test.cpp
)

target_link_libraries(index_concurrent_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(index_concurrent_test
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

gtest_discover_tests(index_gettopn_test)

# PostingList unit tests
add_executable(posting_list_test
  posting_list_test.cpp
)

target_link_libraries(posting_list_test
  mygramdb_index
  GTest::gtest_main
)

gtest_discover_tests(posting_list_test)
