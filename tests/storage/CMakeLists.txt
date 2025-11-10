# Storage tests

add_executable(document_store_test
  document_store_test.cpp
)

target_link_libraries(document_store_test
  mygramdb_storage
  GTest::gtest
  GTest::gtest_main
)

gtest_discover_tests(document_store_test)
