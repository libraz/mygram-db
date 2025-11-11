# Storage tests

add_executable(document_store_test
  document_store_test.cpp
)

target_link_libraries(document_store_test
  mygramdb_storage
  GTest::gtest_main
)

gtest_discover_tests(document_store_test)

# DocumentStore concurrent tests
add_executable(document_store_concurrent_test
  document_store_concurrent_test.cpp
)

target_link_libraries(document_store_concurrent_test
  mygramdb_storage
  GTest::gtest_main
)

gtest_discover_tests(document_store_concurrent_test)

# DocumentStore serialization tests
add_executable(document_store_serialization_test
  document_store_serialization_test.cpp
)

target_link_libraries(document_store_serialization_test
  mygramdb_storage
  GTest::gtest_main
)

gtest_discover_tests(document_store_serialization_test)

# GTID state tests (MySQL only)
if(USE_MYSQL AND MYSQL_FOUND)
  add_executable(gtid_state_test
    gtid_state_test.cpp
  )

  target_link_libraries(gtid_state_test
    mygramdb_storage
    GTest::gtest_main
  )

  gtest_discover_tests(gtid_state_test)

  # Snapshot save/load integration test
  add_executable(snapshot_save_load_test
    snapshot_save_load_test.cpp
  )

  target_link_libraries(snapshot_save_load_test
    mygramdb_storage
    mygramdb_index
    mygramdb_mysql
    mygramdb_query
    mygramdb_utils
    mygramdb_config
  )
endif()
