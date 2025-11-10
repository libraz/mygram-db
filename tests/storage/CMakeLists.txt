# Storage tests

add_executable(document_store_test
  document_store_test.cpp
)

target_link_libraries(document_store_test
  mygramdb_storage
  GTest::gtest_main
)

gtest_discover_tests(document_store_test)

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
endif()
