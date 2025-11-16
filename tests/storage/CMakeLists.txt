# Storage tests

add_executable(document_store_test
  document_store_test.cpp
)

target_link_libraries(document_store_test
  mygramdb_storage
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts
gtest_discover_tests(document_store_test
  PROPERTIES RESOURCE_LOCK storage_io
)

# DocumentStore concurrent tests
add_executable(document_store_concurrent_test
  document_store_concurrent_test.cpp
)

target_link_libraries(document_store_concurrent_test
  mygramdb_storage
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts
gtest_discover_tests(document_store_concurrent_test
  PROPERTIES RESOURCE_LOCK storage_io
)

# DocumentStore serialization tests
add_executable(document_store_serialization_test
  document_store_serialization_test.cpp
)

target_link_libraries(document_store_serialization_test
  mygramdb_storage
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts
gtest_discover_tests(document_store_serialization_test
  PROPERTIES RESOURCE_LOCK storage_io
)

# Dump commands tests (DUMP SAVE/LOAD/VERIFY/INFO)
add_executable(dump_commands_test
  dump_commands_test.cpp
)

target_link_libraries(dump_commands_test
  mygramdb_storage  # Includes mygramdb_index transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts
gtest_discover_tests(dump_commands_test
  PROPERTIES RESOURCE_LOCK storage_io
)

# Dump security tests (TOCTOU protection, symlink attacks)
add_executable(dump_security_test
  dump_security_test.cpp
)

target_link_libraries(dump_security_test
  mygramdb_config   # May include mygramdb_storage via mygramdb_mysql
  mygramdb_storage  # Includes mygramdb_index transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts
gtest_discover_tests(dump_security_test
  PROPERTIES RESOURCE_LOCK storage_io
)

# Dump save/load integration test (MySQL only)
if(USE_MYSQL AND MYSQL_FOUND)
  # Dump save/load integration test
  add_executable(dump_save_load_test
    dump_save_load_test.cpp
  )

  target_link_libraries(dump_save_load_test
    mygramdb_query  # Includes mygramdb_storage, mygramdb_index transitively
    mygramdb_config # Includes mygramdb_mysql transitively
    GTest::gtest_main
  )
endif()
