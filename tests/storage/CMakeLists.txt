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
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
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
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK storage_io
)

# DocumentStore stress tests (marked as SLOW for CI)
add_executable(document_store_stress_test
  document_store_stress_test.cpp
)

target_link_libraries(document_store_stress_test
  mygramdb_storage
  GTest::gtest_main
)

# Note: Stress tests are designed to detect concurrency bugs like use-after-free
# through high memory pressure. They are excluded from regular CI runs.
gtest_discover_tests(document_store_stress_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "SLOW"
    RESOURCE_LOCK storage_io
    TIMEOUT 60
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
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK storage_io
)

# DocumentStore DocID overflow detection tests
add_executable(document_store_docid_overflow_test
  document_store_docid_overflow_test.cpp
)

target_link_libraries(document_store_docid_overflow_test
  mygramdb_storage
  GTest::gtest_main
)

gtest_discover_tests(document_store_docid_overflow_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
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
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK storage_io
)

# Dump security tests (TOCTOU protection, symlink attacks)
add_executable(dump_security_test
  dump_security_test.cpp
)

target_link_libraries(dump_security_test
  mygramdb_storage  # Includes mygramdb_config transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts
gtest_discover_tests(dump_security_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK storage_io
)

# Endian serialization tests (BUG-0076)
add_executable(endian_serialization_test
  endian_serialization_test.cpp
)

target_include_directories(endian_serialization_test PRIVATE
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(endian_serialization_test
  GTest::gtest_main
)

gtest_discover_tests(endian_serialization_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Atomic write tests (BUG-0077)
add_executable(atomic_write_test
  atomic_write_test.cpp
)

target_link_libraries(atomic_write_test
  mygramdb_storage
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts
gtest_discover_tests(atomic_write_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK storage_io
)

# Dump save/load integration test (MySQL only)
# NOTE: This is a manual integration test that requires a running MySQL instance.
# It is not automatically run by CTest because it depends on external MySQL setup.
if(USE_MYSQL AND MYSQL_FOUND)
  # Dump save/load integration test
  add_executable(dump_save_load_test
    dump_save_load_test.cpp
  )

  target_link_libraries(dump_save_load_test
    mygramdb_cache   # Includes mygramdb_query, mygramdb_storage, mygramdb_index, mygramdb_config transitively
    mygramdb_mysql   # Needed for mysql::Connection
    mygramdb_loader  # Needed for InitialLoader
  )

  # This is a manual integration test - do not add to CTest
  # Run manually with: ./bin/dump_save_load_test
endif()
