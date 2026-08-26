# Persistence compatibility corpus

# Emits the checked-in fixtures under corpus/. Built so that it keeps
# compiling, never run as part of a test: regenerating the corpus during a test
# run would turn a compatibility check into a round-trip of the current code.
add_executable(generate_compat_corpus
  generate_corpus.cpp
)

target_link_libraries(generate_compat_corpus
  mygramdb_storage  # Includes mygramdb_index and mygramdb_config transitively
)

target_include_directories(generate_compat_corpus PRIVATE
  ${CMAKE_SOURCE_DIR}/tests
)

# Loads every checked-in fixture through the real read paths.
add_executable(persistence_corpus_test
  persistence_corpus_test.cpp
)

target_link_libraries(persistence_corpus_test
  mygramdb_storage
  GTest::gtest_main
)

target_include_directories(persistence_corpus_test PRIVATE
  ${CMAKE_SOURCE_DIR}/tests
  ${CMAKE_SOURCE_DIR}/src
)

target_compile_definitions(persistence_corpus_test PRIVATE
  MYGRAMDB_COMPAT_CORPUS_DIR="${CMAKE_CURRENT_SOURCE_DIR}/corpus"
)

gtest_discover_tests(persistence_corpus_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK storage_io
)

# Writes one in-memory state through both dump containers and diffs the restores.
add_executable(dump_container_parity_test
  dump_container_parity_test.cpp
)

target_link_libraries(dump_container_parity_test
  mygramdb_storage
  GTest::gtest_main
)

target_include_directories(dump_container_parity_test PRIVATE
  ${CMAKE_SOURCE_DIR}/tests
  ${CMAKE_SOURCE_DIR}/src
)

gtest_discover_tests(dump_container_parity_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK storage_io
)
