add_library(mygramdb_index STATIC
  index.cpp
  index_optimization.cpp
  index_serialization.cpp
  posting_list.cpp
  bm25_scorer.cpp
)

target_include_directories(mygramdb_index PUBLIC
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(mygramdb_index PUBLIC
  mygramdb_utils
  roaring
  spdlog::spdlog
  absl::flat_hash_map
)

if(BUILD_TESTS)
  target_compile_definitions(mygramdb_index PUBLIC MYGRAMDB_INDEX_TEST_HOOKS)
endif()
