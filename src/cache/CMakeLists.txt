add_library(mygramdb_cache STATIC
  result_compressor.cpp
  query_cache.cpp
  invalidation_manager.cpp
  invalidation_queue.cpp
  cache_manager.cpp
)

target_include_directories(mygramdb_cache PUBLIC
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(mygramdb_cache PUBLIC
  mygramdb_query  # For Query AST types and cache key generation
  mygramdb_utils
  lz4              # For LZ4 compression
  spdlog::spdlog
)
