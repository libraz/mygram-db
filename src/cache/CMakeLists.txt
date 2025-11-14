add_library(mygramdb_cache STATIC
  md5.cpp
  cache_key.cpp
  query_normalizer.cpp
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
  mygramdb_query
  mygramdb_utils
  lz4              # For LZ4 compression
  spdlog::spdlog
)
