add_library(mygramdb_storage STATIC
  dump_manager.cpp
  document_store.cpp
  document_store_persistence.cpp
  document_store_retrieval.cpp
  dump_format_v1.cpp
  dump_format_v1_config.cpp
  dump_format_v1_integrity.cpp
  dump_format_v2.cpp
  filter_index.cpp
)

target_include_directories(mygramdb_storage PUBLIC
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(mygramdb_storage PUBLIC
  spdlog::spdlog
  mygramdb_utils
  mygramdb_index
  mygramdb_config
  ${ZLIB_LIBRARIES}
)
