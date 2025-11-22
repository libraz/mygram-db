add_library(mygramdb_storage STATIC
  dump_manager.cpp
  document_store.cpp
  dump_format_v1.cpp
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
