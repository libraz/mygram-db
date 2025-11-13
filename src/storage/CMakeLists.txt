set(STORAGE_SOURCES
  dump_manager.cpp
  document_store.cpp
  dump_format_v1.cpp
)

# Add snapshot_builder if MySQL is available
if(USE_MYSQL AND MYSQL_FOUND)
  list(APPEND STORAGE_SOURCES snapshot_builder.cpp)
endif()

add_library(mygramdb_storage STATIC
  ${STORAGE_SOURCES}
)

target_include_directories(mygramdb_storage PUBLIC
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(mygramdb_storage PUBLIC
  spdlog::spdlog
  mygramdb_utils
  mygramdb_index
  ${ZLIB_LIBRARIES}
)

# Don't link MySQL here to avoid circular dependency
# Components that need both storage and mysql should link both explicitly
