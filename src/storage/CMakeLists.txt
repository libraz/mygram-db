set(STORAGE_SOURCES
  snapshot.cpp
  document_store.cpp
)

# Add snapshot_builder and gtid_state if MySQL is available
if(USE_MYSQL AND MYSQL_FOUND)
  list(APPEND STORAGE_SOURCES snapshot_builder.cpp)
  list(APPEND STORAGE_SOURCES gtid_state.cpp)
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
)

# Don't link MySQL here to avoid circular dependency
# Components that need both storage and mysql should link both explicitly
