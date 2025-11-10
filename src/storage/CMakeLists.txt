set(STORAGE_SOURCES
  snapshot.cpp
  document_store.cpp
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
)

# Link MySQL library if available
if(USE_MYSQL AND MYSQL_FOUND)
  target_link_libraries(mygramdb_storage PUBLIC mygramdb_mysql)
endif()
