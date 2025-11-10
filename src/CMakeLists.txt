# Main executable and library

# Generate version header from template
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/version.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/version.h
  @ONLY
)

# Make version.h available to all targets
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Subdirectories
add_subdirectory(config)
add_subdirectory(mysql)
add_subdirectory(index)
add_subdirectory(storage)
add_subdirectory(server)
add_subdirectory(utils)
add_subdirectory(query)
add_subdirectory(cli)

# Main executable
add_executable(mygramdb main.cpp)

target_link_libraries(mygramdb PRIVATE
  mygramdb_config
  mygramdb_index
  mygramdb_storage
  mygramdb_server
  mygramdb_utils
  mygramdb_query
  spdlog::spdlog
  Threads::Threads
)

# Link MySQL library if available
if(USE_MYSQL AND MYSQL_FOUND)
  target_link_libraries(mygramdb PRIVATE mygramdb_mysql)
endif()

target_include_directories(mygramdb PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
