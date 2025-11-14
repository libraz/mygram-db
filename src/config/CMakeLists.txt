# Generate embedded schema header
#
# This automatically embeds config-schema.json into config_schema_embedded.h at build time.
# When config-schema.json is modified:
#   1. CMAKE_CONFIGURE_DEPENDS triggers CMake reconfiguration
#   2. configure_file() regenerates config_schema_embedded.h with new content
#   3. mygramdb_config is rebuilt automatically
#
# No manual intervention needed - just edit config-schema.json and rebuild.

# Read schema file content
file(READ ${CMAKE_CURRENT_SOURCE_DIR}/config-schema.json SCHEMA_JSON_CONTENT)

# Configure the embedded header
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/config_schema_embedded.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/config_schema_embedded.h
  @ONLY
)

# Make CMake reconfigure when schema file changes (enables auto-regeneration)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
  ${CMAKE_CURRENT_SOURCE_DIR}/config-schema.json
)

add_library(mygramdb_config STATIC
  config.cpp
  config_help.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/config_schema_embedded.h  # Add as source to track changes
)

target_include_directories(mygramdb_config PUBLIC
  ${CMAKE_SOURCE_DIR}/src
  ${CMAKE_CURRENT_BINARY_DIR}  # For generated header
)

target_link_libraries(mygramdb_config PUBLIC
  yaml-cpp
  nlohmann_json::nlohmann_json
  nlohmann_json_schema_validator
  spdlog::spdlog
)

# Link MySQL library if available (for GTID validation)
if(USE_MYSQL AND MYSQL_FOUND)
  target_link_libraries(mygramdb_config PRIVATE
    mygramdb_mysql
  )
endif()
