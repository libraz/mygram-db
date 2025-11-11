# Generate embedded schema header
file(READ ${CMAKE_CURRENT_SOURCE_DIR}/config-schema.json SCHEMA_JSON_CONTENT)
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/config_schema_embedded.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/config_schema_embedded.h
  @ONLY
)

add_library(mygramdb_config STATIC
  config.cpp
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
  target_link_libraries(mygramdb_config PUBLIC
    mygramdb_mysql
  )
endif()
