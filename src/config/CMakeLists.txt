add_library(mygramdb_config STATIC
  config.cpp
)

target_include_directories(mygramdb_config PUBLIC
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(mygramdb_config PUBLIC
  yaml-cpp
  spdlog::spdlog
)

# Link MySQL library if available (for GTID validation)
if(USE_MYSQL AND MYSQL_FOUND)
  target_link_libraries(mygramdb_config PUBLIC
    mygramdb_mysql
  )
endif()
