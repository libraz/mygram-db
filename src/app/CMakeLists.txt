# Application layer library

add_library(mygramdb_app STATIC
  application.cpp
  command_line_parser.cpp
  configuration_manager.cpp
  mysql_reconnection_handler.cpp
  server_orchestrator.cpp
  signal_manager.cpp
)

target_include_directories(mygramdb_app PUBLIC
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(mygramdb_app PUBLIC
  mygramdb_config
  mygramdb_server
  mygramdb_utils
  spdlog::spdlog
)

# Link MySQL and loader libraries if enabled
if(USE_MYSQL)
  target_link_libraries(mygramdb_app PUBLIC
    mygramdb_mysql
    mygramdb_loader
  )
endif()
