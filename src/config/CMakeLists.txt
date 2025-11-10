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
