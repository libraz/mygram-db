add_library(mygramdb_query STATIC
  query_parser.cpp
)

target_include_directories(mygramdb_query PUBLIC
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(mygramdb_query PUBLIC
  spdlog::spdlog
)
