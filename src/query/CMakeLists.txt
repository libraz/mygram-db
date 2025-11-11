add_library(mygramdb_query STATIC
  query_parser.cpp
  query_ast.cpp
  result_sorter.cpp
)

target_include_directories(mygramdb_query PUBLIC
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(mygramdb_query PUBLIC
  mygramdb_index
  mygramdb_storage
  mygramdb_utils
  spdlog::spdlog
)
