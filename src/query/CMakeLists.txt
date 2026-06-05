add_library(mygramdb_query STATIC
  query_parser.cpp
  query_parser_commands.cpp
  query_parser_clauses.cpp
  query_ast.cpp
  result_sorter.cpp
  query_normalizer.cpp
  synonym_dictionary.cpp
  highlighter.cpp
)

target_include_directories(mygramdb_query PUBLIC
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(mygramdb_query PUBLIC
  mygramdb_storage  # Includes mygramdb_index transitively
  mygramdb_cache_key
  mygramdb_utils
  spdlog::spdlog
)
