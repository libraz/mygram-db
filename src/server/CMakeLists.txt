add_library(mygramdb_server STATIC
  tcp_server.cpp
  http_server.cpp
  thread_pool.cpp
)

target_include_directories(mygramdb_server PUBLIC
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(mygramdb_server PUBLIC
  mygramdb_query
  mygramdb_index
  mygramdb_storage
  mygramdb_utils
  spdlog::spdlog
  httplib::httplib
  nlohmann_json::nlohmann_json
  Threads::Threads
)
