add_library(mygramdb_server STATIC
  tcp_server.cpp
  http_server.cpp
  thread_pool.cpp
  server_stats.cpp
  response_formatter.cpp
  statistics_service.cpp
  table_catalog.cpp
  connection_acceptor.cpp
  request_dispatcher.cpp
  snapshot_scheduler.cpp
  handlers/command_handler.cpp
  handlers/search_handler.cpp
  handlers/document_handler.cpp
  handlers/dump_handler.cpp
  handlers/admin_handler.cpp
  handlers/replication_handler.cpp
  handlers/debug_handler.cpp
  handlers/cache_handler.cpp
  handlers/sync_handler.cpp
)

target_include_directories(mygramdb_server PUBLIC
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(mygramdb_server PUBLIC
  mygramdb_cache      # Includes mygramdb_query transitively
  httplib::httplib
  nlohmann_json::nlohmann_json
  Threads::Threads
)
