# MySQL module

if(USE_MYSQL AND MYSQL_FOUND)
  add_library(mygramdb_mysql
    connection.cpp
    connection_validator.cpp
    binlog_reader.cpp
    binlog_reader_threads.cpp
    binlog_reader_utils.cpp
    binlog_event_parser.cpp
    binlog_filter_evaluator.cpp
    binlog_event_processor.cpp
    table_metadata.cpp
    rows_parser.cpp
    rows_parser_field_decoder.cpp
    rows_parser_filter.cpp
    gtid_encoder.cpp
    mariadb_gtid.cpp
    mysql_binlog_stream.cpp
    mariadb_binlog_stream.cpp
    mariadb_event_parser.cpp
  )

  target_link_libraries(mygramdb_mysql
    ${MYSQL_LIBRARIES}
    mygramdb_utils
    mygramdb_storage  # Includes mygramdb_index transitively
    spdlog
  )

  target_include_directories(mygramdb_mysql PUBLIC
    ${PROJECT_SOURCE_DIR}/src
  )
endif()
