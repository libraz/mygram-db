# MySQL module

if(USE_MYSQL AND MYSQL_FOUND)
  add_library(mygramdb_mysql
    connection.cpp
    binlog_reader.cpp
    table_metadata.cpp
    rows_parser.cpp
  )

  target_link_libraries(mygramdb_mysql
    ${MYSQL_LIBRARIES}
    mygramdb_utils
    mygramdb_index
    mygramdb_storage
    spdlog
  )

  target_include_directories(mygramdb_mysql PUBLIC
    ${PROJECT_SOURCE_DIR}/src
  )
endif()
