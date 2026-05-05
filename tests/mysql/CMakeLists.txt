# MySQL connection tests

# GTID encoder tests (no MySQL dependency)
add_executable(gtid_encoder_test
  gtid_encoder_test.cpp
)

target_link_libraries(gtid_encoder_test
  mygramdb_mysql
  GTest::gtest_main
)

gtest_discover_tests(gtid_encoder_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# RuntimeVariableManager MySQL reconnection callback tests (no MySQL dependency)
add_executable(runtime_variable_manager_mysql_test
  runtime_variable_manager_test.cpp
)

target_link_libraries(runtime_variable_manager_mysql_test
  mygramdb_config
  GTest::gtest_main
)

gtest_discover_tests(runtime_variable_manager_mysql_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Binlog CRC32 verification tests (no MySQL dependency)
add_executable(binlog_crc32_test
  binlog_crc32_test.cpp
)

target_link_libraries(binlog_crc32_test
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(binlog_crc32_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# NullBinlogReader tests (no MySQL dependency)
add_executable(null_binlog_reader_test
  null_binlog_reader_test.cpp
)

target_link_libraries(null_binlog_reader_test
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(null_binlog_reader_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# IBinlogReader::Stop() synchronous-contract tests (CR-9, no MySQL dependency)
add_executable(binlog_reader_stop_contract_test
  binlog_reader_stop_contract_test.cpp
)

target_link_libraries(binlog_reader_stop_contract_test
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(binlog_reader_stop_contract_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

if(USE_MYSQL AND MYSQL_FOUND)
  # MariaDB GTID parsing tests (no MySQL connection required)
  add_executable(mariadb_gtid_test
    mariadb_gtid_test.cpp
  )

  target_link_libraries(mariadb_gtid_test
    mygramdb_mysql
    GTest::gtest_main
  )

  gtest_discover_tests(mariadb_gtid_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
  )

  # ENUM/SET field decoder tests
  add_executable(rows_parser_enum_set_test
    rows_parser_enum_set_test.cpp
  )

  target_link_libraries(rows_parser_enum_set_test
    mygramdb_mysql
    GTest::gtest_main
  )

  gtest_discover_tests(rows_parser_enum_set_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
  )

  # JSON field decoder tests
  add_executable(rows_parser_json_decode_test
    rows_parser_json_decode_test.cpp
  )

  target_link_libraries(rows_parser_json_decode_test
    mygramdb_mysql
    GTest::gtest_main
  )

  gtest_discover_tests(rows_parser_json_decode_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
  )

  # Table metadata cache tests (no MySQL connection required)
  add_executable(table_metadata_test
    table_metadata_test.cpp
  )

  target_link_libraries(table_metadata_test
    mygramdb_mysql
    GTest::gtest_main
  )

  gtest_discover_tests(table_metadata_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
  )

  add_executable(mysql_connection_test
    connection_test.cpp
  )

  target_link_libraries(mysql_connection_test
    mygramdb_mysql
    GTest::gtest_main
  )

  # RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts and race conditions
  gtest_discover_tests(mysql_connection_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  # Binlog reader tests (split into focused files)

  add_executable(binlog_reader_core_test
    binlog_reader_core_test.cpp
  )

  target_link_libraries(binlog_reader_core_test
    mygramdb_server
    GTest::gtest_main
    GTest::gmock
  )

  # LABELS SLOW: Contains sleep_for-based timing assertions (e.g., PushBlocksWhenQueueFull)
  gtest_discover_tests(binlog_reader_core_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
    LABELS "SLOW"
  )

  add_executable(binlog_reader_events_test
    binlog_reader_events_test.cpp
  )

  target_link_libraries(binlog_reader_events_test
    mygramdb_server
    GTest::gtest_main
    GTest::gmock
  )

  gtest_discover_tests(binlog_reader_events_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  add_executable(binlog_reader_multitable_test
    binlog_reader_multitable_test.cpp
  )

  target_link_libraries(binlog_reader_multitable_test
    mygramdb_server
    GTest::gtest_main
    GTest::gmock
  )

  gtest_discover_tests(binlog_reader_multitable_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  # Rows parser tests
  add_executable(rows_parser_test
    rows_parser_test.cpp
  )

  target_link_libraries(rows_parser_test
    mygramdb_mysql  # Includes mygramdb_storage and mygramdb_config transitively
    GTest::gtest_main
  )

  # RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts and race conditions
  gtest_discover_tests(rows_parser_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  # Connection validation unit tests (no MySQL connection required)
  add_executable(connection_validate_unit_test
    connection_validate_unit_test.cpp
  )

  target_link_libraries(connection_validate_unit_test
    mygramdb_mysql
    GTest::gtest_main
    GTest::gmock
  )

  gtest_discover_tests(connection_validate_unit_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
  )

  # Connection validator unit tests (no MySQL connection required)
  add_executable(connection_validator_unit_test
    connection_validator_unit_test.cpp
  )

  target_link_libraries(connection_validator_unit_test
    mygramdb_mysql
    GTest::gtest_main
    GTest::gmock
  )

  gtest_discover_tests(connection_validator_unit_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
  )

  # Binlog parsing tests (MySQL 8.0 compatibility)
  add_executable(binlog_parsing_test
    binlog_parsing_test.cpp
  )

  target_link_libraries(binlog_parsing_test
    mygramdb_mysql  # Includes mygramdb_storage and mygramdb_config transitively
    GTest::gtest_main
  )

  # RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts and race conditions
  gtest_discover_tests(binlog_parsing_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  # Binlog filter validation tests (SECURITY: Input validation)
  add_executable(binlog_filter_validation_test
    binlog_filter_validation_test.cpp
  )

  target_link_libraries(binlog_filter_validation_test
    mygramdb_server
    GTest::gtest_main
  )

  # Binlog filter evaluator tests
  add_executable(binlog_filter_evaluator_test
    binlog_filter_evaluator_test.cpp
  )

  target_link_libraries(binlog_filter_evaluator_test
    mygramdb_server
    GTest::gtest_main
  )

  gtest_discover_tests(binlog_filter_evaluator_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
  )

  # RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts
  gtest_discover_tests(binlog_filter_validation_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  # Binlog reader resource management tests
  add_executable(binlog_reader_resource_test
    binlog_reader_resource_test.cpp
  )

  target_link_libraries(binlog_reader_resource_test
    mygramdb_server
    GTest::gtest_main
  )

  # RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts
  gtest_discover_tests(binlog_reader_resource_test
    DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
    PROPERTIES RESOURCE_LOCK mysql_connection
  )

  # Binlog reader resource unit tests (no MySQL connection required)
  add_executable(binlog_reader_resource_unit_test
    binlog_reader_resource_unit_test.cpp
  )

  target_link_libraries(binlog_reader_resource_unit_test
    mygramdb_server
    GTest::gtest_main
  )

  gtest_discover_tests(binlog_reader_resource_unit_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
  )

  # MariaDB event parser tests (no MySQL connection required)
  add_executable(mariadb_event_parser_test
    mariadb_event_parser_test.cpp
  )

  target_link_libraries(mariadb_event_parser_test
    mygramdb_mysql
    GTest::gtest_main
  )

  gtest_discover_tests(mariadb_event_parser_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
  )

  # Binlog event processor tests
  add_executable(binlog_event_processor_test
    binlog_event_processor_test.cpp
  )

  target_link_libraries(binlog_event_processor_test
    mygramdb_server
    GTest::gtest_main
  )

  gtest_discover_tests(binlog_event_processor_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
  )

endif()
