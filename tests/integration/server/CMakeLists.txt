# Server Integration Tests

# GTID Dump Integration Tests
add_executable(gtid_dump_integration_test
  gtid_dump_test.cpp
)

target_link_libraries(gtid_dump_integration_test
  mygramdb_server  # Includes all dependencies transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts (dump files)
gtest_discover_tests(gtid_dump_integration_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK storage_io
)

# Multi-Table Integration Tests
add_executable(multi_table_integration_test
  multi_table_test.cpp
)

target_link_libraries(multi_table_integration_test
  mygramdb_server
  GTest::gtest_main
)
target_include_directories(multi_table_integration_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding conflicts
gtest_discover_tests(multi_table_integration_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# Dual-mode CI (design doc Phase 2.6, deferred to Phase 3): the gtest_discover_tests
# call above registers the binary's tests under the reactor default. Add a second
# whole-binary ctest entry that runs the same binary with MYGRAMDB_TEST_IO_MODEL=blocking
# so the blocking fallback I/O model keeps integration coverage.
add_test(
  NAME multi_table_integration_test.BlockingMode
  COMMAND multi_table_integration_test
)
set_tests_properties(multi_table_integration_test.BlockingMode PROPERTIES
  ENVIRONMENT "MYGRAMDB_TEST_IO_MODEL=blocking"
  LABELS "INTEGRATION_BLOCKING"
  RESOURCE_LOCK server_port
  TIMEOUT 120
)

# End-to-End Integration Tests
add_executable(end_to_end_integration_test
  end_to_end_test.cpp
)

target_link_libraries(end_to_end_integration_test
  mygramdb_server
  GTest::gtest_main
)
target_include_directories(end_to_end_integration_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding conflicts
gtest_discover_tests(end_to_end_integration_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# Dual-mode CI: re-run the same binary with MYGRAMDB_TEST_IO_MODEL=blocking.
add_test(
  NAME end_to_end_integration_test.BlockingMode
  COMMAND end_to_end_integration_test
)
set_tests_properties(end_to_end_integration_test.BlockingMode PROPERTIES
  ENVIRONMENT "MYGRAMDB_TEST_IO_MODEL=blocking"
  LABELS "INTEGRATION_BLOCKING"
  RESOURCE_LOCK server_port
  TIMEOUT 120
)

# Stress Tests
add_executable(stress_test
  stress_test.cpp
)

target_link_libraries(stress_test
  mygramdb_storage  # Includes mygramdb_index transitively
  GTest::gtest_main
)

# Stress/load tests involve large-scale concurrent operations (20-110 seconds)
gtest_discover_tests(stress_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "LOAD"
    TIMEOUT 300  # 5 minutes timeout
)

# Thread pool saturation regression tests
#
# Regression gate for the blocking I/O model's starvation failure mode:
# ConnectionIOHandler holds one worker thread per persistent connection for
# its entire lifetime, so W idle clients starve every subsequent request
# until recv() idle timeouts fire.
#
# LOAD-labeled so it runs only via `make test-load` / `ctest -L LOAD`,
# not in `make test-fast` / CI.
add_executable(thread_pool_saturation_test
  thread_pool_saturation_test.cpp
)

target_link_libraries(thread_pool_saturation_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: opens many concurrent sockets and binds a test server,
# so it must not race with other server/port-using tests.
gtest_discover_tests(thread_pool_saturation_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "LOAD"
    RESOURCE_LOCK server_port
    TIMEOUT 120  # 2 minutes: starvation test waits up to 1.5s per late client
)

# Variable Handler Integration Tests
add_executable(variable_handler_integration_test
  variable_handler_test.cpp
)

target_link_libraries(variable_handler_integration_test
  mygramdb_server  # Includes mygramdb_config transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding conflicts
gtest_discover_tests(variable_handler_integration_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# Verify Text Integration Tests
add_executable(verify_text_integration_test
  verify_text_test.cpp
)

target_link_libraries(verify_text_integration_test
  mygramdb_server
  GTest::gtest_main
)
target_include_directories(verify_text_integration_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding and file I/O conflicts
gtest_discover_tests(verify_text_integration_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    RESOURCE_LOCK server_port
)

# Dual-mode CI: re-run the same binary with MYGRAMDB_TEST_IO_MODEL=blocking.
add_test(
  NAME verify_text_integration_test.BlockingMode
  COMMAND verify_text_integration_test
)
set_tests_properties(verify_text_integration_test.BlockingMode PROPERTIES
  ENVIRONMENT "MYGRAMDB_TEST_IO_MODEL=blocking"
  LABELS "INTEGRATION_BLOCKING"
  RESOURCE_LOCK server_port
  TIMEOUT 120
)

# Reactor starvation regression tests (Phase 2.T7)
#
# Goal test: reactor mode must NOT exhibit the blocking I/O model's
# starvation failure. Three tests:
#   T1 — positive control: reactor with 128 persistent clients + late client
#   T2 — throughput: 256 clients × 10 INFO requests, no ERR SERVER_BUSY
#   T3 — negative control: blocking mode starves under 8 persistent clients
add_executable(reactor_starvation_regression_test
  reactor_starvation_regression_test.cpp
)

target_link_libraries(reactor_starvation_regression_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(reactor_starvation_regression_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "INTEGRATION"
    RESOURCE_LOCK server_port
    TIMEOUT 120
)

# Reactor I/O model integration tests (Phase 2.T6)
#
# Exercises the end-to-end reactor path: config io_model="reactor" ->
# IoReactor (epoll/kqueue) -> ReactorConnection drain-task pattern ->
# RequestDispatcher.  Tests skip at runtime on platforms without epoll/kqueue.
add_executable(reactor_integration_test
  reactor_integration_test.cpp
)

target_link_libraries(reactor_integration_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: opens concurrent sockets and binds a test server port
gtest_discover_tests(reactor_integration_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "INTEGRATION"
    RESOURCE_LOCK server_port
    TIMEOUT 120  # 2 minutes: concurrent-clients tests can take ~30 s
)
