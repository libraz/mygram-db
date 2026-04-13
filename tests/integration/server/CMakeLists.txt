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

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding conflicts
gtest_discover_tests(multi_table_integration_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# End-to-End Integration Tests
add_executable(end_to_end_integration_test
  end_to_end_test.cpp
)

target_link_libraries(end_to_end_integration_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding conflicts
gtest_discover_tests(end_to_end_integration_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
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
# Positive-control suite for the reactor I/O model: confirms that idle
# persistent clients do NOT pin worker threads and that late clients are
# served promptly even when many persistent clients are connected.
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

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding and file I/O conflicts
gtest_discover_tests(verify_text_integration_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    RESOURCE_LOCK server_port
)

# Reactor starvation regression tests (Phase 2.T7)
#
# Reactor-mode invariant checks:
#   T1 — 128 persistent clients + late client served promptly
#   T2 — 256 clients × 10 INFO requests, no ERR SERVER_BUSY
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
# Exercises the end-to-end reactor path: IoReactor (epoll/kqueue) ->
# ReactorConnection drain-task pattern -> RequestDispatcher.
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
    RUN_SERIAL TRUE
    TIMEOUT 120  # 2 minutes: concurrent-clients tests can take ~30 s
)
