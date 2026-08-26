# ServerStats tests

add_executable(operation_coordinator_test
  operation_coordinator_test.cpp
)

target_link_libraries(operation_coordinator_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(operation_coordinator_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(server_stats_test
  server_stats_test.cpp
)

target_link_libraries(server_stats_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid server stats state conflicts
gtest_add_tests(
  TARGET server_stats_test
  SOURCES server_stats_test.cpp
  TEST_LIST server_stats_registered_tests
)
set_tests_properties(${server_stats_registered_tests}
  PROPERTIES RESOURCE_LOCK server_state
)
list(FIND server_stats_registered_tests
  "ServerStatsTest.ActiveConnectionsDecrementSaturatesAtZero"
  active_connections_saturation_index
)
if(active_connections_saturation_index EQUAL -1)
  message(FATAL_ERROR
    "Exact CONCURRENCY test not found: ServerStatsTest.ActiveConnectionsDecrementSaturatesAtZero")
endif()
set_property(TEST "ServerStatsTest.ActiveConnectionsDecrementSaturatesAtZero"
  APPEND PROPERTY LABELS "CONCURRENCY"
)

# ResponseFormatter tests

add_executable(response_formatter_test
  response_formatter_test.cpp
  response_formatter_get_test.cpp
)

target_link_libraries(response_formatter_test
  mygramdb_server  # Includes all dependencies transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid formatter state conflicts
gtest_discover_tests(response_formatter_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_state
)

# ServerLifecycleManager tests

add_executable(server_lifecycle_manager_test
  server_lifecycle_manager_test.cpp
)

target_link_libraries(server_lifecycle_manager_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(server_lifecycle_manager_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# TCP Server tests (split into focused files)

add_executable(tcp_server_basic_test
  tcp_server_basic_test.cpp
)

target_link_libraries(tcp_server_basic_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(tcp_server_basic_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

add_executable(tcp_server_search_test
  tcp_server_search_test.cpp
)

target_link_libraries(tcp_server_search_test
  mygramdb_server
  GTest::gtest_main
)

add_dependencies(tcp_server_search_test mygram-cli)
target_compile_definitions(tcp_server_search_test PRIVATE
  MYGRAM_CLI_BINARY_PATH="$<TARGET_FILE:mygram-cli>"
)

gtest_discover_tests(tcp_server_search_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "SLOW"
    RESOURCE_LOCK server_port
)

add_executable(tcp_server_commands_test
  tcp_server_commands_test.cpp
)

target_link_libraries(tcp_server_commands_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(tcp_server_commands_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "SLOW"
    RESOURCE_LOCK server_port
)

add_executable(tcp_server_advanced_test
  tcp_server_advanced_test.cpp
)

target_link_libraries(tcp_server_advanced_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(tcp_server_advanced_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# CR-3 / CR-10 lifecycle / shutdown-ordering tests
add_executable(tcp_server_lifecycle_test
  tcp_server_lifecycle_test.cpp
)

target_compile_definitions(tcp_server_lifecycle_test PRIVATE MYGRAMDB_SYNC_TEST_HOOKS)

target_link_libraries(tcp_server_lifecycle_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(tcp_server_lifecycle_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    RESOURCE_LOCK server_port
    TIMEOUT 30
)

# Thread Pool tests

add_executable(thread_pool_test
  thread_pool_test.cpp
)

target_link_libraries(thread_pool_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid thread resource contention.
mygramdb_add_split_gtests(thread_pool_test
  SOURCE thread_pool_test.cpp
  RESOURCE_LOCK thread_resources
  SLOW_TIMEOUT 30
  ALL_CONCURRENCY
  SLOW_TESTS
    ThreadPoolTest.SubmitAndExecute
    ThreadPoolTest.MultipleTasks
    ThreadPoolTest.QueueSize
    ThreadPoolTest.BoundedQueue
    ThreadPoolTest.QueueFullRejectsTasks
    ThreadPoolTest.Shutdown
    ThreadPoolTest.ShutdownRejectsTasks
    ThreadPoolTest.DestructorWaitsForTasks
    ThreadPoolTest.ExceptionHandling
    ThreadPoolTest.ShutdownWithTimeoutWakesPromptlyOnDrain
    ThreadPoolTest.ShutdownWithTimeoutAbortsAfterDeadline
    ThreadPoolTest.ShutdownTimeoutDiscardsPendingTasks
)

# Thread Pool shutdown tests

add_executable(thread_pool_shutdown_test
  thread_pool_shutdown_test.cpp
)

target_link_libraries(thread_pool_shutdown_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid thread resource contention
# On macOS, run serially to avoid scheduler timing issues
if(APPLE)
  gtest_discover_tests(thread_pool_shutdown_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
    PROPERTIES
      RESOURCE_LOCK thread_resources
      RUN_SERIAL TRUE
      LABELS "SLOW"
      TIMEOUT 30
  )
else()
  gtest_discover_tests(thread_pool_shutdown_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
    PROPERTIES
      LABELS "SLOW"
      RESOURCE_LOCK thread_resources
      TIMEOUT 30
  )
endif()

# HTTP Server tests (split into focused files)

add_executable(http_server_basic_test
  http_server_basic_test.cpp
)

target_link_libraries(http_server_basic_test
  mygramdb_server
  httplib::httplib
  nlohmann_json::nlohmann_json
  GTest::gtest_main
)

target_include_directories(http_server_basic_test PRIVATE ${CMAKE_BINARY_DIR}/src)

gtest_discover_tests(http_server_basic_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(http_server_search_test
  http_server_search_test.cpp
)

target_link_libraries(http_server_search_test
  mygramdb_server
  httplib::httplib
  nlohmann_json::nlohmann_json
  GTest::gtest_main
)

target_include_directories(http_server_search_test PRIVATE ${CMAKE_BINARY_DIR}/src)

gtest_discover_tests(http_server_search_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Cross-protocol consistency: same pipeline used by HTTP and TCP
add_executable(http_tcp_consistency_test
  http_tcp_consistency_test.cpp
)

target_link_libraries(http_tcp_consistency_test
  mygramdb_server
  mygramclient_static
  httplib::httplib
  nlohmann_json::nlohmann_json
  GTest::gtest_main
)

target_include_directories(http_tcp_consistency_test PRIVATE ${CMAKE_BINARY_DIR}/src)

gtest_discover_tests(http_tcp_consistency_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(http_server_document_test
  http_server_document_test.cpp
)

target_link_libraries(http_server_document_test
  mygramdb_server
  httplib::httplib
  nlohmann_json::nlohmann_json
  GTest::gtest_main
)

target_include_directories(http_server_document_test PRIVATE ${CMAKE_BINARY_DIR}/src)

gtest_discover_tests(http_server_document_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

add_executable(http_server_features_test
  http_server_features_test.cpp
)

target_link_libraries(http_server_features_test
  mygramdb_server
  httplib::httplib
  nlohmann_json::nlohmann_json
  GTest::gtest_main
)

target_include_directories(http_server_features_test PRIVATE ${CMAKE_BINARY_DIR}/src)

gtest_discover_tests(http_server_features_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "SLOW"
)

add_executable(http_server_test
  http_server_test.cpp
)

target_link_libraries(http_server_test
  mygramdb_server
  httplib::httplib
  nlohmann_json::nlohmann_json
  GTest::gtest_main
)

target_include_directories(http_server_test PRIVATE ${CMAKE_BINARY_DIR}/src)
gtest_discover_tests(http_server_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# DumpProgress thread safety tests

add_executable(dump_progress_test
  dump_progress_test.cpp
)

target_link_libraries(dump_progress_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid threading conflicts
gtest_discover_tests(dump_progress_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    RESOURCE_LOCK thread_resources
    TIMEOUT 30
)

# DumpHandler tests

add_executable(dump_handler_test
  dump_handler_test.cpp
)

target_link_libraries(dump_handler_test
  mygramdb_server  # Includes all dependencies transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts (dump files)
gtest_discover_tests(dump_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK storage_io
)

# Dump Auto-Save tests

add_executable(dump_auto_save_test
  dump_auto_save_test.cpp
)

target_link_libraries(dump_auto_save_test
  mygramdb_server  # Includes all dependencies transitively
  GTest::gtest_main
)

# DumpAutoSave tests use sleep-based timing (2-4 seconds each)
gtest_discover_tests(dump_auto_save_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "SLOW"
    RESOURCE_LOCK storage_io
    TIMEOUT 30
)

# SyncHandler tests

add_executable(sync_handler_test
  sync_handler_test.cpp
)

target_link_libraries(sync_handler_test
  mygramdb_server  # Includes all dependencies transitively
  GTest::gtest_main
  GTest::gmock
)

# RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts
gtest_discover_tests(sync_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK mysql_connection
)

# Config Handler tests

add_executable(config_handler_test
  config_handler_test.cpp
)

target_link_libraries(config_handler_test
  mygramdb_server  # Includes all dependencies transitively
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid server state conflicts
gtest_discover_tests(config_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# RequestDispatcher tests

add_executable(request_dispatcher_test
  request_dispatcher_test.cpp
)

target_link_libraries(request_dispatcher_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid request dispatcher conflicts
gtest_discover_tests(request_dispatcher_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_port
)

# SyncOperationManager Cancel Replication Restart tests (H-3 regression)

add_executable(sync_cancel_replication_test
  sync_cancel_replication_test.cpp
)

target_link_libraries(sync_cancel_replication_test
  mygramdb_server
  GTest::gtest_main
)
target_compile_definitions(sync_cancel_replication_test PRIVATE MYGRAMDB_SYNC_TEST_HOOKS)

# RESOURCE_LOCK: Prevent parallel execution to avoid threading conflicts
gtest_discover_tests(sync_cancel_replication_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    RESOURCE_LOCK thread_resources
    TIMEOUT 30
)

# SyncOperationManager Deadlock Prevention tests

add_executable(sync_operation_manager_deadlock_test
  sync_operation_manager_deadlock_test.cpp
)

target_link_libraries(sync_operation_manager_deadlock_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid threading conflicts
gtest_discover_tests(sync_operation_manager_deadlock_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    RESOURCE_LOCK thread_resources
    TIMEOUT 30
)

# SyncOperationManager API tests (idle-state helpers)

add_executable(sync_operation_manager_test
  sync_operation_manager_test.cpp
)

target_compile_definitions(sync_operation_manager_test PRIVATE MYGRAMDB_SYNC_TEST_HOOKS)

target_link_libraries(sync_operation_manager_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(sync_operation_manager_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    RESOURCE_LOCK thread_resources
    LABELS CONCURRENCY
)

# RateLimiter tests

add_executable(rate_limiter_test
  rate_limiter_test.cpp
)

target_link_libraries(rate_limiter_test
  mygramdb_server
  GTest::gtest_main
)

mygramdb_add_split_gtests(rate_limiter_test
  SOURCE rate_limiter_test.cpp
  RESOURCE_LOCK thread_resources
  SLOW_TIMEOUT 30
  SLOW_TESTS
    TokenBucketTest.TokenRefill
    TokenBucketTest.CapacityLimit
    RateLimiterTest.RealisticRefill
    RateLimiterTest.NoDeadlockUnderConcurrentLoad
    RateLimiterTest.GetStatsDoesNotBlockAllowRequest
    RateLimiterTest.BackgroundSweeperRemovesExpiredBuckets
    RateLimiterTest.AllowRequestNoLongerPerformsInlineCleanup
)

# RateLimiter cleanup tests

add_executable(rate_limiter_cleanup_test
  rate_limiter_cleanup_test.cpp
)

target_link_libraries(rate_limiter_cleanup_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid threading conflicts
# SLOW: MemoryUsageDoesntGrowUnbounded takes 5.5+ seconds
gtest_discover_tests(rate_limiter_cleanup_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "SLOW"
    RESOURCE_LOCK thread_resources
)

# ConnectionLimit tests (SECURITY: Resource exhaustion prevention)

add_executable(connection_limit_test
  connection_limit_test.cpp
)

target_link_libraries(connection_limit_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid port binding conflicts
gtest_discover_tests(connection_limit_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "SLOW"
    RESOURCE_LOCK server_port
    TIMEOUT 30
)

# Health Endpoint tests

add_executable(health_endpoint_test
  health_endpoint_test.cpp
)

target_link_libraries(health_endpoint_test
  mygramdb_server
  httplib::httplib
  nlohmann_json::nlohmann_json
  GTest::gtest_main
)

target_include_directories(health_endpoint_test PRIVATE ${CMAKE_BINARY_DIR}/src)

# On macOS, run serially to avoid network stack resource contention
if(APPLE)
  gtest_discover_tests(health_endpoint_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
    PROPERTIES
      RUN_SERIAL TRUE
  )
else()
  gtest_discover_tests(health_endpoint_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
  )
endif()

# ReplicationHandler tests (regression tests for replication bugs)

add_executable(replication_handler_test
  replication_handler_test.cpp
)

target_link_libraries(replication_handler_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid MySQL connection conflicts
gtest_discover_tests(replication_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK mysql_connection
)

# DebugHandler tests (DEBUG and OPTIMIZE commands)

add_executable(debug_handler_test
  debug_handler_test.cpp
)

target_link_libraries(debug_handler_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid server state conflicts
gtest_discover_tests(debug_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_state
)

# AdminHandler tests (defensive guards on INFO / CONFIG paths)

add_executable(admin_handler_test
  admin_handler_test.cpp
)

target_link_libraries(admin_handler_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(admin_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_state
)

# CacheHandler tests (response framing consistency for ENABLE/DISABLE)

add_executable(cache_handler_test
  cache_handler_test.cpp
)

target_link_libraries(cache_handler_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(cache_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_state
)

# FacetHandler tests (synonym expansion routing through ExecuteFullPipeline)

add_executable(facet_handler_test
  facet_handler_test.cpp
)

target_link_libraries(facet_handler_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(facet_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_state
)

# VariableHandler tests (SET/SHOW VARIABLES commands)

add_executable(variable_handler_test
  variable_handler_test.cpp
)

target_link_libraries(variable_handler_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid server state conflicts
gtest_discover_tests(variable_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_state
)

# TableCatalog tests

add_executable(table_catalog_test
  table_catalog_test.cpp
)

target_link_libraries(table_catalog_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(table_catalog_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# SnapshotScheduler tests

add_executable(snapshot_scheduler_test
  snapshot_scheduler_test.cpp
)

target_link_libraries(snapshot_scheduler_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid file I/O conflicts.
mygramdb_add_split_gtests(snapshot_scheduler_test
  SOURCE snapshot_scheduler_test.cpp
  RESOURCE_LOCK storage_io
  SLOW_TIMEOUT 30
  SLOW_TESTS
    SnapshotSchedulerTest.AutoSnapshotSkipsWhileDumpLoadInProgress
    SnapshotSchedulerTest.AutoSnapshotSkipsWhileOptimizeInProgress
    SnapshotSchedulerTest.AutoSnapshotSkipsWhileSyncInProgress
    SnapshotSchedulerTest.CleanupRemovesOldAutoAndManualDumpTempFiles
    SnapshotSchedulerTest.CleanupContinuesOnDeleteFailure
    SnapshotSchedulerTest.StartStopRapidly
    SnapshotSchedulerTest.TakeSnapshotPausesAndResumesReplication
    SnapshotSchedulerTest.TakeSnapshotSkipsReplicationControlWhenStopped
    SnapshotSchedulerTest.AutoSnapshotDoesNotRestartIfManualStopWinsRaceBeforePause
    SnapshotSchedulerTest.AutoSnapshotRejectsEmptyGtidAfterManualStopRace
    SnapshotSchedulerTest.AutoSnapshotDoesNotRestartWhenAnotherOperationHoldsPause
    SnapshotSchedulerTest.AutoSnapshotPauseResumeStillWorksOnCleanCounter
    SnapshotSchedulerTest.EmptyGtidSnapshotResumesReplication
)

# SearchHandler tests

add_executable(search_handler_test
  search_handler_test.cpp
)

target_link_libraries(search_handler_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(search_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_state
)

# CommandHandler base helper tests (shared loading-state guard)

add_executable(command_handler_test
  command_handler_test.cpp
)

target_link_libraries(command_handler_test
  mygramdb_server
  GTest::gtest_main
)
target_compile_definitions(command_handler_test PRIVATE MYGRAMDB_SYNC_TEST_HOOKS)

gtest_discover_tests(command_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_state
)

# SyncInstancePreservation tests (Bug fix: replication events not reflected in search)

add_executable(sync_instance_preservation_test
  sync_instance_preservation_test.cpp
)

target_link_libraries(sync_instance_preservation_test
  mygramdb_storage
  GTest::gtest_main
)

gtest_discover_tests(sync_instance_preservation_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# ConnectionAcceptor Unix socket tests

add_executable(connection_acceptor_unix_test
  connection_acceptor_unix_test.cpp
)

target_link_libraries(connection_acceptor_unix_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(connection_acceptor_unix_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_socket
)


# ServerTypes tests (ServerConfig::FromConfig)

add_executable(server_types_test
  server_types_test.cpp
)

target_link_libraries(server_types_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(server_types_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# SearchPipeline tests (filter evaluation, text verification, cache staleness)

add_executable(search_pipeline_test
  search_pipeline_test.cpp
)

target_link_libraries(search_pipeline_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(search_pipeline_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_state
)

# SearchPipeline Japanese synonym E2E tests
add_executable(search_pipeline_synonym_jp_test
  search_pipeline_synonym_jp_test.cpp
)

target_link_libraries(search_pipeline_synonym_jp_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(search_pipeline_synonym_jp_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# ConnectionAcceptor TCP socket tests

add_executable(connection_acceptor_tcp_test
  connection_acceptor_tcp_test.cpp
)

target_link_libraries(connection_acceptor_tcp_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(connection_acceptor_tcp_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES
    LABELS "SLOW"
    RESOURCE_LOCK server_socket
    TIMEOUT 30
)

# Socket utilities tests (TrySetSockOpt helper)
add_executable(socket_utils_test
  socket_utils_test.cpp
)

target_link_libraries(socket_utils_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(socket_utils_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Replication-pause counter tests (Scope RAII wrapper)
add_executable(replication_pause_counter_test
  replication_pause_counter_test.cpp
)

target_link_libraries(replication_pause_counter_test
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: counter is process-wide; tests must run sequentially
gtest_discover_tests(replication_pause_counter_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK replication_pause_counter
)

add_subdirectory(reactor)

# IoReactor tests (Step 2.T5)

add_executable(io_reactor_test
  io_reactor_test.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/reactor/mock_event_multiplexer.cpp
)

target_compile_definitions(io_reactor_test PRIVATE MYGRAMDB_REACTOR_CONNECTION_TEST_HOOKS)

target_link_libraries(io_reactor_test
  mygramdb_server
  GTest::gtest_main
)

target_include_directories(io_reactor_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/reactor
)

mygramdb_add_split_gtests(io_reactor_test
  SOURCE io_reactor_test.cpp
  RESOURCE_LOCK thread_resources
  SLOW_TIMEOUT 30
  ALL_CONCURRENCY
  SLOW_TESTS
    IoReactorTest.EventDispatchRoutesToCorrectConnection
    IoReactorTest.PolledEventFromOldRegistrationCannotCloseReusedFd
    IoReactorTest.IdleConnectionIsReaped
    IoReactorTest.ActiveConnectionIsNotReaped
    IoReactorTest.InitialReadTimeoutClosesConnectionDespitePartialBytes
    IoReactorTest.StopReturnsPromptlyWithLongPollTimeout
    IoReactorTest.StopReturnsPromptlyWithRealBackend
    IoReactorTest.RegisterRaceWithStopLeavesNoStaleEntries
    IoReactorTest.RegisterPublishIsSerializedBeforeStopClear
)

# ReactorConnection unit tests (Step 2.T4)

add_executable(reactor_connection_test
  reactor_connection_test.cpp
)

target_link_libraries(reactor_connection_test
  mygramdb_server
  GTest::gtest_main
)

target_compile_definitions(reactor_connection_test PRIVATE MYGRAMDB_REACTOR_CONNECTION_TEST_HOOKS)

gtest_add_tests(
  TARGET reactor_connection_test
  SOURCES reactor_connection_test.cpp
  TEST_LIST reactor_connection_registered_tests
)
set_tests_properties(${reactor_connection_registered_tests}
  PROPERTIES RESOURCE_LOCK thread_resources
)
foreach(reactor_connection_concurrency_test
    "ReactorConnectionNoDispatcherTest.ReadOverflowErrorHoldsWriteMutexAcrossSend"
    "ReactorConnectionTest.ThreadPoolQueueExhaustionSendsServerBusyBeforeClosing")
  list(FIND reactor_connection_registered_tests
    "${reactor_connection_concurrency_test}"
    reactor_connection_concurrency_index
  )
  if(reactor_connection_concurrency_index EQUAL -1)
    message(FATAL_ERROR
      "Exact CONCURRENCY test not found: ${reactor_connection_concurrency_test}")
  endif()
  set_property(TEST "${reactor_connection_concurrency_test}"
    APPEND PROPERTY LABELS "CONCURRENCY"
  )
endforeach()

# SearchPipeline mixed CJK/ASCII exact-text tests

add_executable(search_pipeline_hybrid_exact_test
  search_pipeline_hybrid_exact_test.cpp
)

target_link_libraries(search_pipeline_hybrid_exact_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(search_pipeline_hybrid_exact_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# SearchPipeline NOT-term verification tests

add_executable(search_pipeline_not_verification_test
  search_pipeline_not_verification_test.cpp
)

target_link_libraries(search_pipeline_not_verification_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(search_pipeline_not_verification_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# SearchPipeline DOUBLE filter path parity tests

add_executable(search_pipeline_double_filter_test
  search_pipeline_double_filter_test.cpp
)

target_link_libraries(search_pipeline_double_filter_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(search_pipeline_double_filter_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# FacetHandler cache key normalization tests

add_executable(facet_handler_cache_key_test
  facet_handler_cache_key_test.cpp
)

target_link_libraries(facet_handler_cache_key_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(facet_handler_cache_key_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK server_state
)
