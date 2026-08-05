# Integration Tests
# These tests verify end-to-end functionality and component interactions

# MySQL Integration Tests
add_subdirectory(mysql)

# Server Integration Tests
add_subdirectory(server)

# Cache Integration Tests
add_subdirectory(cache)

if(UNIX)
  add_test(
    NAME DaemonModeIntegrationTest
    COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/scripts/daemon_mode.sh $<TARGET_FILE:mygramdb>
  )
  set_tests_properties(DaemonModeIntegrationTest PROPERTIES
    LABELS "INTEGRATION"
    RESOURCE_LOCK daemon_process
    TIMEOUT 15
  )

  add_test(
    NAME RootPreventionIntegrationTest
    COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/scripts/root_prevention.sh $<TARGET_FILE:mygramdb>
  )
  set_tests_properties(RootPreventionIntegrationTest PROPERTIES
    LABELS "INTEGRATION"
    TIMEOUT 10
  )
endif()
