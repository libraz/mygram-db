# MygramClient tests

add_executable(mygramclient_test
  mygramclient_test.cpp
)

target_link_libraries(mygramclient_test
  mygramclient_static
  mygramdb_server
  GTest::gtest_main
)

# RESOURCE_LOCK: Prevent parallel execution to avoid server connection conflicts
gtest_discover_tests(mygramclient_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK client_server_connection
)

# Search expression parser tests

add_executable(search_expression_test
  search_expression_test.cpp
)

target_link_libraries(search_expression_test
  mygramclient_static
  GTest::gtest_main
)

gtest_discover_tests(search_expression_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

find_package(Python3 COMPONENTS Interpreter QUIET)
if(Python3_Interpreter_FOUND)
  add_test(
    NAME MygramClientCtypesSmokeTest
    COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/ctypes_smoke_test.py
      $<TARGET_FILE:mygramclient_shared>
  )
  set_tests_properties(MygramClientCtypesSmokeTest PROPERTIES
    DEPENDS mygramclient_shared
    RESOURCE_LOCK client_server_connection
  )
endif()
