# MygramClient tests

# Test-only client build with C API fault injection enabled. The installed
# mygramclient_static/mygramclient_shared targets never compile these hooks.
add_library(mygramclient_static_testing STATIC
  ${CMAKE_SOURCE_DIR}/src/client/mygramclient.cpp
  ${CMAKE_SOURCE_DIR}/src/client/mygramclient_c.cpp
  ${CMAKE_SOURCE_DIR}/src/client/search_expression.cpp
)
target_include_directories(mygramclient_static_testing
  PUBLIC ${CMAKE_SOURCE_DIR}/src
  PRIVATE ${CMAKE_SOURCE_DIR}/src/client
)
target_compile_definitions(mygramclient_static_testing PRIVATE MYGRAMCLIENT_C_TEST_HOOKS)
target_link_libraries(mygramclient_static_testing PUBLIC Threads::Threads)

add_executable(mygramclient_test
  mygramclient_test.cpp
)

target_link_libraries(mygramclient_test
  mygramclient_static_testing
  mygramdb_server
  GTest::gtest_main
)
target_compile_definitions(mygramclient_test PRIVATE MYGRAMCLIENT_C_TEST_HOOKS)

# RESOURCE_LOCK: Prevent parallel execution to avoid server connection conflicts
gtest_discover_tests(mygramclient_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
  PROPERTIES RESOURCE_LOCK client_server_connection
)

add_executable(c_api_v2_smoke c_api_v2_smoke.c)
set_target_properties(c_api_v2_smoke PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(c_api_v2_smoke PRIVATE mygramclient_static)
add_test(NAME CApiV2CompileLinkSmokeTest COMMAND c_api_v2_smoke)

# A `CC` of the form "ccache gcc" leaves CMAKE_C_COMPILER holding only the
# launcher, with the real compiler in CMAKE_C_COMPILER_ARG1. Forwarding the
# first half alone makes the consumer project invoke a bare launcher, so split
# the pair back into a compiler and a launcher the consumer can accept.
function(mygramdb_split_compiler_launcher compiler_var arg1_var out_compiler out_launcher)
  set(${out_compiler} "${${compiler_var}}" PARENT_SCOPE)
  set(${out_launcher} "" PARENT_SCOPE)
  if(NOT "${${arg1_var}}" STREQUAL "")
    string(STRIP "${${arg1_var}}" stripped_arg1)
    if(NOT "${stripped_arg1}" STREQUAL "")
      set(${out_compiler} "${stripped_arg1}" PARENT_SCOPE)
      set(${out_launcher} "${${compiler_var}}" PARENT_SCOPE)
    endif()
  endif()
endfunction()

mygramdb_split_compiler_launcher(CMAKE_C_COMPILER CMAKE_C_COMPILER_ARG1
  consumer_c_compiler consumer_c_launcher)
mygramdb_split_compiler_launcher(CMAKE_CXX_COMPILER CMAKE_CXX_COMPILER_ARG1
  consumer_cxx_compiler consumer_cxx_launcher)

# A coverage build instruments the client library, so linking it from an
# uninstrumented consumer leaves the gcov symbols undefined. The consumer links
# the runtime too, which keeps the export and header checks meaningful under
# coverage instead of dropping the test from the only job that runs it.
set(consumer_exe_linker_flags "")
if(ENABLE_COVERAGE)
  set(consumer_exe_linker_flags "--coverage")
endif()

# Validate the installed SDK from a separate consumer project. This catches
# missing transitive headers, broken CMake exports, and unusable pkg-config
# metadata that build-tree targets cannot expose.
add_test(
  NAME MygramClientInstalledConsumerTest
  COMMAND ${CMAKE_COMMAND}
    -DMYGRAMDB_BUILD_DIR=${CMAKE_BINARY_DIR}
    -DMYGRAMDB_BUILD_CONFIG=$<CONFIG>
    -DMYGRAMDB_C_COMPILER=${consumer_c_compiler}
    -DMYGRAMDB_CXX_COMPILER=${consumer_cxx_compiler}
    -DMYGRAMDB_C_COMPILER_LAUNCHER=${consumer_c_launcher}
    -DMYGRAMDB_CXX_COMPILER_LAUNCHER=${consumer_cxx_launcher}
    -DMYGRAMDB_EXE_LINKER_FLAGS=${consumer_exe_linker_flags}
    -DMYGRAMDB_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}
    -P ${CMAKE_CURRENT_SOURCE_DIR}/install_consumer_test.cmake
)
set_tests_properties(MygramClientInstalledConsumerTest PROPERTIES RUN_SERIAL TRUE)

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
else()
  message(WARNING "Python 3 interpreter not found: MygramClientCtypesSmokeTest will not be registered")
endif()
