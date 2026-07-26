# Tests

include(GoogleTest)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Register a GoogleTest target once, then apply orthogonal labels to exact
# cases. gtest_add_tests scans sources at configure time, which keeps this
# split compatible with the project's CMake 3.15 floor.
function(mygramdb_add_split_gtests target)
  set(options ALL_CONCURRENCY)
  set(one_value_args SOURCE RESOURCE_LOCK SLOW_TIMEOUT)
  set(multi_value_args SLOW_TESTS CONCURRENCY_TESTS)
  cmake_parse_arguments(SPLIT "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  if(SPLIT_UNPARSED_ARGUMENTS)
    message(FATAL_ERROR
      "mygramdb_add_split_gtests(${target}): unknown arguments: ${SPLIT_UNPARSED_ARGUMENTS}")
  endif()
  if(NOT SPLIT_SOURCE)
    message(FATAL_ERROR "mygramdb_add_split_gtests(${target}): SOURCE is required")
  endif()
  if(NOT SPLIT_RESOURCE_LOCK)
    message(FATAL_ERROR "mygramdb_add_split_gtests(${target}): RESOURCE_LOCK is required")
  endif()
  if(NOT SPLIT_SLOW_TIMEOUT)
    message(FATAL_ERROR "mygramdb_add_split_gtests(${target}): SLOW_TIMEOUT is required")
  endif()
  if(NOT SPLIT_SLOW_TESTS)
    message(FATAL_ERROR "mygramdb_add_split_gtests(${target}): SLOW_TESTS is required")
  endif()
  if(SPLIT_ALL_CONCURRENCY AND SPLIT_CONCURRENCY_TESTS)
    message(FATAL_ERROR
      "mygramdb_add_split_gtests(${target}): use ALL_CONCURRENCY or CONCURRENCY_TESTS, not both")
  endif()

  gtest_add_tests(
    TARGET ${target}
    SOURCES ${SPLIT_SOURCE}
    TEST_LIST registered_tests
  )

  set_tests_properties(${registered_tests}
    PROPERTIES RESOURCE_LOCK ${SPLIT_RESOURCE_LOCK}
  )

  foreach(slow_test IN LISTS SPLIT_SLOW_TESTS)
    list(FIND registered_tests "${slow_test}" slow_test_index)
    if(slow_test_index EQUAL -1)
      message(FATAL_ERROR
        "mygramdb_add_split_gtests(${target}): exact SLOW test not found: ${slow_test}")
    endif()
    set_tests_properties("${slow_test}"
      PROPERTIES
        LABELS "SLOW"
        TIMEOUT ${SPLIT_SLOW_TIMEOUT}
    )
  endforeach()

  if(SPLIT_ALL_CONCURRENCY)
    set(concurrency_tests ${registered_tests})
  else()
    set(concurrency_tests ${SPLIT_CONCURRENCY_TESTS})
  endif()

  foreach(concurrency_test IN LISTS concurrency_tests)
    list(FIND registered_tests "${concurrency_test}" concurrency_test_index)
    if(concurrency_test_index EQUAL -1)
      message(FATAL_ERROR
        "mygramdb_add_split_gtests(${target}): exact CONCURRENCY test not found: ${concurrency_test}")
    endif()
    set_property(TEST "${concurrency_test}" APPEND PROPERTY LABELS "CONCURRENCY")
  endforeach()
endfunction()

# Signal handler safety tests
add_executable(signal_handler_test signal_handler_test.cpp)
target_link_libraries(signal_handler_test PRIVATE GTest::gtest_main)
gtest_discover_tests(signal_handler_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)

# Unit test subdirectories
add_subdirectory(utils)
add_subdirectory(config)
add_subdirectory(index)
add_subdirectory(storage)
add_subdirectory(loader)
add_subdirectory(query)
add_subdirectory(cache)
add_subdirectory(mysql)
add_subdirectory(server)
add_subdirectory(cli)
add_subdirectory(client)
add_subdirectory(app)

# Integration tests
add_subdirectory(integration)
