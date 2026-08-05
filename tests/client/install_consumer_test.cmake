cmake_minimum_required(VERSION 3.15)

foreach(required_variable IN ITEMS
    MYGRAMDB_BUILD_DIR
    MYGRAMDB_C_COMPILER
    MYGRAMDB_CXX_COMPILER
    MYGRAMDB_INSTALL_LIBDIR)
  if(NOT DEFINED ${required_variable} OR "${${required_variable}}" STREQUAL "")
    message(FATAL_ERROR "${required_variable} is required")
  endif()
endforeach()

set(test_root "${MYGRAMDB_BUILD_DIR}/tests/client/installed-consumer")
set(install_prefix "${test_root}/prefix")
set(consumer_build_dir "${test_root}/build")
set(example_build_dir "${test_root}/example-build")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${test_root}")

set(install_command
  "${CMAKE_COMMAND}" --install "${MYGRAMDB_BUILD_DIR}" --prefix "${install_prefix}")
if(DEFINED MYGRAMDB_BUILD_CONFIG AND NOT "${MYGRAMDB_BUILD_CONFIG}" STREQUAL "")
  list(APPEND install_command --config "${MYGRAMDB_BUILD_CONFIG}")
endif()

execute_process(
  COMMAND ${install_command}
  RESULT_VARIABLE install_result
  OUTPUT_VARIABLE install_output
  ERROR_VARIABLE install_error
)
if(NOT install_result EQUAL 0)
  message(FATAL_ERROR
    "Installing MygramDB into the consumer prefix failed:\n${install_output}${install_error}")
endif()

set(previous_pkg_config_path "$ENV{PKG_CONFIG_PATH}")
set(ENV{PKG_CONFIG_PATH} "${install_prefix}/${MYGRAMDB_INSTALL_LIBDIR}/pkgconfig")
if(NOT previous_pkg_config_path STREQUAL "")
  set(ENV{PKG_CONFIG_PATH} "$ENV{PKG_CONFIG_PATH}:${previous_pkg_config_path}")
endif()

execute_process(
  COMMAND "${CMAKE_COMMAND}"
    -S "${CMAKE_CURRENT_LIST_DIR}/install_consumer"
    -B "${consumer_build_dir}"
    "-DCMAKE_PREFIX_PATH=${install_prefix}"
    "-DCMAKE_C_COMPILER=${MYGRAMDB_C_COMPILER}"
    "-DCMAKE_CXX_COMPILER=${MYGRAMDB_CXX_COMPILER}"
  RESULT_VARIABLE configure_result
  OUTPUT_VARIABLE configure_output
  ERROR_VARIABLE configure_error
)
if(NOT configure_result EQUAL 0)
  message(FATAL_ERROR
    "Configuring the installed client consumer failed:\n${configure_output}${configure_error}")
endif()

set(build_command "${CMAKE_COMMAND}" --build "${consumer_build_dir}")
if(DEFINED MYGRAMDB_BUILD_CONFIG AND NOT "${MYGRAMDB_BUILD_CONFIG}" STREQUAL "")
  list(APPEND build_command --config "${MYGRAMDB_BUILD_CONFIG}")
endif()
execute_process(
  COMMAND ${build_command}
  RESULT_VARIABLE build_result
  OUTPUT_VARIABLE build_output
  ERROR_VARIABLE build_error
)
if(NOT build_result EQUAL 0)
  message(FATAL_ERROR
    "Building the installed client consumer failed:\n${build_output}${build_error}")
endif()

execute_process(
  COMMAND "${CMAKE_COMMAND}"
    -S "${install_prefix}/share/doc/mygramdb/examples/client"
    -B "${example_build_dir}"
    "-DCMAKE_PREFIX_PATH=${install_prefix}"
    "-DCMAKE_C_COMPILER=${MYGRAMDB_C_COMPILER}"
    "-DCMAKE_CXX_COMPILER=${MYGRAMDB_CXX_COMPILER}"
  RESULT_VARIABLE example_configure_result
  OUTPUT_VARIABLE example_configure_output
  ERROR_VARIABLE example_configure_error
)
if(NOT example_configure_result EQUAL 0)
  message(FATAL_ERROR
    "Configuring the installed client examples failed:\n${example_configure_output}${example_configure_error}")
endif()

set(example_build_command "${CMAKE_COMMAND}" --build "${example_build_dir}")
if(DEFINED MYGRAMDB_BUILD_CONFIG AND NOT "${MYGRAMDB_BUILD_CONFIG}" STREQUAL "")
  list(APPEND example_build_command --config "${MYGRAMDB_BUILD_CONFIG}")
endif()
execute_process(
  COMMAND ${example_build_command}
  RESULT_VARIABLE example_build_result
  OUTPUT_VARIABLE example_build_output
  ERROR_VARIABLE example_build_error
)
if(NOT example_build_result EQUAL 0)
  message(FATAL_ERROR
    "Building the installed client examples failed:\n${example_build_output}${example_build_error}")
endif()

set(ctest_command "${CMAKE_CTEST_COMMAND}" --test-dir "${consumer_build_dir}" --output-on-failure)
if(DEFINED MYGRAMDB_BUILD_CONFIG AND NOT "${MYGRAMDB_BUILD_CONFIG}" STREQUAL "")
  list(APPEND ctest_command --build-config "${MYGRAMDB_BUILD_CONFIG}")
endif()
execute_process(
  COMMAND ${ctest_command}
  RESULT_VARIABLE test_result
  OUTPUT_VARIABLE test_output
  ERROR_VARIABLE test_error
)
if(NOT test_result EQUAL 0)
  message(FATAL_ERROR
    "Running the installed client consumers failed:\n${test_output}${test_error}")
endif()

message(STATUS "Installed client consumers passed:\n${test_output}")
