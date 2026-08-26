cmake_minimum_required(VERSION 3.15)

# Resolve the installed mygramclient.pc against the layouts GNUInstallDirs
# produces. `lib` hides the defect that a multiarch libdir exposes, because the
# relocatable prefix has to step back over as many components as the libdir has;
# a distribution that sets lib/x86_64-linux-gnu otherwise lands one level above
# the real prefix and reports directories that were never written.

foreach(required_variable IN ITEMS
    MYGRAMDB_SOURCE_DIR
    MYGRAMDB_WORK_DIR
    MYGRAMDB_PKG_CONFIG)
  if(NOT DEFINED ${required_variable} OR "${${required_variable}}" STREQUAL "")
    message(FATAL_ERROR "${required_variable} is required")
  endif()
endforeach()

include(${MYGRAMDB_SOURCE_DIR}/src/client/mygramclient_pkgconfig.cmake)

set(PROJECT_VERSION "0.0.0")

function(run_pkg_config work_dir pkgconfig_dir out_value)
  set(ENV{PKG_CONFIG_PATH} "${pkgconfig_dir}")
  execute_process(
    COMMAND "${MYGRAMDB_PKG_CONFIG}" ${ARGN} mygramclient
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  if(NOT result EQUAL 0)
    message(FATAL_ERROR "pkg-config ${ARGN} failed in ${work_dir}:\n${output}${error}")
  endif()
  set(${out_value} "${output}" PARENT_SCOPE)
endfunction()

function(expect_flag_directory flags option expected_directory layout)
  string(REPLACE " " ";" flag_list "${flags}")
  set(found "")
  foreach(flag IN LISTS flag_list)
    if(flag MATCHES "^${option}(.+)$")
      set(found "${CMAKE_MATCH_1}")
      break()
    endif()
  endforeach()
  if("${found}" STREQUAL "")
    message(FATAL_ERROR "layout '${layout}': no ${option} flag in \"${flags}\"")
  endif()
  get_filename_component(resolved "${found}" ABSOLUTE)
  get_filename_component(expected "${expected_directory}" ABSOLUTE)
  if(NOT resolved STREQUAL expected)
    message(FATAL_ERROR
      "layout '${layout}': ${option} resolved to '${resolved}', but install wrote '${expected}'")
  endif()
  if(NOT IS_DIRECTORY "${resolved}")
    message(FATAL_ERROR "layout '${layout}': ${option} names a directory that does not exist: ${resolved}")
  endif()
endfunction()

set(includedir "include")

foreach(libdir IN ITEMS "lib" "lib64" "lib/x86_64-linux-gnu")
  string(REPLACE "/" "-" layout_name "${libdir}")
  set(prefix "${MYGRAMDB_WORK_DIR}/${layout_name}")
  file(REMOVE_RECURSE "${prefix}")

  # Mirror what the install step writes for this layout.
  file(MAKE_DIRECTORY "${prefix}/${includedir}/mygramdb")
  file(MAKE_DIRECTORY "${prefix}/${libdir}/pkgconfig")
  file(TOUCH "${prefix}/${includedir}/mygramdb/mygramclient_c.h")
  file(TOUCH "${prefix}/${libdir}/libmygramclient.a")

  mygramdb_client_pkgconfig_fields("${libdir}" "${includedir}"
    MYGRAMDB_PC_PREFIX MYGRAMDB_PC_LIBDIR MYGRAMDB_PC_INCLUDEDIR MYGRAMDB_PC_CXX_RUNTIME)
  configure_file(
    "${MYGRAMDB_SOURCE_DIR}/src/client/mygramclient.pc.in"
    "${prefix}/${libdir}/pkgconfig/mygramclient.pc"
    @ONLY
  )

  run_pkg_config("${prefix}" "${prefix}/${libdir}/pkgconfig" cflags --cflags)
  expect_flag_directory("${cflags}" "-I" "${prefix}/${includedir}" "${libdir}")

  run_pkg_config("${prefix}" "${prefix}/${libdir}/pkgconfig" libs --libs)
  expect_flag_directory("${libs}" "-L" "${prefix}/${libdir}" "${libdir}")

  run_pkg_config("${prefix}" "${prefix}/${libdir}/pkgconfig" static_libs --libs --static)
  if(NOT static_libs MATCHES "(^| )-l(std)?c\\+\\+( |$)")
    message(FATAL_ERROR
      "layout '${libdir}': --static --libs names no C++ runtime, so a C consumer cannot link the archive: \"${static_libs}\"")
  endif()

  message(STATUS "layout '${libdir}': cflags=${cflags} libs=${libs} static=${static_libs}")
endforeach()
