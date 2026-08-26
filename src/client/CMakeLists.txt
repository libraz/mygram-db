# MygramDB Client Library

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

# C++ client library (static)
add_library(mygramclient_static STATIC
  mygramclient.cpp
  mygramclient_c.cpp
  search_expression.cpp
)

# Set library properties
set_target_properties(mygramclient_static PROPERTIES
  OUTPUT_NAME mygramclient
  POSITION_INDEPENDENT_CODE ON
  EXPORT_NAME client_static
)

# Include directories
target_include_directories(mygramclient_static
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src/client
)

target_link_libraries(mygramclient_static
  PUBLIC
    Threads::Threads
)

add_library(MygramDB::client_static ALIAS mygramclient_static)

# Shared library (for node-gyp and other dynamic bindings)
add_library(mygramclient_shared SHARED
  mygramclient.cpp
  mygramclient_c.cpp
  search_expression.cpp
)

# Set library properties
set_target_properties(mygramclient_shared PROPERTIES
  OUTPUT_NAME mygramclient
  VERSION 1.0.0
  SOVERSION 1
  EXPORT_NAME client_shared
)

# Include directories
target_include_directories(mygramclient_shared
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src/client
)

target_link_libraries(mygramclient_shared
  PUBLIC
    Threads::Threads
)

add_library(MygramDB::client_shared ALIAS mygramclient_shared)

# Install targets
install(TARGETS mygramclient_static mygramclient_shared
  EXPORT MygramDBClientTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# Install headers
install(FILES
  mygramclient.h
  mygramclient_c.h
  search_expression.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/mygramdb
)

install(FILES
  ${CMAKE_SOURCE_DIR}/src/utils/error.h
  ${CMAKE_SOURCE_DIR}/src/utils/expected.h
  ${CMAKE_SOURCE_DIR}/src/utils/namespace_compat.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/mygramdb/utils
)

set(MYGRAMDB_CLIENT_CMAKE_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/MygramDBClient")
configure_package_config_file(
  MygramDBClientConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/MygramDBClientConfig.cmake
  INSTALL_DESTINATION ${MYGRAMDB_CLIENT_CMAKE_DIR}
)
write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/MygramDBClientConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion
)
install(EXPORT MygramDBClientTargets
  FILE MygramDBClientTargets.cmake
  NAMESPACE MygramDB::
  DESTINATION ${MYGRAMDB_CLIENT_CMAKE_DIR}
)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/MygramDBClientConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/MygramDBClientConfigVersion.cmake
  DESTINATION ${MYGRAMDB_CLIENT_CMAKE_DIR}
)

include(${CMAKE_CURRENT_SOURCE_DIR}/mygramclient_pkgconfig.cmake)
mygramdb_client_pkgconfig_fields(
  "${CMAKE_INSTALL_LIBDIR}" "${CMAKE_INSTALL_INCLUDEDIR}"
  MYGRAMDB_PC_PREFIX MYGRAMDB_PC_LIBDIR MYGRAMDB_PC_INCLUDEDIR MYGRAMDB_PC_CXX_RUNTIME)
configure_file(mygramclient.pc.in ${CMAKE_CURRENT_BINARY_DIR}/mygramclient.pc @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/mygramclient.pc DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
