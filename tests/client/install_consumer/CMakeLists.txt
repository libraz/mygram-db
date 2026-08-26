cmake_minimum_required(VERSION 3.15)
project(MygramDBInstalledClientConsumer LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(MygramDBClient CONFIG REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(MygramClientPkgConfig REQUIRED IMPORTED_TARGET mygramclient)

add_executable(cmake_static_consumer cpp_consumer.cpp)
target_link_libraries(cmake_static_consumer PRIVATE MygramDB::client_static)

add_executable(cmake_shared_consumer c_consumer.c)
target_link_libraries(cmake_shared_consumer PRIVATE MygramDB::client_shared)

add_executable(pkg_config_consumer c_consumer.c)
target_link_libraries(pkg_config_consumer PRIVATE PkgConfig::MygramClientPkgConfig)

# `-lmygramclient` picks the shared library, whose C++ runtime dependency is
# already recorded in the library itself. Only a C program linking the archive
# has to resolve those symbols from Libs.private, so the archive is located
# through the same libdir the .pc reports and linked in place of the -l flag.
set(default_library_suffixes ${CMAKE_FIND_LIBRARY_SUFFIXES})
set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_STATIC_LIBRARY_SUFFIX})
find_library(MYGRAMCLIENT_ARCHIVE
  NAMES mygramclient
  PATHS ${MygramClientPkgConfig_STATIC_LIBRARY_DIRS}
  NO_DEFAULT_PATH
  REQUIRED
)
set(CMAKE_FIND_LIBRARY_SUFFIXES ${default_library_suffixes})

set(mygramclient_static_link_flags "")
foreach(flag IN LISTS MygramClientPkgConfig_STATIC_LDFLAGS)
  if(NOT flag STREQUAL "-lmygramclient")
    list(APPEND mygramclient_static_link_flags "${flag}")
  endif()
endforeach()

add_executable(pkg_config_static_consumer c_consumer.c)
target_include_directories(pkg_config_static_consumer PRIVATE ${MygramClientPkgConfig_STATIC_INCLUDE_DIRS})
target_link_libraries(pkg_config_static_consumer PRIVATE
  ${MYGRAMCLIENT_ARCHIVE}
  ${mygramclient_static_link_flags}
)

enable_testing()
add_test(NAME CMakeStaticConsumer COMMAND cmake_static_consumer)
add_test(NAME CMakeSharedConsumer COMMAND cmake_shared_consumer)
add_test(NAME PkgConfigConsumer COMMAND pkg_config_consumer)
add_test(NAME PkgConfigStaticConsumer COMMAND pkg_config_static_consumer)
