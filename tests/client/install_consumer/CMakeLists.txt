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

enable_testing()
add_test(NAME CMakeStaticConsumer COMMAND cmake_static_consumer)
add_test(NAME CMakeSharedConsumer COMMAND cmake_shared_consumer)
add_test(NAME PkgConfigConsumer COMMAND pkg_config_consumer)
