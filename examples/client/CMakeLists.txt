cmake_minimum_required(VERSION 3.15)
project(MygramDBClientExamples LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(MygramDBClient CONFIG REQUIRED)

add_executable(mygramdb_cpp_search cpp_search.cpp)
target_link_libraries(mygramdb_cpp_search PRIVATE MygramDB::client_static)

add_executable(mygramdb_c_search c_search.c)
target_link_libraries(mygramdb_c_search PRIVATE MygramDB::client_shared)
