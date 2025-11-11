# TCP Server tests

add_executable(tcp_server_test
  tcp_server_test.cpp
)

target_link_libraries(tcp_server_test
  mygramdb_server
  mygramdb_mysql
  GTest::gtest_main
)

gtest_discover_tests(tcp_server_test)

# Thread Pool tests

add_executable(thread_pool_test
  thread_pool_test.cpp
)

target_link_libraries(thread_pool_test
  mygramdb_server
  GTest::gtest_main
)

gtest_discover_tests(thread_pool_test)

# HTTP Server tests

add_executable(http_server_test
  http_server_test.cpp
)

target_link_libraries(http_server_test
  mygramdb_server
  mygramdb_mysql
  httplib::httplib
  nlohmann_json::nlohmann_json
  GTest::gtest_main
)

target_include_directories(http_server_test PRIVATE ${CMAKE_BINARY_DIR}/src)

gtest_discover_tests(http_server_test)
