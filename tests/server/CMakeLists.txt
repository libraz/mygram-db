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
