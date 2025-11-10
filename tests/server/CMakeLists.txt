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
