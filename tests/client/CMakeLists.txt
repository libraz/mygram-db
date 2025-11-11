# MygramClient tests

add_executable(mygramclient_test
  mygramclient_test.cpp
)

target_link_libraries(mygramclient_test
  mygramclient_static
  mygramdb_server
  mygramdb_mysql
  GTest::gtest_main
)

gtest_discover_tests(mygramclient_test)

# Search expression parser tests

add_executable(search_expression_test
  search_expression_test.cpp
)

target_link_libraries(search_expression_test
  mygramclient_static
  GTest::gtest_main
)

gtest_discover_tests(search_expression_test)
