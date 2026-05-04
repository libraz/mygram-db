# CLI (mygram-cli) response parsing and config tests

add_executable(mygram_cli_test
  mygram_cli_test.cpp
)

target_include_directories(mygram_cli_test PRIVATE
  ${CMAKE_SOURCE_DIR}/src
  ${CMAKE_BINARY_DIR}/src
)

# CLI source includes client library headers and calls into
# mygramdb::client::MygramClient, so the test must link against the same
# static library used by the production CLI binary.
target_link_libraries(mygram_cli_test PRIVATE
  mygramclient_static
  mygramdb_utils
  GTest::gtest_main
)

gtest_discover_tests(mygram_cli_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)
