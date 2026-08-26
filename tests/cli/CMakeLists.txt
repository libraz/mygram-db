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

# The tab-completion keyword sets only exist when the CLI is built with
# readline, and readline also takes over the interactive prompt. That makes it
# a separate binary rather than a compile definition on the test above, whose
# prompt assertions describe the no-readline build.
find_library(READLINE_LIBRARY readline)
if(READLINE_LIBRARY)
  add_executable(mygram_cli_completion_test
    mygram_cli_completion_test.cpp
  )

  target_include_directories(mygram_cli_completion_test PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}/src
  )

  target_link_libraries(mygram_cli_completion_test PRIVATE
    mygramclient_static
    mygramdb_utils
    ${READLINE_LIBRARY}
    GTest::gtest_main
  )
  target_compile_definitions(mygram_cli_completion_test PRIVATE HAVE_READLINE)

  gtest_discover_tests(mygram_cli_completion_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 30
  )
else()
  message(WARNING "readline not found: mygram_cli_completion_test will not be registered")
endif()
