# EventMultiplexer tests — parameterised across Mock / Epoll / Kqueue backends.

add_executable(event_multiplexer_test
  event_multiplexer_test.cpp
  mock_event_multiplexer.cpp
)

target_link_libraries(event_multiplexer_test
  mygramdb_server
  GTest::gtest_main
)

# The mock header lives alongside the test; the real backend headers are found
# via mygramdb_server's include path.
target_include_directories(event_multiplexer_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
)

gtest_discover_tests(event_multiplexer_test
  DISCOVERY_MODE POST_BUILD
  DISCOVERY_TIMEOUT 30
)
