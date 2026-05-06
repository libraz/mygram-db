add_executable(mygram-cli
  mygram-cli.cpp
)

target_include_directories(mygram-cli PRIVATE ${CMAKE_SOURCE_DIR}/src)

# Link against the C++ client library (handles socket I/O, hostname
# resolution, response framing, and timeouts).
target_link_libraries(mygram-cli PRIVATE
  mygramclient_static
  mygramdb_utils
)

# Try to find and use readline library for better CLI experience
find_library(READLINE_LIBRARY readline)
if(READLINE_LIBRARY)
  message(STATUS "Found readline library: ${READLINE_LIBRARY}")
  target_link_libraries(mygram-cli PRIVATE ${READLINE_LIBRARY})
  target_compile_definitions(mygram-cli PRIVATE HAVE_READLINE)
else()
  message(STATUS "readline library not found, building without tab completion")
endif()

install(TARGETS mygram-cli
  RUNTIME DESTINATION bin
)
