add_executable(mygram-cli
  mygram-cli.cpp
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
