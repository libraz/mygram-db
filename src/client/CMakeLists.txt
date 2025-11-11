# MygramDB Client Library

# C++ client library (static)
add_library(mygramclient_static STATIC
  mygramclient.cpp
  mygramclient_c.cpp
  search_expression.cpp
)

# Set library properties
set_target_properties(mygramclient_static PROPERTIES
  OUTPUT_NAME mygramclient
  POSITION_INDEPENDENT_CODE ON
)

# Include directories
target_include_directories(mygramclient_static
  PUBLIC
    ${CMAKE_SOURCE_DIR}/src
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src/client
)

# Link libraries (none needed - client is self-contained)

# Shared library (for node-gyp and other dynamic bindings)
add_library(mygramclient_shared SHARED
  mygramclient.cpp
  mygramclient_c.cpp
  search_expression.cpp
)

# Set library properties
set_target_properties(mygramclient_shared PROPERTIES
  OUTPUT_NAME mygramclient
  VERSION 1.0.0
  SOVERSION 1
)

# Include directories
target_include_directories(mygramclient_shared
  PUBLIC
    ${CMAKE_SOURCE_DIR}/src
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src/client
)

# Install targets
install(TARGETS mygramclient_static mygramclient_shared
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)

# Install headers
install(FILES
  mygramclient.h
  mygramclient_c.h
  search_expression.h
  DESTINATION include/mygramdb
)
