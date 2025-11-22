# Initial Loader module
# Responsible for loading initial data from MySQL into storage

add_library(mygramdb_loader STATIC
  initial_loader.cpp
)

target_include_directories(mygramdb_loader PUBLIC
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(mygramdb_loader PUBLIC
  mygramdb_storage  # For DocumentStore access
  mygramdb_config   # For TableConfig
  PRIVATE
  mygramdb_mysql    # For MySQL connection (PRIVATE to avoid circular dependency)
)
