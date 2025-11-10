add_library(mygramdb_index STATIC
  index.cpp
  posting_list.cpp
)

target_include_directories(mygramdb_index PUBLIC
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(mygramdb_index PUBLIC
  mygramdb_utils
  roaring
  spdlog::spdlog
)
