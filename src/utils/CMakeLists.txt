add_library(mygramdb_utils STATIC
  atomic_file_writer.cpp
  edit_distance.cpp
  string_utils.cpp
  network_utils.cpp
  memory_utils.cpp
  daemon_utils.cpp
  datetime_converter.cpp
  safe_path.cpp
  sql_utils.cpp
  periodic_worker.cpp
)

target_include_directories(mygramdb_utils PUBLIC
  ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(mygramdb_utils PUBLIC
  spdlog::spdlog
  roaring
  ${ZLIB_LIBRARIES}
)

target_include_directories(mygramdb_utils SYSTEM PUBLIC
  ${ZLIB_INCLUDE_DIRS}
)

# Link ICU if available
if(USE_ICU AND ICU_FOUND)
  target_link_libraries(mygramdb_utils PUBLIC ${ICU_LIBRARIES})
  if(ICU_INCLUDE_DIRS)
    target_include_directories(mygramdb_utils SYSTEM PUBLIC ${ICU_INCLUDE_DIRS})
  endif()
endif()
