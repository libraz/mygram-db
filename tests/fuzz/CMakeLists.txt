if(NOT USE_MYSQL OR NOT MYSQL_FOUND)
  message(FATAL_ERROR "BUILD_FUZZERS requires USE_MYSQL and a MySQL client library")
endif()

add_executable(binary_input_fuzz
  binary_input_fuzz.cpp
)

target_link_libraries(binary_input_fuzz PRIVATE
  mygramdb_mysql
  mygramdb_storage
)

target_compile_options(binary_input_fuzz PRIVATE
  -fsanitize=address,undefined
  -fno-omit-frame-pointer
)

target_link_options(binary_input_fuzz PRIVATE
  -fsanitize=address,undefined
)

target_compile_definitions(binary_input_fuzz PRIVATE MYGRAMDB_STANDALONE_FUZZER=1)
