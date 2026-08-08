if(NOT USE_MYSQL OR NOT MYSQL_FOUND)
  message(FATAL_ERROR "BUILD_FUZZERS requires USE_MYSQL and a MySQL client library")
endif()

# Sanitizer and coverage instrumentation is set for the whole build by the
# top-level CMakeLists when BUILD_FUZZERS is ON, so nothing is added per target
# here. FUZZER_ENGINE selects the driver:
#
# standalone: self-contained binary driven by a deterministic PRNG. Builds with
#             any compiler and needs no corpus, so a crash is reproducible from
#             the iteration count alone.
# libfuzzer:  links -fsanitize=fuzzer so libFuzzer supplies main(), coverage
#             feedback and corpus management. Needs a Clang that ships it.
add_executable(binary_input_fuzz
  binary_input_fuzz.cpp
)

target_link_libraries(binary_input_fuzz PRIVATE
  mygramdb_mysql
  mygramdb_storage
  mygramdb_query
  mygramdb_config
)

if(FUZZER_ENGINE STREQUAL "libfuzzer")
  target_link_options(binary_input_fuzz PRIVATE -fsanitize=fuzzer)
else()
  target_compile_definitions(binary_input_fuzz PRIVATE MYGRAMDB_STANDALONE_FUZZER=1)
endif()
