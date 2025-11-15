# Third-party dependencies
# These will be fetched via FetchContent or found via find_package

include(FetchContent)

# Set policy for yaml-cpp to suppress deprecation warnings
if(POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif()

# yaml-cpp for YAML parsing (using master for latest CMake compatibility)
FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG master
)
set(YAML_CPP_FORMAT_SOURCE OFF CACHE BOOL "" FORCE)
set(YAML_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_INSTALL OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml-cpp)

# Mark yaml-cpp includes as system to exclude from clang-tidy
get_target_property(YAML_CPP_IID yaml-cpp INTERFACE_INCLUDE_DIRECTORIES)
if(YAML_CPP_IID)
  set_target_properties(yaml-cpp PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${YAML_CPP_IID}")
endif()

# Google Test for testing (using v1.15.2 for better C++17 support)
if(BUILD_TESTS)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  # Mark googletest includes as system to exclude from clang-tidy
  get_target_property(GTEST_IID gtest INTERFACE_INCLUDE_DIRECTORIES)
  if(GTEST_IID)
    set_target_properties(gtest PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${GTEST_IID}")
  endif()
  get_target_property(GTEST_MAIN_IID gtest_main INTERFACE_INCLUDE_DIRECTORIES)
  if(GTEST_MAIN_IID)
    set_target_properties(gtest_main PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${GTEST_MAIN_IID}")
  endif()
  get_target_property(GMOCK_IID gmock INTERFACE_INCLUDE_DIRECTORIES)
  if(GMOCK_IID)
    set_target_properties(gmock PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${GMOCK_IID}")
  endif()
endif()

# spdlog for logging
FetchContent_Declare(
  spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG v1.12.0
)
FetchContent_MakeAvailable(spdlog)

# Mark spdlog includes as system to exclude from clang-tidy
get_target_property(SPDLOG_IID spdlog INTERFACE_INCLUDE_DIRECTORIES)
if(SPDLOG_IID)
  set_target_properties(spdlog PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${SPDLOG_IID}")
endif()

# LZ4 for result compression
FetchContent_Declare(
  lz4
  GIT_REPOSITORY https://github.com/lz4/lz4.git
  GIT_TAG v1.9.4
)
FetchContent_GetProperties(lz4)
if(NOT lz4_POPULATED)
  FetchContent_Populate(lz4)
endif()
if(NOT TARGET lz4)
  add_library(lz4 STATIC
    ${lz4_SOURCE_DIR}/lib/lz4.c
    ${lz4_SOURCE_DIR}/lib/lz4hc.c
    ${lz4_SOURCE_DIR}/lib/lz4frame.c
    ${lz4_SOURCE_DIR}/lib/xxhash.c)
  target_include_directories(lz4 PUBLIC ${lz4_SOURCE_DIR}/lib)
  set_target_properties(lz4 PROPERTIES POSITION_INDEPENDENT_CODE ON)
endif()

get_target_property(LZ4_IID lz4 INTERFACE_INCLUDE_DIRECTORIES)
if(LZ4_IID)
  set_target_properties(lz4 PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${LZ4_IID}")
endif()

# CRoaring for Roaring bitmaps (using v4.0.0 for better CMake compatibility)
FetchContent_Declare(
  roaring
  GIT_REPOSITORY https://github.com/RoaringBitmap/CRoaring.git
  GIT_TAG v4.0.0
)
set(ENABLE_ROARING_TESTS OFF CACHE BOOL "" FORCE)
set(ROARING_BUILD_STATIC ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(roaring)

# Mark roaring includes as system to exclude from clang-tidy
get_target_property(ROARING_IID roaring INTERFACE_INCLUDE_DIRECTORIES)
if(ROARING_IID)
  set_target_properties(roaring PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${ROARING_IID}")
endif()

# nlohmann/json for JSON parsing
FetchContent_Declare(
  nlohmann_json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG v3.11.3
)
set(JSON_BuildTests OFF CACHE BOOL "" FORCE)
set(JSON_Install OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(nlohmann_json)

# Mark nlohmann_json includes as system to exclude from clang-tidy
get_target_property(NLOHMANN_JSON_IID nlohmann_json INTERFACE_INCLUDE_DIRECTORIES)
if(NLOHMANN_JSON_IID)
  set_target_properties(nlohmann_json PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${NLOHMANN_JSON_IID}")
endif()

# pboettch/json-schema-validator for JSON Schema validation
FetchContent_Declare(
  json-schema-validator
  GIT_REPOSITORY https://github.com/pboettch/json-schema-validator.git
  GIT_TAG 2.3.0
)
set(JSON_VALIDATOR_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(JSON_VALIDATOR_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(json-schema-validator)

# Mark json-schema-validator includes as system to exclude from clang-tidy
get_target_property(JSON_VALIDATOR_IID nlohmann_json_schema_validator INTERFACE_INCLUDE_DIRECTORIES)
if(JSON_VALIDATOR_IID)
  set_target_properties(nlohmann_json_schema_validator PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${JSON_VALIDATOR_IID}")
endif()

# cpp-httplib for HTTP server (header-only)
FetchContent_Declare(
  httplib
  GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
  GIT_TAG v0.15.3
)
set(HTTPLIB_REQUIRE_OPENSSL OFF CACHE BOOL "" FORCE)
set(HTTPLIB_REQUIRE_ZLIB OFF CACHE BOOL "" FORCE)
set(HTTPLIB_REQUIRE_BROTLI OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(httplib)

# Mark httplib includes as system to exclude from clang-tidy
# Note: httplib::httplib is an ALIAS, need to use the actual target 'httplib'
if(TARGET httplib)
  get_target_property(HTTPLIB_IID httplib INTERFACE_INCLUDE_DIRECTORIES)
  if(HTTPLIB_IID)
    set_target_properties(httplib PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${HTTPLIB_IID}")
  endif()
endif()

# Note: libmysqlclient should be installed on the system
# find_package(MySQL REQUIRED) will be called in src/mysql/CMakeLists.txt
