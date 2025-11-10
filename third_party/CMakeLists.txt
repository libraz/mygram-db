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
endif()

# spdlog for logging
FetchContent_Declare(
  spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG v1.12.0
)
FetchContent_MakeAvailable(spdlog)

# CRoaring for Roaring bitmaps (using v4.0.0 for better CMake compatibility)
FetchContent_Declare(
  roaring
  GIT_REPOSITORY https://github.com/RoaringBitmap/CRoaring.git
  GIT_TAG v4.0.0
)
set(ENABLE_ROARING_TESTS OFF CACHE BOOL "" FORCE)
set(ROARING_BUILD_STATIC ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(roaring)

# Note: libmysqlclient should be installed on the system
# find_package(MySQL REQUIRED) will be called in src/mysql/CMakeLists.txt
