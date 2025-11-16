cmake_minimum_required(VERSION 3.15)
# Set CMake policy to suppress deprecation warnings from dependencies
cmake_policy(SET CMP0048 NEW)
cmake_policy(SET CMP0077 NEW)

# Get version from environment variable or git tag
if(DEFINED ENV{MYGRAMDB_VERSION})
  set(PROJECT_VERSION_FROM_ENV "$ENV{MYGRAMDB_VERSION}")
  message(STATUS "Version from environment: ${PROJECT_VERSION_FROM_ENV}")
  project(MygramDB VERSION ${PROJECT_VERSION_FROM_ENV} LANGUAGES CXX C)
else()
  # Get version from git tag
  execute_process(
    COMMAND git describe --tags --abbrev=0
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_TAG
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
  )

  # Parse version from tag (remove 'v' prefix if present)
  if(GIT_TAG)
    string(REGEX REPLACE "^v" "" PROJECT_VERSION_FROM_GIT "${GIT_TAG}")
    message(STATUS "Version from git tag: ${PROJECT_VERSION_FROM_GIT}")
  else()
    set(PROJECT_VERSION_FROM_GIT "0.0.0")
    message(WARNING "No git tag found, using default version: ${PROJECT_VERSION_FROM_GIT}")
  endif()

  project(MygramDB VERSION ${PROJECT_VERSION_FROM_GIT} LANGUAGES CXX C)
endif()

# C++ Standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# macOS specific settings
if(APPLE)
  # Get macOS SDK path
  execute_process(
    COMMAND xcrun --show-sdk-path
    OUTPUT_VARIABLE MACOS_SDK_PATH
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )

  # Use SDK path if available
  if(MACOS_SDK_PATH)
    message(STATUS "Using macOS SDK path: ${MACOS_SDK_PATH}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -isysroot ${MACOS_SDK_PATH}")

    # Explicitly specify the path to C++ standard library headers
    include_directories(SYSTEM "${MACOS_SDK_PATH}/usr/include/c++/v1")
    include_directories(SYSTEM "${MACOS_SDK_PATH}/usr/include")
  endif()

  # Add Homebrew paths
  if(EXISTS "/usr/local/include")
    include_directories(SYSTEM "/usr/local/include")
    link_directories("/usr/local/lib")
  endif()

  if(EXISTS "/opt/homebrew/include")
    include_directories(SYSTEM "/opt/homebrew/include")
    link_directories("/opt/homebrew/lib")
  endif()

  # macOS-specific compilation options
  add_compile_options(-Wno-unused-command-line-argument -Qunused-arguments)
endif()

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
  set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fno-omit-frame-pointer")
  set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
endif()

# Export compile commands for clang-tidy
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Options
option(BUILD_TESTS "Build tests" ON)
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(ENABLE_COVERAGE "Enable code coverage" OFF)
option(USE_ICU "Use ICU for Unicode normalization" ON)

# AddressSanitizer
if(ENABLE_ASAN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
  set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fsanitize=address")
endif()

# ThreadSanitizer
if(ENABLE_TSAN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
  set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fsanitize=thread")
endif()

# Code Coverage
if(ENABLE_COVERAGE)
  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    message(STATUS "Enabling code coverage")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -O0 -g")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage -O0 -g")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --coverage")
  else()
    message(WARNING "Code coverage is only supported with GCC or Clang")
  endif()
endif()

# Find packages
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)

# MySQL Client Library detection
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(MYSQL mysqlclient)
  if(MYSQL_FOUND)
    message(STATUS "Found MySQL client via pkg-config")
    message(STATUS "  Include dirs: ${MYSQL_INCLUDE_DIRS}")
    message(STATUS "  Libraries: ${MYSQL_LINK_LIBRARIES}")
    # Use LINK_LIBRARIES which includes full link flags
    set(MYSQL_LIBRARIES ${MYSQL_LINK_LIBRARIES})
  endif()
endif()

# Fallback to manual detection if pkg-config didn't work
if(NOT MYSQL_FOUND)
  if(APPLE)
    # macOS-specific paths - prioritize mysql-client@8.4
    execute_process(
      COMMAND brew --prefix mysql-client@8.4
      OUTPUT_VARIABLE BREW_MYSQL_PREFIX
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET
      RESULT_VARIABLE BREW_MYSQL_RESULT
    )

    if(BREW_MYSQL_RESULT EQUAL 0 AND BREW_MYSQL_PREFIX)
      message(STATUS "Found Homebrew mysql-client@8.4 at: ${BREW_MYSQL_PREFIX}")
      set(MYSQL_BREW_INCLUDE "${BREW_MYSQL_PREFIX}/include/mysql")
      set(MYSQL_BREW_LIB "${BREW_MYSQL_PREFIX}/lib")
    endif()

    find_path(MYSQL_INCLUDE_DIRS mysql.h
      HINTS
        ${MYSQL_BREW_INCLUDE}
        /opt/homebrew/opt/mysql-client@8.4/include/mysql
        /opt/homebrew/opt/mysql-client/include/mysql
        /usr/local/opt/mysql-client/include/mysql
        /opt/homebrew/include/mysql
        /usr/local/include/mysql
    )

    find_library(MYSQL_LIBRARY
      NAMES mysqlclient
      HINTS
        ${MYSQL_BREW_LIB}
        /opt/homebrew/opt/mysql-client@8.4/lib
        /opt/homebrew/opt/mysql-client/lib
        /usr/local/opt/mysql-client/lib
        /opt/homebrew/lib
        /usr/local/lib
    )
  else()
    # Linux-specific paths
    find_path(MYSQL_INCLUDE_DIRS mysql.h
      HINTS
        /usr/include/mysql
        /usr/local/include/mysql
    )

    find_library(MYSQL_LIBRARY
      NAMES mysqlclient
      HINTS
        /usr/lib
        /usr/lib64
        /usr/local/lib
        /usr/lib/x86_64-linux-gnu
    )
  endif()

  if(MYSQL_LIBRARY AND MYSQL_INCLUDE_DIRS)
    set(MYSQL_LIBRARIES ${MYSQL_LIBRARY})
    set(MYSQL_FOUND TRUE)
    message(STATUS "Found MySQL client: ${MYSQL_LIBRARIES}")
    message(STATUS "  Include dirs: ${MYSQL_INCLUDE_DIRS}")
  else()
    message(FATAL_ERROR "MySQL client library not found. Please install it:")
    message(FATAL_ERROR "  macOS:  brew install mysql-client@8.4")
    message(FATAL_ERROR "  Linux:  sudo apt-get install libmysqlclient-dev")
  endif()
endif()

if(MYSQL_FOUND)
  include_directories(SYSTEM ${MYSQL_INCLUDE_DIRS})
endif()

# ICU detection (similar to suzume-feedmill)
if(USE_ICU)
  find_package(PkgConfig QUIET)
  if(PkgConfig_FOUND)
    pkg_check_modules(ICU icu-uc icu-io icu-i18n)
    if(ICU_FOUND)
      message(STATUS "Found ICU via pkg-config")
      message(STATUS "  Include dirs: ${ICU_INCLUDE_DIRS}")
      message(STATUS "  Libraries: ${ICU_LIBRARIES}")
    endif()
  endif()

  # Fallback to manual detection if pkg-config didn't work
  if(NOT ICU_FOUND)
    if(APPLE)
      # macOS-specific paths
      execute_process(
        COMMAND brew --prefix icu4c
        OUTPUT_VARIABLE BREW_ICU_PREFIX
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
        RESULT_VARIABLE BREW_ICU_RESULT
      )

      if(BREW_ICU_RESULT EQUAL 0 AND BREW_ICU_PREFIX)
        message(STATUS "Found Homebrew icu4c at: ${BREW_ICU_PREFIX}")
        set(ICU_BREW_INCLUDE "${BREW_ICU_PREFIX}/include")
        set(ICU_BREW_LIB "${BREW_ICU_PREFIX}/lib")
      endif()

      find_path(ICU_INCLUDE_DIRS unicode/uchar.h
        HINTS
          ${ICU_BREW_INCLUDE}
          /opt/homebrew/opt/icu4c/include
          /usr/local/opt/icu4c/include
          /opt/homebrew/include
          /usr/local/include
      )

      find_library(ICU_UC_LIBRARY
        NAMES icuuc
        HINTS
          ${ICU_BREW_LIB}
          /opt/homebrew/opt/icu4c/lib
          /usr/local/opt/icu4c/lib
          /opt/homebrew/lib
          /usr/local/lib
      )

      find_library(ICU_IO_LIBRARY
        NAMES icuio
        HINTS
          ${ICU_BREW_LIB}
          /opt/homebrew/opt/icu4c/lib
          /usr/local/opt/icu4c/lib
          /opt/homebrew/lib
          /usr/local/lib
      )

      find_library(ICU_I18N_LIBRARY
        NAMES icui18n
        HINTS
          ${ICU_BREW_LIB}
          /opt/homebrew/opt/icu4c/lib
          /usr/local/opt/icu4c/lib
          /opt/homebrew/lib
          /usr/local/lib
      )
    else()
      # Linux-specific paths
      find_path(ICU_INCLUDE_DIRS unicode/uchar.h
        HINTS /usr/include /usr/local/include
      )

      find_library(ICU_UC_LIBRARY
        NAMES icuuc
        HINTS /usr/lib /usr/lib64 /usr/local/lib
      )

      find_library(ICU_IO_LIBRARY
        NAMES icuio
        HINTS /usr/lib /usr/lib64 /usr/local/lib
      )

      find_library(ICU_I18N_LIBRARY
        NAMES icui18n
        HINTS /usr/lib /usr/lib64 /usr/local/lib
      )
    endif()

    if(ICU_UC_LIBRARY AND ICU_IO_LIBRARY AND ICU_I18N_LIBRARY)
      set(ICU_LIBRARIES ${ICU_UC_LIBRARY} ${ICU_IO_LIBRARY} ${ICU_I18N_LIBRARY})
      set(ICU_FOUND TRUE)
      message(STATUS "Found ICU: ${ICU_LIBRARIES}")
    endif()
  endif()

  if(ICU_FOUND)
    add_definitions(-DUSE_ICU)
    if(ICU_INCLUDE_DIRS)
      include_directories(SYSTEM ${ICU_INCLUDE_DIRS})
    endif()
  else()
    message(WARNING "ICU not found. Unicode normalization will use fallback implementation.")
    message(WARNING "To install ICU:")
    message(WARNING "  macOS:  brew install icu4c")
    message(WARNING "  Linux:  sudo apt-get install libicu-dev")
  endif()
endif()

# MySQL Client detection
option(USE_MYSQL "Use MySQL client library" ON)

if(USE_MYSQL)
  # Try pkg-config first
  find_package(PkgConfig QUIET)

  if(PKG_CONFIG_FOUND)
    pkg_check_modules(MYSQL mysqlclient)
  endif()

  # If pkg-config didn't find it, try manual detection
  if(NOT MYSQL_FOUND)
    if(APPLE)
      # Check Homebrew paths for MySQL on macOS
      execute_process(
        COMMAND brew --prefix mysql-client
        OUTPUT_VARIABLE BREW_MYSQL_PREFIX
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
        RESULT_VARIABLE BREW_MYSQL_RESULT
      )

      if(BREW_MYSQL_RESULT EQUAL 0 AND BREW_MYSQL_PREFIX)
        message(STATUS "Found Homebrew mysql-client at: ${BREW_MYSQL_PREFIX}")
        set(MYSQL_BREW_INCLUDE "${BREW_MYSQL_PREFIX}/include/mysql")
        set(MYSQL_BREW_LIB "${BREW_MYSQL_PREFIX}/lib")
      endif()

      find_path(MYSQL_INCLUDE_DIRS mysql.h
        HINTS
          ${MYSQL_BREW_INCLUDE}
          /opt/homebrew/opt/mysql-client/include/mysql
          /usr/local/opt/mysql-client/include/mysql
          /opt/homebrew/opt/mysql/include/mysql
          /usr/local/opt/mysql/include/mysql
          /usr/local/include/mysql
      )

      find_library(MYSQL_LIBRARIES
        NAMES mysqlclient
        HINTS
          ${MYSQL_BREW_LIB}
          /opt/homebrew/opt/mysql-client/lib
          /usr/local/opt/mysql-client/lib
          /opt/homebrew/opt/mysql/lib
          /usr/local/opt/mysql/lib
          /usr/local/lib
      )
    else()
      # Linux paths
      find_path(MYSQL_INCLUDE_DIRS mysql.h
        PATHS
          /usr/include/mysql
          /usr/local/include/mysql
          /usr/mysql/include/mysql
      )

      find_library(MYSQL_LIBRARIES
        NAMES mysqlclient
        PATHS
          /usr/lib
          /usr/local/lib
          /usr/lib/x86_64-linux-gnu
          /usr/lib/mysql
      )
    endif()

    if(MYSQL_INCLUDE_DIRS AND MYSQL_LIBRARIES)
      set(MYSQL_FOUND TRUE)
      message(STATUS "Found MySQL: ${MYSQL_LIBRARIES}")
    endif()
  endif()

  if(MYSQL_FOUND)
    add_definitions(-DUSE_MYSQL)
    if(MYSQL_INCLUDE_DIRS)
      include_directories(SYSTEM ${MYSQL_INCLUDE_DIRS})
    endif()
  else()
    message(WARNING "MySQL client not found. MySQL integration will be disabled.")
    message(WARNING "To install MySQL client:")
    message(WARNING "  macOS:  brew install mysql-client")
    message(WARNING "  Linux:  sudo apt-get install libmysqlclient-dev")
  endif()
endif()

# Third-party dependencies
add_subdirectory(third_party)

# Suppress warnings from third-party libraries
if(TARGET roaring)
  target_compile_options(roaring PRIVATE -w)
endif()
if(TARGET spdlog)
  target_compile_options(spdlog PRIVATE -w)
endif()
if(TARGET yaml-cpp)
  target_compile_options(yaml-cpp PRIVATE -w)
endif()
if(TARGET nlohmann_json)
  target_compile_options(nlohmann_json INTERFACE -w)
endif()
if(TARGET nlohmann_json_schema_validator)
  target_compile_options(nlohmann_json_schema_validator PRIVATE -w)
endif()

# Source
add_subdirectory(src)

# Tests
if(BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

# Code Coverage Report Targets
if(ENABLE_COVERAGE)
  find_program(LCOV_PATH lcov)
  find_program(GENHTML_PATH genhtml)

  if(LCOV_PATH AND GENHTML_PATH)
    # Get number of processors for parallel test execution
    include(ProcessorCount)
    ProcessorCount(N)
    if(NOT N EQUAL 0)
      set(CTEST_PARALLEL_ARGS --parallel ${N})
    endif()

    # Add target to generate HTML coverage report
    add_custom_target(coverage
      COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/coverage
      COMMAND ${LCOV_PATH} --directory . --zerocounters
      COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure ${CTEST_PARALLEL_ARGS}
      COMMAND ${LCOV_PATH} --directory . --capture --output-file ${CMAKE_BINARY_DIR}/coverage/coverage.info --ignore-errors inconsistent,format,mismatch,negative --rc derive_function_end_line=0
      COMMAND ${LCOV_PATH} --extract ${CMAKE_BINARY_DIR}/coverage/coverage.info
        '${CMAKE_SOURCE_DIR}/src/*'
        --output-file ${CMAKE_BINARY_DIR}/coverage/coverage_filtered.info --ignore-errors inconsistent,format,mismatch,negative,unused,source --rc derive_function_end_line=0
      COMMAND ${GENHTML_PATH} ${CMAKE_BINARY_DIR}/coverage/coverage_filtered.info
        --output-directory ${CMAKE_BINARY_DIR}/coverage/html --ignore-errors source,inconsistent,corrupt,category --rc derive_function_end_line=0
      COMMAND ${CMAKE_COMMAND} -E echo "Coverage report generated: ${CMAKE_BINARY_DIR}/coverage/html/index.html"
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
      COMMENT "Generating code coverage report"
    )

    # Add target to clean coverage data
    add_custom_target(coverage-clean
      COMMAND ${LCOV_PATH} --directory . --zerocounters
      COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}/coverage
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
      COMMENT "Cleaning coverage data"
    )
  else()
    message(WARNING "lcov and genhtml not found. Coverage report target will not be available.")
    message(WARNING "Install with: brew install lcov (macOS) or sudo apt-get install lcov (Linux)")
  endif()
endif()

# Install
install(TARGETS mygramdb DESTINATION bin)

# Install configuration examples
install(FILES examples/config.yaml DESTINATION etc/mygramdb RENAME config.yaml.example)
install(FILES examples/config-minimal.yaml DESTINATION etc/mygramdb)
install(FILES examples/config.json DESTINATION etc/mygramdb RENAME config.json.example)
install(FILES examples/config-minimal.json DESTINATION etc/mygramdb)

# Install JSON Schema (for reference - embedded in binary)
install(FILES src/config/config-schema.json DESTINATION share/mygramdb)

# Install documentation
install(FILES README.md LICENSE DESTINATION share/doc/mygramdb)
