/**
 * @file test_io_model_override.h
 * @brief Shared helper for dual-mode CI: lets integration tests pick up an
 *        `io_model` override from the environment at runtime.
 *
 * Design doc Phase 2.6 (deferred to Phase 3): we want the same integration
 * test binary to be exercised under both `io_model=reactor` (the Phase 3
 * default) and `io_model=blocking` (the fallback). Rather than duplicate
 * the test fixtures, the test reads `MYGRAMDB_TEST_IO_MODEL` once at setup
 * and applies it to its `ServerConfig`. CMake registers a second ctest
 * entry per test binary with that environment variable set to "blocking",
 * giving us "two runs, one binary".
 *
 * Usage in a test fixture's SetUp():
 *
 *   ServerConfig config_;
 *   mygramdb::server::test::ApplyIoModelOverride(config_);
 *   // ... rest of setup ...
 */
#pragma once

#include <cstdlib>
#include <string>

#include "server/server_types.h"

namespace mygramdb::server::test {

/**
 * @brief Apply the `MYGRAMDB_TEST_IO_MODEL` environment variable to the
 *        given `ServerConfig`, if set.
 *
 * Recognised values: "reactor", "blocking". Any other value is ignored
 * (the config's existing io_model is left untouched). If the environment
 * variable is unset the function is a no-op.
 */
inline void ApplyIoModelOverride(ServerConfig& config) {
  const char* env = std::getenv("MYGRAMDB_TEST_IO_MODEL");
  if (env == nullptr) {
    return;
  }
  const std::string value(env);
  if (value == "reactor" || value == "blocking") {
    config.io_model = value;
  }
}

}  // namespace mygramdb::server::test
