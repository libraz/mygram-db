/**
 * @file value_canonicalizer.h
 * @brief Shared canonical string representation for snapshot and binlog values
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <string>
#include <string_view>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql {

enum class CanonicalValueKind : uint8_t {
  kText,
  kDecimal,
  kTemporal,
  kFloat,
  kDouble,
  kBitset,
};

/**
 * @brief Canonicalize a textual MySQL value from either ingestion path.
 */
[[nodiscard]] std::string CanonicalizeColumnValue(std::string_view value, CanonicalValueKind kind);

/**
 * @brief Format an epoch timestamp in the UTC representation returned by the snapshot connection.
 */
[[nodiscard]] mygram::utils::Expected<std::string, mygram::utils::Error> FormatUtcTimestamp(uint32_t epoch_seconds,
                                                                                            uint32_t microseconds = 0,
                                                                                            uint8_t precision = 0);

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
