/**
 * @file value_canonicalizer.h
 * @brief Shared canonical string representation for snapshot and binlog values
 *
 * The snapshot path hands over the result-set text of a column; the binlog path
 * hands over the libmybinlog Value decoded from a row image. Both reduce to the
 * one string a document is keyed and indexed by.
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "mybinlog/status.h"
#include "mybinlog/value.h"
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
 * @brief Canonical text of a FLOAT or DOUBLE value: shortest digits that round-trip, classic locale.
 *
 * The one formatter for both ingestion paths, so the process locale cannot make the snapshot and
 * binlog text of the same value differ.
 */
[[nodiscard]] std::string FormatFloatingPoint(float value);
[[nodiscard]] std::string FormatFloatingPoint(double value);

/**
 * @brief Format an epoch timestamp in the UTC representation returned by the snapshot connection.
 */
[[nodiscard]] mygram::utils::Expected<std::string, mygram::utils::Error> FormatUtcTimestamp(uint32_t epoch_seconds,
                                                                                            uint32_t microseconds = 0,
                                                                                            uint8_t precision = 0);

/**
 * @brief Internal error code for a libmybinlog status raised while decoding a row or a value.
 *
 * kTruncated and kLimitExceeded are kMySQLFieldTruncated, kInvalidMetadata and
 * kMalformedValue are kMySQLInvalidMetadata, kUnsupported is kMySQLUnsupportedType.
 * Any other status is kMySQLBinlogError.
 */
[[nodiscard]] mygram::utils::ErrorCode RowDecodeErrorCode(mybinlog::Status status);

/**
 * @brief Canonical text of a value decoded from a binlog row image.
 *
 * Byte-identical to what the snapshot path publishes for the same stored value
 * of an accepted column type. NULL is the empty string. A value the FULL row
 * image and non-PARTIAL_JSON stream never carry (kAbsent, kJsonDiff) is an error.
 *
 * @param value Decoded value; its bytes may be moved from
 * @param labels ENUM labels or SET members in declaration order; nullptr renders the ordinal or bitmask as a number
 * @return The canonical text, or an error whose code follows RowDecodeErrorCode()
 */
[[nodiscard]] mygram::utils::Expected<std::string, mygram::utils::Error> CanonicalizeBinlogValue(
    mybinlog::Value&& value, const std::vector<std::string>* labels);

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
