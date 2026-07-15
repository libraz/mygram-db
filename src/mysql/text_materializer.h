/**
 * @file text_materializer.h
 * @brief Shared snapshot/binlog text-source materialization contract
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <string>

#include "config/config.h"
#include "mysql/rows_parser.h"

namespace mygramdb::mysql {

enum class TextValueState : uint8_t {
  kAbsent,
  kNull,
  kPresent,
};

struct MaterializedText {
  TextValueState state = TextValueState::kAbsent;
  std::string value;

  [[nodiscard]] bool IsAvailable() const { return state != TextValueState::kAbsent; }
};

/**
 * Materialize configured text columns with one contract for snapshots and
 * row-based binlog images.
 *
 * - Every configured source column must be present. A missing column returns
 *   kAbsent so FULL-image replication can fail closed.
 * - A single explicit NULL returns kNull; a present empty string returns
 *   kPresent with an empty value.
 * - CONCAT ignores NULL/empty contributors and places the configured delimiter
 *   only between non-empty contributors. If all contributors are NULL the
 *   result is kNull; otherwise it is kPresent (which may still be empty).
 */
inline MaterializedText MaterializeTextSource(const RowData& row, const config::TextSourceConfig& source) {
  if (!source.column.empty()) {
    const std::string* value = row.FindColumnValue(source.column);
    const bool is_null = row.IsColumnNull(source.column);
    if (value == nullptr && !is_null) {
      return {};
    }
    if (is_null) {
      return {TextValueState::kNull, {}};
    }
    return {TextValueState::kPresent, value == nullptr ? std::string{} : *value};
  }

  if (source.concat.empty()) {
    return {};
  }

  MaterializedText result{TextValueState::kNull, {}};
  bool has_non_empty_contributor = false;
  for (const auto& column : source.concat) {
    const std::string* value = row.FindColumnValue(column);
    const bool is_null = row.IsColumnNull(column);
    if (value == nullptr && !is_null) {
      return {};
    }
    if (is_null) {
      continue;
    }
    result.state = TextValueState::kPresent;
    if (value == nullptr || value->empty()) {
      continue;
    }
    if (has_non_empty_contributor) {
      result.value += source.delimiter;
    }
    result.value += *value;
    has_non_empty_contributor = true;
  }
  return result;
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
