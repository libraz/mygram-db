/**
 * @file dump_format_limits.h
 * @brief Shared size and count limits for serialized storage formats
 *
 * Keep format limits here so writers and readers use the same values. Limits
 * that happen to have the same numeric value but belong to different formats
 * remain separate entries because their compatibility contracts differ.
 */

#pragma once

#include <cstdint>

namespace mygramdb::storage::dump_format_limits {

namespace config {

constexpr uint32_t kMaxIdentifierLength = 1024;
constexpr uint32_t kMaxConfigValueLength = 4 * 1024;
constexpr uint32_t kMaxPathLength = 8 * 1024;
constexpr uint32_t kMaxTableCount = 10000;
constexpr uint32_t kMaxConcatColumnCount = 1000;
constexpr uint32_t kMaxRequiredFilterCount = 1000;
constexpr uint32_t kMaxOptionalFilterCount = 1000;
constexpr uint32_t kMaxCidrCount = 10000;

}  // namespace config

namespace container {

constexpr uint32_t kMaxGtidLength = 64 * 1024;
constexpr uint32_t kMaxGeneralStringLength = 1024 * 1024;
constexpr uint32_t kMaxConfigSectionLength = 16 * 1024 * 1024;
constexpr uint32_t kMaxStatsSectionLength = kMaxConfigSectionLength;
constexpr uint64_t kMaxSectionDataLength = 4ULL * 1024 * 1024 * 1024;
constexpr uint32_t kMaxTableContextNameLength = config::kMaxIdentifierLength;

constexpr bool IsValidV2SectionDataLength(uint64_t length) {
  return length <= kMaxSectionDataLength;
}

}  // namespace container

namespace docstore {

constexpr uint32_t kMaxGtidLength = 1024;
constexpr uint64_t kMaxDocumentCount = 1000000000;
constexpr uint32_t kMaxPrimaryKeyLength = 1024 * 1024;
constexpr uint32_t kMaxFilterCount = 1000;
constexpr uint32_t kMaxFilterNameLength = 1024;
constexpr uint32_t kMaxFilterStringLength = 64 * 1024;
constexpr uint32_t kMaxNormalizedTextLength = 16 * 1024 * 1024;
constexpr uint32_t kMaxOriginalTextLength = kMaxNormalizedTextLength;

}  // namespace docstore

}  // namespace mygramdb::storage::dump_format_limits
