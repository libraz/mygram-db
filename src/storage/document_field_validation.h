/**
 * @file document_field_validation.h
 * @brief Shared validation for document fields accepted by DocumentStore
 */

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "storage/document_store.h"
#include "storage/dump_format_limits.h"

namespace mygramdb::storage {

/// Shared bound predicate for ingress and persistence checks.
inline constexpr bool IsDocumentFieldLengthValid(uint64_t actual, uint64_t limit) {
  return actual <= limit;
}

/// Validate one field length without copying the field value.
inline Expected<void, Error> ValidateDocumentFieldLength(std::string_view field, uint64_t actual, uint64_t limit) {
  if (IsDocumentFieldLengthValid(actual, limit)) {
    return {};
  }
  const std::string message =
      std::string(field) + " length " + std::to_string(actual) + " exceeds maximum allowed " + std::to_string(limit);
  return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageWriteError, message));
}

inline Expected<void, Error> ValidateDocumentPrimaryKey(std::string_view primary_key) {
  return ValidateDocumentFieldLength("primary_key", primary_key.size(),
                                     dump_format_limits::docstore::kMaxPrimaryKeyLength);
}

inline Expected<void, Error> ValidateDocumentFilterFields(const FilterMap& filters) {
  using namespace dump_format_limits::docstore;
  if (auto result = ValidateDocumentFieldLength("filter_count", filters.size(), kMaxFilterCount); !result) {
    return result;
  }
  for (const auto& [name, value] : filters) {
    if (auto result = ValidateDocumentFieldLength("filter_name", name.size(), kMaxFilterNameLength); !result) {
      return result;
    }
    if (const auto* string_value = std::get_if<std::string>(&value); string_value != nullptr) {
      if (auto result = ValidateDocumentFieldLength("filter_string", string_value->size(), kMaxFilterStringLength);
          !result) {
        return result;
      }
    }
  }
  return {};
}

inline Expected<void, Error> ValidateDocumentTextFields(std::string_view normalized_text,
                                                        std::string_view original_text) {
  using namespace dump_format_limits::docstore;
  if (auto result = ValidateDocumentFieldLength("normalized_text", normalized_text.size(), kMaxNormalizedTextLength);
      !result) {
    return result;
  }
  return ValidateDocumentFieldLength("original_text", original_text.size(), kMaxOriginalTextLength);
}

/// Validate all caller-supplied fields before a document is indexed or stored.
inline Expected<void, Error> ValidateIncomingDocumentFields(std::string_view primary_key, const FilterMap& filters,
                                                            std::string_view normalized_text,
                                                            std::string_view original_text) {
  if (auto result = ValidateDocumentPrimaryKey(primary_key); !result) {
    return result;
  }
  if (auto result = ValidateDocumentFilterFields(filters); !result) {
    return result;
  }
  return ValidateDocumentTextFields(normalized_text, original_text);
}

}  // namespace mygramdb::storage
