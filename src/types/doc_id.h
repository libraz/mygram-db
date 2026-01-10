/**
 * @file doc_id.h
 * @brief Unified DocId type definition
 *
 * This header provides a centralized definition of DocId to ensure
 * consistent usage across the codebase. The type is defined in a single
 * location to:
 * 1. Prevent inconsistent type definitions
 * 2. Enable future migration to a strongly-typed wrapper
 * 3. Improve maintainability
 */

#ifndef MYGRAMDB_TYPES_DOC_ID_H_
#define MYGRAMDB_TYPES_DOC_ID_H_

#include <cstdint>
#include <functional>
#include <limits>

namespace mygramdb {

/**
 * @brief Document identifier type
 *
 * DocId is a unique identifier for documents in the index and document store.
 * It supports up to 4 billion documents (2^32 - 1).
 *
 * Design decisions:
 * - uint32_t provides sufficient range for most use cases
 * - Aligned across index, storage, and cache components
 * - May be upgraded to a strongly-typed wrapper in the future
 */
using DocId = uint32_t;

/**
 * @brief Invalid/sentinel DocId value
 *
 * Used to indicate an invalid or uninitialized DocId.
 */
constexpr DocId kInvalidDocId = std::numeric_limits<DocId>::max();

/**
 * @brief Maximum valid DocId value
 *
 * The maximum DocId that can be assigned (one less than kInvalidDocId).
 */
constexpr DocId kMaxValidDocId = kInvalidDocId - 1;

/**
 * @brief Check if a DocId is valid
 *
 * @param doc_id The DocId to check
 * @return true if valid, false otherwise
 */
inline bool IsValidDocId(DocId doc_id) { return doc_id != kInvalidDocId; }

}  // namespace mygramdb

// Re-export to namespace aliases for backward compatibility
namespace mygramdb::storage {
using mygramdb::DocId;
using mygramdb::IsValidDocId;
using mygramdb::kInvalidDocId;
using mygramdb::kMaxValidDocId;
}  // namespace mygramdb::storage

namespace mygramdb::index {
using mygramdb::DocId;
using mygramdb::IsValidDocId;
using mygramdb::kInvalidDocId;
using mygramdb::kMaxValidDocId;
}  // namespace mygramdb::index

namespace mygramdb::query {
using mygramdb::DocId;
using mygramdb::IsValidDocId;
using mygramdb::kInvalidDocId;
using mygramdb::kMaxValidDocId;
}  // namespace mygramdb::query

namespace mygramdb::cache {
using mygramdb::DocId;
using mygramdb::IsValidDocId;
using mygramdb::kInvalidDocId;
using mygramdb::kMaxValidDocId;
}  // namespace mygramdb::cache

// Note: std::hash<DocId> is not needed because DocId is a type alias for uint32_t,
// and std::hash<uint32_t> is already defined by the standard library.

#endif  // MYGRAMDB_TYPES_DOC_ID_H_
