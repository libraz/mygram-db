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

}  // namespace mygramdb

// Re-export to namespace aliases for backward compatibility
namespace mygramdb::storage {
using mygramdb::DocId;
}  // namespace mygramdb::storage

namespace mygramdb::index {
using mygramdb::DocId;
}  // namespace mygramdb::index

namespace mygramdb::query {
using mygramdb::DocId;
}  // namespace mygramdb::query

namespace mygramdb::cache {
using mygramdb::DocId;
}  // namespace mygramdb::cache

#endif  // MYGRAMDB_TYPES_DOC_ID_H_
