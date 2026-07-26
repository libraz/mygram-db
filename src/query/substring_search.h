/**
 * @file substring_search.h
 * @brief Shared normalized-text substring fallback
 */

#pragma once

#include <cstddef>
#include <string_view>
#include <vector>

#include "storage/document_store.h"
#include "types/doc_id.h"

namespace mygramdb::query {

inline constexpr size_t kNormalizedSubstringChunkSize = 1024;

/**
 * @brief Find documents containing a normalized term without materializing all texts
 *
 * Results are returned in ascending DocID order.
 */
inline std::vector<storage::DocId> SearchNormalizedSubstring(std::string_view normalized_term,
                                                             const storage::DocumentStore& doc_store,
                                                             size_t chunk_size = kNormalizedSubstringChunkSize) {
  if (normalized_term.empty() || chunk_size == 0) {
    return {};
  }

  std::vector<storage::DocId> matches;
  doc_store.VisitNormalizedTextChunks(chunk_size,
                                      [&](const std::vector<storage::DocumentStore::NormalizedTextEntry>& chunk) {
                                        for (const auto& entry : chunk) {
                                          if (entry.text.find(normalized_term) != std::string::npos) {
                                            matches.push_back(entry.doc_id);
                                          }
                                        }
                                        return true;
                                      });
  return matches;
}

}  // namespace mygramdb::query
