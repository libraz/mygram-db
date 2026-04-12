/**
 * @file document_store_retrieval.cpp
 * @brief Document store retrieval and query methods
 */

#include <spdlog/spdlog.h>

#include <algorithm>

#include "storage/document_store.h"
#include "storage/filter_index.h"
#include "utils/structured_log.h"

namespace mygramdb::storage {

std::optional<Document> DocumentStore::GetDocument(DocId doc_id) const {
  std::shared_lock lock(mutex_);

  auto pk_it = doc_id_to_pk_.find(doc_id);
  if (pk_it == doc_id_to_pk_.end()) {
    return std::nullopt;
  }

  Document doc;
  doc.doc_id = doc_id;
  doc.primary_key = pk_it->second;

  // Get filters if they exist
  auto filter_it = doc_filters_.find(doc_id);
  if (filter_it != doc_filters_.end()) {
    doc.filters = filter_it->second;
  }

  return doc;
}

std::optional<DocId> DocumentStore::GetDocId(std::string_view primary_key) const {
  std::shared_lock lock(mutex_);
  // BUG-0081: absl::flat_hash_map with TransparentStringHash enables heterogeneous lookup
  // No temporary std::string allocation required
  auto iterator = pk_to_doc_id_.find(primary_key);
  if (iterator == pk_to_doc_id_.end()) {
    return std::nullopt;
  }
  return iterator->second;
}

std::optional<std::string> DocumentStore::GetPrimaryKey(DocId doc_id) const {
  std::shared_lock lock(mutex_);
  auto iterator = doc_id_to_pk_.find(doc_id);
  if (iterator == doc_id_to_pk_.end()) {
    return std::nullopt;
  }
  return iterator->second;
}

std::vector<std::string> DocumentStore::GetPrimaryKeysBatch(const std::vector<DocId>& doc_ids) const {
  // Single lock acquisition for all lookups - much more efficient than N individual calls
  std::shared_lock lock(mutex_);

  std::vector<std::string> results;
  results.reserve(doc_ids.size());

  for (const auto& doc_id : doc_ids) {
    auto iterator = doc_id_to_pk_.find(doc_id);
    if (iterator != doc_id_to_pk_.end()) {
      results.push_back(iterator->second);
    } else {
      results.emplace_back();  // Empty string for missing doc
    }
  }

  return results;
}

std::optional<FilterValue> DocumentStore::GetFilterValue(DocId doc_id, std::string_view filter_name) const {
  std::shared_lock lock(mutex_);
  auto doc_it = doc_filters_.find(doc_id);
  if (doc_it == doc_filters_.end()) {
    return std::nullopt;
  }

  auto filter_it = doc_it->second.find(filter_name);
  if (filter_it == doc_it->second.end()) {
    return std::nullopt;
  }

  return filter_it->second;
}

std::vector<std::optional<FilterValue>> DocumentStore::GetFilterValuesBatch(const std::vector<DocId>& doc_ids,
                                                                            const std::string& column) const {
  std::shared_lock lock(mutex_);

  std::vector<std::optional<FilterValue>> results;
  results.reserve(doc_ids.size());

  for (const auto& doc_id : doc_ids) {
    auto doc_it = doc_filters_.find(doc_id);
    if (doc_it == doc_filters_.end()) {
      results.emplace_back(std::nullopt);
      continue;
    }
    auto filter_it = doc_it->second.find(column);
    if (filter_it == doc_it->second.end()) {
      results.emplace_back(std::nullopt);
    } else {
      results.emplace_back(filter_it->second);
    }
  }

  return results;
}

std::vector<DocId> DocumentStore::FilterByValue(std::string_view filter_name, const FilterValue& value) const {
  std::shared_lock lock(mutex_);
  std::vector<DocId> results;

  for (const auto& [doc_id, filters] : doc_filters_) {
    auto iterator = filters.find(filter_name);
    if (iterator != filters.end() && iterator->second == value) {
      results.push_back(doc_id);
    }
  }

  // Sort results for consistency
  std::sort(results.begin(), results.end());

  return results;
}

bool DocumentStore::HasFilterColumn(std::string_view filter_name) const {
  std::shared_lock lock(mutex_);

  // Check if any document has this filter column
  return std::any_of(doc_filters_.begin(), doc_filters_.end(), [&filter_name](const auto& doc_filter) {
    return doc_filter.second.find(filter_name) != doc_filter.second.end();
  });
}

std::vector<DocId> DocumentStore::GetAllDocIds() const {
  std::shared_lock lock(mutex_);
  std::vector<DocId> results;
  results.reserve(doc_id_to_pk_.size());

  for (const auto& [doc_id, unused_pk] : doc_id_to_pk_) {
    (void)unused_pk;  // Mark as intentionally unused
    results.push_back(doc_id);
  }

  // Sort results for consistency with set operations
  std::sort(results.begin(), results.end());

  return results;
}

std::shared_ptr<const FilterIndex> DocumentStore::GetFilterIndex() const {
  std::shared_lock lock(mutex_);
  return filter_index_;
}

std::optional<std::string> DocumentStore::GetNormalizedText(DocId doc_id) const {
  std::shared_lock lock(mutex_);
  auto it = doc_texts_.find(doc_id);
  if (it == doc_texts_.end()) {
    return std::nullopt;
  }
  return it->second;
}

}  // namespace mygramdb::storage
