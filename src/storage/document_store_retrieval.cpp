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

std::vector<std::vector<std::optional<FilterValue>>> DocumentStore::GetFilterValuesBatchMultiColumn(
    const std::vector<DocId>& doc_ids, const std::vector<std::string>& columns) const {
  std::shared_lock lock(mutex_);

  std::vector<std::vector<std::optional<FilterValue>>> all_results;
  all_results.reserve(columns.size());

  for (const auto& column : columns) {
    std::vector<std::optional<FilterValue>> col_results;
    col_results.reserve(doc_ids.size());

    for (const auto& doc_id : doc_ids) {
      auto doc_it = doc_filters_.find(doc_id);
      if (doc_it == doc_filters_.end()) {
        col_results.emplace_back(std::nullopt);
        continue;
      }
      auto filter_it = doc_it->second.find(column);
      if (filter_it == doc_it->second.end()) {
        col_results.emplace_back(std::nullopt);
      } else {
        col_results.emplace_back(filter_it->second);
      }
    }

    all_results.push_back(std::move(col_results));
  }

  return all_results;
}

std::vector<DocId> DocumentStore::FilterByValue(std::string_view filter_name, const FilterValue& value) const {
  std::shared_lock lock(mutex_);

  // Monostate (NULL) values are not indexed — fall back to scan
  if (!filter_index_ || std::holds_alternative<std::monostate>(value)) {
    std::vector<DocId> results;
    for (const auto& [doc_id, filters] : doc_filters_) {
      auto iterator = filters.find(filter_name);
      if (iterator != filters.end() && iterator->second == value) {
        results.push_back(doc_id);
      }
    }
    std::sort(results.begin(), results.end());
    return results;
  }

  auto serialized = FilterIndex::SerializeFilterValue(value);
  auto bitmap = filter_index_->GetEqBitmap(std::string(filter_name), serialized);
  if (!bitmap) {
    return {};
  }

  uint32_t card = roaring_bitmap_get_cardinality(bitmap.get());
  std::vector<DocId> results(card);
  roaring_bitmap_to_uint32_array(bitmap.get(), results.data());
  return results;  // Already sorted (roaring guarantees sorted order)
}

bool DocumentStore::HasFilterColumn(std::string_view filter_name) const {
  std::shared_lock lock(mutex_);
  if (filter_index_) {
    return filter_index_->HasColumn(filter_name);
  }
  return false;
}

std::vector<DocId> DocumentStore::GetAllDocIds() const {
  std::vector<DocId> results;
  {
    std::shared_lock lock(mutex_);
    results.reserve(doc_id_to_pk_.size());

    for (const auto& [doc_id, unused_pk] : doc_id_to_pk_) {
      (void)unused_pk;  // Mark as intentionally unused
      results.push_back(doc_id);
    }
  }  // Release lock before sorting

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

std::vector<std::optional<std::string>> DocumentStore::GetNormalizedTextBatch(const std::vector<DocId>& doc_ids) const {
  std::shared_lock lock(mutex_);
  std::vector<std::optional<std::string>> results;
  results.reserve(doc_ids.size());
  for (const auto& doc_id : doc_ids) {
    auto it = doc_texts_.find(doc_id);
    if (it != doc_texts_.end()) {
      results.push_back(it->second);
    } else {
      results.emplace_back(std::nullopt);
    }
  }
  return results;
}

}  // namespace mygramdb::storage
