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
    doc.filters = MaterializeFiltersLocked(filter_it->second);
  }

  return doc;
}

std::vector<std::optional<Document>> DocumentStore::GetDocumentsBatch(const std::vector<DocId>& doc_ids) const {
  // Single lock acquisition for all lookups - much more efficient than N individual calls
  std::shared_lock lock(mutex_);

  std::vector<std::optional<Document>> results;
  results.reserve(doc_ids.size());

  for (const auto& doc_id : doc_ids) {
    auto pk_it = doc_id_to_pk_.find(doc_id);
    if (pk_it == doc_id_to_pk_.end()) {
      results.emplace_back(std::nullopt);
      continue;
    }

    Document doc;
    doc.doc_id = doc_id;
    doc.primary_key = pk_it->second;

    auto filter_it = doc_filters_.find(doc_id);
    if (filter_it != doc_filters_.end()) {
      doc.filters = MaterializeFiltersLocked(filter_it->second);
    }

    results.emplace_back(std::move(doc));
  }

  return results;
}

std::optional<DocId> DocumentStore::GetDocId(std::string_view primary_key) const {
  std::shared_lock lock(mutex_);
  // absl::flat_hash_map with TransparentStringHash enables heterogeneous lookup.
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

std::vector<std::optional<std::string>> DocumentStore::GetPrimaryKeysBatch(const std::vector<DocId>& doc_ids) const {
  // Single lock acquisition for all lookups - much more efficient than N individual calls
  std::shared_lock lock(mutex_);

  std::vector<std::optional<std::string>> results;
  results.reserve(doc_ids.size());

  for (const auto& doc_id : doc_ids) {
    auto iterator = doc_id_to_pk_.find(doc_id);
    if (iterator != doc_id_to_pk_.end()) {
      results.push_back(iterator->second);
    } else {
      results.emplace_back(std::nullopt);
    }
  }

  return results;
}

std::optional<FilterValue> DocumentStore::GetFilterValue(DocId doc_id, std::string_view filter_name) const {
  std::shared_lock lock(mutex_);
  const auto column_id = FindFilterColumnLocked(filter_name);
  if (!column_id.has_value()) {
    return std::nullopt;
  }

  auto doc_it = doc_filters_.find(doc_id);
  if (doc_it == doc_filters_.end()) {
    return std::nullopt;
  }

  const FilterValue* value = doc_it->second.Find(*column_id);
  if (value == nullptr) {
    return std::nullopt;
  }
  return *value;
}

std::optional<std::string> DocumentStore::ResolveFilterColumnName(std::string_view filter_name) const {
  std::shared_lock lock(mutex_);
  if (!filter_index_) {
    return std::nullopt;
  }
  return filter_index_->ResolveColumnName(filter_name);
}

std::vector<std::optional<FilterValue>> DocumentStore::GetFilterValuesBatch(const std::vector<DocId>& doc_ids,
                                                                            const std::string& column) const {
  std::shared_lock lock(mutex_);

  std::vector<std::optional<FilterValue>> results;
  results.reserve(doc_ids.size());

  // The column name is resolved to its interned id once instead of hashed per
  // document.
  const auto column_id = FindFilterColumnLocked(column);
  if (!column_id.has_value()) {
    results.resize(doc_ids.size(), std::nullopt);
    return results;
  }

  for (const auto& doc_id : doc_ids) {
    auto doc_it = doc_filters_.find(doc_id);
    if (doc_it == doc_filters_.end()) {
      results.emplace_back(std::nullopt);
      continue;
    }
    const FilterValue* value = doc_it->second.Find(*column_id);
    if (value == nullptr) {
      results.emplace_back(std::nullopt);
    } else {
      results.emplace_back(*value);
    }
  }

  return results;
}

std::vector<std::vector<std::optional<FilterValue>>> DocumentStore::GetFilterValuesBatchMultiColumn(
    const std::vector<DocId>& doc_ids, const std::vector<std::string>& columns) const {
  std::shared_lock lock(mutex_);

  std::vector<std::vector<std::optional<FilterValue>>> all_results(columns.size());
  for (auto& col_results : all_results) {
    col_results.resize(doc_ids.size(), std::nullopt);
  }

  // Outer loop over doc_ids to avoid redundant doc_filters_ lookups per column.
  // Each doc_id lookup is O(1) amortized for flat_hash_map, but doing it once
  // Fetching once per doc instead of once per (doc, column) pair saves repeated lookups per doc.
  // The result layout is preserved as [column][doc_index].
  std::vector<std::optional<FilterColumnId>> column_ids;
  column_ids.reserve(columns.size());
  for (const auto& column : columns) {
    column_ids.push_back(FindFilterColumnLocked(column));
  }

  for (size_t di = 0; di < doc_ids.size(); ++di) {
    auto doc_it = doc_filters_.find(doc_ids[di]);
    if (doc_it == doc_filters_.end()) {
      continue;  // all columns already nullopt
    }
    for (size_t ci = 0; ci < columns.size(); ++ci) {
      if (!column_ids[ci].has_value()) {
        continue;
      }
      const FilterValue* value = doc_it->second.Find(*column_ids[ci]);
      if (value != nullptr) {
        all_results[ci][di] = *value;
      }
    }
  }

  return all_results;
}

std::vector<DocId> DocumentStore::FilterByValue(std::string_view filter_name, const FilterValue& value) const {
  std::shared_lock lock(mutex_);

  // Monostate (NULL) values are not indexed — fall back to scan
  if (!filter_index_ || std::holds_alternative<std::monostate>(value)) {
    std::vector<DocId> results;
    const auto column_id = FindFilterColumnLocked(filter_name);
    if (!column_id.has_value()) {
      return results;
    }
    for (const auto& [doc_id, filters] : doc_filters_) {
      const FilterValue* stored = filters.Find(*column_id);
      if (stored != nullptr && *stored == value) {
        results.push_back(doc_id);
      }
    }
    std::sort(results.begin(), results.end());
    return results;
  }

  auto serialized = FilterIndex::SerializeFilterValue(value);
  std::vector<DocId> results;
  filter_index_->CollectEqDocIds(filter_name, serialized, results);
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

void DocumentStore::VisitNormalizedTextsFor(const std::vector<DocId>& doc_ids, size_t max_doc_ids_per_chunk,
                                            const SelectedNormalizedTextVisitor& visitor) const {
  if (doc_ids.empty() || max_doc_ids_per_chunk == 0 || !visitor) {
    return;
  }

  std::vector<std::optional<std::string>> chunk;
  chunk.reserve(std::min(max_doc_ids_per_chunk, doc_ids.size()));

  for (size_t begin = 0; begin < doc_ids.size(); begin += max_doc_ids_per_chunk) {
    const size_t end = std::min(begin + max_doc_ids_per_chunk, doc_ids.size());
    chunk.clear();
    {
      std::shared_lock lock(mutex_);
      for (size_t i = begin; i < end; ++i) {
        auto it = doc_texts_.find(doc_ids[i]);
        if (it != doc_texts_.end()) {
          chunk.emplace_back(it->second);
        } else {
          chunk.emplace_back(std::nullopt);
        }
      }
    }

    // Visitor runs without the store mutex so that long predicates (fuzzy
    // matching, synonym expansion) do not stall binlog apply.
    for (size_t i = begin; i < end; ++i) {
      const auto& text = chunk[i - begin];
      if (!visitor(i, doc_ids[i], text.has_value() ? &text.value() : nullptr)) {
        return;
      }
    }
  }
}

void DocumentStore::VisitNormalizedTextChunks(size_t max_doc_ids_per_chunk,
                                              const NormalizedTextChunkVisitor& visitor) const {
  if (max_doc_ids_per_chunk == 0 || !visitor) {
    return;
  }

  uint64_t end_doc_id_exclusive;
  size_t normalized_text_count;
  {
    std::shared_lock lock(mutex_);
    end_doc_id_exclusive =
        next_doc_id_ == 0 ? static_cast<uint64_t>(UINT32_MAX) + 1 : static_cast<uint64_t>(next_doc_id_);
    normalized_text_count = doc_texts_.size();
  }
  if (end_doc_id_exclusive <= 1 || normalized_text_count == 0) {
    return;
  }

  // Walking a dense numeric range avoids allocating and sorting one DocId per
  // stored text. Sparse stores still snapshot only their candidate IDs so the
  // walk does not scan a large exhausted ID range.
  const uint64_t max_dense_range = static_cast<uint64_t>(normalized_text_count) * 4;
  const uint64_t last_doc_id = end_doc_id_exclusive - 1;
  const bool dense_range = last_doc_id <= max_dense_range;

  const auto append_text_if_present_locked = [this](DocId doc_id, std::vector<NormalizedTextEntry>& chunk) {
    const auto it = doc_texts_.find(doc_id);
    if (it != doc_texts_.end()) {
      chunk.push_back({it->first, it->second});
    }
  };
  const auto visit_nonempty_chunk = [&visitor](const std::vector<NormalizedTextEntry>& chunk) {
    return chunk.empty() || visitor(chunk);
  };

  if (dense_range) {
    for (uint64_t begin_doc_id = 1; begin_doc_id < end_doc_id_exclusive;) {
      const uint64_t range_size =
          std::min<uint64_t>(static_cast<uint64_t>(max_doc_ids_per_chunk), end_doc_id_exclusive - begin_doc_id);
      const uint64_t end_doc_id = begin_doc_id + range_size;
      std::vector<NormalizedTextEntry> chunk;
      chunk.reserve(static_cast<size_t>(end_doc_id - begin_doc_id));
      {
        std::shared_lock lock(mutex_);
        for (uint64_t doc_id = begin_doc_id; doc_id < end_doc_id; ++doc_id) {
          const auto it = doc_texts_.find(static_cast<DocId>(doc_id));
          if (it != doc_texts_.end()) {
            chunk.push_back({it->first, it->second});
          }
        }
      }
      if (!chunk.empty() && !visitor(chunk)) {
        return;
      }
      begin_doc_id = end_doc_id;
    }
    return;
  }

  std::vector<DocId> text_doc_ids;
  {
    std::shared_lock lock(mutex_);
    text_doc_ids.reserve(doc_texts_.size());
    for (const auto& [doc_id, unused_text] : doc_texts_) {
      (void)unused_text;
      if (doc_id != 0 && static_cast<uint64_t>(doc_id) < end_doc_id_exclusive) {
        text_doc_ids.push_back(doc_id);
      }
    }
  }
  std::sort(text_doc_ids.begin(), text_doc_ids.end());

  for (size_t begin = 0; begin < text_doc_ids.size();) {
    const size_t end = begin + std::min(max_doc_ids_per_chunk, text_doc_ids.size() - begin);
    std::vector<NormalizedTextEntry> chunk;
    chunk.reserve(end - begin);
    {
      std::shared_lock lock(mutex_);
      for (size_t i = begin; i < end; ++i) {
        append_text_if_present_locked(text_doc_ids[i], chunk);
      }
    }

    if (!visit_nonempty_chunk(chunk)) {
      return;
    }
    begin = end;
  }
}

std::optional<std::string> DocumentStore::GetOriginalText(DocId doc_id) const {
  std::shared_lock lock(mutex_);
  auto it = original_texts_.find(doc_id);
  if (it == original_texts_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::vector<std::optional<std::string>> DocumentStore::GetOriginalTextBatch(const std::vector<DocId>& doc_ids) const {
  std::shared_lock lock(mutex_);
  std::vector<std::optional<std::string>> results;
  results.reserve(doc_ids.size());
  for (const auto doc_id : doc_ids) {
    auto it = original_texts_.find(doc_id);
    if (it != original_texts_.end()) {
      results.push_back(it->second);
    } else {
      results.emplace_back(std::nullopt);
    }
  }
  return results;
}

}  // namespace mygramdb::storage
