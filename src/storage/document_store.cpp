/**
 * @file document_store.cpp
 * @brief Document store implementation - mutation operations
 */

#include "storage/document_store.h"

#include <spdlog/spdlog.h>

#include <cmath>
#include <memory>

#include "storage/filter_index.h"
#include "utils/structured_log.h"

namespace mygramdb::storage {

using mygram::utils::ErrorCode;

DocumentStore::DocumentStore() : filter_index_(std::make_shared<FilterIndex>()) {}

DocumentStore::~DocumentStore() = default;

Expected<DocId, Error> DocumentStore::AddDocument(std::string_view primary_key,
                                                  const std::unordered_map<std::string, FilterValue>& filters,
                                                  std::string_view normalized_text) {
  std::unique_lock lock(mutex_);

  // Check if primary key already exists (convert string_view to string for unordered_map lookup)
  std::string primary_key_str(primary_key);
  auto iterator = pk_to_doc_id_.find(primary_key_str);
  if (iterator != pk_to_doc_id_.end()) {
    mygram::utils::StructuredLog()
        .Event("storage_warning")
        .Field("type", "duplicate_primary_key")
        .Field("primary_key", primary_key)
        .Field("existing_doc_id", static_cast<uint64_t>(iterator->second))
        .Warn();
    return iterator->second;
  }

  // Check for DocID exhaustion: 0 is reserved or already wrapped
  if (next_doc_id_ == 0) {
    return MakeUnexpected(
        MakeError(ErrorCode::kStorageDocIdExhausted, "DocID space exhausted (4 billion limit reached)", ""));
  }

  // Assign current DocID
  DocId doc_id = next_doc_id_;

  // Increment for next allocation, handling wraparound explicitly
  if (next_doc_id_ == UINT32_MAX) {
    // Last valid ID used, set to 0 to trigger error on next call
    next_doc_id_ = 0;
  } else {
    next_doc_id_++;
  }

  // Store mappings
  doc_id_to_pk_[doc_id] = primary_key_str;
  pk_to_doc_id_[std::move(primary_key_str)] = doc_id;

  // Store filters (index first so failure doesn't leave stale doc_filters_ entry)
  if (!filters.empty()) {
    filter_index_->AddDocument(doc_id, filters);
    doc_filters_[doc_id] = filters;
  }

  // Store normalized text for n-gram post-filter verification
  if (!normalized_text.empty()) {
    doc_texts_[doc_id] = std::string(normalized_text);
  }

  mygram::utils::StructuredLog()
      .Event("document_added")
      .Field("doc_id", static_cast<uint64_t>(doc_id))
      .Field("primary_key", primary_key)
      .Field("filters", static_cast<uint64_t>(filters.size()))
      .Debug();

  return doc_id;
}

Expected<std::vector<DocId>, Error> DocumentStore::AddDocumentBatch(const std::vector<DocumentItem>& documents) {
  std::vector<DocId> doc_ids;
  doc_ids.reserve(documents.size());

  if (documents.empty()) {
    return doc_ids;
  }

  // Single lock for entire batch
  std::unique_lock lock(mutex_);

  for (const auto& doc : documents) {
    // Check if primary key already exists
    auto iterator = pk_to_doc_id_.find(doc.primary_key);
    if (iterator != pk_to_doc_id_.end()) {
      mygram::utils::StructuredLog()
          .Event("storage_warning")
          .Field("type", "duplicate_primary_key_batch")
          .Field("primary_key", doc.primary_key)
          .Field("existing_doc_id", static_cast<uint64_t>(iterator->second))
          .Warn();
      doc_ids.push_back(iterator->second);
      continue;
    }

    // Check for DocID overflow (uint32_t wraparound to 0)
    if (next_doc_id_ == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDocIdExhausted,
                                      "DocID space exhausted (4 billion limit reached) during batch add", ""));
    }

    // Assign current DocID
    DocId doc_id = next_doc_id_;

    // Increment for next allocation, handling wraparound explicitly
    if (next_doc_id_ == UINT32_MAX) {
      // Last valid ID used, set to 0 to trigger error on next call
      next_doc_id_ = 0;
    } else {
      next_doc_id_++;
    }

    // Store mappings
    doc_id_to_pk_[doc_id] = doc.primary_key;
    pk_to_doc_id_[doc.primary_key] = doc_id;

    // Store filters
    if (!doc.filters.empty()) {
      doc_filters_[doc_id] = doc.filters;
      filter_index_->AddDocument(doc_id, doc.filters);
    }

    // Store normalized text for n-gram post-filter verification
    if (!doc.normalized_text.empty()) {
      doc_texts_[doc_id] = doc.normalized_text;
    }

    doc_ids.push_back(doc_id);
  }

  mygram::utils::StructuredLog()
      .Event("documents_batch_added")
      .Field("count", static_cast<uint64_t>(documents.size()))
      .Debug();

  return doc_ids;
}

bool DocumentStore::UpdateDocument(DocId doc_id, const std::unordered_map<std::string, FilterValue>& filters) {
  std::unique_lock lock(mutex_);

  // Check if document exists
  auto iterator = doc_id_to_pk_.find(doc_id);
  if (iterator == doc_id_to_pk_.end()) {
    mygram::utils::StructuredLog()
        .Event("storage_warning")
        .Field("type", "document_not_found")
        .Field("doc_id", static_cast<uint64_t>(doc_id))
        .Warn();
    return false;
  }

  // Get old filters for bitmap update
  auto old_filter_it = doc_filters_.find(doc_id);
  std::unordered_map<std::string, FilterValue> old_filters;
  if (old_filter_it != doc_filters_.end()) {
    old_filters = old_filter_it->second;
  }

  // Update filters
  doc_filters_[doc_id] = filters;
  filter_index_->UpdateDocument(doc_id, old_filters, filters);

  mygram::utils::StructuredLog()
      .Event("document_updated")
      .Field("doc_id", static_cast<uint64_t>(doc_id))
      .Field("filters", static_cast<uint64_t>(filters.size()))
      .Debug();

  return true;
}

bool DocumentStore::RemoveDocument(DocId doc_id) {
  std::unique_lock lock(mutex_);

  // Check if document exists
  auto pk_it = doc_id_to_pk_.find(doc_id);
  if (pk_it == doc_id_to_pk_.end()) {
    return false;
  }

  // Copy the primary key before erasing (avoid use-after-free)
  std::string primary_key = pk_it->second;

  // Remove from filter index before erasing filter data
  auto filter_it = doc_filters_.find(doc_id);
  if (filter_it != doc_filters_.end()) {
    filter_index_->RemoveDocument(doc_id, filter_it->second);
  }

  // Remove mappings
  pk_to_doc_id_.erase(primary_key);
  doc_id_to_pk_.erase(doc_id);

  // Remove filters
  doc_filters_.erase(doc_id);

  // Remove normalized text
  doc_texts_.erase(doc_id);

  mygram::utils::StructuredLog()
      .Event("document_removed")
      .Field("doc_id", static_cast<uint64_t>(doc_id))
      .Field("primary_key", primary_key)
      .Debug();

  return true;
}

void DocumentStore::SetNormalizedText(DocId doc_id, std::string_view text) {
  std::unique_lock lock(mutex_);
  if (text.empty()) {
    doc_texts_.erase(doc_id);
  } else {
    doc_texts_[doc_id] = std::string(text);
  }
}

void DocumentStore::Clear() {
  std::unique_lock lock(mutex_);

  // Swap with empty maps to release memory (clear() doesn't shrink capacity)
  std::unordered_map<DocId, std::string>().swap(doc_id_to_pk_);
  decltype(pk_to_doc_id_)().swap(pk_to_doc_id_);  // absl::flat_hash_map
  std::unordered_map<DocId, std::unordered_map<std::string, FilterValue>>().swap(doc_filters_);
  std::unordered_map<DocId, std::string>().swap(doc_texts_);
  filter_index_ = std::make_shared<FilterIndex>();

  next_doc_id_ = 1;
  mygram::utils::StructuredLog().Event("document_store_cleared").Info();
}

void DocumentStore::Compact() {
  std::unique_lock lock(mutex_);

  // Rehash maps to reduce bucket count to match current size
  // This releases excess memory from previous larger sizes
  doc_id_to_pk_.rehash(doc_id_to_pk_.size());
  pk_to_doc_id_.rehash(pk_to_doc_id_.size());
  doc_filters_.rehash(doc_filters_.size());
  doc_texts_.rehash(doc_texts_.size());

  // Also compact inner filter maps
  for (auto& [doc_id, filters] : doc_filters_) {
    filters.rehash(filters.size());
  }

  mygram::utils::StructuredLog()
      .Event("document_store_compacted")
      .Field("doc_count", static_cast<uint64_t>(doc_id_to_pk_.size()))
      .Debug();
}

size_t DocumentStore::MemoryUsage() const {
  std::shared_lock lock(mutex_);
  size_t total = 0;

  // Include bucket overhead (each bucket is typically a pointer)
  constexpr size_t kPointerSize = sizeof(void*);
  total += doc_id_to_pk_.bucket_count() * kPointerSize;
  total += pk_to_doc_id_.bucket_count() * kPointerSize;
  total += doc_filters_.bucket_count() * kPointerSize;

  // doc_id_to_pk_ - data size
  for (const auto& [doc_id, primary_key_str] : doc_id_to_pk_) {
    total += sizeof(DocId) + primary_key_str.size() + primary_key_str.capacity();
  }

  // pk_to_doc_id_ - data size
  for (const auto& [primary_key_str, doc_id] : pk_to_doc_id_) {
    total += primary_key_str.size() + primary_key_str.capacity() + sizeof(DocId);
  }

  // doc_filters_ (approximate)
  for (const auto& [doc_id, filters] : doc_filters_) {
    total += sizeof(DocId);
    // Include inner map bucket overhead
    total += filters.bucket_count() * kPointerSize;
    for (const auto& [name, value] : filters) {
      total += name.size() + name.capacity();
      total += std::visit(
          [](const auto& filter_value) -> size_t {
            using T = std::decay_t<decltype(filter_value)>;
            if constexpr (std::is_same_v<T, std::string>) {
              return filter_value.size() + filter_value.capacity();
            } else {
              return sizeof(T);
            }
          },
          value);
    }
  }

  // doc_texts_ (normalized text for n-gram verification)
  total += doc_texts_.bucket_count() * kPointerSize;
  for (const auto& [doc_id, text] : doc_texts_) {
    total += sizeof(DocId) + text.size() + text.capacity();
  }

  // Filter index memory
  total += filter_index_->MemoryUsage();

  return total;
}

}  // namespace mygramdb::storage
