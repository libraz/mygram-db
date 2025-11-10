/**
 * @file document_store.cpp
 * @brief Document store implementation
 */

#include "storage/document_store.h"
#include <spdlog/spdlog.h>
#include <algorithm>

namespace mygramdb {
namespace storage {

DocId DocumentStore::AddDocument(
    const std::string& primary_key,
    const std::unordered_map<std::string, FilterValue>& filters) {
  // Check if primary key already exists
  auto it = pk_to_doc_id_.find(primary_key);
  if (it != pk_to_doc_id_.end()) {
    spdlog::warn("Primary key {} already exists with DocID {}", primary_key, it->second);
    return it->second;
  }

  // Assign new DocID
  DocId doc_id = next_doc_id_++;

  // Store mappings
  doc_id_to_pk_[doc_id] = primary_key;
  pk_to_doc_id_[primary_key] = doc_id;

  // Store filters
  if (!filters.empty()) {
    doc_filters_[doc_id] = filters;
  }

  spdlog::debug("Added document: DocID={}, PK={}, filters={}", doc_id, primary_key,
                filters.size());

  return doc_id;
}

bool DocumentStore::UpdateDocument(
    DocId doc_id, const std::unordered_map<std::string, FilterValue>& filters) {
  // Check if document exists
  auto it = doc_id_to_pk_.find(doc_id);
  if (it == doc_id_to_pk_.end()) {
    spdlog::warn("Document {} does not exist", doc_id);
    return false;
  }

  // Update filters
  doc_filters_[doc_id] = filters;

  spdlog::debug("Updated document: DocID={}, filters={}", doc_id, filters.size());

  return true;
}

bool DocumentStore::RemoveDocument(DocId doc_id) {
  // Check if document exists
  auto pk_it = doc_id_to_pk_.find(doc_id);
  if (pk_it == doc_id_to_pk_.end()) {
    return false;
  }

  const std::string& primary_key = pk_it->second;

  // Remove mappings
  pk_to_doc_id_.erase(primary_key);
  doc_id_to_pk_.erase(doc_id);

  // Remove filters
  doc_filters_.erase(doc_id);

  spdlog::debug("Removed document: DocID={}, PK={}", doc_id, primary_key);

  return true;
}

std::optional<Document> DocumentStore::GetDocument(DocId doc_id) const {
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

std::optional<DocId> DocumentStore::GetDocId(const std::string& primary_key) const {
  auto it = pk_to_doc_id_.find(primary_key);
  if (it == pk_to_doc_id_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<std::string> DocumentStore::GetPrimaryKey(DocId doc_id) const {
  auto it = doc_id_to_pk_.find(doc_id);
  if (it == doc_id_to_pk_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<FilterValue> DocumentStore::GetFilterValue(
    DocId doc_id, const std::string& filter_name) const {
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

std::vector<DocId> DocumentStore::FilterByValue(const std::string& filter_name,
                                                const FilterValue& value) const {
  std::vector<DocId> results;

  for (const auto& [doc_id, filters] : doc_filters_) {
    auto it = filters.find(filter_name);
    if (it != filters.end() && it->second == value) {
      results.push_back(doc_id);
    }
  }

  // Sort results for consistency
  std::sort(results.begin(), results.end());

  return results;
}

size_t DocumentStore::MemoryUsage() const {
  size_t total = 0;

  // doc_id_to_pk_
  for (const auto& [doc_id, pk] : doc_id_to_pk_) {
    total += sizeof(DocId) + pk.size();
  }

  // pk_to_doc_id_
  for (const auto& [pk, doc_id] : pk_to_doc_id_) {
    total += pk.size() + sizeof(DocId);
  }

  // doc_filters_ (approximate)
  for (const auto& [doc_id, filters] : doc_filters_) {
    total += sizeof(DocId);
    for (const auto& [name, value] : filters) {
      total += name.size();
      total += std::visit(
          [](const auto& v) -> size_t {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::string>) {
              return v.size();
            } else {
              return sizeof(T);
            }
          },
          value);
    }
  }

  return total;
}

void DocumentStore::Clear() {
  doc_id_to_pk_.clear();
  pk_to_doc_id_.clear();
  doc_filters_.clear();
  next_doc_id_ = 1;
  spdlog::info("Document store cleared");
}

}  // namespace storage
}  // namespace mygramdb
