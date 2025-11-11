/**
 * @file document_store.cpp
 * @brief Document store implementation
 */

#include "storage/document_store.h"

#include <cmath>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstring>
#include <fstream>

namespace mygramdb {
namespace storage {

DocId DocumentStore::AddDocument(
    const std::string& primary_key,
    const std::unordered_map<std::string, FilterValue>& filters) {
  std::unique_lock lock(mutex_);

  // Check if primary key already exists
  auto iterator = pk_to_doc_id_.find(primary_key);
  if (iterator != pk_to_doc_id_.end()) {
    spdlog::warn("Primary key {} already exists with DocID {}", primary_key, iterator->second);
    return iterator->second;
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

std::vector<DocId> DocumentStore::AddDocumentBatch(
    const std::vector<DocumentItem>& documents) {
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
      spdlog::warn("Primary key {} already exists with DocID {}",
                   doc.primary_key, iterator->second);
      doc_ids.push_back(iterator->second);
      continue;
    }

    // Assign new DocID
    DocId doc_id = next_doc_id_++;

    // Store mappings
    doc_id_to_pk_[doc_id] = doc.primary_key;
    pk_to_doc_id_[doc.primary_key] = doc_id;

    // Store filters
    if (!doc.filters.empty()) {
      doc_filters_[doc_id] = doc.filters;
    }

    doc_ids.push_back(doc_id);
  }

  spdlog::debug("Added batch of {} documents", documents.size());

  return doc_ids;
}

bool DocumentStore::UpdateDocument(
    DocId doc_id, const std::unordered_map<std::string, FilterValue>& filters) {
  std::unique_lock lock(mutex_);

  // Check if document exists
  auto iterator = doc_id_to_pk_.find(doc_id);
  if (iterator == doc_id_to_pk_.end()) {
    spdlog::warn("Document {} does not exist", doc_id);
    return false;
  }

  // Update filters
  doc_filters_[doc_id] = filters;

  spdlog::debug("Updated document: DocID={}, filters={}", doc_id, filters.size());

  return true;
}

bool DocumentStore::RemoveDocument(DocId doc_id) {
  std::unique_lock lock(mutex_);

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

std::optional<DocId> DocumentStore::GetDocId(const std::string& primary_key) const {
  std::shared_lock lock(mutex_);
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

std::optional<FilterValue> DocumentStore::GetFilterValue(
    DocId doc_id, const std::string& filter_name) const {
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

std::vector<DocId> DocumentStore::FilterByValue(const std::string& filter_name,
                                                const FilterValue& value) const {
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

size_t DocumentStore::MemoryUsage() const {
  std::shared_lock lock(mutex_);
  size_t total = 0;

  // doc_id_to_pk_
  for (const auto& [doc_id, primary_key_str] : doc_id_to_pk_) {
    total += sizeof(DocId) + primary_key_str.size();
  }

  // pk_to_doc_id_
  for (const auto& [primary_key_str, doc_id] : pk_to_doc_id_) {
    total += primary_key_str.size() + sizeof(DocId);
  }

  // doc_filters_ (approximate)
  for (const auto& [doc_id, filters] : doc_filters_) {
    total += sizeof(DocId);
    for (const auto& [name, value] : filters) {
      total += name.size();
      total += std::visit(
          [](const auto& filter_value) -> size_t {
            using T = std::decay_t<decltype(filter_value)>;
            if constexpr (std::is_same_v<T, std::string>) {
              return filter_value.size();
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
  std::unique_lock lock(mutex_);
  doc_id_to_pk_.clear();
  pk_to_doc_id_.clear();
  doc_filters_.clear();
  next_doc_id_ = 1;
  spdlog::info("Document store cleared");
}

bool DocumentStore::SaveToFile(const std::string& filepath, const std::string& replication_gtid) const {
  try {
    std::ofstream ofs(filepath, std::ios::binary);
    if (!ofs) {
      spdlog::error("Failed to open file for writing: {}", filepath);
      return false;
    }

    // File format:
    // [4 bytes: magic "MGDS"] [4 bytes: version] [4 bytes: next_doc_id]
    // [4 bytes: gtid_length] [gtid_length bytes: GTID string]
    // [8 bytes: doc_count] [doc_id -> pk mappings...]
    // [filters...]

    // Write magic number
    ofs.write("MGDS", 4);

    // Write version
    uint32_t version = 1;
    ofs.write(reinterpret_cast<const char*>(&version), sizeof(version));

    uint32_t next_id;
    uint64_t doc_count;

    // Lock scope: read data structures
    {
      std::shared_lock lock(mutex_);

      // Write next_doc_id
      next_id = static_cast<uint32_t>(next_doc_id_);
      ofs.write(reinterpret_cast<const char*>(&next_id), sizeof(next_id));

      // Write GTID (for replication position)
      auto gtid_len = static_cast<uint32_t>(replication_gtid.size());
      ofs.write(reinterpret_cast<const char*>(&gtid_len), sizeof(gtid_len));
      if (gtid_len > 0) {
        ofs.write(replication_gtid.data(), gtid_len);
      }

      // Write document count
      doc_count = doc_id_to_pk_.size();
      ofs.write(reinterpret_cast<const char*>(&doc_count), sizeof(doc_count));

      // Write doc_id -> pk mappings
      for (const auto& [doc_id, primary_key_str] : doc_id_to_pk_) {
        // Write doc_id
        auto doc_id_value = static_cast<uint32_t>(doc_id);
        ofs.write(reinterpret_cast<const char*>(&doc_id_value), sizeof(doc_id_value));

        // Write pk length and pk
        auto pk_len = static_cast<uint32_t>(primary_key_str.size());
        ofs.write(reinterpret_cast<const char*>(&pk_len), sizeof(pk_len));
        ofs.write(primary_key_str.data(), pk_len);

        // Write filters for this document
        auto filter_it = doc_filters_.find(doc_id);
        uint32_t filter_count = 0;
        if (filter_it != doc_filters_.end()) {
          filter_count = static_cast<uint32_t>(filter_it->second.size());
        }
        ofs.write(reinterpret_cast<const char*>(&filter_count), sizeof(filter_count));

        if (filter_count > 0) {
          for (const auto& [name, value] : filter_it->second) {
            // Write filter name
            auto name_len = static_cast<uint32_t>(name.size());
            ofs.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
            ofs.write(name.data(), name_len);

            // Write filter type and value
            auto type_idx = static_cast<uint8_t>(value.index());
            ofs.write(reinterpret_cast<const char*>(&type_idx), sizeof(type_idx));

            std::visit([&ofs](const auto& filter_value) {
              using T = std::decay_t<decltype(filter_value)>;
              if constexpr (std::is_same_v<T, std::monostate>) {
                // std::monostate (NULL) has no data to write
              } else if constexpr (std::is_same_v<T, std::string>) {
                auto str_len = static_cast<uint32_t>(filter_value.size());
                ofs.write(reinterpret_cast<const char*>(&str_len), sizeof(str_len));
                ofs.write(filter_value.data(), str_len);
              } else {
                ofs.write(reinterpret_cast<const char*>(&filter_value), sizeof(filter_value));
              }
            }, value);
          }
        }
      }
    }

    ofs.close();
    spdlog::info("Saved document store to {}: {} documents, {} MB",
                 filepath, doc_count, MemoryUsage() / (1024 * 1024));
    return true;
  } catch (const std::exception& e) {
    spdlog::error("Exception while saving document store: {}", e.what());
    return false;
  }
}

bool DocumentStore::LoadFromFile(const std::string& filepath, std::string* replication_gtid) {
  try {
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs) {
      spdlog::error("Failed to open file for reading: {}", filepath);
      return false;
    }

    // Read and verify magic number
    std::array<char, 4> magic{};
    ifs.read(magic.data(), magic.size());
    if (std::memcmp(magic.data(), "MGDS", 4) != 0) {
      spdlog::error("Invalid document store file format (bad magic number)");
      return false;
    }

    // Read version
    uint32_t version = 0;
    ifs.read(reinterpret_cast<char*>(&version), sizeof(version));
    if (version != 1) {
      spdlog::error("Unsupported document store file version: {}", version);
      return false;
    }

    // Read next_doc_id
    uint32_t next_id = 0;
    ifs.read(reinterpret_cast<char*>(&next_id), sizeof(next_id));
    next_doc_id_ = static_cast<DocId>(next_id);

    // Read GTID (for replication position)
    uint32_t gtid_len = 0;
    ifs.read(reinterpret_cast<char*>(&gtid_len), sizeof(gtid_len));
    if (gtid_len > 0) {
      std::string gtid(gtid_len, '\0');
      ifs.read(gtid.data(), gtid_len);
      if (replication_gtid != nullptr) {
        *replication_gtid = gtid;
      }
    } else if (replication_gtid != nullptr) {
      replication_gtid->clear();
    }

    // Read document count
    uint64_t doc_count = 0;
    ifs.read(reinterpret_cast<char*>(&doc_count), sizeof(doc_count));

    // Load into new maps to minimize lock time
    std::unordered_map<DocId, std::string> new_doc_id_to_pk;
    std::unordered_map<std::string, DocId> new_pk_to_doc_id;
    std::unordered_map<DocId, std::unordered_map<std::string, FilterValue>> new_doc_filters;

    // Read doc_id -> pk mappings and filters
    for (uint64_t i = 0; i < doc_count; ++i) {
      // Read doc_id
      uint32_t doc_id_value = 0;
      ifs.read(reinterpret_cast<char*>(&doc_id_value), sizeof(doc_id_value));
      auto doc_id = static_cast<DocId>(doc_id_value);

      // Read pk length and pk
      uint32_t pk_len = 0;
      ifs.read(reinterpret_cast<char*>(&pk_len), sizeof(pk_len));

      std::string primary_key_str(pk_len, '\0');
      ifs.read(primary_key_str.data(), pk_len);

      new_doc_id_to_pk[doc_id] = primary_key_str;
      new_pk_to_doc_id[primary_key_str] = doc_id;

      // Read filters
      uint32_t filter_count = 0;
      ifs.read(reinterpret_cast<char*>(&filter_count), sizeof(filter_count));

      if (filter_count > 0) {
        std::unordered_map<std::string, FilterValue> filters;

        for (uint32_t j = 0; j < filter_count; ++j) {
          // Read filter name
          uint32_t name_len = 0;
          ifs.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));

          std::string name(name_len, '\0');
          ifs.read(name.data(), name_len);

          // Read filter type
          uint8_t type_idx = 0;
          ifs.read(reinterpret_cast<char*>(&type_idx), sizeof(type_idx));

          // Read filter value based on type
          FilterValue value;
          switch (type_idx) {
            case 0: { // std::monostate (NULL)
              value = std::monostate{};
              break;
            }
            case 1: { // bool
              bool bool_value = false;
              ifs.read(reinterpret_cast<char*>(&bool_value), sizeof(bool_value));
              value = bool_value;
              break;
            }
            case 2: { // int8_t
              int8_t int8_value = 0;
              ifs.read(reinterpret_cast<char*>(&int8_value), sizeof(int8_value));
              value = int8_value;
              break;
            }
            case 3: { // uint8_t
              uint8_t uint8_value = 0;
              ifs.read(reinterpret_cast<char*>(&uint8_value), sizeof(uint8_value));
              value = uint8_value;
              break;
            }
            case 4: { // int16_t
              int16_t int16_value = 0;
              ifs.read(reinterpret_cast<char*>(&int16_value), sizeof(int16_value));
              value = int16_value;
              break;
            }
            case 5: { // uint16_t
              uint16_t uint16_value = 0;
              ifs.read(reinterpret_cast<char*>(&uint16_value), sizeof(uint16_value));
              value = uint16_value;
              break;
            }
            case 6: { // int32_t
              int32_t int32_value = 0;
              ifs.read(reinterpret_cast<char*>(&int32_value), sizeof(int32_value));
              value = int32_value;
              break;
            }
            case 7: { // uint32_t
              uint32_t uint32_value = 0;
              ifs.read(reinterpret_cast<char*>(&uint32_value), sizeof(uint32_value));
              value = uint32_value;
              break;
            }
            case 8: { // int64_t
              int64_t int64_value = 0;
              ifs.read(reinterpret_cast<char*>(&int64_value), sizeof(int64_value));
              value = int64_value;
              break;
            }
            case 9: { // uint64_t
              uint64_t uint64_value = 0;
              ifs.read(reinterpret_cast<char*>(&uint64_value), sizeof(uint64_value));
              value = uint64_value;
              break;
            }
            case 10: { // std::string
              uint32_t str_len = 0;
              ifs.read(reinterpret_cast<char*>(&str_len), sizeof(str_len));
              std::string string_value(str_len, '\0');
              ifs.read(string_value.data(), str_len);
              value = string_value;
              break;
            }
            case 11: { // double
              double double_value = NAN;
              ifs.read(reinterpret_cast<char*>(&double_value), sizeof(double_value));
              value = double_value;
              break;
            }
            default:
              spdlog::error("Unknown filter type index: {}", type_idx);
              return false;
          }

          filters[name] = value;
        }

        new_doc_filters[doc_id] = filters;
      }
    }

    ifs.close();

    // Swap the loaded data in with minimal lock time
    {
      std::unique_lock lock(mutex_);
      doc_id_to_pk_ = std::move(new_doc_id_to_pk);
      pk_to_doc_id_ = std::move(new_pk_to_doc_id);
      doc_filters_ = std::move(new_doc_filters);
      next_doc_id_ = next_id;
    }

    spdlog::info("Loaded document store from {}: {} documents, {} MB",
                 filepath, doc_count, MemoryUsage() / (1024 * 1024));
    return true;
  } catch (const std::exception& e) {
    spdlog::error("Exception while loading document store: {}", e.what());
    return false;
  }
}

}  // namespace storage
}  // namespace mygramdb
