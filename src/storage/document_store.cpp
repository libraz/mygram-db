/**
 * @file document_store.cpp
 * @brief Document store implementation
 */

#include "storage/document_store.h"
#include <spdlog/spdlog.h>
#include <algorithm>
#include <fstream>
#include <cstring>

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

    // Write next_doc_id
    uint32_t next_id = static_cast<uint32_t>(next_doc_id_);
    ofs.write(reinterpret_cast<const char*>(&next_id), sizeof(next_id));

    // Write GTID (for replication position)
    uint32_t gtid_len = static_cast<uint32_t>(replication_gtid.size());
    ofs.write(reinterpret_cast<const char*>(&gtid_len), sizeof(gtid_len));
    if (gtid_len > 0) {
      ofs.write(replication_gtid.data(), gtid_len);
    }

    // Write document count
    uint64_t doc_count = doc_id_to_pk_.size();
    ofs.write(reinterpret_cast<const char*>(&doc_count), sizeof(doc_count));

    // Write doc_id -> pk mappings
    for (const auto& [doc_id, pk] : doc_id_to_pk_) {
      // Write doc_id
      uint32_t id = static_cast<uint32_t>(doc_id);
      ofs.write(reinterpret_cast<const char*>(&id), sizeof(id));

      // Write pk length and pk
      uint32_t pk_len = static_cast<uint32_t>(pk.size());
      ofs.write(reinterpret_cast<const char*>(&pk_len), sizeof(pk_len));
      ofs.write(pk.data(), pk_len);

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
          uint32_t name_len = static_cast<uint32_t>(name.size());
          ofs.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
          ofs.write(name.data(), name_len);

          // Write filter type and value
          uint8_t type_idx = static_cast<uint8_t>(value.index());
          ofs.write(reinterpret_cast<const char*>(&type_idx), sizeof(type_idx));

          std::visit([&ofs](const auto& v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::string>) {
              uint32_t str_len = static_cast<uint32_t>(v.size());
              ofs.write(reinterpret_cast<const char*>(&str_len), sizeof(str_len));
              ofs.write(v.data(), str_len);
            } else {
              ofs.write(reinterpret_cast<const char*>(&v), sizeof(v));
            }
          }, value);
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
    char magic[4];
    ifs.read(magic, 4);
    if (std::memcmp(magic, "MGDS", 4) != 0) {
      spdlog::error("Invalid document store file format (bad magic number)");
      return false;
    }

    // Read version
    uint32_t version;
    ifs.read(reinterpret_cast<char*>(&version), sizeof(version));
    if (version != 1) {
      spdlog::error("Unsupported document store file version: {}", version);
      return false;
    }

    // Read next_doc_id
    uint32_t next_id;
    ifs.read(reinterpret_cast<char*>(&next_id), sizeof(next_id));
    next_doc_id_ = static_cast<DocId>(next_id);

    // Read GTID (for replication position)
    uint32_t gtid_len;
    ifs.read(reinterpret_cast<char*>(&gtid_len), sizeof(gtid_len));
    if (gtid_len > 0) {
      std::string gtid(gtid_len, '\0');
      ifs.read(&gtid[0], gtid_len);
      if (replication_gtid) {
        *replication_gtid = gtid;
      }
    } else if (replication_gtid) {
      replication_gtid->clear();
    }

    // Read document count
    uint64_t doc_count;
    ifs.read(reinterpret_cast<char*>(&doc_count), sizeof(doc_count));

    // Clear existing data
    doc_id_to_pk_.clear();
    pk_to_doc_id_.clear();
    doc_filters_.clear();

    // Read doc_id -> pk mappings and filters
    for (uint64_t i = 0; i < doc_count; ++i) {
      // Read doc_id
      uint32_t id;
      ifs.read(reinterpret_cast<char*>(&id), sizeof(id));
      DocId doc_id = static_cast<DocId>(id);

      // Read pk length and pk
      uint32_t pk_len;
      ifs.read(reinterpret_cast<char*>(&pk_len), sizeof(pk_len));

      std::string pk(pk_len, '\0');
      ifs.read(&pk[0], pk_len);

      doc_id_to_pk_[doc_id] = pk;
      pk_to_doc_id_[pk] = doc_id;

      // Read filters
      uint32_t filter_count;
      ifs.read(reinterpret_cast<char*>(&filter_count), sizeof(filter_count));

      if (filter_count > 0) {
        std::unordered_map<std::string, FilterValue> filters;

        for (uint32_t j = 0; j < filter_count; ++j) {
          // Read filter name
          uint32_t name_len;
          ifs.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));

          std::string name(name_len, '\0');
          ifs.read(&name[0], name_len);

          // Read filter type
          uint8_t type_idx;
          ifs.read(reinterpret_cast<char*>(&type_idx), sizeof(type_idx));

          // Read filter value based on type
          FilterValue value;
          switch (type_idx) {
            case 0: { // bool
              bool v;
              ifs.read(reinterpret_cast<char*>(&v), sizeof(v));
              value = v;
              break;
            }
            case 1: { // int8_t
              int8_t v;
              ifs.read(reinterpret_cast<char*>(&v), sizeof(v));
              value = v;
              break;
            }
            case 2: { // uint8_t
              uint8_t v;
              ifs.read(reinterpret_cast<char*>(&v), sizeof(v));
              value = v;
              break;
            }
            case 3: { // int16_t
              int16_t v;
              ifs.read(reinterpret_cast<char*>(&v), sizeof(v));
              value = v;
              break;
            }
            case 4: { // uint16_t
              uint16_t v;
              ifs.read(reinterpret_cast<char*>(&v), sizeof(v));
              value = v;
              break;
            }
            case 5: { // int32_t
              int32_t v;
              ifs.read(reinterpret_cast<char*>(&v), sizeof(v));
              value = v;
              break;
            }
            case 6: { // uint32_t
              uint32_t v;
              ifs.read(reinterpret_cast<char*>(&v), sizeof(v));
              value = v;
              break;
            }
            case 7: { // int64_t
              int64_t v;
              ifs.read(reinterpret_cast<char*>(&v), sizeof(v));
              value = v;
              break;
            }
            case 8: { // std::string
              uint32_t str_len;
              ifs.read(reinterpret_cast<char*>(&str_len), sizeof(str_len));
              std::string v(str_len, '\0');
              ifs.read(&v[0], str_len);
              value = v;
              break;
            }
            case 9: { // double
              double v;
              ifs.read(reinterpret_cast<char*>(&v), sizeof(v));
              value = v;
              break;
            }
            default:
              spdlog::error("Unknown filter type index: {}", type_idx);
              return false;
          }

          filters[name] = value;
        }

        doc_filters_[doc_id] = filters;
      }
    }

    ifs.close();
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
