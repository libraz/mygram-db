/**
 * @file document_store.cpp
 * @brief Document store implementation
 */

#include "storage/document_store.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <fstream>

namespace mygramdb::storage {

namespace {

// Binary I/O constants
constexpr size_t kBytesPerKilobyte = 1024;
constexpr size_t kBytesPerMegabyte = kBytesPerKilobyte * 1024;

// FilterValue type indices for serialization
// These map to std::variant<std::monostate, bool, int8_t, uint8_t, int16_t, uint16_t, int32_t,
// uint32_t, int64_t, uint64_t, std::string, double>
constexpr uint8_t kTypeIndexMonostate = 0;
constexpr uint8_t kTypeIndexBool = 1;
constexpr uint8_t kTypeIndexInt8 = 2;
constexpr uint8_t kTypeIndexUInt8 = 3;
constexpr uint8_t kTypeIndexInt16 = 4;
constexpr uint8_t kTypeIndexUInt16 = 5;
constexpr uint8_t kTypeIndexInt32 = 6;
constexpr uint8_t kTypeIndexUInt32 = 7;
constexpr uint8_t kTypeIndexInt64 = 8;
constexpr uint8_t kTypeIndexUInt64 = 9;
constexpr uint8_t kTypeIndexString = 10;
constexpr uint8_t kTypeIndexDouble = 11;

/**
 * @brief Helper to write binary data to output stream
 *
 * std::ofstream::write() requires const char* but we work with typed data.
 * This helper encapsulates the required type conversion.
 *
 * Why reinterpret_cast is necessary:
 * - std::ofstream::write() signature: write(const char*, streamsize)
 * - We need to write binary representations of typed objects (uint32_t, etc.)
 * - reinterpret_cast to const char* is the standard pattern for binary I/O
 * - This is type-safe as we're writing the exact binary representation
 *
 * @tparam T Type of data to write
 * @param output_stream Output stream
 * @param data Reference to data to write
 */
template <typename T>
inline void WriteBinary(std::ostream& output_stream, const T& data) {
  // Suppressing clang-tidy warning for standard binary I/O pattern
  output_stream.write(reinterpret_cast<const char*>(&data),  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                      sizeof(T));
}

/**
 * @brief Helper to read binary data from input stream
 *
 * std::ifstream::read() requires char* but we work with typed data.
 * This helper encapsulates the required type conversion.
 *
 * Why reinterpret_cast is necessary:
 * - std::ifstream::read() signature: read(char*, streamsize)
 * - We need to read binary data into typed objects (uint32_t, etc.)
 * - reinterpret_cast to char* is the standard pattern for binary I/O
 * - This is type-safe as we're reading the exact binary representation
 *
 * @tparam T Type of data to read
 * @param input_stream Input stream
 * @param data Reference to data to read into
 */
template <typename T>
inline void ReadBinary(std::istream& input_stream, T& data) {
  // Suppressing clang-tidy warning for standard binary I/O pattern
  input_stream.read(reinterpret_cast<char*>(&data),  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                    sizeof(T));
}

}  // namespace

DocId DocumentStore::AddDocument(const std::string& primary_key,
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

  spdlog::debug("Added document: DocID={}, PK={}, filters={}", doc_id, primary_key, filters.size());

  return doc_id;
}

std::vector<DocId> DocumentStore::AddDocumentBatch(const std::vector<DocumentItem>& documents) {
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
      spdlog::warn("Primary key {} already exists with DocID {}", doc.primary_key, iterator->second);
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

bool DocumentStore::UpdateDocument(DocId doc_id, const std::unordered_map<std::string, FilterValue>& filters) {
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

std::optional<FilterValue> DocumentStore::GetFilterValue(DocId doc_id, const std::string& filter_name) const {
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

std::vector<DocId> DocumentStore::FilterByValue(const std::string& filter_name, const FilterValue& value) const {
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

bool DocumentStore::HasFilterColumn(const std::string& filter_name) const {
  std::shared_lock lock(mutex_);

  // Check if any document has this filter column
  return std::any_of(doc_filters_.begin(), doc_filters_.end(), [&filter_name](const auto& doc_filter) {
    return doc_filter.second.find(filter_name) != doc_filter.second.end();
  });
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
    WriteBinary(ofs, version);

    uint32_t next_id = 0;
    uint64_t doc_count = 0;

    // Lock scope: read data structures
    {
      std::shared_lock lock(mutex_);

      // Write next_doc_id
      next_id = static_cast<uint32_t>(next_doc_id_);
      WriteBinary(ofs, next_id);

      // Write GTID (for replication position)
      auto gtid_len = static_cast<uint32_t>(replication_gtid.size());
      WriteBinary(ofs, gtid_len);
      if (gtid_len > 0) {
        ofs.write(replication_gtid.data(), gtid_len);
      }

      // Write document count
      doc_count = doc_id_to_pk_.size();
      WriteBinary(ofs, doc_count);

      // Write doc_id -> pk mappings
      for (const auto& [doc_id, primary_key_str] : doc_id_to_pk_) {
        // Write doc_id
        auto doc_id_value = static_cast<uint32_t>(doc_id);
        WriteBinary(ofs, doc_id_value);

        // Write pk length and pk
        auto pk_len = static_cast<uint32_t>(primary_key_str.size());
        WriteBinary(ofs, pk_len);
        ofs.write(primary_key_str.data(), pk_len);

        // Write filters for this document
        auto filter_it = doc_filters_.find(doc_id);
        uint32_t filter_count = 0;
        if (filter_it != doc_filters_.end()) {
          filter_count = static_cast<uint32_t>(filter_it->second.size());
        }
        WriteBinary(ofs, filter_count);

        if (filter_count > 0) {
          for (const auto& [name, value] : filter_it->second) {
            // Write filter name
            auto name_len = static_cast<uint32_t>(name.size());
            WriteBinary(ofs, name_len);
            ofs.write(name.data(), name_len);

            // Write filter type and value
            auto type_idx = static_cast<uint8_t>(value.index());
            WriteBinary(ofs, type_idx);

            std::visit(
                [&ofs](const auto& filter_value) {
                  using T = std::decay_t<decltype(filter_value)>;
                  if constexpr (std::is_same_v<T, std::monostate>) {
                    // std::monostate (NULL) has no data to write
                  } else if constexpr (std::is_same_v<T, std::string>) {
                    auto str_len = static_cast<uint32_t>(filter_value.size());
                    WriteBinary(ofs, str_len);
                    ofs.write(filter_value.data(), str_len);
                  } else {
                    WriteBinary(ofs, filter_value);
                  }
                },
                value);
          }
        }
      }
    }

    ofs.close();
    spdlog::info("Saved document store to {}: {} documents, {} MB", filepath, doc_count,
                 MemoryUsage() / kBytesPerMegabyte);
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
    ReadBinary(ifs, version);
    if (version != 1) {
      spdlog::error("Unsupported document store file version: {}", version);
      return false;
    }

    // Read next_doc_id (will be set later under lock)
    uint32_t next_id = 0;
    ReadBinary(ifs, next_id);

    // Read GTID (for replication position)
    constexpr uint32_t kMaxGTIDLength = 1024;
    constexpr uint64_t kMaxDocumentCount = 1000000000;  // 1 billion documents
    uint32_t gtid_len = 0;
    ReadBinary(ifs, gtid_len);
    if (!ifs.good()) {
      spdlog::error("Failed to read GTID length from snapshot");
      return false;
    }
    if (gtid_len > kMaxGTIDLength) {
      spdlog::error("GTID length {} exceeds maximum allowed {}", gtid_len, kMaxGTIDLength);
      return false;
    }
    if (gtid_len > 0) {
      std::string gtid(gtid_len, '\0');
      ifs.read(gtid.data(), gtid_len);
      if (!ifs.good()) {
        spdlog::error("Failed to read GTID data from snapshot");
        return false;
      }
      if (replication_gtid != nullptr) {
        *replication_gtid = gtid;
      }
    } else if (replication_gtid != nullptr) {
      replication_gtid->clear();
    }

    // Read document count
    uint64_t doc_count = 0;
    ReadBinary(ifs, doc_count);
    if (!ifs.good()) {
      spdlog::error("Failed to read document count from snapshot");
      return false;
    }
    if (doc_count > kMaxDocumentCount) {
      spdlog::error("Document count {} exceeds maximum allowed {}", doc_count, kMaxDocumentCount);
      return false;
    }

    // Load into new maps to minimize lock time
    std::unordered_map<DocId, std::string> new_doc_id_to_pk;
    std::unordered_map<std::string, DocId> new_pk_to_doc_id;
    std::unordered_map<DocId, std::unordered_map<std::string, FilterValue>> new_doc_filters;

    // Read doc_id -> pk mappings and filters
    for (uint64_t i = 0; i < doc_count; ++i) {
      // Read doc_id
      uint32_t doc_id_value = 0;
      ReadBinary(ifs, doc_id_value);
      auto doc_id = static_cast<DocId>(doc_id_value);

      // Read pk length and pk
      constexpr uint32_t kMaxPKLength = 1024 * 1024;  // 1MB max for primary key
      constexpr uint32_t kMaxFilterCount = 1000;
      constexpr uint32_t kMaxFilterNameLength = 1024;
      constexpr uint32_t kMaxFilterStringLength = 64 * 1024;  // 64KB max for filter string
      uint32_t pk_len = 0;
      ReadBinary(ifs, pk_len);
      if (pk_len > kMaxPKLength) {
        spdlog::error("Primary key length {} exceeds maximum allowed {}", pk_len, kMaxPKLength);
        return false;
      }

      std::string primary_key_str(pk_len, '\0');
      ifs.read(primary_key_str.data(), pk_len);

      new_doc_id_to_pk[doc_id] = primary_key_str;
      new_pk_to_doc_id[primary_key_str] = doc_id;

      // Read filters
      uint32_t filter_count = 0;
      ReadBinary(ifs, filter_count);
      if (filter_count > kMaxFilterCount) {
        spdlog::error("Filter count {} exceeds maximum allowed {}", filter_count, kMaxFilterCount);
        return false;
      }

      if (filter_count > 0) {
        std::unordered_map<std::string, FilterValue> filters;

        for (uint32_t j = 0; j < filter_count; ++j) {
          // Read filter name
          uint32_t name_len = 0;
          ReadBinary(ifs, name_len);
          if (name_len > kMaxFilterNameLength) {
            spdlog::error("Filter name length {} exceeds maximum allowed {}", name_len, kMaxFilterNameLength);
            return false;
          }

          std::string name(name_len, '\0');
          ifs.read(name.data(), name_len);

          // Read filter type
          uint8_t type_idx = 0;
          ReadBinary(ifs, type_idx);

          // Read filter value based on type
          FilterValue value;
          switch (type_idx) {
            case kTypeIndexMonostate: {  // std::monostate (NULL)
              value = std::monostate{};
              break;
            }
            case kTypeIndexBool: {  // bool
              bool bool_value = false;
              ReadBinary(ifs, bool_value);
              value = bool_value;
              break;
            }
            case kTypeIndexInt8: {  // int8_t
              int8_t int8_value = 0;
              ReadBinary(ifs, int8_value);
              value = int8_value;
              break;
            }
            case kTypeIndexUInt8: {  // uint8_t
              uint8_t uint8_value = 0;
              ReadBinary(ifs, uint8_value);
              value = uint8_value;
              break;
            }
            case kTypeIndexInt16: {  // int16_t
              int16_t int16_value = 0;
              ReadBinary(ifs, int16_value);
              value = int16_value;
              break;
            }
            case kTypeIndexUInt16: {  // uint16_t
              uint16_t uint16_value = 0;
              ReadBinary(ifs, uint16_value);
              value = uint16_value;
              break;
            }
            case kTypeIndexInt32: {  // int32_t
              int32_t int32_value = 0;
              ReadBinary(ifs, int32_value);
              value = int32_value;
              break;
            }
            case kTypeIndexUInt32: {  // uint32_t
              uint32_t uint32_value = 0;
              ReadBinary(ifs, uint32_value);
              value = uint32_value;
              break;
            }
            case kTypeIndexInt64: {  // int64_t
              int64_t int64_value = 0;
              ReadBinary(ifs, int64_value);
              value = int64_value;
              break;
            }
            case kTypeIndexUInt64: {  // uint64_t
              uint64_t uint64_value = 0;
              ReadBinary(ifs, uint64_value);
              value = uint64_value;
              break;
            }
            case kTypeIndexString: {  // std::string
              uint32_t str_len = 0;
              ReadBinary(ifs, str_len);
              if (str_len > kMaxFilterStringLength) {
                spdlog::error("Filter string length {} exceeds maximum allowed {}", str_len, kMaxFilterStringLength);
                return false;
              }
              std::string string_value(str_len, '\0');
              ifs.read(string_value.data(), str_len);
              value = string_value;
              break;
            }
            case kTypeIndexDouble: {  // double
              double double_value = NAN;
              ReadBinary(ifs, double_value);
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

    spdlog::info("Loaded document store from {}: {} documents, {} MB", filepath, doc_count,
                 MemoryUsage() / kBytesPerMegabyte);
    return true;
  } catch (const std::exception& e) {
    spdlog::error("Exception while loading document store: {}", e.what());
    return false;
  }
}

bool DocumentStore::SaveToStream(std::ostream& output_stream, const std::string& replication_gtid) const {
  try {
    // File format:
    // [4 bytes: magic "MGDS"] [4 bytes: version] [4 bytes: next_doc_id]
    // [4 bytes: gtid_length] [gtid_length bytes: GTID string]
    // [8 bytes: doc_count] [doc_id -> pk mappings...]
    // [filters...]

    // Write magic number
    output_stream.write("MGDS", 4);

    // Write version
    uint32_t version = 1;
    WriteBinary(output_stream, version);

    uint32_t next_id = 0;
    uint64_t doc_count = 0;

    // Lock scope: read data structures
    {
      std::shared_lock lock(mutex_);

      // Write next_doc_id
      next_id = static_cast<uint32_t>(next_doc_id_);
      WriteBinary(output_stream, next_id);

      // Write GTID (for replication position)
      auto gtid_len = static_cast<uint32_t>(replication_gtid.size());
      WriteBinary(output_stream, gtid_len);
      if (gtid_len > 0) {
        output_stream.write(replication_gtid.data(), static_cast<std::streamsize>(gtid_len));
      }

      // Write document count
      doc_count = doc_id_to_pk_.size();
      WriteBinary(output_stream, doc_count);

      // Write doc_id -> pk mappings
      for (const auto& [doc_id, primary_key_str] : doc_id_to_pk_) {
        // Write doc_id
        auto doc_id_value = static_cast<uint32_t>(doc_id);
        WriteBinary(output_stream, doc_id_value);

        // Write pk length and pk
        auto pk_len = static_cast<uint32_t>(primary_key_str.size());
        WriteBinary(output_stream, pk_len);
        output_stream.write(primary_key_str.data(), static_cast<std::streamsize>(pk_len));

        // Write filters for this document
        auto filter_it = doc_filters_.find(doc_id);
        uint32_t filter_count = 0;
        if (filter_it != doc_filters_.end()) {
          filter_count = static_cast<uint32_t>(filter_it->second.size());
        }
        WriteBinary(output_stream, filter_count);

        if (filter_count > 0) {
          for (const auto& [name, value] : filter_it->second) {
            // Write filter name
            auto name_len = static_cast<uint32_t>(name.size());
            WriteBinary(output_stream, name_len);
            output_stream.write(name.data(), static_cast<std::streamsize>(name_len));

            // Write filter type and value
            auto type_idx = static_cast<uint8_t>(value.index());
            WriteBinary(output_stream, type_idx);

            std::visit(
                [&output_stream](const auto& filter_value) {
                  using T = std::decay_t<decltype(filter_value)>;
                  if constexpr (std::is_same_v<T, std::monostate>) {
                    // std::monostate (NULL) has no data to write
                  } else if constexpr (std::is_same_v<T, std::string>) {
                    auto str_len = static_cast<uint32_t>(filter_value.size());
                    WriteBinary(output_stream, str_len);
                    output_stream.write(filter_value.data(), static_cast<std::streamsize>(str_len));
                  } else {
                    WriteBinary(output_stream, filter_value);
                  }
                },
                value);
          }
        }
      }
    }

    if (!output_stream.good()) {
      spdlog::error("Stream error while saving document store");
      return false;
    }

    spdlog::debug("Saved document store to stream: {} documents", doc_count);
    return true;
  } catch (const std::exception& e) {
    spdlog::error("Exception while saving document store to stream: {}", e.what());
    return false;
  }
}

bool DocumentStore::LoadFromStream(std::istream& input_stream, std::string* replication_gtid) {
  try {
    // Read and verify magic number
    std::array<char, 4> magic{};
    input_stream.read(magic.data(), magic.size());
    if (std::memcmp(magic.data(), "MGDS", 4) != 0) {
      spdlog::error("Invalid document store stream format (bad magic number)");
      return false;
    }

    // Read version
    uint32_t version = 0;
    ReadBinary(input_stream, version);
    if (version != 1) {
      spdlog::error("Unsupported document store stream version: {}", version);
      return false;
    }

    // Read next_doc_id (will be set later under lock)
    uint32_t next_id = 0;
    ReadBinary(input_stream, next_id);

    // Read GTID (for replication position)
    constexpr uint32_t kMaxGTIDLength = 1024;
    constexpr uint64_t kMaxDocumentCount = 1000000000;  // 1 billion documents
    uint32_t gtid_len = 0;
    ReadBinary(input_stream, gtid_len);
    if (gtid_len > kMaxGTIDLength) {
      spdlog::error("GTID length {} exceeds maximum allowed {}", gtid_len, kMaxGTIDLength);
      return false;
    }
    if (gtid_len > 0) {
      std::string gtid(gtid_len, '\0');
      input_stream.read(gtid.data(), static_cast<std::streamsize>(gtid_len));
      if (replication_gtid != nullptr) {
        *replication_gtid = gtid;
      }
    } else if (replication_gtid != nullptr) {
      replication_gtid->clear();
    }

    // Read document count
    uint64_t doc_count = 0;
    ReadBinary(input_stream, doc_count);
    if (doc_count > kMaxDocumentCount) {
      spdlog::error("Document count {} exceeds maximum allowed {}", doc_count, kMaxDocumentCount);
      return false;
    }

    // Load into new maps to minimize lock time
    std::unordered_map<DocId, std::string> new_doc_id_to_pk;
    std::unordered_map<std::string, DocId> new_pk_to_doc_id;
    std::unordered_map<DocId, std::unordered_map<std::string, FilterValue>> new_doc_filters;

    // Read doc_id -> pk mappings and filters
    for (uint64_t i = 0; i < doc_count; ++i) {
      // Read doc_id
      uint32_t doc_id_value = 0;
      ReadBinary(input_stream, doc_id_value);
      auto doc_id = static_cast<DocId>(doc_id_value);

      // Read pk length and pk
      constexpr uint32_t kMaxPKLength = 1024 * 1024;  // 1MB max for primary key
      constexpr uint32_t kMaxFilterCount = 1000;
      constexpr uint32_t kMaxFilterNameLength = 1024;
      constexpr uint32_t kMaxFilterStringLength = 64 * 1024;  // 64KB max for filter string
      uint32_t pk_len = 0;
      ReadBinary(input_stream, pk_len);
      if (pk_len > kMaxPKLength) {
        spdlog::error("Primary key length {} exceeds maximum allowed {}", pk_len, kMaxPKLength);
        return false;
      }

      std::string primary_key_str(pk_len, '\0');
      input_stream.read(primary_key_str.data(), static_cast<std::streamsize>(pk_len));

      new_doc_id_to_pk[doc_id] = primary_key_str;
      new_pk_to_doc_id[primary_key_str] = doc_id;

      // Read filters
      uint32_t filter_count = 0;
      ReadBinary(input_stream, filter_count);
      if (filter_count > kMaxFilterCount) {
        spdlog::error("Filter count {} exceeds maximum allowed {}", filter_count, kMaxFilterCount);
        return false;
      }

      if (filter_count > 0) {
        std::unordered_map<std::string, FilterValue> filters;

        for (uint32_t j = 0; j < filter_count; ++j) {
          // Read filter name
          uint32_t name_len = 0;
          ReadBinary(input_stream, name_len);
          if (name_len > kMaxFilterNameLength) {
            spdlog::error("Filter name length {} exceeds maximum allowed {}", name_len, kMaxFilterNameLength);
            return false;
          }

          std::string name(name_len, '\0');
          input_stream.read(name.data(), static_cast<std::streamsize>(name_len));

          // Read filter type
          uint8_t type_idx = 0;
          ReadBinary(input_stream, type_idx);

          // Read filter value based on type
          FilterValue value;
          switch (type_idx) {
            case kTypeIndexMonostate: {  // std::monostate (NULL)
              value = std::monostate{};
              break;
            }
            case kTypeIndexBool: {  // bool
              bool bool_value = false;
              ReadBinary(input_stream, bool_value);
              value = bool_value;
              break;
            }
            case kTypeIndexInt8: {  // int8_t
              int8_t int8_value = 0;
              ReadBinary(input_stream, int8_value);
              value = int8_value;
              break;
            }
            case kTypeIndexUInt8: {  // uint8_t
              uint8_t uint8_value = 0;
              ReadBinary(input_stream, uint8_value);
              value = uint8_value;
              break;
            }
            case kTypeIndexInt16: {  // int16_t
              int16_t int16_value = 0;
              ReadBinary(input_stream, int16_value);
              value = int16_value;
              break;
            }
            case kTypeIndexUInt16: {  // uint16_t
              uint16_t uint16_value = 0;
              ReadBinary(input_stream, uint16_value);
              value = uint16_value;
              break;
            }
            case kTypeIndexInt32: {  // int32_t
              int32_t int32_value = 0;
              ReadBinary(input_stream, int32_value);
              value = int32_value;
              break;
            }
            case kTypeIndexUInt32: {  // uint32_t
              uint32_t uint32_value = 0;
              ReadBinary(input_stream, uint32_value);
              value = uint32_value;
              break;
            }
            case kTypeIndexInt64: {  // int64_t
              int64_t int64_value = 0;
              ReadBinary(input_stream, int64_value);
              value = int64_value;
              break;
            }
            case kTypeIndexUInt64: {  // uint64_t
              uint64_t uint64_value = 0;
              ReadBinary(input_stream, uint64_value);
              value = uint64_value;
              break;
            }
            case kTypeIndexString: {  // std::string
              uint32_t str_len = 0;
              ReadBinary(input_stream, str_len);
              if (str_len > kMaxFilterStringLength) {
                spdlog::error("Filter string length {} exceeds maximum allowed {}", str_len, kMaxFilterStringLength);
                return false;
              }
              std::string str_value(str_len, '\0');
              input_stream.read(str_value.data(), static_cast<std::streamsize>(str_len));
              value = str_value;
              break;
            }
            case kTypeIndexDouble: {  // double
              double double_value = NAN;
              ReadBinary(input_stream, double_value);
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

    if (!input_stream.good()) {
      spdlog::error("Stream error while loading document store");
      return false;
    }

    // Swap the loaded data in with minimal lock time
    {
      std::unique_lock lock(mutex_);
      doc_id_to_pk_ = std::move(new_doc_id_to_pk);
      pk_to_doc_id_ = std::move(new_pk_to_doc_id);
      doc_filters_ = std::move(new_doc_filters);
      next_doc_id_ = next_id;
    }

    spdlog::debug("Loaded document store from stream: {} documents", doc_count);
    return true;
  } catch (const std::exception& e) {
    spdlog::error("Exception while loading document store from stream: {}", e.what());
    return false;
  }
}

}  // namespace mygramdb::storage
