/**
 * @file document_store_persistence.cpp
 * @brief Document store persistence (SaveToFile, LoadFromFile, SaveToStream, LoadFromStream)
 */

#include <spdlog/spdlog.h>

#include <cmath>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>

#include "storage/document_store.h"
#include "storage/document_store_internal.h"
#include "storage/filter_index.h"
#include "utils/atomic_file_writer.h"
#include "utils/binary_io.h"
#include "utils/constants.h"
#include "utils/structured_log.h"

namespace mygramdb::storage {

using internal::kTypeIndexBool;
using internal::kTypeIndexDouble;
using internal::kTypeIndexInt16;
using internal::kTypeIndexInt32;
using internal::kTypeIndexInt64;
using internal::kTypeIndexInt8;
using internal::kTypeIndexMonostate;
using internal::kTypeIndexString;
using internal::kTypeIndexTimeValue;
using internal::kTypeIndexUInt16;
using internal::kTypeIndexUInt32;
using internal::kTypeIndexUInt64;
using internal::kTypeIndexUInt8;
using internal::ReadBinary;
using internal::WriteBinary;
using mygram::utils::ErrorCode;

namespace {

// Use shared byte unit constants from utils/constants.h
using mygram::constants::kBytesPerMegabyte;

// Validation constants for deserialization bounds checking
constexpr uint32_t kMaxGTIDLength = 1024;
constexpr uint64_t kMaxDocumentCount = 1000000000;  // 1 billion documents
constexpr uint32_t kMaxPKLength = 1024 * 1024;      // 1MB max for primary key
constexpr uint32_t kMaxFilterCount = 1000;
constexpr uint32_t kMaxFilterNameLength = 1024;
constexpr uint32_t kMaxFilterStringLength = 64 * 1024;           // 64KB max for filter string
constexpr uint32_t kMaxNormalizedTextLength = 16 * 1024 * 1024;  // 16MB

}  // namespace

bool DocumentStore::SerializeDocuments(std::ostream& out, const std::string& replication_gtid) const {
  // File format:
  // [4 bytes: magic "MGDS"] [4 bytes: version] [4 bytes: next_doc_id]
  // [4 bytes: gtid_length] [gtid_length bytes: GTID string]
  // [8 bytes: doc_count] [doc_id -> pk mappings...]
  // [filters...]
  // v2+: [4 bytes: normalized_text_length] [normalized_text_length bytes: text]

  // Write magic number
  out.write("MGDS", 4);

  // Write version (v2 adds doc_texts_ serialization)
  uint32_t version = 2;
  WriteBinary(out, version);

  // Write GTID (for replication position)
  auto gtid_len = static_cast<uint32_t>(replication_gtid.size());
  {
    std::shared_lock lock(mutex_);

    // Write next_doc_id
    auto next_id = static_cast<uint32_t>(next_doc_id_);
    WriteBinary(out, next_id);

    WriteBinary(out, gtid_len);
    if (gtid_len > 0) {
      out.write(replication_gtid.data(), static_cast<std::streamsize>(gtid_len));
    }

    // Check stream after writing header section
    if (!out.good())
      return false;

    // Write document count
    auto doc_count = static_cast<uint64_t>(doc_id_to_pk_.size());
    WriteBinary(out, doc_count);

    // Write doc_id -> pk mappings
    for (const auto& [doc_id, primary_key_str] : doc_id_to_pk_) {
      // Write doc_id
      auto doc_id_value = static_cast<uint32_t>(doc_id);
      WriteBinary(out, doc_id_value);

      // Write pk length and pk
      auto pk_len = static_cast<uint32_t>(primary_key_str.size());
      WriteBinary(out, pk_len);
      out.write(primary_key_str.data(), static_cast<std::streamsize>(pk_len));

      // Write filters for this document
      auto filter_it = doc_filters_.find(doc_id);
      uint32_t filter_count = 0;
      if (filter_it != doc_filters_.end()) {
        filter_count = static_cast<uint32_t>(filter_it->second.size());
      }
      WriteBinary(out, filter_count);

      if (filter_count > 0) {
        for (const auto& [name, value] : filter_it->second) {
          // Write filter name
          auto name_len = static_cast<uint32_t>(name.size());
          WriteBinary(out, name_len);
          out.write(name.data(), static_cast<std::streamsize>(name_len));

          // Write filter type and value
          auto type_idx = static_cast<uint8_t>(value.index());
          WriteBinary(out, type_idx);

          std::visit(
              [&out](const auto& filter_value) {
                using T = std::decay_t<decltype(filter_value)>;
                if constexpr (std::is_same_v<T, std::monostate>) {
                  // std::monostate (NULL) has no data to write
                } else if constexpr (std::is_same_v<T, std::string>) {
                  auto str_len = static_cast<uint32_t>(filter_value.size());
                  WriteBinary(out, str_len);
                  out.write(filter_value.data(), static_cast<std::streamsize>(str_len));
                } else if constexpr (std::is_same_v<T, TimeValue>) {
                  WriteBinary(out, filter_value.seconds);
                } else {
                  WriteBinary(out, filter_value);
                }
              },
              value);
        }
      }

      // Write normalized text (v2+)
      auto text_it = doc_texts_.find(doc_id);
      if (text_it != doc_texts_.end()) {
        auto text_len = static_cast<uint32_t>(text_it->second.size());
        WriteBinary(out, text_len);
        out.write(text_it->second.data(), static_cast<std::streamsize>(text_len));
      } else {
        uint32_t text_len = 0;
        WriteBinary(out, text_len);
      }

      // Periodic check to detect write failures early (e.g., disk full)
      if (!out.good())
        return false;
    }
  }

  return out.good();
}

Expected<void, Error> DocumentStore::DeserializeDocuments(std::istream& in, std::string* replication_gtid,
                                                          const std::string& context) {
  // Read and verify magic number
  std::array<char, 4> magic{};
  in.read(magic.data(), magic.size());
  if (std::memcmp(magic.data(), "MGDS", 4) != 0) {
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageCorrupted,
                                    "Invalid document store file format (bad magic number)", context));
  }

  // Read version
  uint32_t version = 0;
  if (!ReadBinary(in, version)) {
    return MakeUnexpected(
        MakeError(mygram::utils::ErrorCode::kStorageReadError, "Failed to read document store version", context));
  }
  if (version < 1 || version > 2) {
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageCorrupted,
                                    "Unsupported document store file version: " + std::to_string(version), context));
  }

  // Read next_doc_id (will be set later under lock)
  uint32_t next_id = 0;
  if (!ReadBinary(in, next_id)) {
    return MakeUnexpected(
        MakeError(mygram::utils::ErrorCode::kStorageReadError, "Failed to read next_doc_id from snapshot", context));
  }

  // Read GTID (for replication position)
  uint32_t gtid_len = 0;
  if (!ReadBinary(in, gtid_len)) {
    return MakeUnexpected(
        MakeError(mygram::utils::ErrorCode::kStorageReadError, "Failed to read GTID length from snapshot", context));
  }
  if (gtid_len > kMaxGTIDLength) {
    return MakeUnexpected(MakeError(
        mygram::utils::ErrorCode::kStorageCorrupted,
        "GTID length " + std::to_string(gtid_len) + " exceeds maximum allowed " + std::to_string(kMaxGTIDLength),
        context));
  }
  if (gtid_len > 0) {
    std::string gtid(gtid_len, '\0');
    in.read(gtid.data(), static_cast<std::streamsize>(gtid_len));
    if (in.gcount() != static_cast<std::streamsize>(gtid_len)) {
      return MakeUnexpected(
          MakeError(mygram::utils::ErrorCode::kStorageReadError, "Failed to read GTID data from snapshot", context));
    }
    if (replication_gtid != nullptr) {
      *replication_gtid = gtid;
    }
  } else if (replication_gtid != nullptr) {
    replication_gtid->clear();
  }

  // Read document count
  uint64_t doc_count = 0;
  if (!ReadBinary(in, doc_count)) {
    return MakeUnexpected(
        MakeError(mygram::utils::ErrorCode::kStorageReadError, "Failed to read document count from snapshot", context));
  }
  if (doc_count > kMaxDocumentCount) {
    return MakeUnexpected(MakeError(
        mygram::utils::ErrorCode::kStorageCorrupted,
        "Document count " + std::to_string(doc_count) + " exceeds maximum allowed " + std::to_string(kMaxDocumentCount),
        context));
  }

  // Load into new maps to minimize lock time
  absl::flat_hash_map<DocId, std::string> new_doc_id_to_pk;
  absl::flat_hash_map<std::string, DocId, mygram::utils::TransparentStringHash, mygram::utils::TransparentStringEqual>
      new_pk_to_doc_id;
  absl::flat_hash_map<DocId, FilterMap> new_doc_filters;
  absl::flat_hash_map<DocId, std::string> new_doc_texts;
  DocId max_loaded_doc_id = 0;

  // Reserve capacity to avoid rehashing during bulk insertion
  new_doc_id_to_pk.reserve(static_cast<size_t>(doc_count));
  new_pk_to_doc_id.reserve(static_cast<size_t>(doc_count));
  new_doc_filters.reserve(static_cast<size_t>(doc_count));
  new_doc_texts.reserve(static_cast<size_t>(doc_count));

  // Read doc_id -> pk mappings and filters
  for (uint64_t i = 0; i < doc_count; ++i) {
    // Read doc_id
    uint32_t doc_id_value = 0;
    if (!ReadBinary(in, doc_id_value)) {
      return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageReadError,
                                      "Failed to read doc_id for document " + std::to_string(i), context));
    }
    auto doc_id = static_cast<DocId>(doc_id_value);
    if (doc_id > max_loaded_doc_id) {
      max_loaded_doc_id = doc_id;
    }

    // Read pk length and pk
    uint32_t pk_len = 0;
    if (!ReadBinary(in, pk_len)) {
      return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageReadError,
                                      "Failed to read primary key length for doc_id " + std::to_string(doc_id),
                                      context));
    }
    if (pk_len > kMaxPKLength) {
      return MakeUnexpected(MakeError(
          mygram::utils::ErrorCode::kStorageCorrupted,
          "Primary key length " + std::to_string(pk_len) + " exceeds maximum allowed " + std::to_string(kMaxPKLength),
          context));
    }

    std::string primary_key_str(pk_len, '\0');
    in.read(primary_key_str.data(), static_cast<std::streamsize>(pk_len));
    if (in.gcount() != static_cast<std::streamsize>(pk_len)) {
      return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageCorrupted,
                                      "Failed to read primary key data at document " + std::to_string(i), context));
    }

    new_doc_id_to_pk[doc_id] = primary_key_str;
    new_pk_to_doc_id[primary_key_str] = doc_id;

    // Read filters
    uint32_t filter_count = 0;
    if (!ReadBinary(in, filter_count)) {
      return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageReadError,
                                      "Failed to read filter count for doc_id " + std::to_string(doc_id), context));
    }
    if (filter_count > kMaxFilterCount) {
      return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageCorrupted,
                                      "Filter count " + std::to_string(filter_count) + " exceeds maximum allowed " +
                                          std::to_string(kMaxFilterCount),
                                      context));
    }

    if (filter_count > 0) {
      FilterMap filters;

      for (uint32_t j = 0; j < filter_count; ++j) {
        // Read filter name
        uint32_t name_len = 0;
        if (!ReadBinary(in, name_len)) {
          return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageReadError,
                                          "Failed to read filter name length for doc_id " + std::to_string(doc_id),
                                          context));
        }
        if (name_len > kMaxFilterNameLength) {
          return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageCorrupted,
                                          "Filter name length " + std::to_string(name_len) +
                                              " exceeds maximum allowed " + std::to_string(kMaxFilterNameLength),
                                          context));
        }

        std::string name(name_len, '\0');
        in.read(name.data(), static_cast<std::streamsize>(name_len));
        if (in.gcount() != static_cast<std::streamsize>(name_len)) {
          return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageCorrupted,
                                          "Failed to read filter name data at document " + std::to_string(i), context));
        }

        // Read filter type
        uint8_t type_idx = 0;
        if (!ReadBinary(in, type_idx)) {
          return MakeUnexpected(
              MakeError(mygram::utils::ErrorCode::kStorageCorrupted, "Failed to read filter type", context));
        }

        // Read filter value based on type
        FilterValue value;
        switch (type_idx) {
          case kTypeIndexMonostate: {  // std::monostate (NULL)
            value = std::monostate{};
            break;
          }
          case kTypeIndexBool: {  // bool
            bool bool_value = false;
            if (!ReadBinary(in, bool_value)) {
              return MakeUnexpected(
                  MakeError(mygram::utils::ErrorCode::kStorageCorrupted, "Failed to read bool filter value", context));
            }
            value = bool_value;
            break;
          }
          case kTypeIndexInt8: {  // int8_t
            int8_t int8_value = 0;
            if (!ReadBinary(in, int8_value)) {
              return MakeUnexpected(
                  MakeError(mygram::utils::ErrorCode::kStorageCorrupted, "Failed to read int8 filter value", context));
            }
            value = int8_value;
            break;
          }
          case kTypeIndexUInt8: {  // uint8_t
            uint8_t uint8_value = 0;
            if (!ReadBinary(in, uint8_value)) {
              return MakeUnexpected(
                  MakeError(mygram::utils::ErrorCode::kStorageCorrupted, "Failed to read uint8 filter value", context));
            }
            value = uint8_value;
            break;
          }
          case kTypeIndexInt16: {  // int16_t
            int16_t int16_value = 0;
            if (!ReadBinary(in, int16_value)) {
              return MakeUnexpected(
                  MakeError(mygram::utils::ErrorCode::kStorageCorrupted, "Failed to read int16 filter value", context));
            }
            value = int16_value;
            break;
          }
          case kTypeIndexUInt16: {  // uint16_t
            uint16_t uint16_value = 0;
            if (!ReadBinary(in, uint16_value)) {
              return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageCorrupted,
                                              "Failed to read uint16 filter value", context));
            }
            value = uint16_value;
            break;
          }
          case kTypeIndexInt32: {  // int32_t
            int32_t int32_value = 0;
            if (!ReadBinary(in, int32_value)) {
              return MakeUnexpected(
                  MakeError(mygram::utils::ErrorCode::kStorageCorrupted, "Failed to read int32 filter value", context));
            }
            value = int32_value;
            break;
          }
          case kTypeIndexUInt32: {  // uint32_t
            uint32_t uint32_value = 0;
            if (!ReadBinary(in, uint32_value)) {
              return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageCorrupted,
                                              "Failed to read uint32 filter value", context));
            }
            value = uint32_value;
            break;
          }
          case kTypeIndexInt64: {  // int64_t
            int64_t int64_value = 0;
            if (!ReadBinary(in, int64_value)) {
              return MakeUnexpected(
                  MakeError(mygram::utils::ErrorCode::kStorageCorrupted, "Failed to read int64 filter value", context));
            }
            value = int64_value;
            break;
          }
          case kTypeIndexUInt64: {  // uint64_t
            uint64_t uint64_value = 0;
            if (!ReadBinary(in, uint64_value)) {
              return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageCorrupted,
                                              "Failed to read uint64 filter value", context));
            }
            value = uint64_value;
            break;
          }
          case kTypeIndexTimeValue: {  // TimeValue
            TimeValue time_value{};
            if (!ReadBinary(in, time_value.seconds)) {
              return MakeUnexpected(
                  MakeError(mygram::utils::ErrorCode::kStorageCorrupted, "Failed to read time filter value", context));
            }
            value = time_value;
            break;
          }
          case kTypeIndexString: {  // std::string
            uint32_t str_len = 0;
            if (!ReadBinary(in, str_len)) {
              return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageCorrupted,
                                              "Failed to read string filter length", context));
            }
            if (str_len > kMaxFilterStringLength) {
              return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageCorrupted,
                                              "Filter string length " + std::to_string(str_len) +
                                                  " exceeds maximum allowed " + std::to_string(kMaxFilterStringLength),
                                              context));
            }
            std::string string_value(str_len, '\0');
            in.read(string_value.data(), static_cast<std::streamsize>(str_len));
            if (in.gcount() != static_cast<std::streamsize>(str_len)) {
              return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageCorrupted,
                                              "Failed to read filter string data at document " + std::to_string(i),
                                              context));
            }
            value = string_value;
            break;
          }
          case kTypeIndexDouble: {  // double
            double double_value = NAN;
            if (!ReadBinary(in, double_value)) {
              return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageCorrupted,
                                              "Failed to read double filter value", context));
            }
            value = double_value;
            break;
          }
          default:
            return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageCorrupted,
                                            "Unknown filter type index: " + std::to_string(type_idx), context));
        }

        filters[name] = value;
      }

      new_doc_filters[doc_id] = filters;
    }

    // Read normalized text (v2+)
    if (version >= 2) {
      uint32_t text_len = 0;
      if (!ReadBinary(in, text_len)) {
        return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageReadError,
                                        "Failed to read normalized text length for doc_id " + std::to_string(doc_id),
                                        context));
      }
      if (text_len > kMaxNormalizedTextLength) {
        return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageCorrupted,
                                        "Normalized text length " + std::to_string(text_len) +
                                            " exceeds maximum allowed " + std::to_string(kMaxNormalizedTextLength),
                                        context));
      }
      if (text_len > 0) {
        std::string text(text_len, '\0');
        in.read(text.data(), static_cast<std::streamsize>(text_len));
        if (in.gcount() != static_cast<std::streamsize>(text_len)) {
          return MakeUnexpected(
              MakeError(mygram::utils::ErrorCode::kStorageReadError,
                        "Stream error while reading normalized text for doc_id " + std::to_string(doc_id), context));
        }
        new_doc_texts[doc_id] = std::move(text);
      }
    }
  }

  if (in.bad() || in.fail()) {
    return MakeUnexpected(
        MakeError(mygram::utils::ErrorCode::kStorageReadError, "Stream error after reading all documents", context));
  }

  // Rebuild filter index from loaded data
  auto new_filter_index = std::make_shared<FilterIndex>();
  for (const auto& [doc_id, filters] : new_doc_filters) {
    new_filter_index->AddDocument(doc_id, filters);
  }

  DocId restored_next_id = next_id;
  if (max_loaded_doc_id == UINT32_MAX) {
    restored_next_id = 0;
  } else if (max_loaded_doc_id > 0 && restored_next_id <= max_loaded_doc_id) {
    restored_next_id = max_loaded_doc_id + 1;
  }

  // Swap the loaded data in with minimal lock time
  {
    std::unique_lock lock(mutex_);
    doc_id_to_pk_ = std::move(new_doc_id_to_pk);
    pk_to_doc_id_ = std::move(new_pk_to_doc_id);
    doc_filters_ = std::move(new_doc_filters);
    doc_texts_ = std::move(new_doc_texts);
    filter_index_ = std::move(new_filter_index);
    next_doc_id_ = restored_next_id;
    RecomputePrimaryKeyDocIdOrderLocked();
  }

  return {};
}

Expected<void, Error> DocumentStore::SaveToFile(const std::string& filepath,
                                                const std::string& replication_gtid) const {
  mygram::utils::AtomicFileWriter writer(filepath);
  const auto& temp_filepath = writer.GetTempPath();
  try {
    std::ofstream ofs(temp_filepath, std::ios::binary);
    if (!ofs) {
      return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageWriteError,
                                      "Failed to open temp file for writing", temp_filepath));
    }

    if (!SerializeDocuments(ofs, replication_gtid)) {
      return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageWriteError,
                                      "Stream error while serializing documents", temp_filepath));
    }

    ofs.close();

    // Atomic commit: fsync temp file, rename to final path, fsync directory
    if (auto result = writer.Commit(); !result) {
      return result;
    }

    mygram::utils::StructuredLog()
        .Event("document_store_saved")
        .Field("path", filepath)
        .Field("documents", static_cast<uint64_t>(Size()))
        .Field("memory_mb", static_cast<uint64_t>(MemoryUsage() / kBytesPerMegabyte))
        .Info();
    return {};
  } catch (const std::exception& e) {
    // writer destructor will clean up temp file
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageWriteError,
                                    std::string("Exception while saving document store: ") + e.what(), filepath));
  }
}

Expected<void, Error> DocumentStore::LoadFromFile(const std::string& filepath, std::string* replication_gtid) {
  try {
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs) {
      return MakeUnexpected(
          MakeError(mygram::utils::ErrorCode::kStorageReadError, "Failed to open file for reading", filepath));
    }

    auto result = DeserializeDocuments(ifs, replication_gtid, filepath);
    if (!result) {
      return result;
    }

    mygram::utils::StructuredLog()
        .Event("document_store_loaded")
        .Field("path", filepath)
        .Field("documents", static_cast<uint64_t>(Size()))
        .Field("memory_mb", static_cast<uint64_t>(MemoryUsage() / kBytesPerMegabyte))
        .Info();
    return {};
  } catch (const std::exception& e) {
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageReadError,
                                    std::string("Exception while loading document store: ") + e.what(), filepath));
  }
}

Expected<void, Error> DocumentStore::SaveToStream(std::ostream& output_stream,
                                                  const std::string& replication_gtid) const {
  try {
    if (!SerializeDocuments(output_stream, replication_gtid)) {
      return MakeUnexpected(
          MakeError(mygram::utils::ErrorCode::kStorageWriteError, "Stream error while saving document store"));
    }

    mygram::utils::StructuredLog()
        .Event("document_store_saved_to_stream")
        .Field("documents", static_cast<uint64_t>(Size()))
        .Debug();
    return {};
  } catch (const std::exception& e) {
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageWriteError,
                                    std::string("Exception while saving document store to stream: ") + e.what()));
  }
}

Expected<void, Error> DocumentStore::LoadFromStream(std::istream& input_stream, std::string* replication_gtid) {
  try {
    auto result = DeserializeDocuments(input_stream, replication_gtid, "stream");
    if (!result) {
      return result;
    }

    mygram::utils::StructuredLog()
        .Event("document_store_loaded_from_stream")
        .Field("documents", static_cast<uint64_t>(Size()))
        .Debug();
    return {};
  } catch (const std::exception& e) {
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageReadError,
                                    std::string("Exception while loading document store from stream: ") + e.what()));
  }
}

}  // namespace mygramdb::storage
