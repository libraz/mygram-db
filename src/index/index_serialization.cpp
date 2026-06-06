/**
 * @file index_serialization.cpp
 * @brief Index serialization/deserialization implementations (Save/Load)
 */

#include <spdlog/spdlog.h>

#include <cstring>
#include <fstream>

#include "index/index.h"

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

#include "utils/binary_io.h"
#include "utils/constants.h"
#include "utils/crc32.h"
#include "utils/endian_utils.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/structured_log.h"

namespace mygramdb::index {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

namespace {

// Current serialization format version (writes V3 with full n-gram config and CRC32 trailer)
constexpr uint32_t kFormatVersionV1 = 1;
constexpr uint32_t kFormatVersionV2 = 2;
constexpr uint32_t kFormatVersionV3 = 3;
constexpr uint32_t kCurrentFormatVersion = kFormatVersionV3;

// Size of the CRC32 checksum trailer (4 bytes)
constexpr size_t kCRC32Size = 4;

}  // namespace

Expected<void, Error> Index::SaveToFile(const std::string& filepath) const {
  try {
    std::ofstream ofs(filepath, std::ios::binary);
    if (!ofs) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "file_open_failed")
          .Field("operation", "save")
          .Field("filepath", filepath)
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageWriteError, "Failed to open file for writing", filepath));
    }

    // Serialize to stream (includes CRC32 trailer in V2 format)
    auto result = SaveToStream(ofs);
    if (!result) {
      return result;
    }

    uint64_t term_count = 0;
    {
      std::shared_lock<std::shared_mutex> lock(postings_mutex_);
      term_count = term_postings_.size();
    }

    ofs.close();

    // Ensure data is flushed to disk to prevent data loss on OS crash
#ifndef _WIN32
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg) - open() requires varargs
    int file_desc = open(filepath.c_str(), O_RDWR);
    if (file_desc >= 0) {
      if (fsync(file_desc) != 0) {
        mygram::utils::StructuredLog()
            .Event("storage_warning")
            .Field("operation", "fsync")
            .Field("filepath", filepath)
            .Field("errno", static_cast<int64_t>(errno))
            .Warn();
      }
      close(file_desc);
    }
#endif

    mygram::utils::StructuredLog()
        .Event("index_saved")
        .Field("path", filepath)
        .Field("terms", term_count)
        .Field("memory_mb", static_cast<uint64_t>(MemoryUsage() / mygram::constants::kBytesPerMegabyte))
        .Info();
    return {};
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("index_io_error")
        .Field("type", "exception_during_save")
        .Field("operation", "save")
        .Field("error", e.what())
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kIndexSerializationFailed, "Exception during index save", e.what()));
  }
}

Expected<void, Error> Index::SaveToStream(std::ostream& output_stream) const {
  try {
    // V3 format:
    // [4 bytes: magic "MGIX"] [4 bytes: version=3] [4 bytes: ngram_size]
    // [4 bytes: kanji_ngram_size] [1 byte: cross_boundary_ngrams]
    // [8 bytes: term_count] [terms and posting lists...]
    // [4 bytes: CRC32 of all preceding data]
    //
    // Write directly to the output stream while computing CRC32 incrementally.
    // This avoids the previous approach of buffering all data in an ostringstream
    // then copying it, which caused 2x peak memory usage for large indexes.

    uint32_t running_crc = 0;

    // Helper: write data to output stream and update CRC32 incrementally
    auto crc_write = [&](const void* data, size_t size) {
      output_stream.write(static_cast<const char*>(data), static_cast<std::streamsize>(size));
      running_crc = mygram::utils::UpdateCRC32(running_crc, data, size);
    };

    // Helper: write a binary value with CRC update
    auto crc_write_binary = [&](auto value) {
      auto le_value = mygram::utils::ToLittleEndian(value);
      crc_write(&le_value, sizeof(le_value));
    };

    // Write magic number
    crc_write("MGIX", 4);

    // Write version
    crc_write_binary(kCurrentFormatVersion);

    // Write ngram_size
    crc_write_binary(static_cast<uint32_t>(ngram_size_));
    crc_write_binary(static_cast<uint32_t>(kanji_ngram_size_));
    const uint8_t cross_boundary = cross_boundary_ngrams_ ? 1 : 0;
    crc_write(&cross_boundary, sizeof(cross_boundary));

    // RCU snapshot: Take short shared_lock to copy term->PostingList pairs, then
    // serialize lock-free. This allows searches to proceed during serialization.
    std::vector<std::pair<std::string, std::shared_ptr<PostingList>>> snapshot;
    {
      std::shared_lock<std::shared_mutex> lock(postings_mutex_);
      snapshot.reserve(term_postings_.size());
      for (const auto& [term, posting] : term_postings_) {
        snapshot.emplace_back(term, posting);
      }
    }  // Lock released here — searches can proceed

    // Write term count
    auto term_count = static_cast<uint64_t>(snapshot.size());
    crc_write_binary(term_count);

    // Write each term and its posting list (lock-free)
    for (const auto& [term, posting] : snapshot) {
      // Write term length and term
      crc_write_binary(static_cast<uint32_t>(term.size()));
      crc_write(term.data(), term.size());

      // Serialize posting list to buffer
      std::vector<uint8_t> posting_data;
      if (!posting->Serialize(posting_data)) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "posting_serialization_failed")
            .Field("operation", "save_to_stream")
            .Field("term", term)
            .Error();
        return MakeUnexpected(
            MakeError(ErrorCode::kIndexSerializationFailed, "Failed to serialize posting list", term));
      }

      // Write posting list size and data
      auto posting_size = static_cast<uint64_t>(posting_data.size());
      crc_write_binary(posting_size);
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O of raw bytes
      crc_write(reinterpret_cast<const char*>(posting_data.data()), posting_data.size());
    }

    // Write CRC32 trailer (not included in the checksum itself)
    mygram::utils::WriteBinary(output_stream, running_crc);

    if (!output_stream.good()) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "stream_error")
          .Field("operation", "save_to_stream")
          .Error();
      return MakeUnexpected(
          MakeError(ErrorCode::kIndexSerializationFailed, "Stream write error during index serialization"));
    }

    mygram::utils::StructuredLog().Event("index_saved_to_stream").Field("terms", term_count).Debug();
    return {};
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("index_io_error")
        .Field("type", "exception_during_save")
        .Field("operation", "save_to_stream")
        .Field("error", e.what())
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kIndexSerializationFailed, "Exception during stream save", e.what()));
  }
}

Expected<void, Error> Index::LoadFromFile(const std::string& filepath) {
  try {
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "file_open_failed")
          .Field("operation", "load")
          .Field("filepath", filepath)
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageFileNotFound, "Failed to open index file", filepath));
    }

    // Read file directly into a string and deserialize via LoadFromData.
    // This avoids the triple-copy path that LoadFromStream would introduce
    // (file → string → istringstream copy → LoadFromStream re-read).
    std::string file_data((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    ifs.close();

    auto result = LoadFromData(std::move(file_data));
    if (!result) {
      return result;
    }

    uint64_t term_count = 0;
    {
      std::shared_lock<std::shared_mutex> lock(postings_mutex_);
      term_count = term_postings_.size();
    }
    mygram::utils::StructuredLog()
        .Event("index_loaded")
        .Field("path", filepath)
        .Field("terms", term_count)
        .Field("memory_mb", static_cast<uint64_t>(MemoryUsage() / mygram::constants::kBytesPerMegabyte))
        .Info();
    return {};
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("index_io_error")
        .Field("type", "exception_during_load")
        .Field("operation", "load")
        .Field("error", e.what())
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kIndexDeserializationFailed, "Exception during index load", e.what()));
  }
}

Expected<void, Error> Index::LoadFromStream(std::istream& input_stream) {
  // Read entire stream into memory, then delegate to LoadFromData
  std::string all_data((std::istreambuf_iterator<char>(input_stream)), std::istreambuf_iterator<char>());
  return LoadFromData(std::move(all_data));
}

Expected<void, Error> Index::LoadFromData(std::string all_data) {
  try {
    // Minimum V1/V2 size: magic(4) + version(4) + ngram(4) + term_count(8) = 20 bytes
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    // 20: minimum header size (magic + version + ngram_size + term_count)
    constexpr size_t kMinLegacyHeaderSize = 20;
    // 25: V3 adds kanji_ngram_size(4) + cross_boundary_ngrams(1)
    constexpr size_t kMinV3HeaderSize = 25;
    if (all_data.size() < kMinLegacyHeaderSize) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "invalid_format")
          .Field("operation", "load_from_stream")
          .Field("error", "data_too_short")
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageInvalidFormat, "Index data too short to be valid"));
    }
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)

    // Verify magic number
    if (std::memcmp(all_data.data(), "MGIX", 4) != 0) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "invalid_format")
          .Field("operation", "load_from_stream")
          .Field("error", "bad_magic_number")
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageInvalidFormat, "Invalid magic number in index data"));
    }

    // Read version (offset 4)
    uint32_t version = 0;
    std::memcpy(&version, all_data.data() + 4, sizeof(version));
    version = mygram::utils::FromLittleEndian(version);

    if (version != kFormatVersionV1 && version != kFormatVersionV2 && version != kFormatVersionV3) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "unsupported_version")
          .Field("operation", "load_from_stream")
          .Field("version", std::to_string(version))
          .Error();
      return MakeUnexpected(
          MakeError(ErrorCode::kStorageVersionMismatch, "Unsupported index format version", std::to_string(version)));
    }

    // For V2, verify CRC32 checksum before deserializing any data
    size_t data_size = all_data.size();
    if (version == kFormatVersionV2 || version == kFormatVersionV3) {
      const size_t min_header_size = (version == kFormatVersionV3) ? kMinV3HeaderSize : kMinLegacyHeaderSize;
      if (data_size < min_header_size + kCRC32Size) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "invalid_format")
            .Field("operation", "load_from_stream")
            .Field("error", "missing_crc32_trailer")
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kStorageInvalidFormat, "Index data missing CRC32 trailer"));
      }

      // Split data: payload is everything except the trailing 4-byte CRC32
      size_t payload_size = data_size - kCRC32Size;

      // Read stored CRC32 from trailer
      uint32_t stored_crc = 0;
      std::memcpy(&stored_crc, all_data.data() + payload_size, sizeof(stored_crc));
      stored_crc = mygram::utils::FromLittleEndian(stored_crc);

      // Compute CRC32 over the payload
      uint32_t computed_crc = mygram::utils::ComputeCRC32(all_data.data(), payload_size);

      if (stored_crc != computed_crc) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "crc32_mismatch")
            .Field("operation", "load_from_stream")
            .Field("expected_crc", static_cast<uint64_t>(stored_crc))
            .Field("computed_crc", static_cast<uint64_t>(computed_crc))
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kStorageCRCMismatch, "CRC32 checksum mismatch in index data"));
      }

      // Truncate to payload only for deserialization
      data_size = payload_size;
    }

    // Deserialize from the validated payload using a memory stream
    // Skip past magic(4) + version(4) = 8 bytes
    size_t pos = 8;

    // Read ngram_size (offset 8)
    uint32_t ngram = 0;
    std::memcpy(&ngram, all_data.data() + pos, sizeof(ngram));
    ngram = mygram::utils::FromLittleEndian(ngram);
    pos += sizeof(ngram);

    if (static_cast<int>(ngram) != ngram_size_) {
      mygram::utils::StructuredLog()
          .Event("index_ngram_mismatch")
          .Field("stream_ngram", static_cast<uint64_t>(ngram))
          .Field("current_ngram", static_cast<uint64_t>(ngram_size_))
          .Error();
      return MakeUnexpected(MakeError(
          ErrorCode::kStorageVersionMismatch,
          "Index ngram_size mismatch: stream=" + std::to_string(ngram) + " current=" + std::to_string(ngram_size_)));
    }

    if (version == kFormatVersionV3) {
      uint32_t kanji_ngram = 0;
      std::memcpy(&kanji_ngram, all_data.data() + pos, sizeof(kanji_ngram));
      kanji_ngram = mygram::utils::FromLittleEndian(kanji_ngram);
      pos += sizeof(kanji_ngram);

      uint8_t cross_boundary = 0;
      std::memcpy(&cross_boundary, all_data.data() + pos, sizeof(cross_boundary));
      pos += sizeof(cross_boundary);

      if (static_cast<int>(kanji_ngram) != kanji_ngram_size_) {
        mygram::utils::StructuredLog()
            .Event("index_ngram_mismatch")
            .Field("stream_kanji_ngram", static_cast<uint64_t>(kanji_ngram))
            .Field("current_kanji_ngram", static_cast<uint64_t>(kanji_ngram_size_))
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kStorageVersionMismatch,
                                        "Index kanji_ngram_size mismatch: stream=" + std::to_string(kanji_ngram) +
                                            " current=" + std::to_string(kanji_ngram_size_)));
      }

      const bool stream_cross_boundary = cross_boundary != 0;
      if (stream_cross_boundary != cross_boundary_ngrams_) {
        mygram::utils::StructuredLog()
            .Event("index_ngram_mismatch")
            .Field("stream_cross_boundary_ngrams", stream_cross_boundary)
            .Field("current_cross_boundary_ngrams", cross_boundary_ngrams_)
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kStorageVersionMismatch,
                                        std::string("Index cross_boundary_ngrams mismatch: stream=") +
                                            (stream_cross_boundary ? "true" : "false") +
                                            " current=" + (cross_boundary_ngrams_ ? "true" : "false")));
      }
    }

    // Read term count
    uint64_t term_count = 0;
    std::memcpy(&term_count, all_data.data() + pos, sizeof(term_count));
    term_count = mygram::utils::FromLittleEndian(term_count);
    pos += sizeof(term_count);

    // Load into a new map to minimize lock time
    absl::flat_hash_map<std::string, std::shared_ptr<PostingList>, TransparentStringHash, TransparentStringEqual>
        new_postings;

    // Read each term and its posting list
    for (uint64_t i = 0; i < term_count; ++i) {
      if (pos + sizeof(uint32_t) > data_size) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "truncated_data")
            .Field("operation", "load_from_stream")
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kStorageCorrupted, "Truncated index data at term header"));
      }

      // Read term length and term
      uint32_t term_len = 0;
      std::memcpy(&term_len, all_data.data() + pos, sizeof(term_len));
      term_len = mygram::utils::FromLittleEndian(term_len);
      pos += sizeof(term_len);

      // Guard against malformed index files that could cause excessive memory allocation.
      // N-gram terms are typically under 40 UTF-8 bytes; 10000 is a generous safety
      // limit to reject corrupted data without false positives on valid indices.
      constexpr uint32_t kMaxTermLength = 10000;
      if (term_len > kMaxTermLength) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "invalid_term_length")
            .Field("term_len", static_cast<uint64_t>(term_len))
            .Field("max_allowed", static_cast<uint64_t>(kMaxTermLength))
            .Field("operation", "load_from_stream")
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kStorageCorrupted, "Term length exceeds maximum allowed size"));
      }

      if (pos + term_len > data_size) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "truncated_data")
            .Field("operation", "load_from_stream")
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kStorageCorrupted, "Truncated index data at term string"));
      }

      std::string term(all_data.data() + pos, term_len);
      pos += term_len;

      if (pos + sizeof(uint64_t) > data_size) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "truncated_data")
            .Field("operation", "load_from_stream")
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kStorageCorrupted, "Truncated index data at posting list header"));
      }

      // Read posting list size and data
      uint64_t posting_size = 0;
      std::memcpy(&posting_size, all_data.data() + pos, sizeof(posting_size));
      posting_size = mygram::utils::FromLittleEndian(posting_size);
      pos += sizeof(posting_size);

      // Guard against malformed index files that could cause excessive memory allocation.
      // 100M entries is far beyond any realistic posting list size and prevents OOM
      // from corrupted size fields while still allowing very large production indices.
      constexpr uint64_t kMaxPostingSize = 100'000'000;
      if (posting_size > kMaxPostingSize) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "invalid_posting_size")
            .Field("posting_size", posting_size)
            .Field("max_allowed", kMaxPostingSize)
            .Field("operation", "load_from_stream")
            .Error();
        return MakeUnexpected(
            MakeError(ErrorCode::kStorageCorrupted, "Posting list size exceeds maximum allowed size"));
      }

      if (pos + posting_size > data_size) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "truncated_data")
            .Field("operation", "load_from_stream")
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kStorageCorrupted, "Truncated index data at posting list body"));
      }

      std::vector<uint8_t> posting_data(posting_size);
      std::memcpy(posting_data.data(), all_data.data() + pos, posting_size);
      pos += posting_size;

      // Deserialize posting list
      auto posting = std::make_shared<PostingList>(roaring_threshold_);
      size_t offset = 0;
      if (!posting->Deserialize(posting_data, offset)) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "deserialization_failed")
            .Field("operation", "load_from_stream")
            .Field("term", term)
            .Error();
        return MakeUnexpected(
            MakeError(ErrorCode::kIndexDeserializationFailed, "Failed to deserialize posting list", term));
      }

      new_postings[term] = std::move(posting);
    }

    // Swap the loaded data in with minimal lock time
    {
      std::scoped_lock lock(postings_mutex_);
      term_postings_ = std::move(new_postings);
      // Increment generation so any in-progress Optimize() discards stale results
      load_generation_.fetch_add(1, std::memory_order_release);
    }

    mygram::utils::StructuredLog()
        .Event("index_loaded_from_stream")
        .Field("terms", term_count)
        .Field("format_version", static_cast<uint64_t>(version))
        .Debug();
    return {};
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("index_io_error")
        .Field("type", "exception_during_load")
        .Field("operation", "load_from_stream")
        .Field("error", e.what())
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kIndexDeserializationFailed, "Exception during stream load", e.what()));
  }
}

}  // namespace mygramdb::index
