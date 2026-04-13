/**
 * @file index_serialization.cpp
 * @brief Index serialization/deserialization implementations (Save/Load)
 */

#include <spdlog/spdlog.h>

#include <cstring>
#include <fstream>
#include <sstream>

#include "index/index.h"

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

#include "utils/binary_io.h"
#include "utils/constants.h"
#include "utils/crc32.h"
#include "utils/endian_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::index {

namespace {

// Current serialization format version (writes V2 with CRC32 trailer)
constexpr uint32_t kFormatVersionV1 = 1;
constexpr uint32_t kFormatVersionV2 = 2;
constexpr uint32_t kCurrentFormatVersion = kFormatVersionV2;

// Size of the CRC32 checksum trailer (4 bytes)
constexpr size_t kCRC32Size = 4;

}  // namespace

bool Index::SaveToFile(const std::string& filepath) const {
  try {
    std::ofstream ofs(filepath, std::ios::binary);
    if (!ofs) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "file_open_failed")
          .Field("operation", "save")
          .Field("filepath", filepath)
          .Error();
      return false;
    }

    // Serialize to stream (includes CRC32 trailer in V2 format)
    if (!SaveToStream(ofs)) {
      return false;
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
    return true;
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("index_io_error")
        .Field("type", "exception_during_save")
        .Field("operation", "save")
        .Field("error", e.what())
        .Error();
    return false;
  }
}

bool Index::SaveToStream(std::ostream& output_stream) const {
  try {
    // V2 format:
    // [4 bytes: magic "MGIX"] [4 bytes: version=2] [4 bytes: ngram_size]
    // [8 bytes: term_count] [terms and posting lists...]
    // [4 bytes: CRC32 of all preceding data]
    //
    // Serialize all data to an intermediate buffer so we can compute the CRC32
    // before writing to the output stream (which may not be seekable).
    std::ostringstream buffer(std::ios::binary);

    // Write magic number
    buffer.write("MGIX", 4);

    // Write version
    uint32_t version = kCurrentFormatVersion;
    mygram::utils::WriteBinary(buffer, version);

    // Write ngram_size
    auto ngram = static_cast<uint32_t>(ngram_size_);
    mygram::utils::WriteBinary(buffer, ngram);

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
    mygram::utils::WriteBinary(buffer, term_count);

    // Write each term and its posting list (lock-free)
    for (const auto& [term, posting] : snapshot) {
      // Write term length and term
      auto term_len = static_cast<uint32_t>(term.size());
      mygram::utils::WriteBinary(buffer, term_len);
      buffer.write(term.data(), static_cast<std::streamsize>(term_len));

      // Serialize posting list to buffer
      std::vector<uint8_t> posting_data;
      if (!posting->Serialize(posting_data)) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "posting_serialization_failed")
            .Field("operation", "save_to_stream")
            .Field("term", term)
            .Error();
        return false;
      }

      // Write posting list size and data
      uint64_t posting_size = posting_data.size();
      mygram::utils::WriteBinary(buffer, posting_size);
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O of raw bytes
      buffer.write(reinterpret_cast<const char*>(posting_data.data()), static_cast<std::streamsize>(posting_size));
    }

    // Get the serialized data and compute CRC32
    std::string data = buffer.str();
    uint32_t crc = mygram::utils::ComputeCRC32(data.data(), data.size());

    // Write data followed by CRC32 trailer to the actual output stream
    output_stream.write(data.data(), static_cast<std::streamsize>(data.size()));
    mygram::utils::WriteBinary(output_stream, crc);

    if (!output_stream.good()) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "stream_error")
          .Field("operation", "save_to_stream")
          .Error();
      return false;
    }

    mygram::utils::StructuredLog().Event("index_saved_to_stream").Field("terms", term_count).Debug();
    return true;
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("index_io_error")
        .Field("type", "exception_during_save")
        .Field("operation", "save_to_stream")
        .Field("error", e.what())
        .Error();
    return false;
  }
}

bool Index::LoadFromFile(const std::string& filepath) {
  try {
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "file_open_failed")
          .Field("operation", "load")
          .Field("filepath", filepath)
          .Error();
      return false;
    }

    // Read entire file into memory for CRC32 verification
    std::string file_data((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    ifs.close();

    std::istringstream input_stream(file_data, std::ios::binary);
    if (!LoadFromStream(input_stream)) {
      return false;
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
    return true;
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("index_io_error")
        .Field("type", "exception_during_load")
        .Field("operation", "load")
        .Field("error", e.what())
        .Error();
    return false;
  }
}

bool Index::LoadFromStream(std::istream& input_stream) {
  try {
    // Read entire stream into memory for CRC32 verification
    std::string all_data((std::istreambuf_iterator<char>(input_stream)), std::istreambuf_iterator<char>());

    // Minimum size: magic(4) + version(4) + ngram(4) + term_count(8) = 20 bytes
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    // 20: minimum header size (magic + version + ngram_size + term_count)
    constexpr size_t kMinHeaderSize = 20;
    if (all_data.size() < kMinHeaderSize) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "invalid_format")
          .Field("operation", "load_from_stream")
          .Field("error", "data_too_short")
          .Error();
      return false;
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
      return false;
    }

    // Read version (offset 4)
    uint32_t version = 0;
    std::memcpy(&version, all_data.data() + 4, sizeof(version));
    version = mygram::utils::FromLittleEndian(version);

    if (version != kFormatVersionV1 && version != kFormatVersionV2) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "unsupported_version")
          .Field("operation", "load_from_stream")
          .Field("version", std::to_string(version))
          .Error();
      return false;
    }

    // For V2, verify CRC32 checksum before deserializing any data
    size_t data_size = all_data.size();
    if (version == kFormatVersionV2) {
      if (data_size < kMinHeaderSize + kCRC32Size) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "invalid_format")
            .Field("operation", "load_from_stream")
            .Field("error", "missing_crc32_trailer")
            .Error();
        return false;
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
        return false;
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
          .Warn();
      // Continue anyway, but this might cause issues
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
        return false;
      }

      // Read term length and term
      uint32_t term_len = 0;
      std::memcpy(&term_len, all_data.data() + pos, sizeof(term_len));
      term_len = mygram::utils::FromLittleEndian(term_len);
      pos += sizeof(term_len);

      if (pos + term_len > data_size) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "truncated_data")
            .Field("operation", "load_from_stream")
            .Error();
        return false;
      }

      std::string term(all_data.data() + pos, term_len);
      pos += term_len;

      if (pos + sizeof(uint64_t) > data_size) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "truncated_data")
            .Field("operation", "load_from_stream")
            .Error();
        return false;
      }

      // Read posting list size and data
      uint64_t posting_size = 0;
      std::memcpy(&posting_size, all_data.data() + pos, sizeof(posting_size));
      posting_size = mygram::utils::FromLittleEndian(posting_size);
      pos += sizeof(posting_size);

      if (pos + posting_size > data_size) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "truncated_data")
            .Field("operation", "load_from_stream")
            .Error();
        return false;
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
        return false;
      }

      new_postings[term] = std::move(posting);
    }

    // Swap the loaded data in with minimal lock time
    {
      std::scoped_lock lock(postings_mutex_);
      term_postings_ = std::move(new_postings);
    }

    mygram::utils::StructuredLog()
        .Event("index_loaded_from_stream")
        .Field("terms", term_count)
        .Field("format_version", static_cast<uint64_t>(version))
        .Debug();
    return true;
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("index_io_error")
        .Field("type", "exception_during_load")
        .Field("operation", "load_from_stream")
        .Field("error", e.what())
        .Error();
    return false;
  }
}

}  // namespace mygramdb::index
