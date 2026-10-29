// Local fuzz harness for untrusted binlog events, query text and dump files.
// Build explicitly with -DBUILD_FUZZERS=ON; this is intentionally not a CI gate.
//
// Two engines are supported. The default builds a self-contained binary with a
// deterministic PRNG driver, so the harness runs anywhere without libFuzzer.
// -DFUZZER_ENGINE=libfuzzer instead links -fsanitize=fuzzer and lets libFuzzer
// supply main(), coverage feedback and a corpus.

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "mybinlog/json.h"
#include "mybinlog/mariadb_gtid.h"
#include "mysql/binlog_event_parser.h"
#include "mysql/rows_parser.h"
#include "mysql/table_metadata.h"
#include "query/query_parser.h"
#include "storage/document_store.h"
#include "storage/dump_format_v1.h"
#include "storage/dump_format_v2.h"

namespace {

using mygramdb::mysql::ColumnMetadata;
using mygramdb::mysql::ColumnType;
using mygramdb::mysql::TableMetadata;

/// Column types the row decoder handles differently enough to be worth steering
/// the fuzzer across. One input byte selects from this table.
constexpr ColumnType kFuzzableColumnTypes[] = {
    ColumnType::kTiny,       ColumnType::kShort, ColumnType::kLong,      ColumnType::kFloat,    ColumnType::kDouble,
    ColumnType::kLongLong,   ColumnType::kInt24, ColumnType::kDate,      ColumnType::kTime2,    ColumnType::kDatetime2,
    ColumnType::kTimestamp2, ColumnType::kYear,  ColumnType::kVarchar,   ColumnType::kBit,      ColumnType::kJson,
    ColumnType::kNewDecimal, ColumnType::kEnum,  ColumnType::kSet,       ColumnType::kTinyBlob, ColumnType::kMediumBlob,
    ColumnType::kLongBlob,   ColumnType::kBlob,  ColumnType::kVarString, ColumnType::kString,   ColumnType::kGeometry,
    ColumnType::kVector,
};

constexpr size_t kMaxFuzzColumns = 16;

/**
 * @brief Derive a table layout from the head of the input.
 *
 * The layout and the row bytes both come from the same input, so the fuzzer can
 * steer a decoder into a column type and then feed it a matching (or
 * deliberately mismatched) payload. Returns the offset where the row payload
 * begins.
 */
size_t BuildTableMetadata(const uint8_t* data, size_t size, TableMetadata& metadata) {
  metadata.table_id = 1;
  metadata.database_name = "fuzz_db";
  metadata.table_name = "fuzz_table";

  if (size == 0) {
    return 0;
  }

  const size_t column_count = (static_cast<size_t>(data[0]) % kMaxFuzzColumns) + 1;
  size_t offset = 1;
  for (size_t i = 0; i < column_count; ++i) {
    ColumnMetadata column;
    column.name = "c" + std::to_string(i);
    column.type = kFuzzableColumnTypes[(offset < size ? data[offset] : i) % std::size(kFuzzableColumnTypes)];
    ++offset;
    // Metadata is type-specific (pack length, precision, fractional digits).
    // Feeding it from the input reaches the validation each decoder does before
    // trusting it.
    const uint8_t low = offset < size ? data[offset] : 0;
    ++offset;
    const uint8_t high = offset < size ? data[offset] : 0;
    ++offset;
    column.metadata = static_cast<uint16_t>((static_cast<uint16_t>(high) << 8U) | low);
    column.is_nullable = (i % 2) == 0;
    metadata.columns.push_back(column);
  }

  return offset < size ? offset : size;
}

/// Row images: the parser must reject malformed layouts and truncated payloads
/// without reading past the event buffer.
void FuzzRowsParser(const uint8_t* data, size_t size) {
  TableMetadata metadata;
  const size_t payload_offset = BuildTableMetadata(data, size, metadata);
  const auto* payload = reinterpret_cast<const unsigned char*>(data) + payload_offset;
  const size_t payload_size = size - payload_offset;
  if (payload_size == 0) {
    return;
  }

  mygramdb::config::TableConfig table_config;
  table_config.name = metadata.table_name;
  table_config.primary_key = "c0";
  table_config.text_source.column = "c1";
  const auto retained = mygramdb::mysql::BuildRetainedColumns(metadata, table_config);

  for (const auto* mask : {static_cast<const mygramdb::mysql::RetainedColumns*>(nullptr), &retained}) {
    (void)mygramdb::mysql::ParseWriteRowsEvent(payload, payload_size, &metadata, "c0", "c1",
                                               mybinlog::EventType::kWriteRowsEvent, mask);
    (void)mygramdb::mysql::ParseUpdateRowsEvent(payload, payload_size, &metadata, "c0", "c1",
                                                mybinlog::EventType::kUpdateRowsEvent, mask);
    (void)mygramdb::mysql::ParseDeleteRowsEvent(payload, payload_size, &metadata, "c0", "c1",
                                                mybinlog::EventType::kDeleteRowsEvent, mask);
  }
}

/// Binary JSON arrives inside row images; a retained JSON column is printed from untrusted bytes.
void FuzzBinaryJson(const uint8_t* data, size_t size) {
  std::string text;
  (void)mybinlog::JsonBinaryToText(data, size, &text);
}

/// Query text is the one surface an unauthenticated client controls directly.
void FuzzQueryParser(const uint8_t* data, size_t size) {
  mygramdb::query::QueryParser parser;
  (void)parser.Parse(std::string_view(reinterpret_cast<const char*>(data), size));
}

/**
 * @brief Load the input as a V2 dump file.
 *
 * ReadDumpV2 takes a path rather than a buffer, so the input is staged in a
 * per-process file that is reused across iterations. A pre-allocated table
 * context lets a well-formed section reach the index and document-store
 * decoders instead of stopping at the envelope.
 */
void FuzzDumpV2(const uint8_t* data, size_t size) {
  static const std::string path =
      (std::filesystem::temp_directory_path() / ("mygramdb-fuzz-dump-" + std::to_string(::getpid()) + ".bin")).string();
  {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
      return;
    }
    out.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(size));
  }

  std::string gtid;
  mygramdb::config::Config config;
  mygramdb::index::Index index;
  mygramdb::storage::DocumentStore doc_store;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;
  contexts["articles"] = {&index, &doc_store};

  std::string source_server_uuid;
  (void)mygramdb::storage::dump_v2::ReadDumpV2(path, gtid, config, contexts, nullptr, nullptr, nullptr, {}, {},
                                               &source_server_uuid);
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (data == nullptr || size == 0 || size > (1U << 20U)) {
    return 0;
  }

  const auto* bytes = reinterpret_cast<const unsigned char*>(data);
  const auto length = static_cast<unsigned long>(size);

  // MySQL and MariaDB parsers must reject arbitrary/truncated frames without
  // dereferencing outside the supplied input.
  (void)mygramdb::mysql::BinlogEventParser::ExtractGTID(bytes, length);
  (void)mygramdb::mysql::BinlogEventParser::ParseTableMapEvent(bytes, length);
  (void)mygramdb::mysql::BinlogEventParser::ExtractQueryString(bytes, length);
  (void)mygramdb::mysql::BinlogEventParser::IsRowsStatementEnd(bytes, length);
  mygramdb::mysql::BinlogFrame frame;
  if (mygramdb::mysql::BinlogEventParser::ReadFrame(bytes, length, &frame) == mybinlog::Status::kOk) {
    mybinlog::MariaDbGtidEvent gtid;
    mybinlog::MariaDbGtidListEvent gtid_list;
    mybinlog::MariaDbAnnotateRowsEvent annotation;
    (void)mybinlog::ParseMariaDbGtidEvent(frame.header, frame.body, frame.body_size, &gtid);
    (void)mybinlog::ParseMariaDbGtidListEvent(frame.header, frame.body, frame.body_size, &gtid_list);
    (void)mybinlog::ParseMariaDbAnnotateRowsEvent(frame.header, frame.body, frame.body_size, &annotation);
  }

  FuzzRowsParser(data, size);
  FuzzBinaryJson(data, size);
  FuzzQueryParser(data, size);

  // Dump header parsers receive the same untrusted byte stream. Header parsing
  // covers all length/count and integrity preconditions before allocation; the
  // V2 case below then continues into the sections themselves.
  std::string payload(reinterpret_cast<const char*>(data), size);
  {
    std::istringstream input(payload);
    mygramdb::storage::dump_v1::HeaderV1 header;
    (void)mygramdb::storage::dump_v1::ReadHeaderV1(input, header);
  }
  {
    std::istringstream input(payload);
    mygramdb::storage::dump_v2::HeaderV2 header;
    (void)mygramdb::storage::dump_v2::ReadHeaderV2(input, header);
  }

  FuzzDumpV2(data, size);

  return 0;
}

#if defined(MYGRAMDB_STANDALONE_FUZZER)
int main(int argc, char** argv) {
  size_t iterations = 100000;
  if (argc == 2) {
    iterations = static_cast<size_t>(std::strtoull(argv[1], nullptr, 10));
  }

  // A fixed seed makes a crashing input reproducible from the iteration count.
  uint64_t state = 0x4D594752414D4442ULL;
  std::vector<uint8_t> input;
  for (size_t iteration = 0; iteration < iterations; ++iteration) {
    state ^= state << 13U;
    state ^= state >> 7U;
    state ^= state << 17U;
    const size_t size = static_cast<size_t>(state & 0x0FFFU);
    input.resize(size);
    for (auto& byte : input) {
      state ^= state << 13U;
      state ^= state >> 7U;
      state ^= state << 17U;
      byte = static_cast<uint8_t>(state);
    }
    LLVMFuzzerTestOneInput(input.data(), input.size());
  }
  return 0;
}
#endif
