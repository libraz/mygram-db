// Local fuzz harness for untrusted binlog events and dump headers.
// Build explicitly with -DBUILD_FUZZERS=ON; this is intentionally not a CI gate.

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

#include "mysql/binlog_event_parser.h"
#include "mysql/mariadb_event_parser.h"
#include "mysql/table_metadata.h"
#include "storage/dump_format_v1.h"
#include "storage/dump_format_v2.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (data == nullptr || size == 0 || size > (1U << 20U)) {
    return 0;
  }

  const auto* bytes = reinterpret_cast<const unsigned char*>(data);
  const auto length = static_cast<unsigned long>(size);

  // MySQL and MariaDB parsers must reject arbitrary/truncated frames without
  // dereferencing outside the supplied input.
  (void)mygramdb::mysql::BinlogEventParser::ExtractGTID(bytes, length);
  (void)mygramdb::mysql::BinlogEventParser::ExtractTaggedGTID(bytes, length);
  (void)mygramdb::mysql::BinlogEventParser::ParseTableMapEvent(bytes, length);
  (void)mygramdb::mysql::BinlogEventParser::ExtractQueryString(bytes, length);
  (void)mygramdb::mysql::BinlogEventParser::IsRowsStatementEnd(bytes, length);
  (void)mygramdb::mysql::MariaDBEventParser::ExtractGTID(bytes, size);
  (void)mygramdb::mysql::MariaDBEventParser::ExtractGTIDFlags(bytes, size);
  (void)mygramdb::mysql::MariaDBEventParser::ParseGTIDList(bytes, size);
  (void)mygramdb::mysql::MariaDBEventParser::ExtractAnnotateRows(bytes, size);

  // Dump header parsers receive the same untrusted byte stream. Full dump
  // loading requires a configured destination store, whereas header parsing
  // covers all length/count and integrity preconditions before allocation.
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
