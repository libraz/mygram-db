/**
 * @file dump_format_v1_test.cpp
 * @brief Unit tests for dump format V1 header serialization
 */

#include "storage/dump_format_v1.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>

#include "index/index.h"
#include "storage/dump_format.h"
#include "storage/dump_format_internal.h"
#include "utils/binary_io.h"
#include "utils/error.h"

using namespace mygramdb::storage::dump_v1;
namespace dump_format_limits = mygramdb::storage::dump_format_limits;
namespace config_limits = dump_format_limits::config;
using mygram::utils::WriteBinary;
using mygramdb::config::Config;
using mygramdb::config::TableConfig;

TEST(DumpFormatV1Test, PendingIndexRestorePreservesConfiguredRoaringThreshold) {
  mygramdb::index::Index serialized_index(2, 1, 0.11);
  ASSERT_TRUE(serialized_index.AddDocument(1, "hello world"));
  std::ostringstream serialized;
  ASSERT_TRUE(serialized_index.SaveToStream(serialized).has_value());

  mygramdb::index::Index live_index(2, 1, 0.73);
  mygramdb::storage::dump_internal::PendingTableLoad pending;
  pending.table_name = "articles";
  pending.index = &live_index;

  std::istringstream input(serialized.str());
  auto result = mygramdb::storage::dump_internal::LoadPendingIndex(pending, input);

  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_NE(pending.loaded_index, nullptr);
  EXPECT_DOUBLE_EQ(pending.loaded_index->GetRoaringThreshold(), 0.73);
  EXPECT_EQ(pending.loaded_index->SearchAnd({"he"}), (std::vector<mygramdb::index::DocId>{1}));
}

/**
 * @brief Test that header_size is non-zero after write
 */
TEST(DumpFormatV1Test, HeaderSizeNonZeroAfterWrite) {
  // Create a header with a GTID
  HeaderV1 header;
  header.header_size = 0;
  header.flags = 0;
  header.dump_timestamp = 1234567890;
  header.total_file_size = 0;
  header.file_crc32 = 0;
  header.gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:42";

  // Calculate expected header size:
  // header_size(4) + flags(4) + dump_timestamp(8) + total_file_size(8) +
  // file_crc32(4) + gtid_length_prefix(4) + gtid_data(N)
  uint32_t expected_size = static_cast<uint32_t>(4 + 4 + 8 + 8 + 4 + 4 + header.gtid.size());
  header.header_size = expected_size;

  // Write header to stream
  std::ostringstream oss;
  auto write_result = WriteHeaderV1(oss, header);
  ASSERT_TRUE(write_result.has_value()) << "WriteHeaderV1 failed: " << write_result.error().message();

  // Read header back
  std::istringstream iss(oss.str());
  HeaderV1 read_header;
  auto read_result = ReadHeaderV1(iss, read_header);
  ASSERT_TRUE(read_result.has_value()) << "ReadHeaderV1 failed: " << read_result.error().message();

  // Verify header_size is non-zero and matches expected value
  EXPECT_GT(read_header.header_size, 0u) << "header_size should be non-zero after write";
  EXPECT_EQ(read_header.header_size, expected_size) << "header_size should match calculated value";

  // Verify other fields round-trip correctly
  EXPECT_EQ(read_header.flags, header.flags);
  EXPECT_EQ(read_header.dump_timestamp, header.dump_timestamp);
  EXPECT_EQ(read_header.gtid, header.gtid);
}

/**
 * @brief Test header_size with empty GTID
 */
TEST(DumpFormatV1Test, HeaderSizeWithEmptyGtid) {
  HeaderV1 header;
  header.gtid = "";
  // Expected: 4 + 4 + 8 + 8 + 4 + 4 + 0 = 32
  uint32_t expected_size = 32;
  header.header_size = expected_size;

  std::ostringstream oss;
  auto write_result = WriteHeaderV1(oss, header);
  ASSERT_TRUE(write_result.has_value());

  std::istringstream iss(oss.str());
  HeaderV1 read_header;
  auto read_result = ReadHeaderV1(iss, read_header);
  ASSERT_TRUE(read_result.has_value());

  EXPECT_EQ(read_header.header_size, expected_size);
}

/**
 * @brief A header_size of zero is what releases up to v1.5.3 left behind
 *
 * Zero marks a release that never wrote the field. Any other value still has
 * to be the exact size the layout implies.
 */
TEST(DumpFormatV1Test, ValidateHeaderAcceptsAnUnrecordedHeaderSize) {
  HeaderV1 header;
  header.gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:42";
  header.total_file_size = 4096;

  header.header_size = kUnrecordedHeaderSizeV1;
  EXPECT_TRUE(ValidateHeaderIntegrityFields(header).has_value());

  header.header_size = static_cast<uint32_t>(32 + header.gtid.size());
  EXPECT_TRUE(ValidateHeaderIntegrityFields(header).has_value());

  header.header_size = static_cast<uint32_t>(32 + header.gtid.size()) + 1;
  auto wrong = ValidateHeaderIntegrityFields(header);
  ASSERT_FALSE(wrong.has_value());
  EXPECT_NE(wrong.error().message().find("Invalid V1 header size"), std::string::npos);
}

/**
 * @brief A table config written before cross_boundary_ngrams existed decodes
 *
 * That layout is one byte shorter. The byte is a serialized bool, so a value
 * above one identifies the shorter layout and belongs to the posting block
 * size that follows; posting.block_size is set here to a value whose least
 * significant octet is neither, which is what makes the two distinguishable.
 */
TEST(DumpFormatV1Test, ConfigWithoutCrossBoundaryNgramsDecodes) {
  Config write_config;
  write_config.mysql.host = "127.0.0.1";
  write_config.mysql.database = "corpus";

  TableConfig table;
  table.name = "articles";
  table.primary_key = "id";
  table.text_source.column = "body";
  table.ngram_size = 2;
  table.kanji_ngram_size = 3;
  table.posting.block_size = 0x1234FF;
  table.cross_boundary_ngrams = true;
  write_config.tables.push_back(table);

  std::ostringstream with_field;
  ASSERT_TRUE(SerializeConfig(with_field, write_config).has_value());

  // Locate the field by serializing the one value that differs: the two
  // buffers can only disagree at the byte that records it.
  write_config.tables[0].cross_boundary_ngrams = false;
  std::ostringstream without_field;
  ASSERT_TRUE(SerializeConfig(without_field, write_config).has_value());

  const std::string recorded = with_field.str();
  const std::string cleared = without_field.str();
  ASSERT_EQ(recorded.size(), cleared.size());
  size_t field_offset = std::string::npos;
  for (size_t i = 0; i < recorded.size(); ++i) {
    if (recorded[i] != cleared[i]) {
      ASSERT_EQ(field_offset, std::string::npos) << "more than one byte differs at " << i;
      field_offset = i;
    }
  }
  ASSERT_NE(field_offset, std::string::npos);

  std::string legacy = recorded;
  legacy.erase(field_offset, 1);

  Config read_config;
  std::istringstream input(legacy);
  auto result = DeserializeConfig(input, read_config);
  ASSERT_TRUE(result.has_value()) << result.error().message();

  ASSERT_EQ(read_config.tables.size(), 1u);
  EXPECT_EQ(read_config.tables[0].name, "articles");
  EXPECT_EQ(read_config.tables[0].ngram_size, 2);
  EXPECT_EQ(read_config.tables[0].kanji_ngram_size, 3);
  EXPECT_EQ(read_config.tables[0].posting.block_size, 0x1234FF);
  // The field is absent, so the running default stands.
  EXPECT_EQ(read_config.tables[0].cross_boundary_ngrams, TableConfig{}.cross_boundary_ngrams);
  EXPECT_EQ(read_config.mysql.host, "127.0.0.1");
  EXPECT_EQ(read_config.mysql.database, "corpus");
}

TEST(DumpFormatV1Test, ConfigRoundTripPreservesPerTableDatabase) {
  Config write_config;
  write_config.mysql.database = "default_db";
  write_config.memory.verify_text = "all";

  TableConfig live;
  live.name = "articles";
  live.database = "live_db";
  live.text_source.column = "body";
  write_config.tables.push_back(live);

  TableConfig archive = live;
  archive.database = "archive_db";
  write_config.tables.push_back(archive);

  std::ostringstream output;
  auto write_result = SerializeConfig(output, write_config);
  ASSERT_TRUE(write_result.has_value()) << write_result.error().message();
  auto metadata_result = SerializeCompatibilityMetadata(output, write_config);
  ASSERT_TRUE(metadata_result.has_value()) << metadata_result.error().message();

  Config read_config;
  std::istringstream input(output.str());
  auto read_result = DeserializeConfig(input, read_config);
  ASSERT_TRUE(read_result.has_value()) << read_result.error().message();
  auto metadata_read_result = DeserializeCompatibilityMetadata(input, read_config);
  ASSERT_TRUE(metadata_read_result.has_value()) << metadata_read_result.error().message();

  ASSERT_EQ(read_config.tables.size(), 2u);
  EXPECT_EQ(read_config.tables[0].name, "articles");
  EXPECT_EQ(read_config.tables[0].database, "live_db");
  EXPECT_EQ(read_config.tables[1].name, "articles");
  EXPECT_EQ(read_config.tables[1].database, "archive_db");
  EXPECT_EQ(read_config.memory.verify_text, "all");
}

TEST(DumpFormatV1Test, SerializeConfigRoundTripsEveryStringAtItsMaximumLength) {
  const auto sized = [](uint32_t length, char value) { return std::string(length, value); };
  const std::string identifier(config_limits::kMaxIdentifierLength, 'i');
  const std::string value(config_limits::kMaxConfigValueLength, 'v');
  const std::string path(config_limits::kMaxPathLength, 'p');

  Config write_config;
  write_config.mysql.host = sized(config_limits::kMaxConfigValueLength, 'h');
  write_config.mysql.database = sized(config_limits::kMaxIdentifierLength, 'd');
  write_config.mysql.binlog_format = sized(config_limits::kMaxIdentifierLength, 'f');
  write_config.mysql.binlog_row_image = sized(config_limits::kMaxIdentifierLength, 'r');

  TableConfig table;
  table.name = sized(config_limits::kMaxIdentifierLength, 'n');
  table.database = sized(config_limits::kMaxIdentifierLength, 'D');
  table.primary_key = sized(config_limits::kMaxIdentifierLength, 'k');
  table.text_source.column = sized(config_limits::kMaxIdentifierLength, 'c');
  table.text_source.concat = {sized(config_limits::kMaxIdentifierLength, 'a')};
  table.text_source.delimiter = sized(config_limits::kMaxIdentifierLength, 'l');
  table.required_filters.push_back(
      {sized(config_limits::kMaxIdentifierLength, 'N'), sized(config_limits::kMaxIdentifierLength, 'T'),
       sized(config_limits::kMaxIdentifierLength, 'O'), sized(config_limits::kMaxConfigValueLength, 'V')});
  table.filters.push_back({sized(config_limits::kMaxIdentifierLength, 'o'),
                           sized(config_limits::kMaxIdentifierLength, 't'), false, false,
                           sized(config_limits::kMaxIdentifierLength, 'b')});
  table.posting.use_roaring = sized(config_limits::kMaxIdentifierLength, 'u');
  write_config.tables.push_back(std::move(table));

  write_config.build.mode = sized(config_limits::kMaxIdentifierLength, 'm');
  write_config.replication.start_from = path;
  write_config.memory.normalize.width = sized(config_limits::kMaxIdentifierLength, 'w');
  write_config.dump.dir = sized(config_limits::kMaxPathLength, 'P');
  write_config.api.tcp.bind = sized(config_limits::kMaxConfigValueLength, 't');
  write_config.api.http.bind = sized(config_limits::kMaxConfigValueLength, 'H');
  write_config.network.allow_cidrs = {value};
  write_config.logging.level = sized(config_limits::kMaxIdentifierLength, 'L');
  write_config.logging.format = sized(config_limits::kMaxIdentifierLength, 'F');

  std::ostringstream output;
  const auto write_result = SerializeConfig(output, write_config);
  ASSERT_TRUE(write_result.has_value()) << write_result.error().message();

  Config read_config;
  std::istringstream input(output.str());
  const auto read_result = DeserializeConfig(input, read_config);
  ASSERT_TRUE(read_result.has_value()) << read_result.error().message();
  ASSERT_EQ(read_config.tables.size(), 1u);

  const auto expect_exact_limit = [](const char* field, const std::string& written, const std::string& loaded,
                                     size_t limit) {
    EXPECT_EQ(written.size(), limit) << field;
    EXPECT_EQ(loaded.size(), limit) << field;
    EXPECT_EQ(loaded, written) << field;
  };
  expect_exact_limit("mysql.host", write_config.mysql.host, read_config.mysql.host,
                     config_limits::kMaxConfigValueLength);
  expect_exact_limit("mysql.database", write_config.mysql.database, read_config.mysql.database,
                     config_limits::kMaxIdentifierLength);
  expect_exact_limit("mysql.binlog_format", write_config.mysql.binlog_format, read_config.mysql.binlog_format,
                     config_limits::kMaxIdentifierLength);
  expect_exact_limit("mysql.binlog_row_image", write_config.mysql.binlog_row_image, read_config.mysql.binlog_row_image,
                     config_limits::kMaxIdentifierLength);

  const auto& written_table = write_config.tables.front();
  const auto& loaded_table = read_config.tables.front();
  expect_exact_limit("tables[0].name", written_table.name, loaded_table.name, config_limits::kMaxIdentifierLength);
  expect_exact_limit("tables[0].database", written_table.database, loaded_table.database,
                     config_limits::kMaxIdentifierLength);
  expect_exact_limit("tables[0].primary_key", written_table.primary_key, loaded_table.primary_key,
                     config_limits::kMaxIdentifierLength);
  expect_exact_limit("tables[0].text_source.column", written_table.text_source.column, loaded_table.text_source.column,
                     config_limits::kMaxIdentifierLength);
  ASSERT_EQ(loaded_table.text_source.concat.size(), written_table.text_source.concat.size());
  ASSERT_EQ(loaded_table.text_source.concat.size(), 1u);
  expect_exact_limit("tables[0].text_source.concat[0]", written_table.text_source.concat[0],
                     loaded_table.text_source.concat[0], config_limits::kMaxIdentifierLength);
  expect_exact_limit("tables[0].text_source.delimiter", written_table.text_source.delimiter,
                     loaded_table.text_source.delimiter, config_limits::kMaxIdentifierLength);
  ASSERT_EQ(loaded_table.required_filters.size(), 1u);
  expect_exact_limit("tables[0].required_filters[0].name", written_table.required_filters[0].name,
                     loaded_table.required_filters[0].name, config_limits::kMaxIdentifierLength);
  expect_exact_limit("tables[0].required_filters[0].type", written_table.required_filters[0].type,
                     loaded_table.required_filters[0].type, config_limits::kMaxIdentifierLength);
  expect_exact_limit("tables[0].required_filters[0].op", written_table.required_filters[0].op,
                     loaded_table.required_filters[0].op, config_limits::kMaxIdentifierLength);
  expect_exact_limit("tables[0].required_filters[0].value", written_table.required_filters[0].value,
                     loaded_table.required_filters[0].value, config_limits::kMaxConfigValueLength);
  ASSERT_EQ(loaded_table.filters.size(), 1u);
  expect_exact_limit("tables[0].filters[0].name", written_table.filters[0].name, loaded_table.filters[0].name,
                     config_limits::kMaxIdentifierLength);
  expect_exact_limit("tables[0].filters[0].type", written_table.filters[0].type, loaded_table.filters[0].type,
                     config_limits::kMaxIdentifierLength);
  expect_exact_limit("tables[0].filters[0].bucket", written_table.filters[0].bucket, loaded_table.filters[0].bucket,
                     config_limits::kMaxIdentifierLength);
  expect_exact_limit("tables[0].posting.use_roaring", written_table.posting.use_roaring,
                     loaded_table.posting.use_roaring, config_limits::kMaxIdentifierLength);

  expect_exact_limit("build.mode", write_config.build.mode, read_config.build.mode,
                     config_limits::kMaxIdentifierLength);
  expect_exact_limit("replication.start_from", write_config.replication.start_from, read_config.replication.start_from,
                     config_limits::kMaxPathLength);
  expect_exact_limit("memory.normalize.width", write_config.memory.normalize.width, read_config.memory.normalize.width,
                     config_limits::kMaxIdentifierLength);
  expect_exact_limit("dump.dir", write_config.dump.dir, read_config.dump.dir, config_limits::kMaxPathLength);
  expect_exact_limit("api.tcp.bind", write_config.api.tcp.bind, read_config.api.tcp.bind,
                     config_limits::kMaxConfigValueLength);
  expect_exact_limit("api.http.bind", write_config.api.http.bind, read_config.api.http.bind,
                     config_limits::kMaxConfigValueLength);
  ASSERT_EQ(read_config.network.allow_cidrs.size(), 1u);
  expect_exact_limit("network.allow_cidrs[0]", write_config.network.allow_cidrs[0], read_config.network.allow_cidrs[0],
                     config_limits::kMaxConfigValueLength);
  expect_exact_limit("logging.level", write_config.logging.level, read_config.logging.level,
                     config_limits::kMaxIdentifierLength);
  expect_exact_limit("logging.format", write_config.logging.format, read_config.logging.format,
                     config_limits::kMaxIdentifierLength);

  Config metadata_config;
  metadata_config.memory.verify_text = identifier;
  const std::string source_uuid(config_limits::kMaxConfigValueLength, 's');
  std::ostringstream metadata_output;
  const auto metadata_write_result = SerializeCompatibilityMetadata(metadata_output, metadata_config, source_uuid);
  ASSERT_TRUE(metadata_write_result.has_value()) << metadata_write_result.error().message();

  Config loaded_metadata;
  mygramdb::storage::DumpSourceIdentity source_identity;
  std::istringstream metadata_input(metadata_output.str());
  const auto metadata_read_result = DeserializeCompatibilityMetadata(metadata_input, loaded_metadata, &source_identity);
  ASSERT_TRUE(metadata_read_result.has_value()) << metadata_read_result.error().message();
  expect_exact_limit("memory.verify_text", metadata_config.memory.verify_text, loaded_metadata.memory.verify_text,
                     config_limits::kMaxIdentifierLength);
  ASSERT_TRUE(source_identity.recorded);
  expect_exact_limit("source_server_uuid", source_uuid, source_identity.uuid, config_limits::kMaxConfigValueLength);
}

TEST(DumpFormatV1Test, SerializeConfigRoundTripsEveryCountAtItsMaximum) {
  Config write_config;
  write_config.mysql.database = "default-db";
  write_config.tables.resize(config_limits::kMaxTableCount);
  for (size_t i = 0; i < write_config.tables.size(); ++i) {
    write_config.tables[i].name = "table-" + std::to_string(i);
    write_config.tables[i].database = "database-" + std::to_string(i);
  }

  TableConfig& counted_table = write_config.tables.front();
  counted_table.text_source.concat.reserve(config_limits::kMaxConcatColumnCount);
  for (uint32_t i = 0; i < config_limits::kMaxConcatColumnCount; ++i) {
    counted_table.text_source.concat.push_back("column-" + std::to_string(i));
  }
  counted_table.required_filters.reserve(config_limits::kMaxRequiredFilterCount);
  for (uint32_t i = 0; i < config_limits::kMaxRequiredFilterCount; ++i) {
    counted_table.required_filters.push_back(
        {"required-" + std::to_string(i), "string", "=", "value-" + std::to_string(i), (i % 2) == 0});
  }
  counted_table.filters.reserve(config_limits::kMaxOptionalFilterCount);
  for (uint32_t i = 0; i < config_limits::kMaxOptionalFilterCount; ++i) {
    counted_table.filters.push_back(
        {"optional-" + std::to_string(i), "string", (i % 2) == 0, (i % 3) == 0, "bucket-" + std::to_string(i)});
  }
  write_config.network.allow_cidrs.reserve(config_limits::kMaxCidrCount);
  for (uint32_t i = 0; i < config_limits::kMaxCidrCount; ++i) {
    write_config.network.allow_cidrs.push_back("cidr-" + std::to_string(i));
  }

  std::ostringstream output;
  const auto write_result = SerializeConfig(output, write_config);
  ASSERT_TRUE(write_result.has_value()) << write_result.error().message();

  Config read_config;
  std::istringstream input(output.str());
  const auto read_result = DeserializeConfig(input, read_config);
  ASSERT_TRUE(read_result.has_value()) << read_result.error().message();

  ASSERT_EQ(read_config.tables.size(), config_limits::kMaxTableCount);
  EXPECT_TRUE(std::equal(write_config.tables.begin(), write_config.tables.end(), read_config.tables.begin(),
                         [](const TableConfig& written, const TableConfig& loaded) {
                           return written.name == loaded.name && written.database == loaded.database;
                         }));

  ASSERT_EQ(read_config.tables.front().text_source.concat.size(), config_limits::kMaxConcatColumnCount);
  EXPECT_TRUE(std::equal(counted_table.text_source.concat.begin(), counted_table.text_source.concat.end(),
                         read_config.tables.front().text_source.concat.begin()));

  const auto& loaded_table = read_config.tables.front();
  ASSERT_EQ(loaded_table.required_filters.size(), config_limits::kMaxRequiredFilterCount);
  EXPECT_TRUE(std::equal(counted_table.required_filters.begin(), counted_table.required_filters.end(),
                         loaded_table.required_filters.begin(), [](const auto& written, const auto& loaded) {
                           return written.name == loaded.name && written.type == loaded.type &&
                                  written.op == loaded.op && written.value == loaded.value &&
                                  written.bitmap_index == loaded.bitmap_index;
                         }));
  ASSERT_EQ(loaded_table.filters.size(), config_limits::kMaxOptionalFilterCount);
  EXPECT_TRUE(std::equal(counted_table.filters.begin(), counted_table.filters.end(), loaded_table.filters.begin(),
                         [](const auto& written, const auto& loaded) {
                           return written.name == loaded.name && written.type == loaded.type &&
                                  written.dict_compress == loaded.dict_compress &&
                                  written.bitmap_index == loaded.bitmap_index && written.bucket == loaded.bucket;
                         }));

  ASSERT_EQ(read_config.network.allow_cidrs.size(), config_limits::kMaxCidrCount);
  EXPECT_TRUE(std::equal(write_config.network.allow_cidrs.begin(), write_config.network.allow_cidrs.end(),
                         read_config.network.allow_cidrs.begin()));
}

TEST(DumpFormatV1Test, SerializeConfigPreflightsEverySerializedStringBeforeWriting) {
  using Mutation = std::pair<std::string, std::function<void(Config&)>>;
  const std::vector<Mutation> oversized_fields = {
      {"mysql.host", [](Config& config) { config.mysql.host.assign(kMaxConfigValueLength + 1, 'x'); }},
      {"mysql.database", [](Config& config) { config.mysql.database.assign(kMaxIdentifierLength + 1, 'x'); }},
      {"mysql.binlog_format", [](Config& config) { config.mysql.binlog_format.assign(kMaxIdentifierLength + 1, 'x'); }},
      {"mysql.binlog_row_image",
       [](Config& config) { config.mysql.binlog_row_image.assign(kMaxIdentifierLength + 1, 'x'); }},
      {"tables[0].name", [](Config& config) { config.tables[0].name.assign(kMaxIdentifierLength + 1, 'x'); }},
      {"tables[0].database", [](Config& config) { config.tables[0].database.assign(kMaxIdentifierLength + 1, 'x'); }},
      {"tables[0].primary_key",
       [](Config& config) { config.tables[0].primary_key.assign(kMaxIdentifierLength + 1, 'x'); }},
      {"tables[0].text_source.column",
       [](Config& config) { config.tables[0].text_source.column.assign(kMaxIdentifierLength + 1, 'x'); }},
      {"tables[0].text_source.concat[0]",
       [](Config& config) {
         config.tables[0].text_source.concat.push_back(std::string(kMaxIdentifierLength + 1, 'x'));
       }},
      {"tables[0].text_source.delimiter",
       [](Config& config) { config.tables[0].text_source.delimiter.assign(kMaxIdentifierLength + 1, 'x'); }},
      {"tables[0].required_filters[0].name",
       [](Config& config) {
         config.tables[0].required_filters.push_back({std::string(kMaxIdentifierLength + 1, 'x'), "string", "=", ""});
       }},
      {"tables[0].required_filters[0].type",
       [](Config& config) {
         config.tables[0].required_filters.push_back({"name", std::string(kMaxIdentifierLength + 1, 'x'), "=", ""});
       }},
      {"tables[0].required_filters[0].op",
       [](Config& config) {
         config.tables[0].required_filters.push_back(
             {"name", "string", std::string(kMaxIdentifierLength + 1, 'x'), ""});
       }},
      {"tables[0].required_filters[0].value",
       [](Config& config) {
         config.tables[0].required_filters.push_back(
             {"name", "string", "=", std::string(kMaxConfigValueLength + 1, 'x')});
       }},
      {"tables[0].filters[0].name",
       [](Config& config) {
         config.tables[0].filters.push_back({std::string(kMaxIdentifierLength + 1, 'x'), "string"});
       }},
      {"tables[0].filters[0].type",
       [](Config& config) {
         config.tables[0].filters.push_back({"name", std::string(kMaxIdentifierLength + 1, 'x')});
       }},
      {"tables[0].filters[0].bucket",
       [](Config& config) {
         config.tables[0].filters.push_back(
             {"name", "string", false, false, std::string(kMaxIdentifierLength + 1, 'x')});
       }},
      {"tables[0].posting.use_roaring",
       [](Config& config) { config.tables[0].posting.use_roaring.assign(kMaxIdentifierLength + 1, 'x'); }},
      {"build.mode", [](Config& config) { config.build.mode.assign(kMaxIdentifierLength + 1, 'x'); }},
      {"replication.start_from", [](Config& config) { config.replication.start_from.assign(kMaxPathLength + 1, 'x'); }},
      {"memory.normalize.width",
       [](Config& config) { config.memory.normalize.width.assign(kMaxIdentifierLength + 1, 'x'); }},
      {"dump.dir", [](Config& config) { config.dump.dir.assign(kMaxPathLength + 1, 'x'); }},
      {"api.tcp.bind", [](Config& config) { config.api.tcp.bind.assign(kMaxConfigValueLength + 1, 'x'); }},
      {"api.http.bind", [](Config& config) { config.api.http.bind.assign(kMaxConfigValueLength + 1, 'x'); }},
      {"network.allow_cidrs[0]",
       [](Config& config) { config.network.allow_cidrs.push_back(std::string(kMaxConfigValueLength + 1, 'x')); }},
      {"logging.level", [](Config& config) { config.logging.level.assign(kMaxIdentifierLength + 1, 'x'); }},
      {"logging.format", [](Config& config) { config.logging.format.assign(kMaxIdentifierLength + 1, 'x'); }},
  };

  for (const auto& [field, mutate] : oversized_fields) {
    Config config;
    TableConfig table;
    table.name = "articles";
    config.tables.push_back(std::move(table));
    mutate(config);

    std::ostringstream output;
    const auto result = SerializeConfig(output, config);
    ASSERT_FALSE(result.has_value()) << field;
    EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageDumpWriteError) << field;
    EXPECT_NE(result.error().message().find(field), std::string::npos) << result.error().message();
    EXPECT_TRUE(output.str().empty()) << field << " must be rejected before serialization starts";
  }
}

TEST(DumpFormatV1Test, SerializeConfigPreflightsEachCountBeforeWriting) {
  using Mutation = std::pair<std::string, std::function<void(Config&)>>;
  const std::vector<Mutation> oversized_counts = {
      {"tables", [](Config& config) { config.tables.resize(dump_format_limits::config::kMaxTableCount + 1); }},
      {"text_source.concat",
       [](Config& config) {
         config.tables.resize(1);
         config.tables[0].text_source.concat.resize(dump_format_limits::config::kMaxConcatColumnCount + 1);
       }},
      {"required_filters",
       [](Config& config) {
         config.tables.resize(1);
         config.tables[0].required_filters.resize(dump_format_limits::config::kMaxRequiredFilterCount + 1);
       }},
      {"filters",
       [](Config& config) {
         config.tables.resize(1);
         config.tables[0].filters.resize(dump_format_limits::config::kMaxOptionalFilterCount + 1);
       }},
      {"network.allow_cidrs",
       [](Config& config) { config.network.allow_cidrs.resize(dump_format_limits::config::kMaxCidrCount + 1); }},
  };

  for (const auto& [field, mutate] : oversized_counts) {
    Config config;
    mutate(config);
    std::ostringstream output;
    const auto result = SerializeConfig(output, config);
    ASSERT_FALSE(result.has_value()) << field;
    EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageDumpWriteError) << field;
    EXPECT_NE(result.error().message().find(field), std::string::npos) << result.error().message();
    EXPECT_TRUE(output.str().empty()) << field << " must be rejected before serialization starts";
  }
}

TEST(DumpFormatV1Test, SerializeCompatibilityMetadataPreflightsStringsBeforeWriting) {
  Config config;
  config.memory.verify_text.assign(kMaxIdentifierLength + 1, 'x');
  std::ostringstream verify_text_output;
  const auto verify_text_result = SerializeCompatibilityMetadata(verify_text_output, config);
  ASSERT_FALSE(verify_text_result.has_value());
  EXPECT_NE(verify_text_result.error().message().find("memory.verify_text"), std::string::npos);
  EXPECT_TRUE(verify_text_output.str().empty());

  config.memory.verify_text = "off";
  const std::string long_uuid(kMaxConfigValueLength + 1, 'x');
  std::ostringstream uuid_output;
  const auto uuid_result = SerializeCompatibilityMetadata(uuid_output, config, long_uuid);
  ASSERT_FALSE(uuid_result.has_value());
  EXPECT_NE(uuid_result.error().message().find("source_server_uuid"), std::string::npos);
  EXPECT_TRUE(uuid_output.str().empty());
}

TEST(DumpFormatV1Test, SerializeConfigDoesNotBoundUnserializedSecretsAndOptions) {
  Config config;
  config.mysql.user.assign(kMaxConfigValueLength + 1, 'u');
  config.mysql.password.assign(kMaxConfigValueLength + 1, 'p');
  config.logging.file.assign(kMaxPathLength + 1, 'l');
  config.api.admin_token.assign(kMaxConfigValueLength + 1, 't');
  std::ostringstream output;

  const auto result = SerializeConfig(output, config);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_FALSE(output.str().empty());
}

TEST(DumpFormatV1Test, WriteRejectsCombinedConfigAboveV1LimitWithoutReplacingTarget) {
  const auto path = std::filesystem::temp_directory_path() / "mygramdb_v1_config_section_over_limit.dmp";
  std::filesystem::create_directories(path.parent_path());
  const auto filepath = path.string();
  const std::string previous_contents = "previous-good-dump";
  {
    std::ofstream output(filepath, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output);
    output.write(previous_contents.data(), static_cast<std::streamsize>(previous_contents.size()));
    ASSERT_TRUE(output.good());
  }

  Config config;
  config.mysql.database = "testdb";
  config.network.allow_cidrs.assign(4097, std::string(kMaxConfigValueLength, 'x'));
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;
  const auto result = WriteDumpV1(filepath, "GTID:1", config, contexts);

  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("combined config"), std::string::npos);
  std::ifstream input(filepath, std::ios::binary);
  ASSERT_TRUE(input);
  const std::string final_contents{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
  EXPECT_EQ(final_contents, previous_contents);
  std::filesystem::remove(path);
}

TEST(DumpFormatV1Test, WriteRejectsOverLengthTableContextNameWithoutReplacingTarget) {
  const auto path = std::filesystem::temp_directory_path() / "mygramdb_v1_table_context_name_ceiling.dmp";
  const std::string previous_contents = "previous-good-dump";
  {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output);
    output.write(previous_contents.data(), static_cast<std::streamsize>(previous_contents.size()));
    ASSERT_TRUE(output.good());
  }

  Config config;
  config.mysql.database = "testdb";
  mygramdb::index::Index index;
  mygramdb::storage::DocumentStore store;
  const std::string table_name(dump_format_limits::container::kMaxTableContextNameLength + 1, 't');
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts{
      {table_name, {&index, &store}}};

  const auto result = WriteDumpV1(path.string(), "GTID:1", config, contexts);
  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("Table context name"), std::string::npos);
  std::ifstream input(path, std::ios::binary);
  ASSERT_TRUE(input);
  const std::string final_contents{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
  EXPECT_EQ(final_contents, previous_contents);
  std::filesystem::remove(path);
}

// ============================================================================
// Config section bounds check
// ============================================================================

/**
 * @brief Test that ReadHeaderV1 fails on truncated stream
 */
TEST(DumpFormatV1Test, ReadHeaderV1FailsOnTruncatedStream) {
  // Write only partial header data (just header_size field)
  std::ostringstream oss;
  uint32_t partial = 32;
  WriteBinary(oss, partial);
  // No more data - stream is truncated

  std::istringstream iss(oss.str());
  HeaderV1 header;
  auto result = ReadHeaderV1(iss, header);
  EXPECT_FALSE(result.has_value()) << "ReadHeaderV1 should fail on truncated stream";
}

/**
 * @brief Every GTID WriteHeaderV1 emits reads back
 *
 * A multi-source position can exceed the path-field bound, and a legacy dump
 * carrying one must stay loadable.
 */
TEST(DumpFormatV1Test, HeaderRoundTripsGtidLongerThanPathLimit) {
  HeaderV1 header;
  header.gtid = std::string(kMaxPathLength + 1024, 'a');
  header.header_size = static_cast<uint32_t>(4 + 4 + 8 + 8 + 4 + 4 + header.gtid.size());

  std::ostringstream oss;
  auto write_result = WriteHeaderV1(oss, header);
  ASSERT_TRUE(write_result.has_value()) << write_result.error().message();

  std::istringstream iss(oss.str());
  HeaderV1 read_header;
  auto read_result = ReadHeaderV1(iss, read_header);
  ASSERT_TRUE(read_result.has_value()) << read_result.error().message();
  EXPECT_EQ(read_header.gtid, header.gtid);
}

/**
 * @brief WriteHeaderV1 rejects a GTID the reader could never accept
 */
TEST(DumpFormatV1Test, WriteHeaderRejectsOverLengthGtidWithoutEmittingBytes) {
  HeaderV1 header;
  header.gtid = std::string(kMaxGtidLength + 1, 'a');
  header.header_size = static_cast<uint32_t>(4 + 4 + 8 + 8 + 4 + 4 + header.gtid.size());

  std::ostringstream oss;
  auto write_result = WriteHeaderV1(oss, header);
  ASSERT_FALSE(write_result.has_value());
  EXPECT_EQ(write_result.error().code(), mygram::utils::ErrorCode::kStorageDumpWriteError);
  EXPECT_NE(write_result.error().message().find("GTID"), std::string::npos);
  EXPECT_NE(write_result.error().message().find(std::to_string(kMaxGtidLength)), std::string::npos);
  EXPECT_TRUE(oss.str().empty());
}

// ============================================================================
// VerifyDumpIntegrity with corrupted header
// ============================================================================

namespace {

/// Create a temporary file path for testing
std::string TestTempFilePath(const std::string& name) {
  auto dir = std::filesystem::temp_directory_path() / "mygramdb_v1_test";
  std::filesystem::create_directories(dir);
  return (dir / (name + ".dmp")).string();
}

/// Clean up a test file
void CleanupTestFile(const std::string& path) {
  std::filesystem::remove(path);
}

void WriteMinimalDumpV1(const std::string& filepath) {
  mygramdb::config::Config config;
  config.mysql.database = "testdb";
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;

  auto result = WriteDumpV1(filepath, "GTID:1", config, contexts);
  ASSERT_TRUE(result.has_value()) << result.error().message();
}

}  // namespace

/**
 * @brief Test that VerifyDumpIntegrity propagates ReadHeaderV1 error details
 */
TEST(DumpFormatV1Test, VerifyDumpIntegrityCorruptedHeader) {
  auto filepath = TestTempFilePath("corrupted_header");
  CleanupTestFile(filepath);

  // Write a file with valid magic and version but corrupted/truncated V1 header
  {
    std::ofstream ofs(filepath, std::ios::binary);
    ASSERT_TRUE(ofs.good());
    // Write valid magic number
    ofs.write(mygramdb::storage::dump_format::kMagicNumber.data(), 4);
    // Write valid version (V1 = 1)
    uint32_t version = 1;
    WriteBinary(ofs, version);
    // Write truncated header - just a header_size field with no following data
    uint32_t header_size = 100;  // Claims 100 bytes but file ends here
    WriteBinary(ofs, header_size);
  }

  mygramdb::storage::dump_format::IntegrityError error;
  auto result = VerifyDumpIntegrity(filepath, error);
  EXPECT_FALSE(result.has_value()) << "Should fail with corrupted header";
  EXPECT_EQ(error.type, mygramdb::storage::dump_format::CRCErrorType::FileCRC);
  // Error message should contain details from ReadHeaderV1, not just a generic message
  EXPECT_TRUE(error.message.find("Failed to read V1 header:") != std::string::npos)
      << "Error message should propagate ReadHeaderV1 details, got: " << error.message;

  CleanupTestFile(filepath);
}

/**
 * @brief Test that VerifyDumpIntegrity fails on non-existent file
 */
TEST(DumpFormatV1Test, VerifyDumpIntegrityNonExistentFile) {
  mygramdb::storage::dump_format::IntegrityError error;
  auto result = VerifyDumpIntegrity("/tmp/nonexistent_file_12345.dmp", error);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(error.type, mygramdb::storage::dump_format::CRCErrorType::FileCRC);
}

/**
 * @brief Test that VerifyDumpIntegrity fails on invalid magic number
 */
TEST(DumpFormatV1Test, VerifyDumpIntegrityInvalidMagic) {
  auto filepath = TestTempFilePath("invalid_magic");
  CleanupTestFile(filepath);

  {
    std::ofstream ofs(filepath, std::ios::binary);
    ASSERT_TRUE(ofs.good());
    ofs.write("XXXX", 4);  // Invalid magic
    uint32_t version = 1;
    WriteBinary(ofs, version);
  }

  mygramdb::storage::dump_format::IntegrityError error;
  auto result = VerifyDumpIntegrity(filepath, error);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(error.type, mygramdb::storage::dump_format::CRCErrorType::FileCRC);

  CleanupTestFile(filepath);
}

TEST(DumpFormatV1Test, RejectsZeroTotalFileSizeHeader) {
  auto filepath = TestTempFilePath("zero_total_file_size");
  CleanupTestFile(filepath);
  WriteMinimalDumpV1(filepath);

  {
    std::fstream fs(filepath, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(fs.good());
    fs.seekp(kHeaderTotalFileSizeOffset);
    uint64_t zero = 0;
    ASSERT_TRUE(WriteBinary(fs, zero));
  }

  mygramdb::storage::dump_format::IntegrityError verify_error;
  auto verify_result = VerifyDumpIntegrity(filepath, verify_error);
  EXPECT_FALSE(verify_result.has_value());
  EXPECT_EQ(verify_error.type, mygramdb::storage::dump_format::CRCErrorType::FileCRC);
  EXPECT_NE(verify_error.message.find("total_file_size is zero"), std::string::npos);

  std::string gtid = "unchanged";
  mygramdb::config::Config config;
  mygramdb::storage::dump_format::IntegrityError read_error;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;
  auto read_result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, &read_error);
  EXPECT_FALSE(read_result.has_value());
  EXPECT_EQ(read_error.type, mygramdb::storage::dump_format::CRCErrorType::FileCRC);
  EXPECT_NE(read_error.message.find("total_file_size is zero"), std::string::npos);
  EXPECT_EQ(gtid, "unchanged");

  CleanupTestFile(filepath);
}

TEST(DumpFormatV1Test, RejectsZeroFileCrcHeader) {
  auto filepath = TestTempFilePath("zero_file_crc");
  CleanupTestFile(filepath);
  WriteMinimalDumpV1(filepath);

  {
    std::fstream fs(filepath, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(fs.good());
    fs.seekp(kHeaderFileCRC32Offset);
    uint32_t zero = 0;
    ASSERT_TRUE(WriteBinary(fs, zero));
  }

  mygramdb::storage::dump_format::IntegrityError verify_error;
  auto verify_result = VerifyDumpIntegrity(filepath, verify_error);
  EXPECT_FALSE(verify_result.has_value());
  EXPECT_EQ(verify_error.type, mygramdb::storage::dump_format::CRCErrorType::FileCRC);
  EXPECT_NE(verify_error.message.find("CRC32 checksum mismatch"), std::string::npos);

  std::string gtid = "unchanged";
  mygramdb::config::Config config;
  mygramdb::storage::dump_format::IntegrityError read_error;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;
  auto read_result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, &read_error);
  EXPECT_FALSE(read_result.has_value());
  EXPECT_EQ(read_error.type, mygramdb::storage::dump_format::CRCErrorType::FileCRC);
  EXPECT_NE(read_error.message.find("CRC32 checksum mismatch"), std::string::npos);
  EXPECT_EQ(gtid, "unchanged");

  CleanupTestFile(filepath);
}

// ============================================================================
// ReadDumpV1 error code validation
// ============================================================================

/**
 * @brief Test that ReadDumpV1 returns kStorageVersionMismatch for version too new
 *
 * Validates that ReadDumpV1 returns a
 * kStorageVersionMismatch error code when the dump file has a version
 * number higher than the maximum supported version.
 */
TEST(DumpFormatV1Test, ReadDumpVersionTooNewReturnsVersionMismatch) {
  auto filepath = TestTempFilePath("version_too_new");
  CleanupTestFile(filepath);

  {
    std::ofstream ofs(filepath, std::ios::binary);
    ASSERT_TRUE(ofs.good());
    // Write valid magic number
    ofs.write(mygramdb::storage::dump_format::kMagicNumber.data(), 4);
    // Write version 999 (far beyond max supported)
    uint32_t version = 999;
    WriteBinary(ofs, version);
  }

  std::string gtid;
  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;

  auto result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, nullptr);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageVersionMismatch);
  // Error message should indicate version is too new
  EXPECT_NE(result.error().message().find("too new"), std::string::npos)
      << "Error message should mention 'too new', got: " << result.error().message();
  // Context should contain the filepath
  EXPECT_FALSE(result.error().context().empty());

  CleanupTestFile(filepath);
}

/**
 * @brief Test that ReadDumpV1 returns kStorageDumpReadError for invalid magic
 *
 * Validates that reading a file with wrong magic bytes returns the correct
 * error code with filepath context.
 */
TEST(DumpFormatV1Test, ReadDumpInvalidMagicReturnsDumpReadError) {
  auto filepath = TestTempFilePath("bad_magic");
  CleanupTestFile(filepath);

  {
    std::ofstream ofs(filepath, std::ios::binary);
    ASSERT_TRUE(ofs.good());
    ofs.write("XXXX", 4);  // Invalid magic
    uint32_t version = 1;
    WriteBinary(ofs, version);
  }

  std::string gtid;
  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;

  auto result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, nullptr);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageDumpReadError);
  // Error message should mention magic number
  EXPECT_NE(result.error().message().find("magic"), std::string::npos)
      << "Error message should mention 'magic', got: " << result.error().message();
  // Context should include the filepath
  EXPECT_FALSE(result.error().context().empty());

  CleanupTestFile(filepath);
}

/**
 * @brief Test that ReadDumpV1 returns kStorageDumpReadError for non-existent file
 */
TEST(DumpFormatV1Test, ReadDumpNonExistentFileReturnsReadError) {
  std::string filepath = "/tmp/mygramdb_nonexistent_dump_test_file.dmp";
  std::filesystem::remove(filepath);  // Ensure it doesn't exist

  std::string gtid;
  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;

  auto result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, nullptr);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageDumpReadError);
  EXPECT_FALSE(result.error().context().empty());
}

/**
 * @brief Test that ReadDumpV1 returns kStorageDumpReadError for truncated file
 *
 * A file that has only the magic number but no version should fail.
 */
TEST(DumpFormatV1Test, ReadDumpTruncatedFileReturnsReadError) {
  auto filepath = TestTempFilePath("truncated");
  CleanupTestFile(filepath);

  {
    std::ofstream ofs(filepath, std::ios::binary);
    ASSERT_TRUE(ofs.good());
    // Write only magic, no version
    ofs.write(mygramdb::storage::dump_format::kMagicNumber.data(), 4);
  }

  std::string gtid;
  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;

  auto result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, nullptr);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageDumpReadError);

  CleanupTestFile(filepath);
}

TEST(DumpFormatV1Test, ReadDumpDoesNotPublishGtidOnCrcFailure) {
  auto filepath = TestTempFilePath("crc_failure_gtid");
  CleanupTestFile(filepath);
  WriteMinimalDumpV1(filepath);

  {
    std::fstream fs(filepath, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(fs.good());
    fs.seekg(0, std::ios::end);
    const auto file_size = fs.tellg();
    ASSERT_GT(file_size, std::streampos{kHeaderFileCRC32Offset + static_cast<std::streamoff>(sizeof(uint32_t))});
    fs.seekg(file_size - std::streamoff{1});
    char byte = '\0';
    fs.read(&byte, 1);
    ASSERT_TRUE(fs.good());
    fs.seekp(file_size - std::streamoff{1});
    byte ^= 0x01;
    fs.write(&byte, 1);
    ASSERT_TRUE(fs.good());
  }

  std::string gtid = "unchanged";
  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;
  auto result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, nullptr);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageDumpReadError);
  EXPECT_EQ(gtid, "unchanged");

  CleanupTestFile(filepath);
}

TEST(DumpFormatV1Test, ReadDumpRejectsTruncatedDeclaredConfigSection) {
  auto filepath = TestTempFilePath("truncated_config_section");
  CleanupTestFile(filepath);

  {
    std::ofstream ofs(filepath, std::ios::binary);
    ASSERT_TRUE(ofs.good());
    ofs.write(mygramdb::storage::dump_format::kMagicNumber.data(), 4);
    uint32_t version = static_cast<uint32_t>(mygramdb::storage::dump_format::FormatVersion::V1);
    WriteBinary(ofs, version);

    HeaderV1 header;
    header.header_size = 32;
    header.flags = 0;
    header.dump_timestamp = 1;
    header.total_file_size = 0;
    header.file_crc32 = 0;
    header.gtid = "GTID:truncated-config";
    ASSERT_TRUE(WriteHeaderV1(ofs, header).has_value());

    uint32_t config_len = 4;
    WriteBinary(ofs, config_len);
    ofs.write("xx", 2);
  }

  std::string gtid = "unchanged";
  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;
  auto result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, nullptr);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageDumpReadError);
  EXPECT_EQ(gtid, "unchanged");

  CleanupTestFile(filepath);
}
