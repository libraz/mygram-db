/**
 * @file column_type_support_test.cpp
 * @brief The column type table is what all three type decisions read
 *
 * Whether a MySQL type can back a configured column used to be answered
 * separately by configuration-time validation, by the initial-load path and by
 * the row decoder's case labels. These tests hold the table to the behaviour of
 * the code that reads it: a type it calls decodable has to decode, a type it
 * accepts has to be usable on both paths, and a type it rejects has to be
 * refused before replication starts rather than during it.
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#ifdef USE_MYSQL

#include "mysql/binlog_util.h"
#include "mysql/column_type_support.h"
#include "mysql/ddl_schema_validator.h"
#include "mysql/rows_parser_internal.h"

namespace mygramdb::mysql {
namespace {

using internal::DecodeFieldValue;

/// Every value of ColumnType, as it appears in a TABLE_MAP or a MYSQL_FIELD.
constexpr uint8_t kEnumeratedTypeCodes[] = {1,  2,  3,  4,   5,   7,   8,   9,   10,  11,  12,  13,  14,  15,  16,
                                            17, 18, 19, 242, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255};

/// Metadata words wide enough that every decodable type has a usable one.
constexpr uint16_t kSampleMetadata[] = {0, 1, 2, 3, 4, 0x0A02, 0xFE28};

bool IsEnumerated(uint8_t code) {
  for (const uint8_t enumerated : kEnumeratedTypeCodes) {
    if (enumerated == code) {
      return true;
    }
  }
  return false;
}

/**
 * @brief Whether the row decoder reaches a case for a type code.
 *
 * A decodable type may still refuse a particular buffer as truncated or its
 * metadata as impossible. Only the unsupported-type verdict means no case
 * exists.
 */
bool RowDecoderHasACase(uint8_t code, uint16_t metadata) {
  const std::vector<unsigned char> buffer(16, 0);
  const std::vector<std::string> labels = {"a", "b"};
  auto decoded = DecodeFieldValue(code, buffer.data(), metadata, false, buffer.data() + buffer.size(), false, &labels);
  return decoded.has_value() || decoded.error().code() != mygram::utils::ErrorCode::kMySQLUnsupportedType;
}

config::TableConfig MakeConfig() {
  config::TableConfig config;
  config.database = "testdb";
  config.name = "articles";
  config.primary_key = "id";
  config.text_source.column = "content";
  return config;
}

std::vector<DDLColumnMetadata> MakeColumns() {
  return {
      {"id", "bigint unsigned", "", false, "PRI"},
      {"content", "longtext", "utf8mb4_0900_ai_ci", true, ""},
  };
}

/// Validate a configured table whose text source has the given declared type.
mygram::utils::Expected<ConfiguredTableSchema, mygram::utils::Error> ValidateTextColumnType(
    const std::string& column_type, const std::string& collation = "utf8mb4_0900_ai_ci") {
  auto columns = MakeColumns();
  columns[1].column_type = column_type;
  columns[1].collation = collation;
  return DDLSchemaValidator::ValidateMetadata(MakeConfig(), columns, true);
}

// ===========================================================================
// The table against the decoder
// ===========================================================================

/**
 * @brief A type the table calls decodable has a case in the row decoder.
 *
 * The table is what configuration-time validation trusts when it lets a column
 * through. A type it calls decodable that the decoder has never heard of stops
 * replication on the first row that contains it.
 */
TEST(ColumnTypeSupportTest, EveryDecodableTypeIsDecodedByTheRowDecoder) {
  for (const uint8_t code : kEnumeratedTypeCodes) {
    const auto type = static_cast<ColumnType>(code);
    if (DescribeColumnType(type).binlog_decoding != BinlogDecoding::kDecoded) {
      continue;
    }
    bool decoded_somewhere = false;
    for (const uint16_t metadata : kSampleMetadata) {
      decoded_somewhere = decoded_somewhere || RowDecoderHasACase(code, metadata);
    }
    EXPECT_TRUE(decoded_somewhere) << "column type " << static_cast<int>(code) << " is called decodable";
  }
}

/**
 * @brief A type the table does not call decodable is refused by the decoder.
 */
TEST(ColumnTypeSupportTest, TypesOutsideTheDecodableSetAreRefusedByTheRowDecoder) {
  for (int candidate = 0; candidate <= 255; ++candidate) {
    const auto code = static_cast<uint8_t>(candidate);
    if (DescribeColumnType(static_cast<ColumnType>(code)).binlog_decoding == BinlogDecoding::kDecoded) {
      continue;
    }
    for (const uint16_t metadata : kSampleMetadata) {
      EXPECT_FALSE(RowDecoderHasACase(code, metadata)) << "column type " << candidate << " metadata " << metadata;
    }
  }
}

/**
 * @brief The field sizer and the decoder cover the same types.
 *
 * The row loop advances by the size the sizer computes and reads the value the
 * decoder produces. A type only one of them knows either skips the wrong number
 * of bytes or drops a value the row needs, and both corrupt every column after
 * it in the same row.
 */
TEST(ColumnTypeSupportTest, TheFieldSizerCoversExactlyTheDecodableTypes) {
  const std::vector<unsigned char> buffer(16, 0);
  for (int candidate = 0; candidate <= 255; ++candidate) {
    const auto code = static_cast<uint8_t>(candidate);
    bool sized_somewhere = false;
    for (const uint16_t metadata : kSampleMetadata) {
      sized_somewhere = sized_somewhere || binlog_util::calc_field_size(code, buffer.data(), metadata) != 0;
    }
    const bool decodable =
        DescribeColumnType(static_cast<ColumnType>(code)).binlog_decoding == BinlogDecoding::kDecoded;
    EXPECT_EQ(sized_somewhere, decodable) << "column type " << candidate;
  }
}

/**
 * @brief A type code outside the enumeration is refused everywhere.
 */
TEST(ColumnTypeSupportTest, TypeCodesOutsideTheEnumerationFailClosed) {
  for (int candidate = 0; candidate <= 255; ++candidate) {
    const auto code = static_cast<uint8_t>(candidate);
    if (IsEnumerated(code)) {
      continue;
    }
    const auto support = DescribeColumnType(static_cast<ColumnType>(code));
    EXPECT_EQ(support.binlog_decoding, BinlogDecoding::kUndecodable) << candidate;
    EXPECT_EQ(support.acceptance, ColumnAcceptance::kRejectedUndecodable) << candidate;
  }
}

/**
 * @brief Nothing is accepted that the binlog path cannot read.
 */
TEST(ColumnTypeSupportTest, AnAcceptedTypeIsNeverUndecodable) {
  for (int candidate = 0; candidate <= 255; ++candidate) {
    const auto support = DescribeColumnType(static_cast<ColumnType>(candidate));
    if (support.acceptance != ColumnAcceptance::kAccepted) {
      continue;
    }
    EXPECT_NE(support.binlog_decoding, BinlogDecoding::kUndecodable) << candidate;
  }
}

// ===========================================================================
// Declared types
// ===========================================================================

/**
 * @brief A declared type resolves to the code its row events carry.
 */
TEST(ColumnTypeSupportTest, DeclaredTypesResolveToTheCodeTheirRowEventsUse) {
  const std::vector<std::pair<std::string, ColumnType>> declared = {
      {"bigint unsigned", ColumnType::LONGLONG},
      {"INT(11)", ColumnType::LONG},
      {"integer", ColumnType::LONG},
      {"tinyint(1)", ColumnType::TINY},
      {"decimal(10,2)", ColumnType::NEWDECIMAL},
      {"numeric(10,2)", ColumnType::NEWDECIMAL},
      {"double precision", ColumnType::DOUBLE},
      {"real", ColumnType::DOUBLE},
      {"varchar(64)", ColumnType::VARCHAR},
      {"char(3)", ColumnType::STRING},
      {"longtext", ColumnType::LONG_BLOB},
      {"enum('draft','published')", ColumnType::ENUM},
      {"set('a','b')", ColumnType::SET},
      {"timestamp(3)", ColumnType::TIMESTAMP},
      {"time(6)", ColumnType::TIME},
      {"bit(12)", ColumnType::BIT},
      {"json", ColumnType::JSON},
      {"point", ColumnType::GEOMETRY},
  };
  for (const auto& [column_type, expected] : declared) {
    auto resolved = ColumnTypeFromDeclaredType(column_type);
    ASSERT_TRUE(resolved.has_value()) << column_type;
    EXPECT_EQ(*resolved, expected) << column_type;
  }
}

/**
 * @brief A declared type this build has never decoded resolves to nothing.
 *
 * Guessing at an unknown type is what lets a column through to a path that
 * cannot read it, so a name the table does not list is refused instead.
 */
TEST(ColumnTypeSupportTest, AnUnknownDeclaredTypeResolvesToNothing) {
  for (const auto* column_type : {"inet6", "uuid", "hstore", ""}) {
    EXPECT_FALSE(ColumnTypeFromDeclaredType(column_type).has_value()) << column_type;
    EXPECT_FALSE(UnusableColumnTypeReason(column_type).empty()) << column_type;
  }
}

// ===========================================================================
// Configuration-time acceptance
// ===========================================================================

/**
 * @brief A configured column whose type the two paths render differently is refused.
 *
 * Without this the configuration starts, the snapshot indexes one rendering and
 * replication publishes another, and the disagreement is only visible as a
 * document that no update ever reaches.
 */
TEST(ColumnTypeSupportTest, ConfiguredColumnsWhoseTypeCannotAgreeAreRefusedAtStartup) {
  for (const auto* column_type : {"json", "geometry", "point", "vector(3)", "float"}) {
    auto result = ValidateTextColumnType(column_type, "");
    ASSERT_FALSE(result) << column_type;
    EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLInvalidSchema) << column_type;
    EXPECT_NE(result.error().message().find("rendered differently"), std::string::npos)
        << column_type << ": " << result.error().message();
    EXPECT_EQ(result.error().context(), "testdb.articles.content") << column_type;
  }
}

/**
 * @brief A configured column of a type no row event carries is refused at startup.
 */
TEST(ColumnTypeSupportTest, ConfiguredColumnsOfAnUndecodableTypeAreRefusedAtStartup) {
  auto result = ValidateTextColumnType("inet6", "");
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLInvalidSchema);
  EXPECT_NE(result.error().message().find("not a type this build decodes"), std::string::npos)
      << result.error().message();
}

/**
 * @brief A ZEROFILL column is refused, whichever numeric type carries it.
 *
 * The padding is applied when the server renders the column as text, so the
 * initial snapshot reads `00042` where the row image carries `42`.
 */
TEST(ColumnTypeSupportTest, ZerofillColumnsAreRefusedAtStartup) {
  for (const auto* column_type : {"int(5) unsigned zerofill", "decimal(10,2) unsigned zerofill"}) {
    auto result = ValidateTextColumnType(column_type, "");
    ASSERT_FALSE(result) << column_type;
    EXPECT_NE(result.error().message().find("ZEROFILL"), std::string::npos) << result.error().message();
  }
}

/**
 * @brief The types both paths agree on stay configurable.
 */
TEST(ColumnTypeSupportTest, TypesThatAgreeRemainConfigurable) {
  const std::vector<std::pair<std::string, std::string>> configurable = {
      {"varchar(64)", "utf8mb4_0900_ai_ci"},
      {"longtext", "utf8mb4_0900_ai_ci"},
      {"char(10)", "ascii_general_ci"},
      {"enum('draft','published')", "utf8mb4_0900_ai_ci"},
      {"set('a','b')", "utf8mb4_0900_ai_ci"},
      {"bigint unsigned", ""},
      {"decimal(10,2)", ""},
      {"double", ""},
      {"datetime(6)", ""},
      {"timestamp(3)", ""},
      {"time(3)", ""},
      {"date", ""},
      {"year", ""},
      {"bit(12)", ""},
  };
  for (const auto& [column_type, collation] : configurable) {
    auto result = ValidateTextColumnType(column_type, collation);
    EXPECT_TRUE(result) << column_type << ": " << (result ? "" : result.error().message());
  }
}

}  // namespace
}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
