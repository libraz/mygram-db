/**
 * @file column_type_support.h
 * @brief The single table deciding what each MySQL column type means to both ingest paths.
 *
 * A configured column is read twice: once as text from the initial-load result
 * set, once as a binary row image from the binlog. A type is only usable when
 * the two produce the same string for the same stored value, so the three
 * decisions that used to be made separately - how the snapshot text is
 * normalized, whether the row decoder can decode the type, and whether a
 * configured column may have the type at all - are recorded here together.
 *
 * Every row carries all three, because a type that is normalized but not
 * decodable, or decodable but normalized differently, is exactly the
 * combination that produces two documents for one row. ColumnTypeSupport has
 * no default member initializers and one three-argument constructor, so a row
 * cannot be written with a field left out, and the switch below has no default
 * label, so an enumerator added to ColumnType is a compile error
 * (-Werror=switch) until its row is written.
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "mysql/table_metadata.h"
#include "mysql/value_canonicalizer.h"

namespace mygramdb::mysql {

/**
 * @brief What the binlog row decoder can do with a column type code.
 */
enum class BinlogDecoding : uint8_t {
  kDecoded,       ///< DecodeFieldValue has a case for this type code.
  kNotOnTheWire,  ///< No row event carries this code; the server writes a sibling code instead.
  kUndecodable,   ///< No case decodes it, so a row containing it stops replication.
};

/**
 * @brief Whether a configured column may have this type.
 */
enum class ColumnAcceptance : uint8_t {
  kAccepted,               ///< Both paths produce the same string for every stored value.
  kRejectedPathsDisagree,  ///< Both paths produce a string, and the two differ.
  kRejectedUndecodable,    ///< The binlog path cannot produce a string at all.
};

/**
 * @brief One row of the table: everything both ingest paths need about a type.
 */
struct ColumnTypeSupport {
  CanonicalValueKind snapshot_normalization;  ///< Kind CanonicalizeColumnValue applies to the snapshot text.
  BinlogDecoding binlog_decoding;             ///< What the row decoder does with the wire code.
  ColumnAcceptance acceptance;                ///< Whether a configured column may have this type.

  constexpr ColumnTypeSupport(CanonicalValueKind normalization, BinlogDecoding decoding, ColumnAcceptance verdict)
      : snapshot_normalization(normalization), binlog_decoding(decoding), acceptance(verdict) {}
};

/**
 * @brief Describe one MySQL column type.
 *
 * Sibling codes for one MySQL type share a single case group, so the pair
 * cannot drift: the initial-load result set reports TIME for a TIME(n) column
 * while the binlog writes TIME2, and either code has to answer identically.
 *
 * @param type Column type code, from a TABLE_MAP event or a MYSQL_FIELD
 * @return The row for @p type, or the fail-closed row for a code outside the enumeration
 */
constexpr ColumnTypeSupport DescribeColumnType(ColumnType type) {
  switch (type) {
    // Integers. Both paths render the stored integer in decimal, and the
    // snapshot text is already the canonical form.
    case ColumnType::TINY:
    case ColumnType::SHORT:
    case ColumnType::LONG:
    case ColumnType::LONGLONG:
    case ColumnType::INT24:
    // YEAR renders as four digits on both paths, including the zero year.
    case ColumnType::YEAR:
      return ColumnTypeSupport{CanonicalValueKind::kText, BinlogDecoding::kDecoded, ColumnAcceptance::kAccepted};

    // FLOAT. A result set renders it to six significant digits, which is fewer
    // than a float needs to be recovered: 16777217 comes back as 16777200
    // while the row image carries the stored 16777216. The lost digits are not
    // in the text at all, so no normalization can bring the two together.
    case ColumnType::FLOAT:
      return ColumnTypeSupport{CanonicalValueKind::kFloat, BinlogDecoding::kDecoded,
                               ColumnAcceptance::kRejectedPathsDisagree};

    // DOUBLE. The result set renders the shortest decimal that reads back as
    // the stored value, so parsing it recovers the row image's value exactly
    // and both sides reprint it the same way.
    case ColumnType::DOUBLE:
      return ColumnTypeSupport{CanonicalValueKind::kDouble, BinlogDecoding::kDecoded, ColumnAcceptance::kAccepted};

    // DECIMAL. Both paths carry every declared digit; only leading zeros and a
    // negative zero differ, and normalization removes both.
    case ColumnType::NEWDECIMAL:
      return ColumnTypeSupport{CanonicalValueKind::kDecimal, BinlogDecoding::kDecoded, ColumnAcceptance::kAccepted};

    // Temporal types. The snapshot text carries exactly the declared fractional
    // precision while the row decoder always writes six fractional digits, so
    // the snapshot side is padded to six. TIMESTAMP agrees only because the
    // load connection pins its session time zone to UTC, which is the offset
    // the row decoder formats in.
    case ColumnType::DATE:
    case ColumnType::DATETIME:
    case ColumnType::DATETIME2:
    case ColumnType::TIMESTAMP:
    case ColumnType::TIMESTAMP2:
    case ColumnType::TIME:
    case ColumnType::TIME2:
      return ColumnTypeSupport{CanonicalValueKind::kTemporal, BinlogDecoding::kDecoded, ColumnAcceptance::kAccepted};

    // BIT. The result set carries the raw big-endian bytes and the row decoder
    // reads the same bytes as an unsigned integer, so the snapshot bytes are
    // read the same way rather than being treated as text.
    case ColumnType::BIT:
      return ColumnTypeSupport{CanonicalValueKind::kBitset, BinlogDecoding::kDecoded, ColumnAcceptance::kAccepted};

    // Character strings. Both paths sanitize the bytes the server stored.
    // Whether the column is character or binary is not visible in the type
    // code - BLOB and TEXT share one - so the binary variants are refused by
    // their declared type name, next to the collation check.
    case ColumnType::VARCHAR:
    case ColumnType::STRING:
    case ColumnType::TINY_BLOB:
    case ColumnType::MEDIUM_BLOB:
    case ColumnType::LONG_BLOB:
    case ColumnType::BLOB:
    // ENUM and SET reach the row decoder as a member ordinal or a bitmask and
    // are turned back into the declared labels, which is what the result set
    // returns. A label list that changed under a running server is caught as a
    // schema change before a row is decoded against it.
    case ColumnType::ENUM:
    case ColumnType::SET:
      return ColumnTypeSupport{CanonicalValueKind::kText, BinlogDecoding::kDecoded, ColumnAcceptance::kAccepted};

    // The client library reports VAR_STRING for a VARCHAR column; a row event
    // always uses VARCHAR for the same column. The type is usable, but a row
    // event carrying this code is not a stream this decoder understands.
    case ColumnType::VAR_STRING:
      return ColumnTypeSupport{CanonicalValueKind::kText, BinlogDecoding::kNotOnTheWire, ColumnAcceptance::kAccepted};

    // JSON. The server renders a document with a space after every ':' and ','
    // and in the order the binary form stores keys, while the row decoder
    // writes the same document compactly. Both are valid JSON for the same
    // value and neither is derivable from the other by normalization.
    case ColumnType::JSON:
    // GEOMETRY and VECTOR. The result set returns the raw payload, which is not
    // UTF-8 and does not survive sanitizing, while the row decoder returns it
    // hex-encoded.
    case ColumnType::GEOMETRY:
    case ColumnType::VECTOR:
      return ColumnTypeSupport{CanonicalValueKind::kText, BinlogDecoding::kDecoded,
                               ColumnAcceptance::kRejectedPathsDisagree};

    // An internal type of the server's own date handling. No case decodes it.
    case ColumnType::NEWDATE:
      return ColumnTypeSupport{CanonicalValueKind::kTemporal, BinlogDecoding::kUndecodable,
                               ColumnAcceptance::kRejectedUndecodable};
  }

  // A code outside the enumeration describes a column this build knows nothing
  // about. Nothing can be said about how its value would decode, so it is
  // refused wherever it is configured.
  return ColumnTypeSupport{CanonicalValueKind::kText, BinlogDecoding::kUndecodable,
                           ColumnAcceptance::kRejectedUndecodable};
}

/**
 * @brief Resolve the type code behind a declared MySQL type.
 *
 * SHOW FULL COLUMNS reports a written type such as `bigint unsigned` or
 * `varchar(64)`, which is how configuration-time validation sees a column. The
 * code returned is the one a row event uses for that type, so a configured
 * column is judged by the same row of the table the two ingest paths read.
 *
 * @param column_type Declared type, with or without its length and attributes
 * @return The type code, or std::nullopt when the declared type is not one this build decodes
 */
[[nodiscard]] std::optional<ColumnType> ColumnTypeFromDeclaredType(std::string_view column_type);

/**
 * @brief Operator-facing reason a declared type cannot back a configured column.
 *
 * @param column_type Declared type as MySQL reported it
 * @return A sentence naming what the type does to the two ingest paths
 */
[[nodiscard]] std::string UnusableColumnTypeReason(std::string_view column_type);

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
