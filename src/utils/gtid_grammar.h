/**
 * @file gtid_grammar.h
 * @brief The one grammar for MySQL and MariaDB GTID position text
 *
 * Every GTID position the server reads, from configuration, a dump, SYNC or
 * the binlog stream, is parsed here: MySQL "uuid[:tag]:interval[:...][,...]"
 * sets and MariaDB "domain-server-seq[,...]" lists. A form accepted on one path
 * is therefore accepted on all of them. Nothing here depends on a MySQL client
 * library, so the configuration loader can use it in every build.
 */

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "utils/error.h"
#include "utils/expected.h"
#include "utils/namespace_compat.h"

namespace mygramdb::utils {

/// Characters in a textual UUID ("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx").
inline constexpr size_t kGtidUuidTextLength = 36;

/**
 * @brief Inclusive range of transaction numbers from one MySQL source
 */
struct MysqlGtidInterval {
  uint64_t start = 0;
  uint64_t end = 0;  // inclusive
};

/**
 * @brief Parsed MySQL GTID set
 *
 * Keyed by the source's lowercase UUID, followed by ":tag" for a tagged entry.
 * Intervals are sorted and merged.
 */
using MysqlGtidSet = std::map<std::string, std::vector<MysqlGtidInterval>>;

/**
 * @brief One MariaDB GTID
 */
struct MariaDBGtidPosition {
  uint32_t domain_id = 0;
  uint32_t server_id = 0;
  uint64_t sequence_no = 0;
};

/**
 * @brief Parse an 8-4-4-4-12 hex UUID, either case
 * @return The 16 bytes, or kMySQLInvalidGTID
 */
Expected<std::array<uint8_t, 16>, Error> ParseUuid(std::string_view uuid_text);

/**
 * @brief Parse a MySQL GTID set
 *
 * Accepts "uuid[:tag]:interval[:interval...][,...]" with whitespace around
 * entries and intervals. A tag is a letter or underscore followed by up to 31
 * alphanumerics or underscores, and every transaction number is at least 1
 * and below UINT64_MAX. Empty input is the empty set.
 *
 * @return Parsed set, or kMySQLInvalidGTID / kOutOfRange naming the first defect
 */
Expected<MysqlGtidSet, Error> ParseMysqlGtidSet(std::string_view gtid_set);

/**
 * @brief Canonical text of a parsed set: lowercase UUIDs in order, merged intervals
 */
std::string FormatMysqlGtidSet(const MysqlGtidSet& gtid_set);

/**
 * @brief Parse one MariaDB GTID "domain-server-seq", surrounding whitespace allowed
 * @return The GTID, or kMariaDBInvalidGTID
 */
Expected<MariaDBGtidPosition, Error> ParseMariaDBGtid(std::string_view gtid);

/**
 * @brief Parse a comma-separated MariaDB GTID list; empty input is the empty list
 * @return The GTIDs in order, or kMariaDBInvalidGTID
 */
Expected<std::vector<MariaDBGtidPosition>, Error> ParseMariaDBGtidList(std::string_view gtid_list);

/**
 * @brief Validate a replication position for either server flavor
 *
 * @param position A non-empty MySQL GTID set or MariaDB GTID list
 * @return Nothing, or kMySQLInvalidGTID when neither grammar accepts it
 */
Expected<void, Error> ValidateGtidPosition(std::string_view position);

}  // namespace mygramdb::utils
