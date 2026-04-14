/**
 * @file mariadb_gtid.h
 * @brief MariaDB GTID parsing and representation
 *
 * MariaDB GTID format: "domain_id-server_id-sequence_number"
 * Example: "0-1-42" means domain 0, server 1, sequence 42
 *
 * A GTID set is comma-separated: "0-1-42,1-1-15"
 * Each domain_id should appear at most once in a set (highest seq wins).
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql {

/**
 * @brief MariaDB GTID representation
 *
 * MariaDB GTIDs have the format "domain_id-server_id-sequence_number".
 * Unlike MySQL GTIDs (UUID:txn_id), MariaDB uses numeric identifiers.
 */
struct MariaDBGTID {
  uint32_t domain_id = 0;    ///< Replication domain ID
  uint32_t server_id = 0;    ///< Server that created the transaction
  uint64_t sequence_no = 0;  ///< Monotonically increasing sequence number

  /**
   * @brief Parse a single MariaDB GTID from string
   * @param gtid_str GTID string in format "domain_id-server_id-sequence_no"
   * @return Expected<MariaDBGTID, Error> with parsed GTID or error
   */
  static mygram::utils::Expected<MariaDBGTID, mygram::utils::Error> Parse(const std::string& gtid_str);

  /**
   * @brief Parse a MariaDB GTID set (comma-separated GTIDs)
   * @param gtid_set_str GTID set string (e.g., "0-1-42,1-1-15")
   * @return Expected<vector<MariaDBGTID>, Error> with parsed GTIDs or error
   */
  static mygram::utils::Expected<std::vector<MariaDBGTID>, mygram::utils::Error> ParseSet(
      const std::string& gtid_set_str);

  /**
   * @brief Convert to string format
   * @return String in format "domain_id-server_id-sequence_no"
   */
  [[nodiscard]] std::string ToString() const;

  /**
   * @brief Convert a GTID set to string format
   * @param gtids Vector of MariaDB GTIDs
   * @return Comma-separated GTID set string
   */
  static std::string SetToString(const std::vector<MariaDBGTID>& gtids);

  /**
   * @brief Check if a string looks like a MariaDB GTID (not MySQL)
   *
   * MariaDB GTIDs have exactly 2 dashes and all segments are numeric.
   * MySQL GTIDs have 4 dashes (UUID format) and hex segments.
   *
   * @param gtid_str GTID string to check
   * @return true if the format matches MariaDB GTID
   */
  static bool IsMariaDBGtidFormat(const std::string& gtid_str);

  bool operator==(const MariaDBGTID& other) const {
    return domain_id == other.domain_id && server_id == other.server_id && sequence_no == other.sequence_no;
  }

  bool operator!=(const MariaDBGTID& other) const { return !(*this == other); }
};

}  // namespace mygramdb::mysql
