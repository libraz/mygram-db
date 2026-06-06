#ifndef MYGRAM_MYSQL_GTID_ENCODER_H_
#define MYGRAM_MYSQL_GTID_ENCODER_H_

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql {

/**
 * @brief Encodes MySQL GTID sets into binary format for binlog replication
 *
 * This class implements the binary encoding format used by MySQL's
 * COM_BINLOG_DUMP_GTID protocol. The binary format is:
 *
 * Binary format:
 *   8 bytes: number of SIDs (UUIDs)
 *   For each SID:
 *     16 bytes: UUID
 *     8 bytes: number of intervals
 *     For each interval:
 *       8 bytes: start transaction number
 *       8 bytes: end transaction number (exclusive)
 *
 * Example: "61d5b289-bccc-11f0-b921-cabbb4ee51f6:1-3" encodes to:
 *   [1, UUID_bytes, 1, 1, 4]
 *   where 4 is exclusive upper bound (represents transactions 1,2,3)
 */
class GtidEncoder {
 public:
  /**
   * @brief Encodes a GTID set string into binary format
   * @param gtid_set String like "uuid:1-3,5-7" or "uuid1:1-3,uuid2:5-7"
   * @return Expected containing binary encoded GTID set, or Error on invalid input
   */
  static mygram::utils::Expected<std::vector<uint8_t>, mygram::utils::Error> Encode(const std::string& gtid_set);

  /**
   * @brief Extract UUID part from a GTID string
   * @param gtid_str GTID string like "uuid:1-5" or "uuid:42"
   * @return UUID portion (e.g., "uuid"), or empty string if format is invalid
   */
  static std::string ExtractUuid(const std::string& gtid_str);

  /**
   * @brief Validate GTID set format
   * @param gtid_set GTID set string (e.g., "uuid:1-5" or "uuid1:1-3,uuid2:1-5")
   * @return true if the GTID set has valid format (each entry has UUID:intervals)
   */
  static bool IsValidGtidSet(const std::string& gtid_set);

  /**
   * @brief Merge one MySQL GTID event into an existing textual GTID set.
   * @param current_gtid Existing textual GTID set
   * @param next_gtid Single GTID event, including optional MySQL 8.4 tag
   * @return Canonical textual GTID set, or nullopt when either input cannot be parsed
   */
  static std::optional<std::string> MergeSingleGtidIntoSet(std::string_view current_gtid, std::string_view next_gtid);

 private:
  struct Interval {
    uint64_t start;
    uint64_t end;  // exclusive
  };

  struct Sid {
    std::array<uint8_t, 16> uuid = {};  // NOLINT(*-magic-numbers) - UUID is 16 bytes by spec
    std::vector<Interval> intervals;
  };

  /**
   * @brief Parses a UUID string into 16 bytes
   * @param uuid_str UUID string like "61d5b289-bccc-11f0-b921-cabbb4ee51f6"
   * @param uuid_bytes Output buffer (16 bytes)
   * @return Expected containing void on success, or Error on invalid UUID format
   */
  static mygram::utils::Expected<void, mygram::utils::Error> ParseUuid(const std::string& uuid_str,
                                                                       uint8_t* uuid_bytes);

  /**
   * @brief Parses an interval string like "1-3" or "5"
   * @param interval_str Interval string
   * @return Expected containing parsed interval, or Error on invalid format
   */
  static mygram::utils::Expected<Interval, mygram::utils::Error> ParseInterval(const std::string& interval_str);

  /**
   * @brief Stores a 64-bit integer in little-endian format
   * @param buffer Output buffer
   * @param value Value to store
   */
  static void StoreInt64(std::vector<uint8_t>& buffer, uint64_t value);
};

}  // namespace mygramdb::mysql

#endif  // MYGRAM_MYSQL_GTID_ENCODER_H_
