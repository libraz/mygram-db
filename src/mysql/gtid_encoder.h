#ifndef MYGRAM_MYSQL_GTID_ENCODER_H_
#define MYGRAM_MYSQL_GTID_ENCODER_H_

#include <cstdint>
#include <string>
#include <vector>

namespace mygram {
namespace mysql {

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
   * @return Binary encoded GTID set
   * @throws std::invalid_argument if GTID set format is invalid
   */
  static std::vector<uint8_t> Encode(const std::string& gtid_set);

 private:
  struct Interval {
    int64_t start;
    int64_t end;  // exclusive
  };

  struct Sid {
    uint8_t uuid[16];
    std::vector<Interval> intervals;
  };

  /**
   * @brief Parses a UUID string into 16 bytes
   * @param uuid_str UUID string like "61d5b289-bccc-11f0-b921-cabbb4ee51f6"
   * @param uuid_bytes Output buffer (16 bytes)
   * @throws std::invalid_argument if UUID format is invalid
   */
  static void ParseUuid(const std::string& uuid_str, uint8_t* uuid_bytes);

  /**
   * @brief Parses an interval string like "1-3" or "5"
   * @param interval_str Interval string
   * @return Parsed interval
   * @throws std::invalid_argument if interval format is invalid
   */
  static Interval ParseInterval(const std::string& interval_str);

  /**
   * @brief Stores a 64-bit integer in little-endian format
   * @param buffer Output buffer
   * @param value Value to store
   */
  static void StoreInt64(std::vector<uint8_t>& buffer, uint64_t value);
};

}  // namespace mysql
}  // namespace mygram

#endif  // MYGRAM_MYSQL_GTID_ENCODER_H_
