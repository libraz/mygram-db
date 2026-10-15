#include "gtid_encoder.h"

#include <algorithm>
#include <optional>

#include "mysql/mariadb_gtid.h"
#include "utils/gtid_grammar.h"

// NOLINTBEGIN(cppcoreguidelines-avoid-*,cppcoreguidelines-pro-*,readability-magic-numbers) - GTID
// binary encoding

namespace mygramdb::mysql {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

mygram::utils::Expected<std::vector<uint8_t>, Error> GtidEncoder::Encode(const std::string& gtid_set) {
  auto parsed = mygram::utils::ParseMysqlGtidSet(gtid_set);
  if (!parsed) {
    return MakeUnexpected(parsed.error());
  }

  size_t total_size = 8;  // n_sids
  for (const auto& [key, intervals] : *parsed) {
    if (key.size() != mygram::utils::kGtidUuidTextLength) {
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLInvalidGTID, "Tagged GTIDs cannot be encoded for COM_BINLOG_DUMP_GTID: " + key));
    }
    total_size += 16 + 8 + (16 * intervals.size());  // UUID, n_intervals, (start, end) pairs
  }

  std::vector<uint8_t> result;
  result.reserve(total_size);
  StoreInt64(result, parsed->size());

  // Lowercase hex keys sort in byte order, which is the order MySQL expects.
  for (const auto& [key, intervals] : *parsed) {
    auto uuid_bytes = mygram::utils::ParseUuid(key);
    if (!uuid_bytes) {
      return MakeUnexpected(uuid_bytes.error());
    }
    result.insert(result.end(), uuid_bytes->begin(), uuid_bytes->end());
    StoreInt64(result, intervals.size());
    for (const auto& interval : intervals) {
      StoreInt64(result, interval.start);
      StoreInt64(result, interval.end + 1);  // exclusive end on the wire
    }
  }
  return result;
}

std::optional<std::string> GtidEncoder::MergeSingleGtidIntoSet(std::string_view current_gtid,
                                                               std::string_view next_gtid) {
  auto next_set = mygram::utils::ParseMysqlGtidSet(next_gtid);
  if (!next_set || next_set->size() != 1 || next_set->begin()->second.size() != 1) {
    return std::nullopt;
  }
  // The grammar merges every entry of one source, so the union is a single parse.
  auto merged = mygram::utils::ParseMysqlGtidSet(std::string(current_gtid) + "," + std::string(next_gtid));
  if (!merged) {
    return std::nullopt;
  }
  return mygram::utils::FormatMysqlGtidSet(*merged);
}

mygram::utils::Expected<bool, Error> GtidEncoder::PositionCovers(std::string_view required,
                                                                 std::string_view available) {
  auto required_set = mygram::utils::ParseMysqlGtidSet(required);
  if (!required_set) {
    return MakeUnexpected(required_set.error());
  }
  auto available_set = mygram::utils::ParseMysqlGtidSet(available);
  if (!available_set) {
    return MakeUnexpected(available_set.error());
  }

  for (const auto& [sid, required_intervals] : *required_set) {
    auto available_iter = available_set->find(sid);
    if (available_iter == available_set->end()) {
      return false;
    }
    const auto& available_intervals = available_iter->second;
    for (const auto& required_interval : required_intervals) {
      const bool covered = std::any_of(available_intervals.begin(), available_intervals.end(),
                                       [&required_interval](const auto& available_interval) {
                                         return available_interval.start <= required_interval.start &&
                                                available_interval.end >= required_interval.end;
                                       });
      if (!covered) {
        return false;
      }
    }
  }
  return true;
}

mygram::utils::Expected<bool, Error> GtidEncoder::PositionCoversAuto(std::string_view required,
                                                                     std::string_view available) {
  const std::string required_text(required);
  const std::string available_text(available);
  auto required_mariadb = MariaDBGTID::ParseSet(required_text);
  auto available_mariadb = MariaDBGTID::ParseSet(available_text);
  if (required_mariadb && available_mariadb) {
    return MariaDBGTID::PositionCovers(required_text, available_text);
  }
  return PositionCovers(required, available);
}

void GtidEncoder::StoreInt64(std::vector<uint8_t>& buffer, uint64_t value) {
  // Store in little-endian format (MySQL protocol uses little-endian)
  buffer.push_back(static_cast<uint8_t>(value & 0xFF));
  buffer.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
  buffer.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
  buffer.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
  buffer.push_back(static_cast<uint8_t>((value >> 32) & 0xFF));
  buffer.push_back(static_cast<uint8_t>((value >> 40) & 0xFF));
  buffer.push_back(static_cast<uint8_t>((value >> 48) & 0xFF));
  buffer.push_back(static_cast<uint8_t>((value >> 56) & 0xFF));
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-avoid-*,cppcoreguidelines-pro-*,readability-magic-numbers)
