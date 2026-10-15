/**
 * @file mariadb_gtid.cpp
 * @brief MariaDB GTID parsing and representation implementation
 */

#include "mysql/mariadb_gtid.h"

#include <algorithm>
#include <map>

#include "utils/gtid_grammar.h"

namespace mygramdb::mysql {

namespace {

MariaDBGTID FromPosition(const mygram::utils::MariaDBGtidPosition& position) {
  MariaDBGTID gtid;
  gtid.domain_id = position.domain_id;
  gtid.server_id = position.server_id;
  gtid.sequence_no = position.sequence_no;
  return gtid;
}

}  // namespace

mygram::utils::Expected<MariaDBGTID, mygram::utils::Error> MariaDBGTID::Parse(const std::string& gtid_str) {
  auto parsed = mygram::utils::ParseMariaDBGtid(gtid_str);
  if (!parsed) {
    return mygram::utils::MakeUnexpected(parsed.error());
  }
  return FromPosition(*parsed);
}

mygram::utils::Expected<std::vector<MariaDBGTID>, mygram::utils::Error> MariaDBGTID::ParseSet(
    const std::string& gtid_set_str) {
  auto parsed = mygram::utils::ParseMariaDBGtidList(gtid_set_str);
  if (!parsed) {
    return mygram::utils::MakeUnexpected(parsed.error());
  }
  std::vector<MariaDBGTID> result;
  result.reserve(parsed->size());
  for (const auto& position : *parsed) {
    result.push_back(FromPosition(position));
  }
  return result;
}

std::string MariaDBGTID::ToString() const {
  return std::to_string(domain_id) + "-" + std::to_string(server_id) + "-" + std::to_string(sequence_no);
}

std::string MariaDBGTID::SetToString(const std::vector<MariaDBGTID>& gtids) {
  std::string result;
  for (size_t i = 0; i < gtids.size(); ++i) {
    if (i > 0) {
      result += ",";
    }
    result += gtids[i].ToString();
  }
  return result;
}

mygram::utils::Expected<bool, mygram::utils::Error> MariaDBGTID::PositionCovers(const std::string& required,
                                                                                const std::string& available) {
  auto required_set = ParseSet(required);
  if (!required_set) {
    return mygram::utils::MakeUnexpected(required_set.error());
  }
  auto available_set = ParseSet(available);
  if (!available_set) {
    return mygram::utils::MakeUnexpected(available_set.error());
  }

  std::map<uint32_t, uint64_t> available_by_domain;
  for (const auto& gtid : *available_set) {
    auto [iter, inserted] = available_by_domain.emplace(gtid.domain_id, gtid.sequence_no);
    if (!inserted) {
      iter->second = std::max(iter->second, gtid.sequence_no);
    }
  }

  for (const auto& gtid : *required_set) {
    auto iter = available_by_domain.find(gtid.domain_id);
    if (iter == available_by_domain.end() || iter->second < gtid.sequence_no) {
      return false;
    }
  }
  return true;
}

bool MariaDBGTID::IsMariaDBGtidFormat(const std::string& gtid_str) {
  return mygram::utils::ParseMariaDBGtid(gtid_str).has_value();
}

}  // namespace mygramdb::mysql
