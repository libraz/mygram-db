/**
 * @file binlog_stream.cpp
 * @brief Shared binlog stream helpers.
 */

#include "mysql/binlog_stream.h"

#ifdef USE_MYSQL

#include "mysql/binlog_reader_internal.h"
#include "utils/structured_log.h"

namespace mygramdb::mysql {

BinlogFetchResult FetchBinlogEvent(Connection& conn, MYSQL_RPL& rpl, std::string_view log_action,
                                   std::string_view error_prefix) {
  BinlogFetchResult result;

  mygram::utils::StructuredLog().Event("binlog_debug").Field("action", log_action).Debug();
  int rc = mysql_binlog_fetch(conn.GetHandle(), &rpl);

  if (rc != 0) {
    unsigned int err_no = mysql_errno(conn.GetHandle());
    const char* err_str = mysql_error(conn.GetHandle());
    result.error_code = err_no;
    result.error_message =
        std::string(error_prefix) + ": " + std::string(err_str) + " (errno: " + std::to_string(err_no) + ")";

    if (err_no == kMySQLErrServerLost) {
      result.status = BinlogFetchResult::Status::kConnectionLost;
    } else if (err_no == kMySQLErrGoneAway) {
      result.status = BinlogFetchResult::Status::kServerGoneAway;
    } else if (err_no == kMySQLErrBinlogPurged) {
      result.status = BinlogFetchResult::Status::kBinlogPurged;
    } else {
      result.status = BinlogFetchResult::Status::kError;
    }
    return result;
  }

  if (rpl.size == 0 || rpl.buffer == nullptr) {
    result.status = BinlogFetchResult::Status::kNoData;
    return result;
  }

  result.status = BinlogFetchResult::Status::kOK;
  result.event_data = rpl.buffer + kBinlogOkByteSize;
  result.event_length = rpl.size - kBinlogOkByteSize;
  return result;
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
