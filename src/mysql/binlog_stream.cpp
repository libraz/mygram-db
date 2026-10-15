/**
 * @file binlog_stream.cpp
 * @brief Shared binlog stream helpers.
 */

#include "mysql/binlog_stream.h"

#ifdef USE_MYSQL

#include "mysql/binlog_reader_internal.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::mysql {

bool IsPurgedPositionError(unsigned int err_no, std::string_view server_message) {
  if (err_no != kMySQLErrBinlogPurged) {
    return false;
  }
  // 1236 is the source's catch-all for fatal dump errors, so the text decides.
  const std::string message = mygram::utils::ToLower(std::string(server_message));
  return message.find("purged binary logs containing gtids") != std::string::npos ||
         message.find("purged required binary logs") != std::string::npos ||
         message.find("binlog files have been purged") != std::string::npos;
}

BinlogFetchResult::Status ClassifyFetchError(unsigned int err_no, std::string_view server_message) {
  if (IsPurgedPositionError(err_no, server_message)) {
    return BinlogFetchResult::Status::kBinlogPurged;
  }
  switch (err_no) {
    case kMySQLErrServerLost:
      return BinlogFetchResult::Status::kConnectionLost;
    case kMySQLErrGoneAway:
    case kMySQLErrServerLostExtended:
    case kMySQLErrServerShutdown:
    case kMySQLErrAbortingConnection:
    case kMySQLErrNetReadError:
    case kMySQLErrNetReadInterrupted:
    case kMySQLErrNetErrorOnWrite:
    case kMySQLErrNetWriteInterrupted:
    case kMariaDBErrConnectionKilled:
    case kMySQLErrClientInteractionTimeout:
      return BinlogFetchResult::Status::kServerGoneAway;
    default:
      return BinlogFetchResult::Status::kError;
  }
}

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

    result.status = ClassifyFetchError(err_no, err_str);
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
