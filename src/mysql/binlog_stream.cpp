/**
 * @file binlog_stream.cpp
 * @brief Shared binlog stream helpers.
 */

#include "mysql/binlog_stream.h"

#ifdef USE_MYSQL

#include "mysql/binlog_reader_internal.h"
#include "utils/structured_log.h"

namespace mygramdb::mysql {

BinlogFetchResult::Status ClassifyFetchError(unsigned int err_no) {
  switch (err_no) {
    case kMySQLErrServerLost:
      return BinlogFetchResult::Status::kConnectionLost;
    case kMySQLErrBinlogPurged:
      return BinlogFetchResult::Status::kBinlogPurged;
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

    result.status = ClassifyFetchError(err_no);
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
