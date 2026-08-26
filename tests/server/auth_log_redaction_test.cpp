/**
 * @file auth_log_redaction_test.cpp
 * @brief Request-log redaction of authentication tokens
 *
 * The dispatcher logs every request it accepts. Any spelling of AUTH the query
 * grammar accepts carries a token in that request, so the redaction decision
 * has to agree with the parser rather than with a second reading of the bytes.
 * These tests drive the spellings where the two readings can disagree: quoting
 * that needs no separator, and Unicode whitespace the grammar treats as a
 * separator.
 */

#include <gtest/gtest.h>
#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>

#include "index/index.h"
#include "server/request_dispatcher.h"
#include "server/server_stats.h"
#include "server/table_catalog.h"
#include "storage/document_store.h"

namespace mygramdb::server {
namespace {

constexpr const char* kAdminToken = "top-secret-admin-token";

/**
 * @brief Capture everything written to the default logger at debug level.
 */
class LogCapture {
 public:
  LogCapture() : previous_logger_(spdlog::default_logger()), previous_level_(spdlog::get_level()) {
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(stream_);
    logger_ = std::make_shared<spdlog::logger>("auth_redaction_capture", sink);
    logger_->set_pattern("%v");
    logger_->set_level(spdlog::level::trace);
    logger_->flush_on(spdlog::level::trace);
    spdlog::set_default_logger(logger_);
    spdlog::set_level(spdlog::level::trace);
  }

  ~LogCapture() {
    spdlog::set_default_logger(previous_logger_);
    spdlog::set_level(previous_level_);
  }

  LogCapture(const LogCapture&) = delete;
  LogCapture& operator=(const LogCapture&) = delete;
  LogCapture(LogCapture&&) = delete;
  LogCapture& operator=(LogCapture&&) = delete;

  std::string Text() {
    logger_->flush();
    return stream_.str();
  }

 private:
  std::ostringstream stream_;
  std::shared_ptr<spdlog::logger> previous_logger_;
  spdlog::level::level_enum previous_level_;
  std::shared_ptr<spdlog::logger> logger_;
};

}  // namespace

class AuthLogRedactionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    table_context_ = std::make_unique<TableContext>();
    table_context_->name = "posts";
    table_context_->config.ngram_size = 2;
    table_context_->index = std::make_unique<index::Index>(2);
    table_context_->doc_store = std::make_unique<storage::DocumentStore>();
    table_contexts_["posts"] = table_context_.get();
    table_catalog_ = std::make_unique<TableCatalog>(table_contexts_);

    ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_catalog = table_catalog_.get(),
        .stats = stats_,
        .full_config = nullptr,
        .dump_dir = "",
        .dump_load_in_progress = dump_load_in_progress_,
        .dump_save_in_progress = dump_save_in_progress_,
        .optimization_in_progress = optimization_in_progress_,
        .replication_paused_for_dump = replication_paused_for_dump_,
        .mysql_reconnecting = mysql_reconnecting_,
#ifdef USE_MYSQL
        .sync_manager = nullptr,
#endif
        .cache_manager = nullptr,
    });

    ServerConfig config;
    config.default_limit = 100;
    config.max_query_length = 10000;
    config.admin_token = kAdminToken;
    dispatcher_ = std::make_unique<RequestDispatcher>(*ctx_, config);
  }

  /** Dispatch one request and return everything it wrote to the log. */
  std::string DispatchAndCaptureLog(const std::string& request) {
    LogCapture capture;
    ConnectionContext conn_ctx;
    last_response_ = dispatcher_->Dispatch(request, conn_ctx);
    return capture.Text();
  }

  ServerStats stats_;
  std::unique_ptr<TableContext> table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<TableCatalog> table_catalog_;
  std::atomic<bool> dump_load_in_progress_{false};
  std::atomic<bool> dump_save_in_progress_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  std::atomic<bool> mysql_reconnecting_{false};
  std::unique_ptr<HandlerContext> ctx_;
  std::unique_ptr<RequestDispatcher> dispatcher_;
  std::string last_response_;
};

TEST_F(AuthLogRedactionTest, QuotedTokenWithNoSeparatorIsRedacted) {
  const std::string log = DispatchAndCaptureLog(std::string("AUTH\"") + kAdminToken + "\"");

  EXPECT_EQ(last_response_.find("OK AUTHENTICATED"), 0U) << last_response_;
  EXPECT_EQ(log.find(kAdminToken), std::string::npos) << log;
  EXPECT_NE(log.find("AUTH <redacted>"), std::string::npos) << log;
}

TEST_F(AuthLogRedactionTest, TokenBehindUnicodeWhitespaceIsRedacted) {
  // U+3000 IDEOGRAPHIC SPACE, which the grammar treats as a separator.
  const std::string log = DispatchAndCaptureLog(std::string("\xE3\x80\x80") + "AUTH " + kAdminToken);

  EXPECT_EQ(last_response_.find("OK AUTHENTICATED"), 0U) << last_response_;
  EXPECT_EQ(log.find(kAdminToken), std::string::npos) << log;
  EXPECT_NE(log.find("AUTH <redacted>"), std::string::npos) << log;
}

TEST_F(AuthLogRedactionTest, SingleQuotedTokenIsRedacted) {
  const std::string log = DispatchAndCaptureLog(std::string("AUTH '") + kAdminToken + "'");

  EXPECT_EQ(last_response_.find("OK AUTHENTICATED"), 0U) << last_response_;
  EXPECT_EQ(log.find(kAdminToken), std::string::npos) << log;
}

TEST_F(AuthLogRedactionTest, RejectedTokenIsRedacted) {
  const std::string log = DispatchAndCaptureLog("AUTH\"a-guessed-token\"");

  EXPECT_EQ(last_response_.find("ERROR"), 0U) << last_response_;
  EXPECT_EQ(log.find("a-guessed-token"), std::string::npos) << log;
}

TEST_F(AuthLogRedactionTest, UnparsableAuthAttemptIsStillLoggedAndStillRedacted) {
  // Extra arguments and an unclosed quote both fail to parse, so the parser
  // cannot say where the token ends. Both must still produce a log line.
  for (const auto& request : {std::string("AUTH ") + kAdminToken + " extra", std::string("AUTH \"") + kAdminToken}) {
    const std::string log = DispatchAndCaptureLog(request);

    EXPECT_EQ(last_response_.find("ERROR"), 0U) << last_response_;
    EXPECT_NE(log.find("request_dispatching"), std::string::npos) << log;
    EXPECT_EQ(log.find(kAdminToken), std::string::npos) << log;
  }
}

TEST_F(AuthLogRedactionTest, RequestsThatAreNotAuthenticationAreLoggedInFull) {
  const std::string log = DispatchAndCaptureLog("SEARCH posts hello");

  EXPECT_NE(log.find("SEARCH posts hello"), std::string::npos) << log;
}

TEST_F(AuthLogRedactionTest, UnparsableRequestsThatAreNotAuthenticationAreLoggedInFull) {
  const std::string log = DispatchAndCaptureLog("this is not a valid command");

  EXPECT_EQ(last_response_.find("ERROR"), 0U) << last_response_;
  EXPECT_NE(log.find("this is not a valid command"), std::string::npos) << log;
}

}  // namespace mygramdb::server
