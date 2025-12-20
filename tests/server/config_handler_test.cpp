/**
 * @file config_handler_test.cpp
 * @brief Integration tests for CONFIG commands in AdminHandler
 */

#include <gtest/gtest.h>

#include "config/config.h"
#include "query/query_parser.h"
#include "server/handlers/admin_handler.h"
#include "server/server_types.h"

namespace mygramdb::server {

class ConfigHandlerTest : public ::testing::Test {
 protected:
  ConfigHandlerTest()
      : ctx_{
            .table_catalog = nullptr,
            .table_contexts = table_contexts_,
            .stats = stats_,
            .full_config = &test_config_,
            .dump_dir = "/tmp",
            .dump_load_in_progress = dump_load_in_progress_,
            .dump_save_in_progress = dump_save_in_progress_,
            .optimization_in_progress = optimization_in_progress_,
            .replication_paused_for_dump = replication_paused_for_dump_,
            .mysql_reconnecting = mysql_reconnecting_,
#ifdef USE_MYSQL
            .binlog_reader = nullptr,
            .sync_manager = nullptr,

#else
            .binlog_reader = nullptr,
#endif
            .cache_manager = nullptr,
        } {
  }

  void SetUp() override {
    // Create a minimal valid config
    test_config_.mysql.host = "127.0.0.1";
    test_config_.mysql.port = 3306;
    test_config_.mysql.user = "test_user";
    test_config_.mysql.password = "secret_password";
    test_config_.mysql.database = "test_db";

    config::TableConfig table;
    table.name = "test_table";
    table.primary_key = "id";
    table.text_source.column = "content";
    test_config_.tables.push_back(table);

    test_config_.replication.enable = true;
    test_config_.replication.server_id = 12345;

    // Create handler
    handler_ = std::make_unique<AdminHandler>(ctx_);
  }

  config::Config test_config_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  ServerStats stats_;
  std::atomic<bool> dump_load_in_progress_{false};
  std::atomic<bool> dump_save_in_progress_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  std::atomic<bool> mysql_reconnecting_{false};
#ifdef USE_MYSQL
#endif
  HandlerContext ctx_;
  std::unique_ptr<AdminHandler> handler_;
  ConnectionContext conn_ctx_;
};

// CONFIG HELP tests

TEST_F(ConfigHandlerTest, ConfigHelpRoot) {
  query::Query query;
  query.type = query::QueryType::CONFIG_HELP;
  query.filepath = "";  // Empty path = root

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("+OK") != std::string::npos);
  EXPECT_TRUE(response.find("mysql") != std::string::npos);
  EXPECT_TRUE(response.find("tables") != std::string::npos);
  EXPECT_TRUE(response.find("replication") != std::string::npos);
}

TEST_F(ConfigHandlerTest, ConfigHelpMysqlSection) {
  query::Query query;
  query.type = query::QueryType::CONFIG_HELP;
  query.filepath = "mysql";

  std::string response = handler_->Handle(query, conn_ctx_);

  // The response format shows properties in list form, not as simple keywords
  EXPECT_TRUE(response.find("+OK") != std::string::npos);
  EXPECT_TRUE(response.find("mysql") != std::string::npos);
  EXPECT_TRUE(response.find("MySQL") != std::string::npos || response.find("Properties") != std::string::npos);
}

TEST_F(ConfigHandlerTest, ConfigHelpSpecificProperty) {
  query::Query query;
  query.type = query::QueryType::CONFIG_HELP;
  query.filepath = "mysql.port";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("+OK") != std::string::npos);
  EXPECT_TRUE(response.find("mysql.port") != std::string::npos);
  EXPECT_TRUE(response.find("integer") != std::string::npos);
  EXPECT_TRUE(response.find("3306") != std::string::npos);
}

TEST_F(ConfigHandlerTest, ConfigHelpInvalidPath) {
  query::Query query;
  query.type = query::QueryType::CONFIG_HELP;
  query.filepath = "nonexistent.path";

  std::string response = handler_->Handle(query, conn_ctx_);

  // Should return error for invalid path
  EXPECT_TRUE(response.find("ERR") != std::string::npos || response.find("not found") != std::string::npos);
}

// CONFIG SHOW tests

TEST_F(ConfigHandlerTest, ConfigShowEntireConfig) {
  query::Query query;
  query.type = query::QueryType::CONFIG_SHOW;
  query.filepath = "";  // Empty = entire config

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("+OK") != std::string::npos);
  EXPECT_TRUE(response.find("mysql:") != std::string::npos);
  EXPECT_TRUE(response.find("host:") != std::string::npos);
  EXPECT_TRUE(response.find("127.0.0.1") != std::string::npos);
  EXPECT_TRUE(response.find("tables:") != std::string::npos);
  EXPECT_TRUE(response.find("test_table") != std::string::npos);
}

TEST_F(ConfigHandlerTest, ConfigShowMasksSensitiveFields) {
  query::Query query;
  query.type = query::QueryType::CONFIG_SHOW;
  query.filepath = "mysql";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("+OK") != std::string::npos);
  EXPECT_TRUE(response.find("password: \"***\"") != std::string::npos);
  EXPECT_TRUE(response.find("secret_password") == std::string::npos);
  EXPECT_TRUE(response.find("test_user") != std::string::npos);
}

TEST_F(ConfigHandlerTest, ConfigShowSpecificSection) {
  query::Query query;
  query.type = query::QueryType::CONFIG_SHOW;
  query.filepath = "mysql";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("+OK") != std::string::npos);
  EXPECT_TRUE(response.find("127.0.0.1") != std::string::npos);
  EXPECT_TRUE(response.find("3306") != std::string::npos);
}

TEST_F(ConfigHandlerTest, ConfigShowSpecificProperty) {
  query::Query query;
  query.type = query::QueryType::CONFIG_SHOW;
  query.filepath = "mysql.port";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("+OK") != std::string::npos);
  EXPECT_TRUE(response.find("3306") != std::string::npos);
}

TEST_F(ConfigHandlerTest, ConfigShowInvalidPath) {
  query::Query query;
  query.type = query::QueryType::CONFIG_SHOW;
  query.filepath = "nonexistent.path";

  std::string response = handler_->Handle(query, conn_ctx_);

  // Should return error for invalid path
  EXPECT_TRUE(response.find("ERR") != std::string::npos);
}

// CONFIG VERIFY tests

TEST_F(ConfigHandlerTest, ConfigVerifyNoFilepath) {
  query::Query query;
  query.type = query::QueryType::CONFIG_VERIFY;
  query.filepath = "";  // Empty filepath

  std::string response = handler_->Handle(query, conn_ctx_);

  // Should return error for missing filepath
  EXPECT_TRUE(response.find("ERR") != std::string::npos);
  EXPECT_TRUE(response.find("filepath") != std::string::npos);
}

TEST_F(ConfigHandlerTest, ConfigVerifyNonExistentFile) {
  query::Query query;
  query.type = query::QueryType::CONFIG_VERIFY;
  query.filepath = "/nonexistent/path/to/config.yaml";

  std::string response = handler_->Handle(query, conn_ctx_);

  // Should return error for non-existent file
  EXPECT_TRUE(response.find("ERR") != std::string::npos);
  EXPECT_TRUE(response.find("validation") != std::string::npos || response.find("failed") != std::string::npos ||
              response.find("bad file") != std::string::npos);
}

// Query Parser integration tests

TEST_F(ConfigHandlerTest, QueryParserConfigHelp) {
  query::QueryParser parser;

  auto query = parser.Parse("CONFIG HELP mysql");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, query::QueryType::CONFIG_HELP);
  EXPECT_EQ(query->filepath, "mysql");
}

TEST_F(ConfigHandlerTest, QueryParserConfigHelpNoPath) {
  query::QueryParser parser;

  auto query = parser.Parse("CONFIG HELP");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, query::QueryType::CONFIG_HELP);
  EXPECT_TRUE(query->filepath.empty());
}

TEST_F(ConfigHandlerTest, QueryParserConfigShow) {
  query::QueryParser parser;

  auto query = parser.Parse("CONFIG SHOW mysql.port");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, query::QueryType::CONFIG_SHOW);
  EXPECT_EQ(query->filepath, "mysql.port");
}

TEST_F(ConfigHandlerTest, QueryParserConfigShowNoPath) {
  query::QueryParser parser;

  auto query = parser.Parse("CONFIG SHOW");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, query::QueryType::CONFIG_SHOW);
  EXPECT_TRUE(query->filepath.empty());
}

TEST_F(ConfigHandlerTest, QueryParserConfigVerify) {
  query::QueryParser parser;

  auto query = parser.Parse("CONFIG VERIFY /path/to/config.yaml");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, query::QueryType::CONFIG_VERIFY);
  EXPECT_EQ(query->filepath, "/path/to/config.yaml");
}

TEST_F(ConfigHandlerTest, QueryParserConfigVerifyNoFilepath) {
  query::QueryParser parser;

  auto query = parser.Parse("CONFIG VERIFY");

  // Parser should report an error for missing filepath
  EXPECT_FALSE(query);
  EXPECT_FALSE(query.error().message().empty());
}

TEST_F(ConfigHandlerTest, QueryParserConfigNoSubcommand) {
  query::QueryParser parser;

  auto query = parser.Parse("CONFIG");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, query::QueryType::CONFIG_SHOW);  // Defaults to SHOW
  EXPECT_TRUE(query->filepath.empty());
}

TEST_F(ConfigHandlerTest, QueryParserConfigInvalidSubcommand) {
  query::QueryParser parser;

  auto query = parser.Parse("CONFIG INVALID");

  EXPECT_FALSE(query);
  EXPECT_FALSE(query.error().message().empty());
}

// Line ending tests for TCP protocol compatibility

TEST_F(ConfigHandlerTest, ConfigHelpUsesCRLFLineEndings) {
  query::Query query;
  query.type = query::QueryType::CONFIG_HELP;
  query.filepath = "";  // Root help

  std::string response = handler_->Handle(query, conn_ctx_);

  // Verify response uses CRLF line endings
  EXPECT_TRUE(response.find("\r\n") != std::string::npos) << "Response should contain CRLF line endings";

  // Verify no bare LF (LF not preceded by CR) - the original bug had mixed line endings
  for (size_t i = 0; i < response.size(); ++i) {
    if (response[i] == '\n' && (i == 0 || response[i - 1] != '\r')) {
      size_t start = (i > 20) ? i - 20 : 0;
      size_t end = std::min(i + 20, response.size());
      std::string context;
      for (size_t j = start; j < end; ++j) {
        unsigned char c = response[j];
        if (c == '\r') {
          context += "\\r";
        } else if (c == '\n') {
          context += "\\n";
        } else if (c >= 32 && c < 127) {
          context += static_cast<char>(c);
        } else {
          context += "?";
        }
      }
      FAIL() << "Found bare LF (not preceded by CR) at position " << i << ". Context: [" << context << "]";
    }
  }

  // Verify response does not end with trailing CRLF (SendResponse adds it)
  EXPECT_FALSE(response.size() >= 2 && response[response.size() - 2] == '\r' && response[response.size() - 1] == '\n')
      << "Response should not end with CRLF (SendResponse adds it)";
}

TEST_F(ConfigHandlerTest, ConfigShowUsesCRLFLineEndings) {
  query::Query query;
  query.type = query::QueryType::CONFIG_SHOW;
  query.filepath = "";  // Entire config

  std::string response = handler_->Handle(query, conn_ctx_);

  // Verify response uses CRLF line endings
  EXPECT_TRUE(response.find("\r\n") != std::string::npos) << "Response should contain CRLF line endings";

  // Verify no bare LF (LF not preceded by CR) - the original bug had mixed line endings
  for (size_t i = 0; i < response.size(); ++i) {
    if (response[i] == '\n' && (i == 0 || response[i - 1] != '\r')) {
      FAIL() << "Found bare LF at position " << i;
    }
  }

  // Verify response does not end with trailing CRLF (SendResponse adds it)
  EXPECT_FALSE(response.size() >= 2 && response[response.size() - 2] == '\r' && response[response.size() - 1] == '\n')
      << "Response should not end with CRLF (SendResponse adds it)";
}

TEST_F(ConfigHandlerTest, ConfigHelpSpecificPathUsesCRLFLineEndings) {
  query::Query query;
  query.type = query::QueryType::CONFIG_HELP;
  query.filepath = "mysql.port";  // Specific property

  std::string response = handler_->Handle(query, conn_ctx_);

  // Verify response uses CRLF line endings
  EXPECT_TRUE(response.find("\r\n") != std::string::npos) << "Response should contain CRLF line endings";

  // Verify no bare LF (LF not preceded by CR) - the original bug had mixed line endings
  for (size_t i = 0; i < response.size(); ++i) {
    if (response[i] == '\n' && (i == 0 || response[i - 1] != '\r')) {
      FAIL() << "Found bare LF at position " << i;
    }
  }
}

TEST_F(ConfigHandlerTest, ConfigShowSpecificSectionUsesCRLFLineEndings) {
  query::Query query;
  query.type = query::QueryType::CONFIG_SHOW;
  query.filepath = "mysql";  // MySQL section

  std::string response = handler_->Handle(query, conn_ctx_);

  // Verify response uses CRLF line endings
  EXPECT_TRUE(response.find("\r\n") != std::string::npos) << "Response should contain CRLF line endings";

  // Verify no bare LF (LF not preceded by CR) - the original bug had mixed line endings
  for (size_t i = 0; i < response.size(); ++i) {
    if (response[i] == '\n' && (i == 0 || response[i - 1] != '\r')) {
      FAIL() << "Found bare LF at position " << i;
    }
  }

  // Verify response does not end with trailing CRLF (SendResponse adds it)
  EXPECT_FALSE(response.size() >= 2 && response[response.size() - 2] == '\r' && response[response.size() - 1] == '\n')
      << "Response should not end with CRLF (SendResponse adds it)";
}

}  // namespace mygramdb::server
