/**
 * @file application_test.cpp
 * @brief Unit tests for Application helper logic (dump directory validation)
 */

#include "app/application.h"

#include <gtest/gtest.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "app/application_lifecycle.h"
#include "app/configuration_manager.h"
#include "app/server_orchestrator.h"
#include "app/signal_manager.h"
#include "mysql/null_binlog_reader.h"
#include "server/http_server.h"
#include "server/operation_coordinator.h"
#include "server/replication_pause_counter.h"
#include "server/tcp_server.h"
#include "support/network_test_utils.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace {

class LifecycleProbeApplication {
 public:
  LifecycleProbeApplication(bool& destroyed, int exit_code) : destroyed_(destroyed), exit_code_(exit_code) {}
  ~LifecycleProbeApplication() { destroyed_ = true; }

  int Run() {
    EXPECT_FALSE(destroyed_);
    return exit_code_;
  }

 private:
  bool& destroyed_;
  int exit_code_;
};

TEST(ApplicationLifecycleTest, DestroysApplicationBeforeReturningToGlobalLibraryTeardown) {
  bool application_destroyed = false;
  mygram::utils::Expected<std::unique_ptr<LifecycleProbeApplication>, mygram::utils::Error> application(
      std::make_unique<LifecycleProbeApplication>(application_destroyed, 23));

  const int result = mygramdb::app::RunApplicationInScope(std::move(application));

  EXPECT_EQ(result, 23);
  EXPECT_TRUE(application_destroyed)
      << "global library teardown runs after this call, so application-owned handles must already be destroyed";
}

TEST(ApplicationLifecycleTest, CreationFailureReturnsBeforeGlobalLibraryTeardown) {
  auto application = mygram::utils::Expected<std::unique_ptr<LifecycleProbeApplication>, mygram::utils::Error>(
      mygram::utils::MakeUnexpected(
          mygram::utils::MakeError(mygram::utils::ErrorCode::kConfigParseError, "invalid configuration")));
  std::ostringstream errors;

  const int result = mygramdb::app::RunApplicationInScope(std::move(application), errors);

  EXPECT_EQ(result, 1);
  EXPECT_NE(errors.str().find("invalid configuration"), std::string::npos);
}

TEST(ApplicationLifecycleTest, ShutdownCancellationDuringStartupIsSuccessfulExit) {
  const auto cancelled =
      mygram::utils::MakeError(mygram::utils::ErrorCode::kCancelled, "Initial load cancelled by shutdown");

  EXPECT_TRUE(mygramdb::app::IsGracefulStartupCancellation(cancelled, true));
  EXPECT_FALSE(mygramdb::app::IsGracefulStartupCancellation(cancelled, false));
}

TEST(ApplicationLifecycleTest, NonCancellationStartupErrorRemainsFailureDuringShutdown) {
  const auto failed = mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLConnectionFailed, "connect failed");

  EXPECT_FALSE(mygramdb::app::IsGracefulStartupCancellation(failed, true));
}

TEST(ApplicationLifecycleTest, RootRejectionStopsBeforeDaemonPathOrFilePreparation) {
  std::vector<std::string> calls;
  const auto result = mygramdb::app::RunPreFileStartupChecks(
      true,
      [&]() -> mygram::utils::Expected<void, mygram::utils::Error> {
        calls.push_back("privilege");
        return mygram::utils::MakeUnexpected(
            mygram::utils::MakeError(mygram::utils::ErrorCode::kPermissionDenied, "root rejected"));
      },
      [&]() -> mygram::utils::Expected<void, mygram::utils::Error> {
        calls.push_back("paths");
        return {};
      });

  ASSERT_FALSE(result);
  EXPECT_EQ(calls, (std::vector<std::string>{"privilege"}));
}

TEST(ApplicationLifecycleTest, DaemonPathsResolveOnlyAfterPrivilegeCheck) {
  std::vector<std::string> calls;
  const auto result = mygramdb::app::RunPreFileStartupChecks(
      true,
      [&]() -> mygram::utils::Expected<void, mygram::utils::Error> {
        calls.push_back("privilege");
        return {};
      },
      [&]() -> mygram::utils::Expected<void, mygram::utils::Error> {
        calls.push_back("paths");
        return {};
      });

  ASSERT_TRUE(result);
  EXPECT_EQ(calls, (std::vector<std::string>{"privilege", "paths"}));
}

/**
 * @brief Validates a dump directory path using the same logic as Application::VerifyDumpDirectory.
 *
 * This standalone function mirrors the validation in application.cpp:
 * - Reject ".." path components before creating any directory
 * - Canonicalize with weakly_canonical
 * - Reject paths containing ".." after normalization
 * - Check directory existence or creatability
 * - Check write permissions via temp file
 *
 * @param dump_dir The directory path to validate
 * @return Empty string on success, error message on failure
 */
std::string ValidateDumpDirectory(const std::string& dump_dir) {
  try {
    std::filesystem::path dump_path(dump_dir);

    for (const auto& component : dump_path) {
      if (component == "..") {
        return "Path contains '..' component before creation: " + dump_path.string();
      }
    }

    // Create directory if it doesn't exist
    if (!std::filesystem::exists(dump_path)) {
      std::filesystem::create_directories(dump_path);
    }

    // Canonicalize and check for ".." components
    std::filesystem::path canonical_dump = std::filesystem::weakly_canonical(dump_path);
    std::filesystem::path normalized = canonical_dump.lexically_normal();

    for (const auto& component : normalized) {
      if (component == "..") {
        return "Path contains '..' component after normalization: " + normalized.string();
      }
    }

    // Check write permissions
    std::filesystem::path test_file = dump_path / ".write_test";
    std::ofstream test_stream(test_file);
    if (!test_stream.is_open()) {
      return "Dump directory is not writable: " + dump_dir;
    }
    test_stream.close();
    std::filesystem::remove(test_file);

    return "";
  } catch (const std::exception& e) {
    return std::string("Failed to verify dump directory: ") + e.what();
  }
}

class DumpDirectoryValidationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // CTest runs individual GoogleTest cases in parallel processes. Give each
    // case a unique directory so one fixture cannot remove another case's log
    // or configuration files during TearDown().
    const auto* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    const auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
    base_dir_ = std::filesystem::temp_directory_path() /
                ("mygramdb_test_dump_" + std::string(test_info != nullptr ? test_info->name() : "unknown") + "_" +
                 std::to_string(nonce));
    std::filesystem::create_directories(base_dir_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(base_dir_, ec);
  }

  std::filesystem::path base_dir_;
};

/**
 * @brief Test that an absolute path outside CWD succeeds (the bug we fixed)
 */
TEST_F(DumpDirectoryValidationTest, AbsolutePathOutsideCwd) {
  std::filesystem::path dump_path = base_dir_ / "dumps";
  std::string result = ValidateDumpDirectory(dump_path.string());
  EXPECT_TRUE(result.empty()) << "Error: " << result;
}

TEST_F(DumpDirectoryValidationTest, PathWithDoubleDotRejectedBeforeCreation) {
  std::filesystem::path nested = base_dir_ / "a" / "b";
  std::filesystem::create_directories(nested);

  std::filesystem::path traversal = nested / ".." / "escaped";
  std::string result = ValidateDumpDirectory(traversal.string());
  EXPECT_FALSE(result.empty());
  EXPECT_NE(result.find("before creation"), std::string::npos);
  EXPECT_FALSE(std::filesystem::exists(base_dir_ / "a" / "escaped"));
}

/**
 * @brief Test that a relative path inside CWD succeeds
 */
TEST_F(DumpDirectoryValidationTest, RelativePathInsideCwd) {
  // Save and change to base_dir_ to test relative paths
  auto original_cwd = std::filesystem::current_path();
  std::filesystem::current_path(base_dir_);

  std::string result = ValidateDumpDirectory("./relative_dumps");
  EXPECT_TRUE(result.empty()) << "Error: " << result;

  // Cleanup
  std::filesystem::remove_all(base_dir_ / "relative_dumps");
  std::filesystem::current_path(original_cwd);
}

/**
 * @brief Test that the /tmp path succeeds (previously rejected by CWD check)
 */
TEST_F(DumpDirectoryValidationTest, TmpPathSucceeds) {
  std::filesystem::path dump_path = std::filesystem::temp_directory_path() / "mygramdb_dump_test_tmp";
  std::string result = ValidateDumpDirectory(dump_path.string());
  EXPECT_TRUE(result.empty()) << "Error: " << result;
  std::filesystem::remove_all(dump_path);
}

TEST_F(DumpDirectoryValidationTest, DaemonPathsAreAbsolutizedBeforeChdir) {
  const auto original_cwd = std::filesystem::current_path();
  struct CwdGuard {
    std::filesystem::path original;
    ~CwdGuard() { std::filesystem::current_path(original); }
  } cwd_guard{original_cwd};
  std::filesystem::current_path(base_dir_);

  std::filesystem::path config_path = base_dir_ / "daemon_paths.yaml";
  std::ofstream config(config_path);
  config << "mysql:\n"
         << "  host: \"127.0.0.1\"\n"
         << "  port: 3306\n"
         << "  user: \"test\"\n"
         << "  password: \"test\"\n"
         << "  database: \"test\"\n"
         << "  ssl_enable: true\n"
         << "  ssl_ca: \"tls/ca.pem\"\n"
         << "  ssl_cert: \"tls/client.pem\"\n"
         << "  ssl_key: \"tls/client.key\"\n"
         << "tables:\n"
         << "  - name: \"test_table\"\n"
         << "    primary_key: \"id\"\n"
         << "    text_source:\n"
         << "      column: \"content\"\n"
         << "    synonyms:\n"
         << "      enable: true\n"
         << "      file: \"dictionaries/test.tsv\"\n"
         << "replication:\n"
         << "  enable: false\n"
         << "  server_id: 12345\n"
         << "dump:\n"
         << "  dir: \"relative_dumps\"\n"
         << "logging:\n"
         << "  file: \"logs/mygramdb.log\"\n"
         << "api:\n"
         << "  unix_socket:\n"
         << "    path: \"run/mygramdb.sock\"\n";
  config.close();

  auto manager = mygramdb::app::ConfigurationManager::Create(config_path.string());
  ASSERT_TRUE(manager.has_value()) << manager.error().to_string();

  auto result = (*manager)->AbsolutizeDaemonPaths();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();

  EXPECT_EQ((*manager)->GetConfig().dump.dir, std::filesystem::absolute("relative_dumps").lexically_normal().string());
  EXPECT_EQ((*manager)->GetConfig().logging.file,
            std::filesystem::absolute("logs/mygramdb.log").lexically_normal().string());
  EXPECT_EQ((*manager)->GetConfig().mysql.ssl_ca, std::filesystem::absolute("tls/ca.pem").lexically_normal().string());
  EXPECT_EQ((*manager)->GetConfig().mysql.ssl_cert,
            std::filesystem::absolute("tls/client.pem").lexically_normal().string());
  EXPECT_EQ((*manager)->GetConfig().mysql.ssl_key,
            std::filesystem::absolute("tls/client.key").lexically_normal().string());
  EXPECT_EQ((*manager)->GetConfig().tables[0].synonyms.file,
            std::filesystem::absolute("dictionaries/test.tsv").lexically_normal().string());
  EXPECT_EQ((*manager)->GetConfig().api.unix_socket.path,
            std::filesystem::absolute("run/mygramdb.sock").lexically_normal().string());

  (void)cwd_guard;
}

TEST_F(DumpDirectoryValidationTest, RunLogsStartupAfterApplyingLoggingConfig) {
  std::filesystem::path log_path = base_dir_ / "mygramdb.log";
  std::filesystem::path dump_file = base_dir_ / "not_a_directory";
  std::ofstream(dump_file) << "regular file";

  std::filesystem::path config_path = base_dir_ / "config.yaml";
  std::ofstream config(config_path);
  config << "mysql:\n"
         << "  host: \"127.0.0.1\"\n"
         << "  port: 3306\n"
         << "  user: \"test\"\n"
         << "  password: \"test\"\n"
         << "  database: \"test\"\n"
         << "tables:\n"
         << "  - name: \"test_table\"\n"
         << "    primary_key: \"id\"\n"
         << "    text_source:\n"
         << "      column: \"content\"\n"
         << "replication:\n"
         << "  enable: false\n"
         << "  server_id: 12345\n"
         << "dump:\n"
         << "  dir: \"" << dump_file.string() << "\"\n"
         << "logging:\n"
         << "  level: \"info\"\n"
         << "  format: \"json\"\n"
         << "  file: \"" << log_path.string() << "\"\n";
  config.close();

  std::string program = "mygramdb";
  std::string config_flag = "-c";
  std::string config_arg = config_path.string();
  char* argv[] = {program.data(), config_flag.data(), config_arg.data()};

  auto app = mygramdb::app::Application::Create(3, argv);
  ASSERT_TRUE(app.has_value()) << app.error().to_string();
  EXPECT_NE((*app)->Run(), 0);

  spdlog::default_logger()->flush();
  std::ifstream log_file(log_path);
  ASSERT_TRUE(log_file.is_open());
  std::string log_contents((std::istreambuf_iterator<char>(log_file)), std::istreambuf_iterator<char>());
  EXPECT_NE(log_contents.find("application_starting"), std::string::npos);

  spdlog::drop_all();
  auto console_sink = std::make_shared<spdlog::sinks::stdout_sink_mt>();
  auto logger = std::make_shared<spdlog::logger>("default", console_sink);
  spdlog::set_default_logger(logger);
}

TEST(ServerOrchestratorReplicationTest, EmptyGtidStillStartsConfiguredBinlogReader) {
  mygramdb::mysql::NullBinlogReader reader;

  EXPECT_TRUE(mygramdb::app::ShouldStartBinlogReaderOnServerStart(&reader, ""));
  EXPECT_TRUE(mygramdb::app::ShouldStartBinlogReaderOnServerStart(&reader, "uuid:1-10"));
  EXPECT_FALSE(mygramdb::app::ShouldStartBinlogReaderOnServerStart(nullptr, ""));
}

TEST(ServerOrchestratorReplicationTest, CollectRequiredTablesUsesConfiguredDatabaseAndNames) {
  std::unordered_map<std::string, std::unique_ptr<mygramdb::server::TableContext>> tables;

  auto articles = std::make_unique<mygramdb::server::TableContext>();
  articles->name = "articles_map_key";
  articles->config.database = "app_db";
  articles->config.name = "articles";
  tables.emplace("articles_map_key", std::move(articles));

  auto comments = std::make_unique<mygramdb::server::TableContext>();
  comments->name = "comments";
  comments->config.database = "archive_db";
  comments->config.name = "comments";
  tables.emplace("comments", std::move(comments));

  auto required_tables = mygramdb::app::CollectRequiredTables(tables);
  std::sort(required_tables.begin(), required_tables.end(),
            [](const auto& lhs, const auto& rhs) { return lhs.DisplayName() < rhs.DisplayName(); });

  ASSERT_EQ(required_tables.size(), 2u);
  EXPECT_EQ(required_tables[0].DisplayName(), "app_db.articles");
  EXPECT_EQ(required_tables[1].DisplayName(), "archive_db.comments");
}

TEST(ServerOrchestratorReplicationTest, ResolveReplicationStartGtidUsesLatestProvider) {
  auto result = mygramdb::app::ResolveReplicationStartGtid("latest", "snapshot-gtid", []() {
    return mygram::utils::Expected<std::string, mygram::utils::Error>("uuid:1-10");
  });

  ASSERT_TRUE(result) << result.error().to_string();
  EXPECT_EQ(*result, "uuid:1-10");
}

TEST(ServerOrchestratorReplicationTest, ResolveReplicationStartGtidAllowsEmptyLatestGtid) {
  auto result = mygramdb::app::ResolveReplicationStartGtid(
      "latest", "snapshot-gtid", []() { return mygram::utils::Expected<std::string, mygram::utils::Error>(""); });

  ASSERT_TRUE(result) << result.error().to_string();
  EXPECT_EQ(*result, "");
}

TEST(ServerOrchestratorReplicationTest, ResolveReplicationStartGtidUsesLatestWhenSnapshotGtidIsUnavailable) {
  bool latest_called = false;
  auto result = mygramdb::app::ResolveReplicationStartGtid("snapshot", "", [&latest_called]() {
    latest_called = true;
    return mygram::utils::Expected<std::string, mygram::utils::Error>("uuid:1-10");
  });

  ASSERT_TRUE(result) << result.error().to_string();
  EXPECT_TRUE(latest_called);
  EXPECT_EQ(*result, "uuid:1-10");
}

TEST(ServerOrchestratorReplicationTest, ResolveReplicationStartGtidPropagatesMissingSnapshotLatestQueryError) {
  auto result = mygramdb::app::ResolveReplicationStartGtid("snapshot", "", []() {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLQueryFailed, "GTID query failed"));
  });

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLQueryFailed);
}

TEST(ServerOrchestratorReplicationTest, ResolveReplicationStartGtidPropagatesLatestQueryError) {
  auto result = mygramdb::app::ResolveReplicationStartGtid("latest", "snapshot-gtid", []() {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLQueryFailed, "SHOW BINARY LOG STATUS failed"));
  });

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLQueryFailed);
  EXPECT_NE(result.error().message().find("SHOW BINARY LOG STATUS failed"), std::string::npos);
}

TEST(ServerOrchestratorReplicationTest, ResolveReplicationStartGtidKeepsSnapshotAndExplicitGtid) {
  bool latest_called = false;
  auto latest_provider = [&latest_called]() {
    latest_called = true;
    return mygram::utils::Expected<std::string, mygram::utils::Error>("latest");
  };

  auto snapshot_result = mygramdb::app::ResolveReplicationStartGtid("snapshot", "snapshot-gtid", latest_provider);
  ASSERT_TRUE(snapshot_result) << snapshot_result.error().to_string();
  EXPECT_EQ(*snapshot_result, "snapshot-gtid");

  auto explicit_result = mygramdb::app::ResolveReplicationStartGtid("gtid=uuid:7", "snapshot-gtid", latest_provider);
  ASSERT_TRUE(explicit_result) << explicit_result.error().to_string();
  EXPECT_EQ(*explicit_result, "uuid:7");
  EXPECT_FALSE(latest_called);
}

TEST(ServerOrchestratorTextStorageTest, BigramTablesStoreTextEvenWhenVerifyTextIsOff) {
  mygramdb::config::Config config;
  config.memory.verify_text = "off";

  mygramdb::config::TableConfig table;
  table.ngram_size = 2;
  table.kanji_ngram_size = 2;

  EXPECT_TRUE(mygramdb::app::ShouldStoreNormalizedTexts(config, table));
}

TEST(ServerOrchestratorTextStorageTest, UnigramTablesStoreTextForHighlightSupport) {
  mygramdb::config::Config config;
  config.memory.verify_text = "off";
  config.bm25.enable = false;

  mygramdb::config::TableConfig table;
  table.ngram_size = 1;
  table.kanji_ngram_size = 1;

  EXPECT_TRUE(mygramdb::app::ShouldStoreNormalizedTexts(config, table));
}

TEST(ServerOrchestratorTextStorageTest, VerifyTextRequiresTextStorage) {
  mygramdb::config::Config config;
  config.memory.verify_text = "all";

  mygramdb::config::TableConfig table;
  table.ngram_size = 1;
  table.kanji_ngram_size = 1;

  EXPECT_TRUE(mygramdb::app::ShouldStoreNormalizedTexts(config, table));
}

TEST(ServerOrchestratorTextStorageTest, Bm25RequiresTextStorage) {
  mygramdb::config::Config config;
  config.memory.verify_text = "off";
  config.bm25.enable = true;

  mygramdb::config::TableConfig table;
  table.ngram_size = 1;
  table.kanji_ngram_size = 1;

  EXPECT_TRUE(mygramdb::app::ShouldStoreNormalizedTexts(config, table));
}

TEST(ServerOrchestratorMysqlStartupTest, SkipsMysqlWhenReplicationAndAutoSnapshotAreDisabled) {
  mygramdb::config::Config config;
  config.replication.enable = false;
  config.replication.auto_initial_snapshot = false;

  EXPECT_FALSE(mygramdb::app::RequiresMysqlConnectionForStartup(config));
}

TEST(ServerOrchestratorMysqlStartupTest, RequiresMysqlForReplicationAutoSnapshotOrStartupDumpFallback) {
  mygramdb::config::Config config;
  config.replication.enable = true;
  config.replication.auto_initial_snapshot = false;
  EXPECT_TRUE(mygramdb::app::RequiresMysqlConnectionForStartup(config));

  config.replication.enable = false;
  config.replication.auto_initial_snapshot = true;
  EXPECT_TRUE(mygramdb::app::RequiresMysqlConnectionForStartup(config));

  config.replication.auto_initial_snapshot = false;
  config.dump.load_on_startup = true;
  EXPECT_TRUE(mygramdb::app::RequiresMysqlConnectionForStartup(config));
}

TEST(InitialDataReadinessTrackerTest, ManualSyncRequiresEveryConfiguredTable) {
  mygramdb::config::TableConfig first;
  first.database = "catalog";
  first.name = "products";
  mygramdb::config::TableConfig second;
  second.database = "catalog";
  second.name = "categories";

  mygramdb::app::InitialDataReadinessTracker tracker({first, second}, false);
  EXPECT_FALSE(tracker.IsReady());

  tracker.MarkTableInitialized("catalog.products");
  EXPECT_FALSE(tracker.IsReady());

  tracker.MarkTableInitialized("catalog.categories");
  EXPECT_TRUE(tracker.IsReady());
}

TEST(InitialDataReadinessTrackerTest, StartupSnapshotInitializesAllTablesIncludingEmptyTables) {
  mygramdb::config::TableConfig table;
  table.database = "catalog";
  table.name = "empty_source";

  mygramdb::app::InitialDataReadinessTracker tracker({table}, true);
  EXPECT_TRUE(tracker.IsReady());
}

TEST(ServerOrchestratorStartupTest, MissingEnabledSynonymFileFailsInitialization) {
  auto signal_manager = mygramdb::app::SignalManager::Create();
  ASSERT_TRUE(signal_manager) << signal_manager.error().to_string();

  mygramdb::config::Config config;
  config.replication.enable = false;
  config.replication.auto_initial_snapshot = false;
  config.api.tcp.bind = "127.0.0.1";
  config.api.tcp.port = 0;
  mygramdb::config::TableConfig table;
  table.name = "posts";
  table.text_source.column = "body";
  table.synonyms.enable = true;
  table.synonyms.file = "/definitely/missing/mygramdb-synonyms.tsv";
  config.tables.push_back(std::move(table));
  const std::string dump_dir = std::filesystem::temp_directory_path().string();

  mygramdb::app::ServerOrchestrator::Dependencies deps{
      .config = config,
      .signal_manager = **signal_manager,
      .dump_dir = dump_dir,
  };
  auto orchestrator = mygramdb::app::ServerOrchestrator::Create(deps);
  ASSERT_TRUE(orchestrator);
  auto result = (*orchestrator)->Initialize();
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().message().find("Failed to load synonyms"), std::string::npos);
}

TEST(ServerOrchestratorStartupTest, HttpSurfaceAdoptsTheQueryCacheOwnedByTheTcpServer) {
  // The cache is created while the TCP server starts, which is after both
  // servers are constructed. If the HTTP server keeps the pointer it was built
  // with, every HTTP request runs outside the cache and the cache reports
  // itself as disabled over HTTP while TCP reports it as enabled -- a split
  // that no single-protocol test can see.
  auto signal_manager = mygramdb::app::SignalManager::Create();
  ASSERT_TRUE(signal_manager) << signal_manager.error().to_string();

  mygramdb::config::Config config;
  config.replication.enable = false;
  config.replication.auto_initial_snapshot = false;
  config.dump.load_on_startup = false;
  config.cache.enabled = true;
  config.api.tcp.bind = "127.0.0.1";
  config.api.tcp.port = mygramdb::testing::FindAvailableLoopbackPort();
  config.api.http.enable = true;
  config.api.http.bind = "127.0.0.1";
  config.api.http.port = mygramdb::testing::FindAvailableLoopbackPort();

  mygramdb::config::TableConfig table;
  table.database = "catalog";
  table.name = "posts";
  table.text_source.column = "body";
  config.tables.push_back(std::move(table));

  const std::string dump_dir = std::filesystem::temp_directory_path().string();
  mygramdb::app::ServerOrchestrator::Dependencies deps{
      .config = config,
      .signal_manager = **signal_manager,
      .dump_dir = dump_dir,
  };
  auto orchestrator = mygramdb::app::ServerOrchestrator::Create(deps);
  ASSERT_TRUE(orchestrator);
  ASSERT_TRUE((*orchestrator)->Initialize());
  ASSERT_TRUE((*orchestrator)->Start());

  auto* tcp_server = (*orchestrator)->GetTcpServerForTesting();
  const auto* http_server = (*orchestrator)->GetHttpServerForTesting();
  ASSERT_NE(tcp_server, nullptr);
  ASSERT_NE(http_server, nullptr);
  ASSERT_NE(tcp_server->GetCacheManager(), nullptr) << "the TCP server never created a cache";
  EXPECT_EQ(http_server->GetCacheManagerForTesting(), tcp_server->GetCacheManager())
      << "the HTTP surface is not sharing the cache the TCP surface owns";

  EXPECT_TRUE((*orchestrator)->Stop());
}

TEST(ServerOrchestratorStartupRetryTest, SucceedsOnFirstAttemptWithoutSleeping) {
  int attempts = 0;
  int sleeps = 0;
  const mygramdb::app::StartupConnectRetryPolicy policy{5, 10, 40};

  auto result = mygramdb::app::ConnectWithStartupRetry(
      [&attempts]() -> mygram::utils::Expected<void, mygram::utils::Error> {
        ++attempts;
        return {};
      },
      policy, [&sleeps](int /*delay_ms*/) { ++sleeps; });

  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(attempts, 1);
  EXPECT_EQ(sleeps, 0);
}

TEST(ServerOrchestratorStartupRetryTest, RetriesUntilSuccess) {
  int attempts = 0;
  int sleeps = 0;
  const mygramdb::app::StartupConnectRetryPolicy policy{5, 10, 40};

  // Fail the first two attempts, then succeed on the third.
  auto result = mygramdb::app::ConnectWithStartupRetry(
      [&attempts]() -> mygram::utils::Expected<void, mygram::utils::Error> {
        ++attempts;
        if (attempts < 3) {
          return mygram::utils::MakeUnexpected(
              mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLConnectionFailed, "not ready"));
        }
        return {};
      },
      policy, [&sleeps](int /*delay_ms*/) { ++sleeps; });

  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(attempts, 3);
  EXPECT_EQ(sleeps, 2);
}

TEST(ServerOrchestratorStartupRetryTest, ReturnsLastErrorAfterExhaustingAttempts) {
  int attempts = 0;
  int sleeps = 0;
  const mygramdb::app::StartupConnectRetryPolicy policy{3, 10, 40};

  auto result = mygramdb::app::ConnectWithStartupRetry(
      [&attempts]() -> mygram::utils::Expected<void, mygram::utils::Error> {
        ++attempts;
        return mygram::utils::MakeUnexpected(
            mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLConnectionFailed, "still not ready"));
      },
      policy, [&sleeps](int /*delay_ms*/) { ++sleeps; });

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLConnectionFailed);
  EXPECT_EQ(attempts, 3);
  EXPECT_EQ(sleeps, 2);
}

TEST(ServerOrchestratorStartupRetryTest, AppliesBoundedExponentialBackoff) {
  std::vector<int> delays;
  const mygramdb::app::StartupConnectRetryPolicy policy{5, 500, 5000};

  auto result = mygramdb::app::ConnectWithStartupRetry(
      []() -> mygram::utils::Expected<void, mygram::utils::Error> {
        return mygram::utils::MakeUnexpected(
            mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLConnectionFailed, "fail"));
      },
      policy, [&delays](int delay_ms) { delays.push_back(delay_ms); });

  ASSERT_FALSE(result.has_value());
  // 5 attempts -> 4 backoff sleeps: 500, 1000, 2000, 4000 (all below the 5000 cap).
  const std::vector<int> expected_delays{500, 1000, 2000, 4000};
  EXPECT_EQ(delays, expected_delays);
}

TEST(ServerOrchestratorShutdownTest, TcpFailureStillTearsDownEveryDependentComponentInOrder) {
  std::vector<std::string> calls;
  const auto tcp_error = mygram::utils::MakeError(mygram::utils::ErrorCode::kTimeout, "TCP connections did not drain");
  mygramdb::app::ServerShutdownActions actions{
      [&calls]() { calls.emplace_back("http"); },
      [&calls, &tcp_error]() -> mygram::utils::Expected<void, mygram::utils::Error> {
        calls.emplace_back("tcp");
        return mygram::utils::MakeUnexpected(tcp_error);
      },
      [&calls]() { calls.emplace_back("binlog"); },
      [&calls]() { calls.emplace_back("mysql"); },
  };

  auto result = mygramdb::app::StopServerComponentsBestEffort(actions);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kTimeout);
  EXPECT_EQ(calls, (std::vector<std::string>{"http", "tcp", "binlog", "mysql"}));
}

TEST(ServerOrchestratorShutdownTest, MissingComponentsAreSkippedAndSuccessfulShutdownReturnsSuccess) {
  std::vector<std::string> calls;
  mygramdb::app::ServerShutdownActions actions{
      {},
      [&calls]() -> mygram::utils::Expected<void, mygram::utils::Error> {
        calls.emplace_back("tcp");
        return {};
      },
      {},
      [&calls]() { calls.emplace_back("mysql"); },
  };

  auto result = mygramdb::app::StopServerComponentsBestEffort(actions);

  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(calls, (std::vector<std::string>{"tcp", "mysql"}));
}

TEST_F(DumpDirectoryValidationTest, ReopenLogFileSwapsDefaultLoggerForRotatedFile) {
  std::filesystem::path log_path = base_dir_ / "mygramdb.log";
  std::filesystem::path rotated_path = base_dir_ / "mygramdb.log.1";
  std::filesystem::path config_path = base_dir_ / "config.yaml";

  std::ofstream config(config_path);
  config << "mysql:\n"
         << "  host: \"127.0.0.1\"\n"
         << "  port: 3306\n"
         << "  user: \"test\"\n"
         << "  password: \"test\"\n"
         << "  database: \"test\"\n"
         << "tables:\n"
         << "  - name: \"test_table\"\n"
         << "    primary_key: \"id\"\n"
         << "    text_source:\n"
         << "      column: \"content\"\n"
         << "replication:\n"
         << "  enable: false\n"
         << "  server_id: 12345\n"
         << "dump:\n"
         << "  dir: \"" << base_dir_.string() << "\"\n"
         << "logging:\n"
         << "  level: \"info\"\n"
         << "  format: \"text\"\n"
         << "  file: \"" << log_path.string() << "\"\n";
  config.close();

  auto manager = mygramdb::app::ConfigurationManager::Create(config_path.string());
  ASSERT_TRUE(manager.has_value()) << manager.error().to_string();
  ASSERT_TRUE((*manager)->ApplyLoggingConfig().has_value());

  spdlog::info("before_rotate_marker");
  spdlog::default_logger()->flush();
  std::filesystem::rename(log_path, rotated_path);

  auto reopen = (*manager)->ReopenLogFile();
  ASSERT_TRUE(reopen.has_value()) << reopen.error().to_string();

  spdlog::info("after_rotate_marker");
  spdlog::default_logger()->flush();

  std::ifstream rotated(rotated_path);
  ASSERT_TRUE(rotated.is_open());
  const std::string rotated_contents((std::istreambuf_iterator<char>(rotated)), std::istreambuf_iterator<char>());

  std::ifstream reopened(log_path);
  ASSERT_TRUE(reopened.is_open());
  const std::string reopened_contents((std::istreambuf_iterator<char>(reopened)), std::istreambuf_iterator<char>());

  EXPECT_NE(rotated_contents.find("before_rotate_marker"), std::string::npos);
  EXPECT_EQ(rotated_contents.find("after_rotate_marker"), std::string::npos);
  EXPECT_NE(reopened_contents.find("after_rotate_marker"), std::string::npos);

  spdlog::drop_all();
  auto console_sink = std::make_shared<spdlog::sinks::stdout_sink_mt>();
  auto logger = std::make_shared<spdlog::logger>("default", console_sink);
  spdlog::set_default_logger(logger);
}

}  // namespace
