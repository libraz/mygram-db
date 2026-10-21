/**
 * @file surface_snapshot_test.cpp
 * @brief Pins the rendered external surface against the checked-in golden file
 *
 * The command and CLI-flag rows are static tables, so they are also checked
 * against what QueryParser, RequestDispatcher and CommandLineParser accept.
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "app/command_line_parser.h"
#include "app/server_orchestrator.h"
#include "app/signal_manager.h"
#include "app/surface_descriptor.h"
#include "config/config.h"
#include "query/query_parser.h"
#include "server/request_dispatcher.h"
#include "server/tcp_server.h"
#include "support/network_test_utils.h"

namespace {

/// Absolute path to the golden, injected by CMake so the test does not depend
/// on the working directory ctest happens to use.
constexpr const char* kSpecDir = MYGRAMDB_SPEC_DIR;
constexpr const char* kGoldenName = "surface.snapshot.txt";

std::string GoldenPath() {
  return std::string(kSpecDir) + "/" + kGoldenName;
}

std::vector<std::string> SplitLines(const std::string& text) {
  std::vector<std::string> lines;
  std::istringstream stream(text);
  std::string line;
  while (std::getline(stream, line)) {
    lines.push_back(line);
  }
  return lines;
}

std::string RegenerationHint() {
  return "A difference here means the server's static external surface changed. Decide whether that change is\n"
         "intended before regenerating the golden: an unintended difference is a behavioural defect, and an\n"
         "intended one needs the golden updated in the same change.\n"
         "Regenerate with: make surface-snapshot";
}

TEST(SurfaceSnapshotTest, MatchesGoldenFile) {
  std::ifstream golden(GoldenPath(), std::ios::binary);
  ASSERT_TRUE(golden.is_open()) << "Golden file not found at " << GoldenPath() << "\n" << RegenerationHint();

  std::ostringstream buffer;
  buffer << golden.rdbuf();
  const std::string expected = buffer.str();
  const std::string actual = mygramdb::app::RenderSurfaceSnapshot();

  if (expected == actual) {
    SUCCEED();
    return;
  }

  const std::vector<std::string> expected_lines = SplitLines(expected);
  const std::vector<std::string> actual_lines = SplitLines(actual);

  size_t index = 0;
  while (index < expected_lines.size() && index < actual_lines.size() && expected_lines[index] == actual_lines[index]) {
    ++index;
  }

  const std::string expected_line = index < expected_lines.size() ? expected_lines[index] : "<end of file>";
  const std::string actual_line = index < actual_lines.size() ? actual_lines[index] : "<end of file>";

  FAIL() << "Rendered surface differs from " << GoldenPath() << "\n"
         << "First difference at line " << (index + 1) << ":\n"
         << "  expected: " << expected_line << "\n"
         << "  actual:   " << actual_line << "\n"
         << "Golden has " << expected_lines.size() << " lines, rendering has " << actual_lines.size() << ".\n"
         << RegenerationHint();
}

/// Lines of one "[section]" of the rendered snapshot.
std::vector<std::string> SnapshotSection(const std::string& section) {
  std::vector<std::string> lines;
  bool inside = false;
  for (const auto& line : SplitLines(mygramdb::app::RenderSurfaceSnapshot())) {
    if (!line.empty() && line.front() == '[') {
      inside = line == "[" + section + "]";
      continue;
    }
    if (inside) {
      lines.push_back(line);
    }
  }
  return lines;
}

struct CommandRow {
  std::string text;
  int min_args;
  int max_args;  ///< -1 when unbounded
  std::string handler;
};

std::vector<CommandRow> SnapshotCommandRows() {
  static const std::regex kRow(R"re(text="([^"]+)" min_args=(\d+) max_args=(\d+|unbounded) .* handler=(\S+))re");
  std::vector<CommandRow> rows;
  for (const auto& line : SnapshotSection("commands")) {
    std::smatch match;
    if (!std::regex_match(line, match, kRow)) {
      ADD_FAILURE() << "unparsable [commands] row: " << line;
      continue;
    }
    rows.push_back({match[1].str(), std::stoi(match[2].str()),
                    match[3].str() == "unbounded" ? -1 : std::stoi(match[3].str()), match[4].str()});
  }
  return rows;
}

/// The command text followed by @p count placeholder arguments.
std::string CommandWithArgs(const CommandRow& row, int count) {
  // SET's one argument is an assignment; every other command takes a bare token.
  const std::string placeholder = row.text == "SET" ? "a=1" : "a";
  std::string command = row.text;
  for (int i = 0; i < count; ++i) {
    command += " " + placeholder;
  }
  return command;
}

/**
 * @brief Every [commands] row agrees with what the parser and the dispatcher do
 *
 * The parser must accept min_args arguments, reject one fewer and, for a
 * bounded row, reject one more; a registered row must have a dispatcher handler.
 */
TEST(SurfaceSnapshotTest, CommandRowsMatchParserAndDispatcher) {
  auto signal_manager = mygramdb::app::SignalManager::Create();
  ASSERT_TRUE(signal_manager) << signal_manager.error().to_string();
  mygramdb::config::Config config;
  config.replication.enable = false;
  config.replication.auto_initial_snapshot = false;
  config.network.allow_cidrs = {"127.0.0.1/32"};
  config.api.tcp.bind = "127.0.0.1";
  config.api.tcp.port = mygramdb::testing::FindAvailableLoopbackPort();
  config.api.http.enable = false;
  mygramdb::config::TableConfig table;
  table.database = "catalog";
  table.name = "posts";
  table.text_source.column = "body";
  config.tables.push_back(std::move(table));
  const std::string dump_dir =
      (std::filesystem::temp_directory_path() / ("surface_snapshot_" + std::to_string(::getpid()))).string();
  std::filesystem::create_directories(dump_dir);

  mygramdb::app::ServerOrchestrator::Dependencies deps{
      .config = config,
      .signal_manager = **signal_manager,
      .dump_dir = dump_dir,
  };
  auto orchestrator = mygramdb::app::ServerOrchestrator::Create(deps);
  ASSERT_TRUE(orchestrator) << orchestrator.error().to_string();
  ASSERT_TRUE((*orchestrator)->Initialize());
  ASSERT_TRUE((*orchestrator)->Start());
  auto* dispatcher = (*orchestrator)->GetTcpServerForTesting()->GetDispatcher();
  ASSERT_NE(dispatcher, nullptr);

  const auto rows = SnapshotCommandRows();
  ASSERT_FALSE(rows.empty());
  std::set<mygramdb::query::QueryType> seen_types;
  for (const auto& row : rows) {
    mygramdb::query::QueryParser parser;
    auto parsed = parser.Parse(CommandWithArgs(row, row.min_args));
    if (row.handler == "none") {
      EXPECT_FALSE(parsed) << row.text << " is described as having no execution path";
      continue;
    }
    if (!parsed) {
      ADD_FAILURE() << row.text << " with min_args=" << row.min_args << " was rejected: " << parsed.error().message();
      continue;
    }
    EXPECT_TRUE(seen_types.insert(parsed->type).second) << row.text << " parses to a type another row already has";

    if (row.min_args > 0) {
      EXPECT_FALSE(parser.Parse(CommandWithArgs(row, row.min_args - 1))) << row.text << " accepted fewer than min_args";
    }
    if (row.max_args >= 0) {
      EXPECT_FALSE(parser.Parse(CommandWithArgs(row, row.max_args + 1))) << row.text << " accepted more than max_args";
    }

#ifndef USE_MYSQL
    // SYNC handlers exist only in builds with MySQL replication.
    if (row.text.rfind("SYNC", 0) == 0) {
      continue;
    }
#endif
    if (row.handler == "registered") {
      EXPECT_TRUE(dispatcher->HasHandler(parsed->type)) << row.text << " has no registered handler";
    } else if (row.handler == "dispatcher-inline") {
      EXPECT_FALSE(dispatcher->HasHandler(parsed->type)) << row.text << " is routed to a registered handler";
    } else {
      ADD_FAILURE() << row.text << " has an unknown handler binding: " << row.handler;
    }
  }

  EXPECT_TRUE((*orchestrator)->Stop());
  std::filesystem::remove_all(dump_dir);
}

/**
 * @brief Every [cli-flags] spelling is accepted by CommandLineParser as described
 */
TEST(SurfaceSnapshotTest, CliFlagRowsMatchCommandLineParser) {
  static const std::regex kRow(R"re((\S+) argument=(yes|no) config_key=\S+)re");
  const auto lines = SnapshotSection("cli-flags");
  ASSERT_FALSE(lines.empty());

  const auto parse = [](std::vector<std::string> args) {
    std::vector<char*> argv;
    for (auto& arg : args) {
      argv.push_back(arg.data());
    }
    return mygramdb::app::CommandLineParser::Parse(static_cast<int>(argv.size()), argv.data());
  };

  for (const auto& line : lines) {
    std::smatch match;
    if (!std::regex_match(line, match, kRow)) {
      ADD_FAILURE() << "unparsable [cli-flags] row: " << line;
      continue;
    }
    const bool takes_argument = match[2].str() == "yes";
    std::stringstream spellings(match[1].str());
    std::string flag;
    while (std::getline(spellings, flag, ',')) {
      if (takes_argument) {
        EXPECT_TRUE(parse({"mygramdb", "base.yaml", flag, "value.json"})) << flag << " rejected its argument";
        EXPECT_FALSE(parse({"mygramdb", "base.yaml", flag})) << flag << " accepted a missing argument";
        continue;
      }
      EXPECT_TRUE(parse({"mygramdb", "base.yaml", flag})) << flag << " was rejected";
      // A flag without an argument leaves the next token as a second positional,
      // unless it short-circuits parsing altogether.
      auto with_extra = parse({"mygramdb", "base.yaml", flag, "extra.yaml"});
      if (with_extra) {
        EXPECT_TRUE(with_extra->show_help || with_extra->show_version || with_extra->show_surface)
            << flag << " consumed a following token";
      }
    }
  }
}

TEST(SurfaceSnapshotTest, RenderingIsDeterministic) {
  EXPECT_EQ(mygramdb::app::RenderSurfaceSnapshot(), mygramdb::app::RenderSurfaceSnapshot());
}

}  // namespace
