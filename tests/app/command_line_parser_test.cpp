/**
 * @file command_line_parser_test.cpp
 * @brief Unit tests for CommandLineParser class
 *
 * Tests command-line argument parsing including flags, options, and error cases.
 */

#include "app/command_line_parser.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace mygramdb::app;

namespace {

/**
 * @brief Helper class to build argc/argv for testing
 */
class ArgvBuilder {
 public:
  explicit ArgvBuilder(const std::string& program_name = "mygramdb") { args_.push_back(program_name); }

  ArgvBuilder& Add(const std::string& arg) {
    args_.push_back(arg);
    return *this;
  }

  int argc() const { return static_cast<int>(args_.size()); }

  // Returns pointer to array of char* (valid as long as ArgvBuilder exists)
  char** argv() {
    ptrs_.clear();
    ptrs_.reserve(args_.size());
    for (auto& arg : args_) {
      ptrs_.push_back(arg.data());
    }
    return ptrs_.data();
  }

 private:
  std::vector<std::string> args_;
  std::vector<char*> ptrs_;
};

}  // namespace

class CommandLineParserTest : public ::testing::Test {};

// ===========================================================================
// Help and version flags
// ===========================================================================

TEST_F(CommandLineParserTest, ShortHelpFlag) {
  ArgvBuilder builder;
  builder.Add("-h");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->show_help);
  EXPECT_FALSE(result->show_version);
}

TEST_F(CommandLineParserTest, LongHelpFlag) {
  ArgvBuilder builder;
  builder.Add("--help");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->show_help);
}

TEST_F(CommandLineParserTest, ShortVersionFlag) {
  ArgvBuilder builder;
  builder.Add("-v");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->show_version);
  EXPECT_FALSE(result->show_help);
}

TEST_F(CommandLineParserTest, LongVersionFlag) {
  ArgvBuilder builder;
  builder.Add("--version");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->show_version);
}

TEST_F(CommandLineParserTest, HelpFlagIgnoresOtherArgs) {
  // Help flag should cause early return, ignoring other arguments
  ArgvBuilder builder;
  builder.Add("--help").Add("--config").Add("file.yaml");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->show_help);
  // Config file should NOT be parsed when help flag is present
  EXPECT_TRUE(result->config_file.empty());
}

// ===========================================================================
// Config file options
// ===========================================================================

TEST_F(CommandLineParserTest, ShortConfigOption) {
  ArgvBuilder builder;
  builder.Add("-c").Add("config.yaml");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->config_file, "config.yaml");
}

TEST_F(CommandLineParserTest, LongConfigOption) {
  ArgvBuilder builder;
  builder.Add("--config").Add("config.yaml");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->config_file, "config.yaml");
}

TEST_F(CommandLineParserTest, PositionalConfigFile) {
  // Backward compatibility: config file without -c flag
  ArgvBuilder builder;
  builder.Add("config.yaml");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->config_file, "config.yaml");
}

TEST_F(CommandLineParserTest, ConfigOptionMissingValue) {
  ArgvBuilder builder;
  builder.Add("-c");  // No file path provided

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("requires"), std::string::npos);
}

TEST_F(CommandLineParserTest, LongConfigOptionMissingValue) {
  ArgvBuilder builder;
  builder.Add("--config");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_FALSE(result.has_value());
}

// ===========================================================================
// Daemon mode
// ===========================================================================

TEST_F(CommandLineParserTest, ShortDaemonFlag) {
  ArgvBuilder builder;
  builder.Add("-d").Add("config.yaml");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->daemon_mode);
}

TEST_F(CommandLineParserTest, LongDaemonFlag) {
  ArgvBuilder builder;
  builder.Add("--daemon").Add("config.yaml");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->daemon_mode);
}

// ===========================================================================
// Config test mode
// ===========================================================================

TEST_F(CommandLineParserTest, ShortConfigTestFlag) {
  ArgvBuilder builder;
  builder.Add("-t").Add("config.yaml");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->config_test_mode);
}

TEST_F(CommandLineParserTest, LongConfigTestFlag) {
  ArgvBuilder builder;
  builder.Add("--config-test").Add("config.yaml");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->config_test_mode);
}

// ===========================================================================
// Schema file option
// ===========================================================================

TEST_F(CommandLineParserTest, ShortSchemaOption) {
  ArgvBuilder builder;
  builder.Add("-s").Add("schema.json").Add("config.yaml");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->schema_file, "schema.json");
  EXPECT_EQ(result->config_file, "config.yaml");
}

TEST_F(CommandLineParserTest, LongSchemaOption) {
  ArgvBuilder builder;
  builder.Add("--schema").Add("schema.json").Add("config.yaml");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->schema_file, "schema.json");
}

TEST_F(CommandLineParserTest, SchemaOptionMissingValue) {
  ArgvBuilder builder;
  builder.Add("-s");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("requires"), std::string::npos);
}

// ===========================================================================
// Combined options
// ===========================================================================

TEST_F(CommandLineParserTest, AllOptionsShortForm) {
  ArgvBuilder builder;
  builder.Add("-c").Add("config.yaml").Add("-d").Add("-t").Add("-s").Add("schema.json");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->config_file, "config.yaml");
  EXPECT_EQ(result->schema_file, "schema.json");
  EXPECT_TRUE(result->daemon_mode);
  EXPECT_TRUE(result->config_test_mode);
}

TEST_F(CommandLineParserTest, AllOptionsLongForm) {
  ArgvBuilder builder;
  builder.Add("--config").Add("config.yaml").Add("--daemon").Add("--config-test").Add("--schema").Add("schema.json");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->config_file, "config.yaml");
  EXPECT_EQ(result->schema_file, "schema.json");
  EXPECT_TRUE(result->daemon_mode);
  EXPECT_TRUE(result->config_test_mode);
}

TEST_F(CommandLineParserTest, MixedShortAndLongOptions) {
  ArgvBuilder builder;
  builder.Add("-c").Add("config.yaml").Add("--daemon").Add("-t");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->config_file, "config.yaml");
  EXPECT_TRUE(result->daemon_mode);
  EXPECT_TRUE(result->config_test_mode);
}

// ===========================================================================
// Error cases
// ===========================================================================

TEST_F(CommandLineParserTest, NoArguments) {
  ArgvBuilder builder;
  // Only program name, no other arguments

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("No arguments"), std::string::npos);
}

TEST_F(CommandLineParserTest, UnknownShortOption) {
  ArgvBuilder builder;
  builder.Add("-x").Add("config.yaml");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("Unknown option"), std::string::npos);
  EXPECT_NE(result.error().message().find("-x"), std::string::npos);
}

TEST_F(CommandLineParserTest, UnknownLongOption) {
  ArgvBuilder builder;
  builder.Add("--unknown").Add("config.yaml");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("Unknown option"), std::string::npos);
  EXPECT_NE(result.error().message().find("--unknown"), std::string::npos);
}

TEST_F(CommandLineParserTest, DuplicatePositionalArgument) {
  // Two positional config files
  ArgvBuilder builder;
  builder.Add("config1.yaml").Add("config2.yaml");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("Unexpected positional"), std::string::npos);
}

TEST_F(CommandLineParserTest, MissingConfigFile) {
  // Only daemon flag, no config file
  ArgvBuilder builder;
  builder.Add("-d");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("Configuration file"), std::string::npos);
}

TEST_F(CommandLineParserTest, InvalidArgcZero) {
  // argc < 1 is invalid
  char* empty_argv[] = {nullptr};

  auto result = CommandLineParser::Parse(0, empty_argv);

  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("argc"), std::string::npos);
}

// ===========================================================================
// Edge cases
// ===========================================================================

TEST_F(CommandLineParserTest, ConfigFileWithSpaces) {
  ArgvBuilder builder;
  builder.Add("-c").Add("path with spaces/config.yaml");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->config_file, "path with spaces/config.yaml");
}

TEST_F(CommandLineParserTest, ConfigFileWithDash) {
  // File name starting with dash could be confused with option
  ArgvBuilder builder;
  builder.Add("-c").Add("-config.yaml");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->config_file, "-config.yaml");
}

TEST_F(CommandLineParserTest, EmptyConfigFileName) {
  ArgvBuilder builder;
  builder.Add("-c").Add("");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  // Empty string config file is rejected
  // The parser requires a non-empty config file
  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("Configuration file"), std::string::npos);
}

TEST_F(CommandLineParserTest, OptionsOrderDoesNotMatter) {
  // Config at the end
  ArgvBuilder builder1;
  builder1.Add("-d").Add("-t").Add("-c").Add("config.yaml");

  auto result1 = CommandLineParser::Parse(builder1.argc(), builder1.argv());
  ASSERT_TRUE(result1.has_value());
  EXPECT_EQ(result1->config_file, "config.yaml");
  EXPECT_TRUE(result1->daemon_mode);
  EXPECT_TRUE(result1->config_test_mode);

  // Config at the beginning
  ArgvBuilder builder2;
  builder2.Add("-c").Add("config.yaml").Add("-d").Add("-t");

  auto result2 = CommandLineParser::Parse(builder2.argc(), builder2.argv());
  ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(result2->config_file, "config.yaml");
  EXPECT_TRUE(result2->daemon_mode);
  EXPECT_TRUE(result2->config_test_mode);
}

TEST_F(CommandLineParserTest, PositionalConfigWithFlags) {
  // Positional config file mixed with flags
  ArgvBuilder builder;
  builder.Add("-d").Add("config.yaml").Add("-t");

  auto result = CommandLineParser::Parse(builder.argc(), builder.argv());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->config_file, "config.yaml");
  EXPECT_TRUE(result->daemon_mode);
  EXPECT_TRUE(result->config_test_mode);
}
