/**
 * @file compose_secret_exposure_test.cpp
 * @brief Asserts no shipped container artifact puts a secret on a command line
 *
 * `/proc/<pid>/cmdline` is readable by any UID sharing the PID namespace, while
 * `/proc/<pid>/environ` is restricted to the process owner. A periodic
 * healthcheck that authenticates with `-p<password>` therefore republishes the
 * credential in the process table on every interval. Compose additionally
 * interpolates a single `$` at parse time, so such a value is also baked into
 * the service definition the daemon stores.
 *
 * The shipped compose files and MySQL init scripts are parsed here rather than
 * reviewed by hand, because the failure is invisible in a working deployment.
 */

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <cctype>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace {

/// Container artifacts distributed with the project, relative to the source root.
const std::vector<std::string>& ShippedComposeFiles() {
  static const std::vector<std::string> kFiles = {
      "docker-compose.yml",
      "docker-compose.bench.yml",
      "support/testing/docker-compose.rpm-test.yml",
      "e2e/docker/docker-compose.yml",
      "e2e/docker/docker-compose.mariadb.yml",
  };
  return kFiles;
}

/// Shell scripts the MySQL images run as init hooks, relative to the source root.
const std::vector<std::string>& ShippedMysqlInitScripts() {
  static const std::vector<std::string> kFiles = {
      "support/docker/mysql/init/02-grant-replication.sh",
      "support/docker/mysql/bench-init/02-load-seed.sh",
  };
  return kFiles;
}

/// Service keys whose entries become the argv of a process the daemon starts.
const std::vector<std::string>& ArgvBearingKeys() {
  static const std::vector<std::string> kKeys = {"healthcheck", "command", "entrypoint"};
  return kKeys;
}

std::filesystem::path SourceRoot() {
  return std::filesystem::path(MYGRAMDB_SOURCE_DIR);
}

bool NamesASecret(const std::string& variable) {
  static const std::vector<std::string> kMarkers = {"PASSWORD", "PASSWD", "SECRET", "TOKEN", "PWD"};
  for (const auto& marker : kMarkers) {
    if (variable.find(marker) != std::string::npos) {
      return true;
    }
  }
  return false;
}

/**
 * @brief Find a `${VAR}` / `$VAR` substitution Compose expands while parsing
 *
 * `$$` is Compose's escape for a literal dollar, so the variable behind it is
 * expanded by the shell inside the container and never reaches the daemon.
 *
 * @return The offending variable name, or an empty string when there is none
 */
std::string ParseTimeSecretSubstitution(const std::string& text) {
  for (size_t i = 0; i < text.size(); ++i) {
    if (text[i] != '$') {
      continue;
    }
    if (i + 1 < text.size() && text[i + 1] == '$') {
      ++i;
      continue;
    }
    size_t start = i + 1;
    if (start < text.size() && text[start] == '{') {
      ++start;
    }
    size_t end = start;
    while (end < text.size() && (std::isalnum(static_cast<unsigned char>(text[end])) != 0 || text[end] == '_')) {
      ++end;
    }
    const std::string variable = text.substr(start, end - start);
    if (NamesASecret(variable)) {
      return variable;
    }
  }
  return "";
}

/**
 * @brief Detect a MySQL client password supplied as a command-line argument
 *
 * A bare `-p` prompts instead of carrying a value, so only an option with an
 * attached value is reported.
 */
bool CarriesInlinePasswordOption(const std::string& text) {
  for (size_t i = 0; i + 2 < text.size(); ++i) {
    const bool starts_token = (i == 0) || (std::isspace(static_cast<unsigned char>(text[i - 1])) != 0);
    if (!starts_token || text[i] != '-' || text[i + 1] != 'p') {
      continue;
    }
    if (std::isspace(static_cast<unsigned char>(text[i + 2])) == 0) {
      return true;
    }
  }
  const size_t long_option = text.find("--password=");
  return long_option != std::string::npos && long_option + 11 < text.size() &&
         std::isspace(static_cast<unsigned char>(text[long_option + 11])) == 0;
}

void ExpectNoSecretInArgv(const std::string& file, const std::string& service, const std::string& key,
                          const std::string& entry) {
  const std::string substitution = ParseTimeSecretSubstitution(entry);
  EXPECT_TRUE(substitution.empty()) << file << " service '" << service << "' expands $" << substitution << " into its "
                                    << key << " at parse time; escape it as $$" << substitution
                                    << " and pass it through the process environment";
  EXPECT_FALSE(CarriesInlinePasswordOption(entry))
      << file << " service '" << service << "' passes a password as an argument in its " << key << ": \"" << entry
      << "\"; use MYSQL_PWD instead";
}

void ExpectNodeHasNoSecretInArgv(const std::string& file, const std::string& service, const std::string& key,
                                 const YAML::Node& node) {
  if (!node) {
    return;
  }
  if (node.IsScalar()) {
    ExpectNoSecretInArgv(file, service, key, node.as<std::string>());
    return;
  }
  if (node.IsSequence()) {
    for (const auto& element : node) {
      ExpectNodeHasNoSecretInArgv(file, service, key, element);
    }
    return;
  }
  if (node.IsMap()) {
    // `healthcheck:` is a map whose `test:` holds the probe argv.
    ExpectNodeHasNoSecretInArgv(file, service, key, node["test"]);
  }
}

}  // namespace

TEST(ComposeSecretExposureTest, ShippedComposeFilesKeepSecretsOutOfProcessArgv) {
  for (const auto& file : ShippedComposeFiles()) {
    const auto path = SourceRoot() / file;
    ASSERT_TRUE(std::filesystem::exists(path)) << "missing compose file: " << path;

    const YAML::Node compose = YAML::LoadFile(path.string());
    const YAML::Node services = compose["services"];
    ASSERT_TRUE(services && services.IsMap()) << file << " has no services map";

    for (const auto& service_entry : services) {
      const std::string service = service_entry.first.as<std::string>();
      for (const auto& key : ArgvBearingKeys()) {
        ExpectNodeHasNoSecretInArgv(file, service, key, service_entry.second[key]);
      }
    }
  }
}

TEST(ComposeSecretExposureTest, ShippedMysqlInitScriptsKeepSecretsOutOfProcessArgv) {
  for (const auto& file : ShippedMysqlInitScripts()) {
    const auto path = SourceRoot() / file;
    ASSERT_TRUE(std::filesystem::exists(path)) << "missing init script: " << path;

    std::ifstream script(path);
    ASSERT_TRUE(script) << "cannot read init script: " << path;

    std::string line;
    size_t line_number = 0;
    while (std::getline(script, line)) {
      ++line_number;
      const size_t first_glyph = line.find_first_not_of(" \t");
      if (first_glyph == std::string::npos || line[first_glyph] == '#') {
        continue;
      }
      EXPECT_FALSE(CarriesInlinePasswordOption(line))
          << file << ":" << line_number << " passes a password as an argument: \"" << line
          << "\"; use MYSQL_PWD instead";
    }
  }
}
