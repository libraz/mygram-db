/**
 * @file compose_port_binding_test.cpp
 * @brief Asserts every published port in the shipped compose files stays on loopback
 *
 * A published Docker port installs a DNAT rule, so a compose entry without an
 * explicit host interface reaches the whole LAN regardless of any host
 * firewall. The shipped stacks carry credentials that are only safe on the
 * machine running them, so the files are parsed here rather than reviewed by
 * hand.
 */

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <filesystem>
#include <string>
#include <vector>

namespace {

/// Compose files distributed with the project, relative to the source root.
const std::vector<std::string>& ShippedComposeFiles() {
  static const std::vector<std::string> kFiles = {
      "docker-compose.yml",
      "docker-compose.bench.yml",
  };
  return kFiles;
}

std::filesystem::path SourceRoot() {
  return std::filesystem::path(MYGRAMDB_SOURCE_DIR);
}

/**
 * @brief Split a short-syntax port entry on the colons that separate its fields
 *
 * Colons inside a `${VAR:-default}` substitution belong to the substitution,
 * not to the entry, so they must not create a field boundary.
 */
std::vector<std::string> SplitPortFields(const std::string& entry) {
  std::vector<std::string> fields;
  std::string current;
  int depth = 0;
  for (size_t i = 0; i < entry.size(); ++i) {
    const char character = entry[i];
    if (character == '$' && i + 1 < entry.size() && entry[i + 1] == '{') {
      ++depth;
      current += character;
      continue;
    }
    if (character == '}' && depth > 0) {
      --depth;
      current += character;
      continue;
    }
    if (character == ':' && depth == 0) {
      fields.push_back(current);
      current.clear();
      continue;
    }
    current += character;
  }
  fields.push_back(current);
  return fields;
}

/**
 * @brief Resolve `${VAR:-default}` / `${VAR-default}` to the default it expands to
 *
 * A host interface that depends on an unset variable is reported as its
 * default, because that is what an operator who copies `.env.example` gets.
 * A substitution without a default resolves to an empty string, which is a
 * wildcard bind and therefore rejected.
 */
std::string ResolveDefault(const std::string& value) {
  if (value.rfind("${", 0) != 0 || value.back() != '}') {
    return value;
  }
  const std::string inner = value.substr(2, value.size() - 3);
  const size_t separator = inner.find('-');
  if (separator == std::string::npos) {
    return "";
  }
  return inner.substr(separator + 1);
}

bool IsLoopbackHostInterface(const std::string& host_interface) {
  const std::string resolved = ResolveDefault(host_interface);
  if (resolved == "localhost" || resolved == "::1" || resolved == "[::1]") {
    return true;
  }
  return resolved.rfind("127.", 0) == 0;
}

/**
 * @brief Extract the host interface of a short-syntax `ports:` entry
 * @return The host interface, or an empty string when the entry publishes to
 *         every interface
 */
std::string HostInterfaceOf(const std::string& entry) {
  std::string without_protocol = entry;
  const size_t protocol = without_protocol.rfind('/');
  if (protocol != std::string::npos) {
    without_protocol = without_protocol.substr(0, protocol);
  }

  if (!without_protocol.empty() && without_protocol.front() == '[') {
    const size_t closing = without_protocol.find(']');
    if (closing != std::string::npos) {
      return without_protocol.substr(0, closing + 1);
    }
  }

  const auto fields = SplitPortFields(without_protocol);
  // "hostIp:hostPort:containerPort" is the only short form that names an
  // interface; "hostPort:containerPort" and a bare "containerPort" do not.
  return fields.size() >= 3 ? fields.front() : std::string();
}

void ExpectEntryBindsLoopback(const std::string& file, const std::string& service, const std::string& entry) {
  const std::string host_interface = HostInterfaceOf(entry);
  EXPECT_FALSE(host_interface.empty())
      << file << " service '" << service << "' publishes \"" << entry
      << "\" on every interface; prefix the host interface with 127.0.0.1 or drop the publish";
  if (host_interface.empty()) {
    return;
  }
  EXPECT_TRUE(IsLoopbackHostInterface(host_interface)) << file << " service '" << service << "' publishes \"" << entry
                                                       << "\" on non-loopback interface '" << host_interface << "'";
}

}  // namespace

TEST(ComposePortBindingTest, ShippedComposeFilesPublishOnlyToLoopback) {
  for (const auto& file : ShippedComposeFiles()) {
    const auto path = SourceRoot() / file;
    ASSERT_TRUE(std::filesystem::exists(path)) << "missing compose file: " << path;

    const YAML::Node compose = YAML::LoadFile(path.string());
    const YAML::Node services = compose["services"];
    ASSERT_TRUE(services && services.IsMap()) << file << " has no services map";

    for (const auto& service_entry : services) {
      const std::string service = service_entry.first.as<std::string>();
      const YAML::Node ports = service_entry.second["ports"];
      if (!ports || !ports.IsSequence()) {
        continue;
      }
      for (const auto& port : ports) {
        if (port.IsMap()) {
          // Long syntax: an omitted host_ip publishes on every interface.
          const YAML::Node host_ip = port["host_ip"];
          const std::string described = host_ip ? host_ip.as<std::string>() : std::string();
          EXPECT_TRUE(!described.empty() && IsLoopbackHostInterface(described))
              << file << " service '" << service << "' publishes a port without a loopback host_ip";
          continue;
        }
        ExpectEntryBindsLoopback(file, service, port.as<std::string>());
      }
    }
  }
}

TEST(ComposePortBindingTest, HostInterfaceExtractionCoversShortSyntaxForms) {
  EXPECT_EQ(HostInterfaceOf("3306:3306"), "");
  EXPECT_EQ(HostInterfaceOf("${MYSQL_PORT:-3306}:3306"), "");
  EXPECT_EQ(HostInterfaceOf("11016"), "");
  EXPECT_EQ(HostInterfaceOf("127.0.0.1:${MYSQL_PORT:-3306}:3306"), "127.0.0.1");
  EXPECT_EQ(HostInterfaceOf("0.0.0.0:3306:3306/tcp"), "0.0.0.0");
  EXPECT_EQ(HostInterfaceOf("[::1]:8080:8080"), "[::1]");

  EXPECT_TRUE(IsLoopbackHostInterface("${MYGRAMDB_BIND_HOST:-127.0.0.1}"));
  EXPECT_FALSE(IsLoopbackHostInterface("${MYGRAMDB_BIND_HOST:-0.0.0.0}"));
  EXPECT_FALSE(IsLoopbackHostInterface("${MYGRAMDB_BIND_HOST}"));
}
