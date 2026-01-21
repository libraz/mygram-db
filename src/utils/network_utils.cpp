/**
 * @file network_utils.cpp
 * @brief Network utility functions implementation
 */

#include "utils/network_utils.h"

#include <arpa/inet.h>
#include <spdlog/spdlog.h>

#include <charconv>
#include <optional>
#include <sstream>

namespace mygramdb::utils {

constexpr int kIPv4BitCount = 32;

std::optional<uint32_t> ParseIPv4(const std::string& ip_str) {
  struct in_addr addr = {};
  if (inet_pton(AF_INET, ip_str.c_str(), &addr) != 1) {
    return std::nullopt;
  }
  // Convert from network byte order to host byte order
  return ntohl(addr.s_addr);
}

std::string IPv4ToString(uint32_t ip_addr) {
  struct in_addr addr = {};
  addr.s_addr = htonl(ip_addr);  // Convert to network byte order
  // NOLINTBEGIN(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
  char buf[INET_ADDRSTRLEN];
  if (inet_ntop(AF_INET, &addr, buf, sizeof(buf)) == nullptr) {
    return "";
  }
  return {buf};
  // NOLINTEND(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
}

bool CIDR::Contains(uint32_t ip_addr) const {
  return (ip_addr & netmask) == network;
}

std::optional<CIDR> CIDR::Parse(const std::string& cidr_str) {
  // Find '/' separator
  size_t slash_pos = cidr_str.find('/');
  if (slash_pos == std::string::npos) {
    return std::nullopt;
  }

  // Parse IP address part
  std::string ip_part = cidr_str.substr(0, slash_pos);
  auto ip_opt = ParseIPv4(ip_part);
  if (!ip_opt) {
    return std::nullopt;
  }

  // Parse prefix length part using std::from_chars (no exceptions)
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) - Required for string_view construction
  std::string_view prefix_str(cidr_str.data() + slash_pos + 1, cidr_str.size() - slash_pos - 1);
  int prefix_length = 0;
  auto [ptr, ec] = std::from_chars(prefix_str.data(), prefix_str.data() + prefix_str.size(), prefix_length);
  if (ec != std::errc() || ptr != prefix_str.data() + prefix_str.size()) {
    return std::nullopt;
  }

  // Validate prefix length
  if (prefix_length < 0 || prefix_length > kIPv4BitCount) {
    return std::nullopt;
  }

  // Calculate netmask
  uint32_t netmask = 0;
  if (prefix_length > 0) {
    netmask = ~((1U << (kIPv4BitCount - prefix_length)) - 1);
  }

  // Calculate network address
  uint32_t network = ip_opt.value() & netmask;

  CIDR cidr = {network, netmask, prefix_length};

  return cidr;
}

bool IsIPAllowed(const std::string& ip_str, const std::vector<std::string>& allow_cidrs) {
  // SECURITY: Default deny when ACL is empty (fail-closed)
  // Users must explicitly configure allowed CIDRs
  if (allow_cidrs.empty()) {
    return false;  // Fail-closed: deny by default
  }

  // Parse client IP
  auto client_ip = ParseIPv4(ip_str);
  if (!client_ip) {
    // Invalid IP format, deny by default
    return false;
  }

  // Check if IP matches any CIDR
  for (const auto& cidr_str : allow_cidrs) {
    auto cidr = CIDR::Parse(cidr_str);
    if (cidr && cidr->Contains(client_ip.value())) {
      return true;
    }
  }

  // IP not in any allowed CIDR range
  return false;
}

bool IsIPAllowed(const std::string& ip_str, const std::vector<CIDR>& parsed_allow_cidrs) {
  // SECURITY: Default deny when ACL is empty (fail-closed)
  // Users must explicitly configure allowed CIDRs
  if (parsed_allow_cidrs.empty()) {
    // Note: We don't log here to avoid issues during static initialization
    // or test discovery. The server initialization code should log this warning.
    return false;  // Fail-closed: deny by default
  }

  auto client_ip = ParseIPv4(ip_str);
  if (!client_ip) {
    return false;
  }

  for (const auto& cidr : parsed_allow_cidrs) {
    if (cidr.Contains(client_ip.value())) {
      return true;
    }
  }

  return false;
}

}  // namespace mygramdb::utils
