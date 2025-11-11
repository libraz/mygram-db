/**
 * @file network_utils.cpp
 * @brief Network utility functions implementation
 */

#include "utils/network_utils.h"

#include <arpa/inet.h>

#include <optional>
#include <sstream>

namespace mygramdb {
namespace utils {

std::optional<uint32_t> ParseIPv4(const std::string& ip_str) {
  struct in_addr addr = {};
  if (inet_pton(AF_INET, ip_str.c_str(), &addr) != 1) {
    return std::nullopt;
  }
  // Convert from network byte order to host byte order
  return ntohl(addr.s_addr);
}

std::string IPv4ToString(uint32_t ip_address) {
  struct in_addr addr = {};
  addr.s_addr = htonl(ip_address);  // Convert to network byte order
  // NOLINTBEGIN(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
  char buf[INET_ADDRSTRLEN];
  if (inet_ntop(AF_INET, &addr, buf, sizeof(buf)) == nullptr) {
    return "";
  }
  return {buf};
  // NOLINTEND(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
}

bool CIDR::Contains(uint32_t ip_address) const {
  return (ip_address & netmask) == network;
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

  // Parse prefix length part
  std::string prefix_str = cidr_str.substr(slash_pos + 1);
  int prefix_length = 0;
  try {
    prefix_length = std::stoi(prefix_str);
  } catch (...) {
    return std::nullopt;
  }

  // Validate prefix length
  if (prefix_length < 0 || prefix_length > 32) {
    return std::nullopt;
  }

  // Calculate netmask
  uint32_t netmask = 0;
  if (prefix_length > 0) {
    netmask = ~((1U << (32 - prefix_length)) - 1);
  }

  // Calculate network address
  uint32_t network = ip_opt.value() & netmask;

  CIDR cidr = {network, netmask, prefix_length};

  return cidr;
}

bool IsIPAllowed(const std::string& ip_str, const std::vector<std::string>& allow_cidrs) {
  // If allow_cidrs is empty, allow all
  if (allow_cidrs.empty()) {
    return true;
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

}  // namespace utils
}  // namespace mygramdb
