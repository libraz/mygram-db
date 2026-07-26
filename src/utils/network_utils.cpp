/**
 * @file network_utils.cpp
 * @brief Network utility functions implementation
 */

#include "utils/network_utils.h"

#include <arpa/inet.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <charconv>
#include <cstring>
#include <iterator>
#include <optional>
#include <sstream>

#include "utils/structured_log.h"

namespace mygramdb::utils {

constexpr int kIPv4BitCount = 32;
constexpr int kIPv6BitCount = 128;

bool IsIpv4Mapped(const in6_addr& address) {
  return IN6_IS_ADDR_V4MAPPED(&address) != 0;
}

uint32_t MappedIpv4HostOrder(const in6_addr& address) {
  uint32_t network_order = 0;
  static_assert(sizeof(network_order) == 4);
  std::memcpy(&network_order, &address.s6_addr[12], sizeof(network_order));
  return ntohl(network_order);
}

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
  return family == AF_INET && (ip_addr & netmask) == network;
}

bool CIDR::Contains(const std::string& ip_str) const {
  struct in_addr ipv4_addr = {};
  if (inet_pton(AF_INET, ip_str.c_str(), &ipv4_addr) == 1) {
    return Contains(ntohl(ipv4_addr.s_addr));
  }

  struct in6_addr ipv6_addr = {};
  if (inet_pton(AF_INET6, ip_str.c_str(), &ipv6_addr) != 1) {
    return false;
  }

  // A dual-stack AF_INET6 listener commonly reports IPv4 peers this way.
  // Match the operator's IPv4 ACL instead of forcing a duplicate ::ffff:
  // entry in network.allow_cidrs.
  if (family == AF_INET && IsIpv4Mapped(ipv6_addr)) {
    return Contains(MappedIpv4HostOrder(ipv6_addr));
  }
  if (family != AF_INET6) {
    return false;
  }

  int bits_remaining = prefix_length;
  for (size_t i = 0; i < network_bytes.size(); ++i) {
    if (bits_remaining <= 0) {
      return true;
    }
    const int bits_this_byte = std::min(bits_remaining, 8);
    const uint8_t mask = static_cast<uint8_t>(0xFFU << (8 - bits_this_byte));
    if ((ipv6_addr.s6_addr[i] & mask) != (network_bytes[i] & mask)) {
      return false;
    }
    bits_remaining -= bits_this_byte;
  }
  return true;
}

std::optional<CIDR> CIDR::Parse(const std::string& cidr_str) {
  // Find '/' separator
  size_t slash_pos = cidr_str.find('/');
  if (slash_pos == std::string::npos) {
    return std::nullopt;
  }

  std::string ip_part = cidr_str.substr(0, slash_pos);

  // Parse prefix length part using std::from_chars (no exceptions)
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) - Required for string_view construction
  std::string_view prefix_str(cidr_str.data() + slash_pos + 1, cidr_str.size() - slash_pos - 1);
  int prefix_length = 0;
  auto [ptr, ec] = std::from_chars(prefix_str.data(), prefix_str.data() + prefix_str.size(), prefix_length);
  if (ec != std::errc() || ptr != prefix_str.data() + prefix_str.size()) {
    return std::nullopt;
  }

  auto ip_opt = ParseIPv4(ip_part);
  if (ip_opt) {
    if (prefix_length < 0 || prefix_length > kIPv4BitCount) {
      return std::nullopt;
    }
    uint32_t netmask = 0;
    if (prefix_length > 0) {
      netmask = ~((1U << (kIPv4BitCount - prefix_length)) - 1);
    }
    CIDR cidr;
    cidr.network = ip_opt.value() & netmask;
    cidr.netmask = netmask;
    cidr.prefix_length = prefix_length;
    cidr.family = AF_INET;
    const uint32_t network_order = htonl(cidr.network);
    std::memcpy(cidr.network_bytes.data(), &network_order, sizeof(network_order));
    return cidr;
  }

  struct in6_addr ipv6_addr = {};
  if (inet_pton(AF_INET6, ip_part.c_str(), &ipv6_addr) != 1 || prefix_length < 0 || prefix_length > kIPv6BitCount) {
    return std::nullopt;
  }
  CIDR cidr;
  cidr.network = 0;
  cidr.netmask = 0;
  cidr.prefix_length = prefix_length;
  cidr.family = AF_INET6;
  std::copy(std::begin(ipv6_addr.s6_addr), std::end(ipv6_addr.s6_addr), cidr.network_bytes.begin());
  int bits_remaining = prefix_length;
  for (auto& byte : cidr.network_bytes) {
    if (bits_remaining >= 8) {
      bits_remaining -= 8;
      continue;
    }
    if (bits_remaining > 0) {
      byte &= static_cast<uint8_t>(0xFFU << (8 - bits_remaining));
      bits_remaining = 0;
      continue;
    }
    byte = 0;
  }
  return cidr;
}

bool IsIPAllowed(const std::string& ip_str, const std::vector<std::string>& allow_cidrs) {
  // SECURITY: Default deny when ACL is empty (fail-closed)
  // Users must explicitly configure allowed CIDRs
  if (allow_cidrs.empty()) {
    return false;  // Fail-closed: deny by default
  }

  // Check if IP matches any CIDR
  for (const auto& cidr_str : allow_cidrs) {
    auto cidr = CIDR::Parse(cidr_str);
    if (cidr && cidr->Contains(ip_str)) {
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

  for (const auto& cidr : parsed_allow_cidrs) {
    if (cidr.Contains(ip_str)) {
      return true;
    }
  }

  return false;
}

std::vector<CIDR> ParseAllowCidrs(const std::vector<std::string>& allow_cidrs) {
  std::vector<CIDR> parsed;
  parsed.reserve(allow_cidrs.size());

  for (const auto& cidr_str : allow_cidrs) {
    auto cidr = CIDR::Parse(cidr_str);
    if (!cidr) {
      StructuredLog().Event("invalid_cidr_entry").Field("cidr", cidr_str).Warn();
      continue;
    }
    parsed.push_back(*cidr);
  }

  return parsed;
}

std::string GetPeerIP(int fd) {
  struct sockaddr_storage addr_storage {};
  socklen_t addr_len = sizeof(addr_storage);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - POSIX socket API
  if (getpeername(fd, reinterpret_cast<struct sockaddr*>(&addr_storage), &addr_len) != 0) {
    return "unknown";
  }
  if (addr_storage.ss_family == AF_INET) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - POSIX socket API
    auto* addr_in = reinterpret_cast<struct sockaddr_in*>(&addr_storage);
    // NOLINTBEGIN(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    char ip_buffer[INET_ADDRSTRLEN] = {};
    if (inet_ntop(AF_INET, &addr_in->sin_addr, ip_buffer, sizeof(ip_buffer)) != nullptr) {
      return {ip_buffer};
    }
    // NOLINTEND(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
  } else if (addr_storage.ss_family == AF_INET6) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - POSIX socket API
    auto* addr_in6 = reinterpret_cast<struct sockaddr_in6*>(&addr_storage);
    // NOLINTBEGIN(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    char ip_buffer[INET6_ADDRSTRLEN] = {};
    if (inet_ntop(AF_INET6, &addr_in6->sin6_addr, ip_buffer, sizeof(ip_buffer)) != nullptr) {
      return {ip_buffer};
    }
    // NOLINTEND(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
  } else if (addr_storage.ss_family == AF_UNIX) {
    // Unix-domain peers have no IP address, but they still need a stable
    // identity for per-client accounting such as the TCP RequestDispatcher's
    // rate limiter. All clients on the same local socket deliberately share
    // one bucket; filesystem ownership/mode remains the authorization gate.
    return "unix";
  }
  return "unknown";
}

}  // namespace mygramdb::utils
