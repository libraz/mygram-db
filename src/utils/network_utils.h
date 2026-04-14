/**
 * @file network_utils.h
 * @brief Network utility functions for IP address and CIDR handling
 */

#pragma once

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace mygram::utils {

/**
 * @brief Helper to safely cast sockaddr_in* to sockaddr* for socket API
 *
 * POSIX socket API requires sockaddr* but we use sockaddr_in for IPv4.
 * This helper centralizes the required reinterpret_cast to a single location.
 *
 * @param addr Pointer to sockaddr_in structure
 * @return Pointer to sockaddr (same memory location, different type)
 */
// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
inline struct sockaddr* ToSockaddr(struct sockaddr_in* addr) {
  return reinterpret_cast<struct sockaddr*>(addr);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}

/**
 * @brief Helper to safely cast sockaddr_un* to sockaddr* for socket API
 *
 * @param addr Pointer to sockaddr_un structure
 * @return Pointer to sockaddr (same memory location, different type)
 */
// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
inline struct sockaddr* ToSockaddrUn(struct sockaddr_un* addr) {
  return reinterpret_cast<struct sockaddr*>(addr);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}

/**
 * @brief CIDR (Classless Inter-Domain Routing) representation
 */
struct CIDR {
  uint32_t network;   // Network address in host byte order
  uint32_t netmask;   // Network mask in host byte order
  int prefix_length;  // Prefix length (0-32)

  /**
   * @brief Check if an IP address is within this CIDR range
   * @param ip_addr IP address in host byte order
   * @return True if IP is within CIDR range
   */
  [[nodiscard]] bool Contains(uint32_t ip_addr) const;

  /**
   * @brief Parse CIDR notation string
   * @param cidr_str CIDR string (e.g., "192.168.1.0/24")
   * @return CIDR object if valid, nullopt otherwise
   */
  static std::optional<CIDR> Parse(const std::string& cidr_str);
};

/**
 * @brief Parse IPv4 address string to uint32_t (host byte order)
 * @param ip_str IP address string (e.g., "192.168.1.1")
 * @return IP address in host byte order, or nullopt if invalid
 */
std::optional<uint32_t> ParseIPv4(const std::string& ip_str);

/**
 * @brief Check if an IP address is allowed by CIDR list
 * @param ip_str IP address string (e.g., "192.168.1.1")
 * @param allow_cidrs List of allowed CIDR ranges
 * @return True if IP is allowed. Returns false (deny) if allow_cidrs is empty (fail-closed)
 */
bool IsIPAllowed(const std::string& ip_str, const std::vector<std::string>& allow_cidrs);

/**
 * @brief Check if an IP address is allowed using pre-parsed CIDR list
 * @param ip_str IP address string
 * @param parsed_allow_cidrs Parsed CIDR list
 * @return True if IP allowed. Returns false (deny) if list is empty (fail-closed)
 */
bool IsIPAllowed(const std::string& ip_str, const std::vector<CIDR>& parsed_allow_cidrs);

/**
 * @brief Convert IPv4 address to string
 * @param ip_addr IP address in host byte order
 * @return IP address string (e.g., "192.168.1.1")
 */
std::string IPv4ToString(uint32_t ip_addr);

/**
 * @brief Parse a list of CIDR strings, logging warnings for invalid entries
 * @param allow_cidrs List of CIDR notation strings (e.g., {"192.168.1.0/24", "10.0.0.0/8"})
 * @return Vector of successfully parsed CIDR objects (invalid entries are skipped)
 */
std::vector<CIDR> ParseAllowCidrs(const std::vector<std::string>& allow_cidrs);

/**
 * @brief Extract peer IP address from a connected socket file descriptor
 *
 * Uses getpeername() and inet_ntop() to resolve the peer IP.
 * Supports both IPv4 (AF_INET) and IPv6 (AF_INET6).
 *
 * @param fd Connected socket file descriptor
 * @return Peer IP address string, or "unknown" on failure
 */
std::string GetPeerIP(int fd);

}  // namespace mygram::utils
