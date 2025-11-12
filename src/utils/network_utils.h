/**
 * @file network_utils.h
 * @brief Network utility functions for IP address and CIDR handling
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace mygramdb::utils {

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
 * @return True if IP is allowed (or if allow_cidrs is empty = allow all)
 */
bool IsIPAllowed(const std::string& ip_str, const std::vector<std::string>& allow_cidrs);

/**
 * @brief Convert IPv4 address to string
 * @param ip_addr IP address in host byte order
 * @return IP address string (e.g., "192.168.1.1")
 */
std::string IPv4ToString(uint32_t ip_addr);

}  // namespace mygramdb::utils
