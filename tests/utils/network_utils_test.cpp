/**
 * @file network_utils_test.cpp
 * @brief Tests for network utility functions
 */

#include "utils/network_utils.h"

#include <gtest/gtest.h>

namespace mygramdb {
namespace utils {

TEST(NetworkUtilsTest, ParseIPv4_Valid) {
  auto ip = ParseIPv4("192.168.1.1");
  ASSERT_TRUE(ip.has_value());
  EXPECT_EQ(ip.value(), 0xC0A80101);  // 192.168.1.1 in hex

  ip = ParseIPv4("127.0.0.1");
  ASSERT_TRUE(ip.has_value());
  EXPECT_EQ(ip.value(), 0x7F000001);  // 127.0.0.1 in hex

  ip = ParseIPv4("0.0.0.0");
  ASSERT_TRUE(ip.has_value());
  EXPECT_EQ(ip.value(), 0x00000000);

  ip = ParseIPv4("255.255.255.255");
  ASSERT_TRUE(ip.has_value());
  EXPECT_EQ(ip.value(), 0xFFFFFFFF);
}

TEST(NetworkUtilsTest, ParseIPv4_Invalid) {
  EXPECT_FALSE(ParseIPv4("").has_value());
  EXPECT_FALSE(ParseIPv4("192.168.1").has_value());
  EXPECT_FALSE(ParseIPv4("192.168.1.256").has_value());
  EXPECT_FALSE(ParseIPv4("192.168.1.1.1").has_value());
  EXPECT_FALSE(ParseIPv4("not-an-ip").has_value());
  EXPECT_FALSE(ParseIPv4("192.168.-1.1").has_value());
}

TEST(NetworkUtilsTest, IPv4ToString) {
  EXPECT_EQ(IPv4ToString(0xC0A80101), "192.168.1.1");
  EXPECT_EQ(IPv4ToString(0x7F000001), "127.0.0.1");
  EXPECT_EQ(IPv4ToString(0x00000000), "0.0.0.0");
  EXPECT_EQ(IPv4ToString(0xFFFFFFFF), "255.255.255.255");
}

TEST(NetworkUtilsTest, CIDR_Parse_Valid) {
  auto cidr = CIDR::Parse("192.168.1.0/24");
  ASSERT_TRUE(cidr.has_value());
  EXPECT_EQ(cidr->network, 0xC0A80100);  // 192.168.1.0
  EXPECT_EQ(cidr->netmask, 0xFFFFFF00);  // 255.255.255.0
  EXPECT_EQ(cidr->prefix_length, 24);

  cidr = CIDR::Parse("10.0.0.0/8");
  ASSERT_TRUE(cidr.has_value());
  EXPECT_EQ(cidr->network, 0x0A000000);  // 10.0.0.0
  EXPECT_EQ(cidr->netmask, 0xFF000000);  // 255.0.0.0
  EXPECT_EQ(cidr->prefix_length, 8);

  cidr = CIDR::Parse("172.16.0.0/16");
  ASSERT_TRUE(cidr.has_value());
  EXPECT_EQ(cidr->network, 0xAC100000);  // 172.16.0.0
  EXPECT_EQ(cidr->netmask, 0xFFFF0000);  // 255.255.0.0
  EXPECT_EQ(cidr->prefix_length, 16);

  cidr = CIDR::Parse("0.0.0.0/0");
  ASSERT_TRUE(cidr.has_value());
  EXPECT_EQ(cidr->network, 0x00000000);
  EXPECT_EQ(cidr->netmask, 0x00000000);
  EXPECT_EQ(cidr->prefix_length, 0);

  cidr = CIDR::Parse("192.168.1.128/32");
  ASSERT_TRUE(cidr.has_value());
  EXPECT_EQ(cidr->network, 0xC0A80180);
  EXPECT_EQ(cidr->netmask, 0xFFFFFFFF);
  EXPECT_EQ(cidr->prefix_length, 32);
}

TEST(NetworkUtilsTest, CIDR_Parse_Invalid) {
  EXPECT_FALSE(CIDR::Parse("").has_value());
  EXPECT_FALSE(CIDR::Parse("192.168.1.0").has_value());     // No prefix
  EXPECT_FALSE(CIDR::Parse("192.168.1.0/").has_value());    // Empty prefix
  EXPECT_FALSE(CIDR::Parse("192.168.1.0/33").has_value());  // Invalid prefix
  EXPECT_FALSE(CIDR::Parse("192.168.1.0/-1").has_value());  // Negative prefix
  EXPECT_FALSE(CIDR::Parse("not-an-ip/24").has_value());
  EXPECT_FALSE(CIDR::Parse("192.168.1.256/24").has_value());
}

TEST(NetworkUtilsTest, CIDR_Contains) {
  auto cidr = CIDR::Parse("192.168.1.0/24");
  ASSERT_TRUE(cidr.has_value());

  // Test IPs within range
  auto ip = ParseIPv4("192.168.1.1");
  ASSERT_TRUE(ip.has_value());
  EXPECT_TRUE(cidr->Contains(ip.value()));

  ip = ParseIPv4("192.168.1.254");
  ASSERT_TRUE(ip.has_value());
  EXPECT_TRUE(cidr->Contains(ip.value()));

  ip = ParseIPv4("192.168.1.0");
  ASSERT_TRUE(ip.has_value());
  EXPECT_TRUE(cidr->Contains(ip.value()));

  ip = ParseIPv4("192.168.1.255");
  ASSERT_TRUE(ip.has_value());
  EXPECT_TRUE(cidr->Contains(ip.value()));

  // Test IPs outside range
  ip = ParseIPv4("192.168.2.1");
  ASSERT_TRUE(ip.has_value());
  EXPECT_FALSE(cidr->Contains(ip.value()));

  ip = ParseIPv4("192.168.0.255");
  ASSERT_TRUE(ip.has_value());
  EXPECT_FALSE(cidr->Contains(ip.value()));

  ip = ParseIPv4("10.0.0.1");
  ASSERT_TRUE(ip.has_value());
  EXPECT_FALSE(cidr->Contains(ip.value()));
}

TEST(NetworkUtilsTest, CIDR_Contains_DifferentPrefixes) {
  // Test /8
  auto cidr = CIDR::Parse("10.0.0.0/8");
  ASSERT_TRUE(cidr.has_value());

  auto ip = ParseIPv4("10.1.2.3");
  ASSERT_TRUE(ip.has_value());
  EXPECT_TRUE(cidr->Contains(ip.value()));

  ip = ParseIPv4("11.0.0.1");
  ASSERT_TRUE(ip.has_value());
  EXPECT_FALSE(cidr->Contains(ip.value()));

  // Test /16
  cidr = CIDR::Parse("172.16.0.0/16");
  ASSERT_TRUE(cidr.has_value());

  ip = ParseIPv4("172.16.255.255");
  ASSERT_TRUE(ip.has_value());
  EXPECT_TRUE(cidr->Contains(ip.value()));

  ip = ParseIPv4("172.17.0.1");
  ASSERT_TRUE(ip.has_value());
  EXPECT_FALSE(cidr->Contains(ip.value()));

  // Test /32 (single host)
  cidr = CIDR::Parse("192.168.1.100/32");
  ASSERT_TRUE(cidr.has_value());

  ip = ParseIPv4("192.168.1.100");
  ASSERT_TRUE(ip.has_value());
  EXPECT_TRUE(cidr->Contains(ip.value()));

  ip = ParseIPv4("192.168.1.101");
  ASSERT_TRUE(ip.has_value());
  EXPECT_FALSE(cidr->Contains(ip.value()));
}

TEST(NetworkUtilsTest, IsIPAllowed_EmptyList) {
  // Empty list should DENY all IPs (fail-closed)
  std::vector<std::string> empty_list;
  EXPECT_FALSE(IsIPAllowed("192.168.1.1", empty_list));
  EXPECT_FALSE(IsIPAllowed("10.0.0.1", empty_list));
  EXPECT_FALSE(IsIPAllowed("172.16.0.1", empty_list));
}

TEST(NetworkUtilsTest, IsIPAllowed_SingleCIDR) {
  std::vector<std::string> allow_cidrs = {"192.168.1.0/24"};

  // Within range
  EXPECT_TRUE(IsIPAllowed("192.168.1.1", allow_cidrs));
  EXPECT_TRUE(IsIPAllowed("192.168.1.254", allow_cidrs));

  // Outside range
  EXPECT_FALSE(IsIPAllowed("192.168.2.1", allow_cidrs));
  EXPECT_FALSE(IsIPAllowed("10.0.0.1", allow_cidrs));
}

TEST(NetworkUtilsTest, IsIPAllowed_MultipleCIDRs) {
  std::vector<std::string> allow_cidrs = {"192.168.1.0/24", "10.0.0.0/8", "172.16.0.0/16"};

  // Within ranges
  EXPECT_TRUE(IsIPAllowed("192.168.1.100", allow_cidrs));
  EXPECT_TRUE(IsIPAllowed("10.1.2.3", allow_cidrs));
  EXPECT_TRUE(IsIPAllowed("172.16.255.255", allow_cidrs));

  // Outside all ranges
  EXPECT_FALSE(IsIPAllowed("192.168.2.1", allow_cidrs));
  EXPECT_FALSE(IsIPAllowed("11.0.0.1", allow_cidrs));
  EXPECT_FALSE(IsIPAllowed("172.17.0.1", allow_cidrs));
}

TEST(NetworkUtilsTest, IsIPAllowed_InvalidIP) {
  std::vector<std::string> allow_cidrs = {"192.168.1.0/24"};

  // Invalid IP format should be denied
  EXPECT_FALSE(IsIPAllowed("not-an-ip", allow_cidrs));
  EXPECT_FALSE(IsIPAllowed("", allow_cidrs));
  EXPECT_FALSE(IsIPAllowed("192.168.1", allow_cidrs));
}

TEST(NetworkUtilsTest, IsIPAllowed_InvalidCIDR) {
  std::vector<std::string> allow_cidrs = {"192.168.1.0/24",
                                          "invalid-cidr",  // Invalid CIDR should be ignored
                                          "10.0.0.0/8"};

  // Should still work with valid CIDRs
  EXPECT_TRUE(IsIPAllowed("192.168.1.1", allow_cidrs));
  EXPECT_TRUE(IsIPAllowed("10.0.0.1", allow_cidrs));
  EXPECT_FALSE(IsIPAllowed("172.16.0.1", allow_cidrs));
}

}  // namespace utils
}  // namespace mygramdb
