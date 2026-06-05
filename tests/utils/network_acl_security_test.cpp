/**
 * @file network_acl_security_test.cpp
 * @brief Tests for network ACL security (fail-closed by default)
 */

#include <gtest/gtest.h>

#include "utils/network_utils.h"

using namespace mygramdb::utils;

/**
 * @brief Test that empty ACL denies all connections (fail-closed)
 */
TEST(NetworkACLSecurityTest, EmptyACLDeniesAll) {
  std::vector<CIDR> empty_acl;

  // Empty ACL should DENY all connections (fail-closed)
  EXPECT_FALSE(IsIPAllowed("127.0.0.1", empty_acl));
  EXPECT_FALSE(IsIPAllowed("192.168.1.1", empty_acl));
  EXPECT_FALSE(IsIPAllowed("10.0.0.1", empty_acl));
  EXPECT_FALSE(IsIPAllowed("8.8.8.8", empty_acl));  // Google DNS (external)
}

/**
 * @brief Test localhost-only ACL
 */
TEST(NetworkACLSecurityTest, LocalhostOnly) {
  std::vector<CIDR> localhost_acl;
  auto localhost_cidr = CIDR::Parse("127.0.0.1/32");
  ASSERT_TRUE(localhost_cidr.has_value());
  localhost_acl.push_back(localhost_cidr.value());

  // Localhost should be allowed
  EXPECT_TRUE(IsIPAllowed("127.0.0.1", localhost_acl));

  // Other IPs should be denied
  EXPECT_FALSE(IsIPAllowed("127.0.0.2", localhost_acl));
  EXPECT_FALSE(IsIPAllowed("192.168.1.1", localhost_acl));
  EXPECT_FALSE(IsIPAllowed("10.0.0.1", localhost_acl));
  EXPECT_FALSE(IsIPAllowed("8.8.8.8", localhost_acl));
}

/**
 * @brief Test private network ACL
 */
TEST(NetworkACLSecurityTest, PrivateNetworkACL) {
  std::vector<CIDR> private_acl;

  auto localhost = CIDR::Parse("127.0.0.1/32");
  auto class_a = CIDR::Parse("10.0.0.0/8");
  auto class_c = CIDR::Parse("192.168.0.0/16");

  ASSERT_TRUE(localhost.has_value());
  ASSERT_TRUE(class_a.has_value());
  ASSERT_TRUE(class_c.has_value());

  private_acl.push_back(localhost.value());
  private_acl.push_back(class_a.value());
  private_acl.push_back(class_c.value());

  // Localhost and private IPs should be allowed
  EXPECT_TRUE(IsIPAllowed("127.0.0.1", private_acl));
  EXPECT_TRUE(IsIPAllowed("10.0.0.1", private_acl));
  EXPECT_TRUE(IsIPAllowed("10.255.255.255", private_acl));
  EXPECT_TRUE(IsIPAllowed("192.168.1.1", private_acl));
  EXPECT_TRUE(IsIPAllowed("192.168.255.254", private_acl));

  // Public IPs should be denied
  EXPECT_FALSE(IsIPAllowed("8.8.8.8", private_acl));      // Google DNS
  EXPECT_FALSE(IsIPAllowed("1.1.1.1", private_acl));      // Cloudflare DNS
  EXPECT_FALSE(IsIPAllowed("172.217.0.0", private_acl));  // Google (not in ACL)
}

/**
 * @brief Test allow-all ACL (explicit 0.0.0.0/0)
 */
TEST(NetworkACLSecurityTest, AllowAllACL) {
  std::vector<CIDR> allow_all_acl;
  auto allow_all = CIDR::Parse("0.0.0.0/0");
  ASSERT_TRUE(allow_all.has_value());
  allow_all_acl.push_back(allow_all.value());

  // All IPs should be allowed
  EXPECT_TRUE(IsIPAllowed("127.0.0.1", allow_all_acl));
  EXPECT_TRUE(IsIPAllowed("192.168.1.1", allow_all_acl));
  EXPECT_TRUE(IsIPAllowed("10.0.0.1", allow_all_acl));
  EXPECT_TRUE(IsIPAllowed("8.8.8.8", allow_all_acl));
  EXPECT_TRUE(IsIPAllowed("1.1.1.1", allow_all_acl));
}

/**
 * @brief Test single IP ACL (/32 mask)
 */
TEST(NetworkACLSecurityTest, SingleIPACL) {
  std::vector<CIDR> single_ip_acl;
  auto single_ip = CIDR::Parse("192.168.1.100/32");
  ASSERT_TRUE(single_ip.has_value());
  single_ip_acl.push_back(single_ip.value());

  // Only this specific IP should be allowed
  EXPECT_TRUE(IsIPAllowed("192.168.1.100", single_ip_acl));

  // Other IPs in same subnet should be denied
  EXPECT_FALSE(IsIPAllowed("192.168.1.1", single_ip_acl));
  EXPECT_FALSE(IsIPAllowed("192.168.1.99", single_ip_acl));
  EXPECT_FALSE(IsIPAllowed("192.168.1.101", single_ip_acl));
  EXPECT_FALSE(IsIPAllowed("192.168.1.255", single_ip_acl));
}

/**
 * @brief Test subnet ACL (/24 mask)
 */
TEST(NetworkACLSecurityTest, SubnetACL) {
  std::vector<CIDR> subnet_acl;
  auto subnet = CIDR::Parse("192.168.1.0/24");
  ASSERT_TRUE(subnet.has_value());
  subnet_acl.push_back(subnet.value());

  // IPs in subnet should be allowed
  EXPECT_TRUE(IsIPAllowed("192.168.1.1", subnet_acl));
  EXPECT_TRUE(IsIPAllowed("192.168.1.100", subnet_acl));
  EXPECT_TRUE(IsIPAllowed("192.168.1.254", subnet_acl));

  // IPs outside subnet should be denied
  EXPECT_FALSE(IsIPAllowed("192.168.0.1", subnet_acl));
  EXPECT_FALSE(IsIPAllowed("192.168.2.1", subnet_acl));
  EXPECT_FALSE(IsIPAllowed("10.0.0.1", subnet_acl));
}

/**
 * @brief Test invalid IP handling
 */
TEST(NetworkACLSecurityTest, InvalidIPHandling) {
  std::vector<CIDR> acl;
  auto allow_all = CIDR::Parse("0.0.0.0/0");
  ASSERT_TRUE(allow_all.has_value());
  acl.push_back(allow_all.value());

  // Invalid IPs should be denied
  EXPECT_FALSE(IsIPAllowed("", acl));
  EXPECT_FALSE(IsIPAllowed("invalid", acl));
  EXPECT_FALSE(IsIPAllowed("256.256.256.256", acl));
  EXPECT_FALSE(IsIPAllowed("192.168.1", acl));      // Incomplete
  EXPECT_FALSE(IsIPAllowed("192.168.1.1.1", acl));  // Too many octets
}

/**
 * @brief Test fail-closed behavior is consistent
 */
TEST(NetworkACLSecurityTest, FailClosedConsistency) {
  std::vector<CIDR> empty_acl;

  // Test multiple times to ensure consistent denial
  for (int i = 0; i < 100; ++i) {
    EXPECT_FALSE(IsIPAllowed("127.0.0.1", empty_acl));
    EXPECT_FALSE(IsIPAllowed("192.168.1.1", empty_acl));
  }
}

/**
 * @brief Test security boundary: Class A private network
 */
TEST(NetworkACLSecurityTest, ClassAPrivateBoundary) {
  std::vector<CIDR> class_a_acl;
  auto class_a = CIDR::Parse("10.0.0.0/8");
  ASSERT_TRUE(class_a.has_value());
  class_a_acl.push_back(class_a.value());

  // Within Class A private range
  EXPECT_TRUE(IsIPAllowed("10.0.0.0", class_a_acl));
  EXPECT_TRUE(IsIPAllowed("10.0.0.1", class_a_acl));
  EXPECT_TRUE(IsIPAllowed("10.255.255.255", class_a_acl));

  // Outside Class A private range
  EXPECT_FALSE(IsIPAllowed("9.255.255.255", class_a_acl));
  EXPECT_FALSE(IsIPAllowed("11.0.0.0", class_a_acl));
}

/**
 * @brief Test security boundary: Class B private network
 */
TEST(NetworkACLSecurityTest, ClassBPrivateBoundary) {
  std::vector<CIDR> class_b_acl;
  auto class_b = CIDR::Parse("172.16.0.0/12");
  ASSERT_TRUE(class_b.has_value());
  class_b_acl.push_back(class_b.value());

  // Within Class B private range (172.16.0.0 - 172.31.255.255)
  EXPECT_TRUE(IsIPAllowed("172.16.0.0", class_b_acl));
  EXPECT_TRUE(IsIPAllowed("172.16.0.1", class_b_acl));
  EXPECT_TRUE(IsIPAllowed("172.31.255.255", class_b_acl));

  // Outside Class B private range
  EXPECT_FALSE(IsIPAllowed("172.15.255.255", class_b_acl));
  EXPECT_FALSE(IsIPAllowed("172.32.0.0", class_b_acl));
}
