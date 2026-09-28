/**
 * @file network_utils_test.cpp
 * @brief Tests for network utility functions
 */

#include "utils/network_utils.h"

#include <gtest/gtest.h>
#include <unistd.h>

namespace mygramdb::utils {

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

TEST(NetworkUtilsTest, CIDR_ParseAndContainsIPv6) {
  auto cidr = CIDR::Parse("2001:db8:abcd::/48");
  ASSERT_TRUE(cidr.has_value());
  EXPECT_EQ(cidr->family, AF_INET6);
  EXPECT_EQ(cidr->prefix_length, 48);
  EXPECT_TRUE(cidr->Contains("2001:db8:abcd::1"));
  EXPECT_TRUE(cidr->Contains("2001:db8:abcd:ffff::1"));
  EXPECT_FALSE(cidr->Contains("2001:db8:abce::1"));
  EXPECT_FALSE(cidr->Contains("192.0.2.1"));

  auto loopback = CIDR::Parse("::1/128");
  ASSERT_TRUE(loopback.has_value());
  EXPECT_TRUE(loopback->Contains("::1"));
  EXPECT_FALSE(loopback->Contains("::2"));
  EXPECT_FALSE(CIDR::Parse("2001:db8::/129").has_value());
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

TEST(NetworkUtilsTest, IsIPAllowedSupportsIPv6AndMappedIPv4) {
  const std::vector<std::string> cidrs = {"::1/128", "2001:db8::/32", "127.0.0.0/8"};
  EXPECT_TRUE(IsIPAllowed("::1", cidrs));
  EXPECT_TRUE(IsIPAllowed("2001:db8:1::42", cidrs));
  EXPECT_FALSE(IsIPAllowed("2001:db9::1", cidrs));
  EXPECT_TRUE(IsIPAllowed("::ffff:127.0.0.1", cidrs));
  EXPECT_FALSE(IsIPAllowed("::ffff:192.0.2.1", cidrs));
}

TEST(NetworkUtilsTest, ParseAllowCidrs_ValidEntries) {
  std::vector<std::string> cidrs = {"192.168.1.0/24", "10.0.0.0/8"};
  auto parsed = ParseAllowCidrs(cidrs);

  ASSERT_EQ(parsed.size(), 2U);
  EXPECT_EQ(parsed[0].network, 0xC0A80100);  // 192.168.1.0
  EXPECT_EQ(parsed[0].prefix_length, 24);
  EXPECT_EQ(parsed[1].network, 0x0A000000);  // 10.0.0.0
  EXPECT_EQ(parsed[1].prefix_length, 8);
}

TEST(NetworkUtilsTest, ParseAllowCidrs_InvalidSkipped) {
  std::vector<std::string> cidrs = {"not-a-cidr"};
  auto parsed = ParseAllowCidrs(cidrs);

  EXPECT_TRUE(parsed.empty());
}

TEST(NetworkUtilsTest, ParseAllowCidrs_Empty) {
  std::vector<std::string> cidrs;
  auto parsed = ParseAllowCidrs(cidrs);

  EXPECT_TRUE(parsed.empty());
}

TEST(NetworkUtilsTest, ParseAllowCidrs_MixedValidInvalid) {
  std::vector<std::string> cidrs = {"192.168.1.0/24", "invalid", "10.0.0.0/8", "", "bad/99"};
  auto parsed = ParseAllowCidrs(cidrs);

  ASSERT_EQ(parsed.size(), 2U);
  EXPECT_EQ(parsed[0].network, 0xC0A80100);  // 192.168.1.0
  EXPECT_EQ(parsed[0].prefix_length, 24);
  EXPECT_EQ(parsed[1].network, 0x0A000000);  // 10.0.0.0
  EXPECT_EQ(parsed[1].prefix_length, 8);
}

TEST(NetworkUtilsTest, GetPeerIP_InvalidFd) {
  // An invalid file descriptor should return "unknown"
  EXPECT_EQ(GetPeerIP(-1), "unknown");
}

TEST(NetworkUtilsTest, GetPeerIP_NonSocketFd) {
  // A valid fd that is not a socket should return "unknown"
  // (stdout is fd 1, not a socket)
  EXPECT_EQ(GetPeerIP(1), "unknown");
}

TEST(NetworkUtilsTest, GetPeerIP_UnixSocketReturnsStableRateLimitKey) {
  int fds[2] = {-1, -1};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

  EXPECT_EQ(GetPeerIP(fds[0]), "unix");
  EXPECT_EQ(GetPeerIP(fds[1]), "unix");

  ::close(fds[0]);
  ::close(fds[1]);
}

// NormalizePeerAddress: TCP (GetPeerIP) and HTTP (cpp-httplib's remote_addr)
// must derive the same string for the same peer, or they disagree on ACL
// admission and split rate-limit accounting for one client across two
// buckets.
TEST(NetworkUtilsTest, NormalizePeerAddress_StripsIPv6ScopeSuffix) {
  EXPECT_EQ(NormalizePeerAddress("fe80::1%eth0"), "fe80::1");
  EXPECT_EQ(NormalizePeerAddress("fe80::abcd%25"), "fe80::abcd");
}

TEST(NetworkUtilsTest, NormalizePeerAddress_LowercasesHexDigits) {
  EXPECT_EQ(NormalizePeerAddress("FE80::1"), "fe80::1");
  EXPECT_EQ(NormalizePeerAddress("2001:DB8::ABCD"), "2001:db8::abcd");
}

TEST(NetworkUtilsTest, NormalizePeerAddress_MapsIPv4MappedIPv6ToPlainIPv4) {
  EXPECT_EQ(NormalizePeerAddress("::ffff:127.0.0.1"), "127.0.0.1");
  EXPECT_EQ(NormalizePeerAddress("::FFFF:192.168.1.1"), "192.168.1.1");
}

TEST(NetworkUtilsTest, NormalizePeerAddress_PlainAddressesAreUnaffected) {
  EXPECT_EQ(NormalizePeerAddress("127.0.0.1"), "127.0.0.1");
  EXPECT_EQ(NormalizePeerAddress("192.168.1.1"), "192.168.1.1");
  EXPECT_EQ(NormalizePeerAddress("::1"), "::1");
  EXPECT_EQ(NormalizePeerAddress("unknown"), "unknown");
  EXPECT_EQ(NormalizePeerAddress("unix"), "unix");
}

TEST(NetworkUtilsTest, NormalizePeerAddress_MakesHttpAndTcpRenderingsOfTheSamePeerAgree) {
  // A link-local IPv6 peer with a zone id, as httplib's getnameinfo() would
  // render it, must normalize to the same string GetPeerIP would derive
  // (which never carries a scope suffix in the first place).
  EXPECT_EQ(NormalizePeerAddress("FE80::1%en0"), NormalizePeerAddress("fe80::1"));
  // An IPv4 client on a dual-stack listener, rendered as IPv4-mapped IPv6 on
  // one surface and plain IPv4 on the other, must normalize to one form.
  EXPECT_EQ(NormalizePeerAddress("::ffff:10.0.0.5"), NormalizePeerAddress("10.0.0.5"));
}

// Without normalization, a scoped and an unscoped rendering of the same
// peer are different strings -- which is exactly what would split one
// client's rate-limit accounting across two buckets depending on which
// surface (TCP or HTTP) it connected through. glibc's inet_pton rejects the
// scope suffix outright, so CIDR::Contains fails closed there too; some
// libc implementations are more lenient about the CIDR check specifically,
// but the plain-string rate-limit/logging key is never routed through
// inet_pton, so it needs this normalization on every platform.
TEST(NetworkUtilsTest, UnnormalizedScopedAndUnscopedRenderingsOfTheSamePeerAreDifferentStrings) {
  EXPECT_NE(std::string("fe80::1%eth0"), std::string("fe80::1"));
}

TEST(NetworkUtilsTest, NormalizedScopedLinkLocalAddressPassesTheSameAclAsUnscoped) {
  const std::vector<std::string> cidrs = {"fe80::/10"};
  EXPECT_TRUE(IsIPAllowed(NormalizePeerAddress("fe80::1%eth0"), cidrs));
  EXPECT_TRUE(IsIPAllowed(NormalizePeerAddress("fe80::1"), cidrs));
  EXPECT_EQ(NormalizePeerAddress("fe80::1%eth0"), NormalizePeerAddress("fe80::1"));
}

}  // namespace mygramdb::utils
