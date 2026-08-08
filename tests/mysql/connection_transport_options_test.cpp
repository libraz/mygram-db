/**
 * @file connection_transport_options_test.cpp
 * @brief Unit tests for the TLS/authentication options applied before connecting.
 */

#ifdef USE_MYSQL

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "mysql/connection.h"
#include "utils/error.h"

namespace mygramdb::mysql {
namespace {

/// One option the client library was asked to accept, in the order requested.
struct AppliedOption {
  enum mysql_option option = MYSQL_OPT_MAX_ALLOWED_PACKET;
  unsigned int uint_value = 0;
  bool bool_value = false;
  std::string string_value;
};

/**
 * @brief Record every option and optionally refuse one of them.
 *
 * Refusing a single named option is how the tests reproduce a client library
 * that does not support it, which is the situation the production code used to
 * merely log about.
 */
class RecordingApplier {
 public:
  explicit RecordingApplier(enum mysql_option refuse = MYSQL_OPT_MAX_ALLOWED_PACKET, bool refuse_any = false)
      : refuse_(refuse), refuse_any_(refuse_any) {}

  bool operator()(enum mysql_option option, const void* value) {
    AppliedOption applied;
    applied.option = option;
    switch (option) {
      case MYSQL_OPT_GET_SERVER_PUBLIC_KEY:
        applied.bool_value = *static_cast<const bool*>(value);
        break;
      case MYSQL_OPT_SSL_MODE:
        applied.uint_value = *static_cast<const unsigned int*>(value);
        break;
      default:
        applied.string_value = static_cast<const char*>(value);
        break;
    }
    applied_.push_back(applied);
    return !refuse_any_ || option != refuse_;
  }

  [[nodiscard]] const std::vector<AppliedOption>& applied() const { return applied_; }

  [[nodiscard]] const AppliedOption* Find(enum mysql_option option) const {
    for (const auto& entry : applied_) {
      if (entry.option == option) {
        return &entry;
      }
    }
    return nullptr;
  }

 private:
  std::vector<AppliedOption> applied_;
  enum mysql_option refuse_;
  bool refuse_any_;
};

Connection::Config VerifyingConfig() {
  Connection::Config config;
  config.ssl_enable = true;
  config.ssl_verify_server_cert = true;
  config.ssl_ca = "/etc/mygramdb/ca.pem";
  config.ssl_cert = "/etc/mygramdb/client.pem";
  config.ssl_key = "/etc/mygramdb/client-key.pem";
  return config;
}

/**
 * @brief A configuration demanding a verified server asks for exactly that.
 */
TEST(MySQLTransportOptionsTest, VerifiedConfigurationRequestsIdentityVerificationAndItsTrustAnchor) {
  RecordingApplier applier;
  const auto config = VerifyingConfig();
  ASSERT_TRUE(ApplyMySQLTransportOptions(config, std::ref(applier)).has_value());

  const auto* mode = applier.Find(MYSQL_OPT_SSL_MODE);
  ASSERT_NE(mode, nullptr);
  EXPECT_EQ(mode->uint_value, static_cast<unsigned int>(SSL_MODE_VERIFY_IDENTITY));

  const auto* certificate_authority = applier.Find(MYSQL_OPT_SSL_CA);
  ASSERT_NE(certificate_authority, nullptr);
  EXPECT_EQ(certificate_authority->string_value, config.ssl_ca);

  // With the channel authenticated there is no reason to accept a server-sent
  // RSA key for the password exchange.
  const auto* public_key = applier.Find(MYSQL_OPT_GET_SERVER_PUBLIC_KEY);
  ASSERT_NE(public_key, nullptr);
  EXPECT_FALSE(public_key->bool_value);
}

/**
 * @brief Encryption without verification stops at REQUIRED.
 */
TEST(MySQLTransportOptionsTest, UnverifiedTlsConfigurationDoesNotClaimIdentityVerification) {
  RecordingApplier applier;
  auto config = VerifyingConfig();
  config.ssl_verify_server_cert = false;
  ASSERT_TRUE(ApplyMySQLTransportOptions(config, std::ref(applier)).has_value());

  const auto* mode = applier.Find(MYSQL_OPT_SSL_MODE);
  ASSERT_NE(mode, nullptr);
  EXPECT_EQ(mode->uint_value, static_cast<unsigned int>(SSL_MODE_REQUIRED));
}

/**
 * @brief TLS left off disables negotiation rather than leaving it to the client default.
 */
TEST(MySQLTransportOptionsTest, DisabledTlsConfigurationTurnsNegotiationOffExplicitly) {
  RecordingApplier applier;
  Connection::Config config;
  ASSERT_FALSE(config.ssl_enable);
  ASSERT_TRUE(ApplyMySQLTransportOptions(config, std::ref(applier)).has_value());

  const auto* mode = applier.Find(MYSQL_OPT_SSL_MODE);
  ASSERT_NE(mode, nullptr);
  EXPECT_EQ(mode->uint_value, static_cast<unsigned int>(SSL_MODE_DISABLED));

  // No certificate paths are pushed when the transport is plaintext, so a
  // stale path in the configuration cannot influence the handshake.
  EXPECT_EQ(applier.Find(MYSQL_OPT_SSL_CA), nullptr);
  EXPECT_EQ(applier.Find(MYSQL_OPT_SSL_CERT), nullptr);
  EXPECT_EQ(applier.Find(MYSQL_OPT_SSL_KEY), nullptr);
}

/**
 * @brief An option the client library will not accept aborts the connection.
 *
 * Continuing would hand the connection to the client default, which is
 * SSL_MODE_PREFERRED: encrypted when the server offers it, plaintext when it
 * does not, and unauthenticated either way. That is strictly weaker than every
 * posture this function can be asked for, including the plaintext one, which
 * asks for negotiation to be off rather than opportunistic.
 */
TEST(MySQLTransportOptionsTest, RefusedOptionFailsInsteadOfFallingBackToTheClientDefault) {
  const std::vector<enum mysql_option> security_relevant = {
      MYSQL_OPT_SSL_MODE, MYSQL_OPT_SSL_CA, MYSQL_OPT_SSL_CERT, MYSQL_OPT_SSL_KEY, MYSQL_OPT_GET_SERVER_PUBLIC_KEY,
  };

  for (const auto option : security_relevant) {
    RecordingApplier applier(option, /*refuse_any=*/true);
    const auto result = ApplyMySQLTransportOptions(VerifyingConfig(), std::ref(applier));
    ASSERT_FALSE(result.has_value()) << "an unaccepted transport option was treated as applied";
    EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLConnectionFailed);
  }

  // The plaintext posture is refused on the same terms: falling through to the
  // client default would start negotiating TLS on a code path whose comment
  // documents a crash during concurrent connection setup.
  RecordingApplier applier(MYSQL_OPT_SSL_MODE, /*refuse_any=*/true);
  Connection::Config plaintext;
  EXPECT_FALSE(ApplyMySQLTransportOptions(plaintext, std::ref(applier)).has_value());
}

/**
 * @brief Refusal happens before any later option is offered.
 */
TEST(MySQLTransportOptionsTest, RefusalStopsBeforeApplyingTheRemainingOptions) {
  RecordingApplier applier(MYSQL_OPT_SSL_MODE, /*refuse_any=*/true);
  ASSERT_FALSE(ApplyMySQLTransportOptions(VerifyingConfig(), std::ref(applier)).has_value());
  EXPECT_EQ(applier.Find(MYSQL_OPT_SSL_CA), nullptr);
  EXPECT_EQ(applier.Find(MYSQL_OPT_SSL_CERT), nullptr);
  EXPECT_EQ(applier.Find(MYSQL_OPT_SSL_KEY), nullptr);
}

/**
 * @brief Unset certificate paths are not pushed as empty strings.
 */
TEST(MySQLTransportOptionsTest, OmittedCertificatePathsAreLeftUnset) {
  RecordingApplier applier;
  Connection::Config config;
  config.ssl_enable = true;
  config.ssl_verify_server_cert = false;
  ASSERT_TRUE(ApplyMySQLTransportOptions(config, std::ref(applier)).has_value());

  EXPECT_EQ(applier.Find(MYSQL_OPT_SSL_CA), nullptr);
  EXPECT_EQ(applier.Find(MYSQL_OPT_SSL_CERT), nullptr);
  EXPECT_EQ(applier.Find(MYSQL_OPT_SSL_KEY), nullptr);
}

/**
 * @brief Plaintext connections still have to authenticate against MySQL 8.4+.
 *
 * caching_sha2_password is the default plugin there, and without a TLS channel
 * the client needs the server's RSA key to send the password at all. The flag
 * is therefore tied to the transport rather than set unconditionally.
 */
TEST(MySQLTransportOptionsTest, PlaintextConnectionsRequestTheServerPublicKey) {
  RecordingApplier applier;
  Connection::Config config;
  ASSERT_TRUE(ApplyMySQLTransportOptions(config, std::ref(applier)).has_value());

  const auto* public_key = applier.Find(MYSQL_OPT_GET_SERVER_PUBLIC_KEY);
  ASSERT_NE(public_key, nullptr);
  EXPECT_TRUE(public_key->bool_value);
}

}  // namespace
}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
