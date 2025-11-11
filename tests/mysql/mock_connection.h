/**
 * @file mock_connection.h
 * @brief Mock MySQL connection for testing
 */

#pragma once

#include <gmock/gmock.h>

#include "mysql/connection.h"

#ifdef USE_MYSQL

namespace mygramdb {
namespace mysql {
namespace testing {

/**
 * @brief Mock MySQL connection for unit testing
 *
 * This mock allows testing binlog reader and other MySQL-dependent
 * components without requiring an actual MySQL server connection.
 */
class MockConnection {
 public:
  MOCK_METHOD(bool, Connect, (), ());
  MOCK_METHOD(void, Disconnect, (), ());
  MOCK_METHOD(bool, IsConnected, (), (const));
  MOCK_METHOD(bool, Ping, (), ());
  MOCK_METHOD(bool, Reconnect, (), ());
  MOCK_METHOD(bool, IsGTIDModeEnabled, (), ());
  MOCK_METHOD(MYSQL*, GetHandle, (), ());
  MOCK_METHOD(std::string, GetLastError, (), (const));
  MOCK_METHOD((std::optional<std::string>), ExecuteQuery, (const std::string& query), ());
};

/**
 * @brief Factory for creating mock connections in tests
 */
class ConnectionFactory {
 public:
  virtual ~ConnectionFactory() = default;
  virtual std::unique_ptr<Connection> Create(const Connection::Config& config) = 0;
};

/**
 * @brief Mock connection factory for testing
 */
class MockConnectionFactory : public ConnectionFactory {
 public:
  MOCK_METHOD(std::unique_ptr<Connection>, Create, (const Connection::Config& config), (override));
};

}  // namespace testing
}  // namespace mysql
}  // namespace mygramdb

#endif  // USE_MYSQL
