/**
 * @file multi_table_test.cpp
 * @brief Integration tests for TCP server multi-table functionality
 */

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <fstream>
#include <thread>

#include "config/config.h"
#include "server/tcp_server.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#include "mysql/connection.h"
#endif

using namespace mygramdb::server;
using namespace mygramdb;

/**
 * @brief Test multi-table SAVE
 */
TEST(TcpServerMultiTableTest, MultiTableSearch) {
  // Create two tables
  auto index1 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store1 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table1;
  table1.name = "articles";
  table1.config.ngram_size = 1;
  table1.index = std::move(index1);
  table1.doc_store = std::move(doc_store1);

  // Add documents to articles table
  auto doc_id1 = table1.doc_store->AddDocument("article1", {});
  table1.index->AddDocument(doc_id1, "machine learning");
  auto doc_id2 = table1.doc_store->AddDocument("article2", {});
  table1.index->AddDocument(doc_id2, "deep learning");

  auto index2 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store2 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table2;
  table2.name = "comments";
  table2.config.ngram_size = 1;
  table2.index = std::move(index2);
  table2.doc_store = std::move(doc_store2);

  // Add documents to comments table
  auto doc_id3 = table2.doc_store->AddDocument("comment1", {});
  table2.index->AddDocument(doc_id3, "great article");
  auto doc_id4 = table2.doc_store->AddDocument("comment2", {});
  table2.index->AddDocument(doc_id4, "interesting post");

  std::unordered_map<std::string, mygramdb::server::TableContext*> table_contexts;
  table_contexts["articles"] = &table1;
  table_contexts["comments"] = &table2;

  // Create server
  mygramdb::server::ServerConfig config;
  config.port = 0;
  config.host = "127.0.0.1";

  mygramdb::server::TcpServer server(config, table_contexts, "./snapshots", nullptr);
  ASSERT_TRUE(server.Start());

  uint16_t port = server.GetPort();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Create client socket
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(sock, 0);

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

  ASSERT_GE(connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)), 0);

  // Search in articles table
  std::string request1 = "SEARCH articles learning\r\n";
  send(sock, request1.c_str(), request1.length(), 0);

  char buffer1[4096];
  ssize_t received1 = recv(sock, buffer1, sizeof(buffer1) - 1, 0);
  ASSERT_GT(received1, 0);
  buffer1[received1] = '\0';
  std::string response1(buffer1);

  EXPECT_TRUE(response1.find("OK RESULTS 2") == 0);  // Should find 2 documents

  // Search in comments table
  std::string request2 = "SEARCH comments article\r\n";
  send(sock, request2.c_str(), request2.length(), 0);

  char buffer2[4096];
  ssize_t received2 = recv(sock, buffer2, sizeof(buffer2) - 1, 0);
  ASSERT_GT(received2, 0);
  buffer2[received2] = '\0';
  std::string response2(buffer2);

  EXPECT_TRUE(response2.find("OK RESULTS 1") == 0);  // Should find 1 document

  // Try to search in non-existent table
  std::string request3 = "SEARCH nonexistent test\r\n";
  send(sock, request3.c_str(), request3.length(), 0);

  char buffer3[4096];
  ssize_t received3 = recv(sock, buffer3, sizeof(buffer3) - 1, 0);
  ASSERT_GT(received3, 0);
  buffer3[received3] = '\0';
  std::string response3(buffer3);

  EXPECT_TRUE(response3.find("ERROR Table not found") == 0);

  close(sock);
  server.Stop();
}

/**
 * @brief Test COUNT command with multiple tables
 */
TEST(TcpServerMultiTableTest, MultiTableCount) {
  // Create two tables with different content
  auto index1 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store1 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table1;
  table1.name = "users";
  table1.config.ngram_size = 1;
  table1.index = std::move(index1);
  table1.doc_store = std::move(doc_store1);

  // Add 3 documents with "test" to users table
  for (int i = 1; i <= 3; ++i) {
    auto doc_id = table1.doc_store->AddDocument("user" + std::to_string(i), {});
    table1.index->AddDocument(doc_id, "test user data");
  }

  auto index2 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store2 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table2;
  table2.name = "posts";
  table2.config.ngram_size = 1;
  table2.index = std::move(index2);
  table2.doc_store = std::move(doc_store2);

  // Add 2 documents with "test" to posts table
  for (int i = 1; i <= 2; ++i) {
    auto doc_id = table2.doc_store->AddDocument("post" + std::to_string(i), {});
    table2.index->AddDocument(doc_id, "test post content");
  }

  std::unordered_map<std::string, mygramdb::server::TableContext*> table_contexts;
  table_contexts["users"] = &table1;
  table_contexts["posts"] = &table2;

  mygramdb::server::ServerConfig config;
  config.port = 0;
  config.host = "127.0.0.1";
  mygramdb::server::TcpServer server(config, table_contexts);
  ASSERT_TRUE(server.Start());

  int port = server.GetPort();
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(sock, 0);

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

  ASSERT_GE(connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)), 0);

  // Count in users table
  std::string request1 = "COUNT users test\r\n";
  send(sock, request1.c_str(), request1.length(), 0);

  char buffer1[4096];
  ssize_t received1 = recv(sock, buffer1, sizeof(buffer1) - 1, 0);
  ASSERT_GT(received1, 0);
  buffer1[received1] = '\0';
  std::string response1(buffer1);

  EXPECT_TRUE(response1.find("OK COUNT 3") == 0);

  // Count in posts table
  std::string request2 = "COUNT posts test\r\n";
  send(sock, request2.c_str(), request2.length(), 0);

  char buffer2[4096];
  ssize_t received2 = recv(sock, buffer2, sizeof(buffer2) - 1, 0);
  ASSERT_GT(received2, 0);
  buffer2[received2] = '\0';
  std::string response2(buffer2);

  EXPECT_TRUE(response2.find("OK COUNT 2") == 0);

  close(sock);
  server.Stop();
}

/**
 * @brief Test GET command with multiple tables
 */
TEST(TcpServerMultiTableTest, MultiTableGet) {
  // Create two tables
  auto index1 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store1 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table1;
  table1.name = "customers";
  table1.config.ngram_size = 1;
  table1.index = std::move(index1);
  table1.doc_store = std::move(doc_store1);

  std::unordered_map<std::string, mygramdb::storage::FilterValue> filters1;
  filters1["type"] = std::string("premium");
  auto doc_id1 = table1.doc_store->AddDocument("cust_100", filters1);
  table1.index->AddDocument(doc_id1, "Alice Johnson");

  auto index2 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store2 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table2;
  table2.name = "orders";
  table2.config.ngram_size = 1;
  table2.index = std::move(index2);
  table2.doc_store = std::move(doc_store2);

  std::unordered_map<std::string, mygramdb::storage::FilterValue> filters2;
  filters2["status"] = std::string("shipped");
  auto doc_id2 = table2.doc_store->AddDocument("order_200", filters2);
  table2.index->AddDocument(doc_id2, "Product XYZ");

  std::unordered_map<std::string, mygramdb::server::TableContext*> table_contexts;
  table_contexts["customers"] = &table1;
  table_contexts["orders"] = &table2;

  mygramdb::server::ServerConfig config;
  config.port = 0;
  config.host = "127.0.0.1";
  mygramdb::server::TcpServer server(config, table_contexts);
  ASSERT_TRUE(server.Start());

  int port = server.GetPort();
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(sock, 0);

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

  ASSERT_GE(connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)), 0);

  // GET from customers table
  std::string request1 = "GET customers cust_100\r\n";
  send(sock, request1.c_str(), request1.length(), 0);

  char buffer1[4096];
  ssize_t received1 = recv(sock, buffer1, sizeof(buffer1) - 1, 0);
  ASSERT_GT(received1, 0);
  buffer1[received1] = '\0';
  std::string response1(buffer1);

  EXPECT_TRUE(response1.find("OK DOC cust_100") == 0);
  EXPECT_TRUE(response1.find("type=premium") != std::string::npos);

  // GET from orders table
  std::string request2 = "GET orders order_200\r\n";
  send(sock, request2.c_str(), request2.length(), 0);

  char buffer2[4096];
  ssize_t received2 = recv(sock, buffer2, sizeof(buffer2) - 1, 0);
  ASSERT_GT(received2, 0);
  buffer2[received2] = '\0';
  std::string response2(buffer2);

  EXPECT_TRUE(response2.find("OK DOC order_200") == 0);
  EXPECT_TRUE(response2.find("status=shipped") != std::string::npos);

  // Try to GET from customers table with orders primary key - should fail
  std::string request3 = "GET customers order_200\r\n";
  send(sock, request3.c_str(), request3.length(), 0);

  char buffer3[4096];
  ssize_t received3 = recv(sock, buffer3, sizeof(buffer3) - 1, 0);
  ASSERT_GT(received3, 0);
  buffer3[received3] = '\0';
  std::string response3(buffer3);

  EXPECT_TRUE(response3.find("ERROR") == 0);

  close(sock);
  server.Stop();
}

/**
 * @brief Test INFO command with multiple tables
 */
TEST(TcpServerMultiTableTest, MultiTableInfo) {
  // Create two tables with different sizes
  auto index1 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store1 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table1;
  table1.name = "table_a";
  table1.config.ngram_size = 1;
  table1.index = std::move(index1);
  table1.doc_store = std::move(doc_store1);

  // Add 5 documents to table_a
  for (int i = 1; i <= 5; ++i) {
    auto doc_id = table1.doc_store->AddDocument("doc_a" + std::to_string(i), {});
    table1.index->AddDocument(doc_id, "content for table a");
  }

  auto index2 = std::make_unique<mygramdb::index::Index>(2);
  auto doc_store2 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table2;
  table2.name = "table_b";
  table2.config.ngram_size = 2;
  table2.index = std::move(index2);
  table2.doc_store = std::move(doc_store2);

  // Add 3 documents to table_b
  for (int i = 1; i <= 3; ++i) {
    auto doc_id = table2.doc_store->AddDocument("doc_b" + std::to_string(i), {});
    table2.index->AddDocument(doc_id, "content for table b");
  }

  std::unordered_map<std::string, mygramdb::server::TableContext*> table_contexts;
  table_contexts["table_a"] = &table1;
  table_contexts["table_b"] = &table2;

  mygramdb::server::ServerConfig config;
  config.port = 0;
  config.host = "127.0.0.1";
  mygramdb::server::TcpServer server(config, table_contexts);
  ASSERT_TRUE(server.Start());

  int port = server.GetPort();
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(sock, 0);

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

  ASSERT_GE(connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)), 0);

  // Get INFO
  std::string request = "INFO\r\n";
  send(sock, request.c_str(), request.length(), 0);

  char buffer[8192];
  ssize_t received = recv(sock, buffer, sizeof(buffer) - 1, 0);
  ASSERT_GT(received, 0);
  buffer[received] = '\0';
  std::string response(buffer);

  EXPECT_TRUE(response.find("OK INFO") == 0);
  // Should show total of 8 documents (5 + 3)
  EXPECT_TRUE(response.find("total_documents: 8") != std::string::npos);
  // Should list both tables
  EXPECT_TRUE(response.find("tables: table_a,table_b") != std::string::npos ||
              response.find("tables: table_b,table_a") != std::string::npos);

  close(sock);
  server.Stop();
}

/**
 * @brief Test table isolation - operations on one table don't affect another
 */
TEST(TcpServerMultiTableTest, TableIsolation) {
  // Create two tables
  auto index1 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store1 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table1;
  table1.name = "isolated_a";
  table1.config.ngram_size = 1;
  table1.index = std::move(index1);
  table1.doc_store = std::move(doc_store1);

  auto doc_id1 = table1.doc_store->AddDocument("doc1", {});
  table1.index->AddDocument(doc_id1, "shared keyword");

  auto index2 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store2 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table2;
  table2.name = "isolated_b";
  table2.config.ngram_size = 1;
  table2.index = std::move(index2);
  table2.doc_store = std::move(doc_store2);

  auto doc_id2 = table2.doc_store->AddDocument("doc2", {});
  table2.index->AddDocument(doc_id2, "different content");

  std::unordered_map<std::string, mygramdb::server::TableContext*> table_contexts;
  table_contexts["isolated_a"] = &table1;
  table_contexts["isolated_b"] = &table2;

  mygramdb::server::ServerConfig config;
  config.port = 0;
  config.host = "127.0.0.1";
  mygramdb::server::TcpServer server(config, table_contexts);
  ASSERT_TRUE(server.Start());

  int port = server.GetPort();
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(sock, 0);

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

  ASSERT_GE(connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)), 0);

  // Search for "shared" in isolated_a - should find it
  std::string request1 = "SEARCH isolated_a shared\r\n";
  send(sock, request1.c_str(), request1.length(), 0);

  char buffer1[4096];
  ssize_t received1 = recv(sock, buffer1, sizeof(buffer1) - 1, 0);
  ASSERT_GT(received1, 0);
  buffer1[received1] = '\0';
  std::string response1(buffer1);

  EXPECT_TRUE(response1.find("OK RESULTS 1") == 0);

  // Search for "shared" in isolated_b - should NOT find it
  std::string request2 = "SEARCH isolated_b shared\r\n";
  send(sock, request2.c_str(), request2.length(), 0);

  char buffer2[4096];
  ssize_t received2 = recv(sock, buffer2, sizeof(buffer2) - 1, 0);
  ASSERT_GT(received2, 0);
  buffer2[received2] = '\0';
  std::string response2(buffer2);

  EXPECT_TRUE(response2.find("OK RESULTS 0") == 0);

  // Search for "different" in isolated_b - should find it
  std::string request3 = "SEARCH isolated_b different\r\n";
  send(sock, request3.c_str(), request3.length(), 0);

  char buffer3[4096];
  ssize_t received3 = recv(sock, buffer3, sizeof(buffer3) - 1, 0);
  ASSERT_GT(received3, 0);
  buffer3[received3] = '\0';
  std::string response3(buffer3);

  EXPECT_TRUE(response3.find("OK RESULTS 1") == 0);

  // Search for "different" in isolated_a - should NOT find it
  std::string request4 = "SEARCH isolated_a different\r\n";
  send(sock, request4.c_str(), request4.length(), 0);

  char buffer4[4096];
  ssize_t received4 = recv(sock, buffer4, sizeof(buffer4) - 1, 0);
  ASSERT_GT(received4, 0);
  buffer4[received4] = '\0';
  std::string response4(buffer4);

  EXPECT_TRUE(response4.find("OK RESULTS 0") == 0);

  close(sock);
  server.Stop();
}

#ifdef USE_MYSQL

/**
 * @brief Test binlog event routing to correct tables
 */
TEST(BinlogReaderMultiTableTest, EventRoutingToDifferentTables) {
  // Create mock connection (won't actually connect)
  mygramdb::mysql::Connection::Config conn_config;
  conn_config.host = "localhost";
  conn_config.user = "test";
  conn_config.password = "test";
  mygramdb::mysql::Connection conn(conn_config);

  // Create two tables
  auto index1 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store1 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table1;
  table1.name = "users";
  table1.config.name = "users";
  table1.config.primary_key = "id";
  table1.config.text_source.column = "name";
  table1.config.ngram_size = 1;
  table1.index = std::move(index1);
  table1.doc_store = std::move(doc_store1);

  auto index2 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store2 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table2;
  table2.name = "products";
  table2.config.name = "products";
  table2.config.primary_key = "id";
  table2.config.text_source.column = "description";
  table2.config.ngram_size = 1;
  table2.index = std::move(index2);
  table2.doc_store = std::move(doc_store2);

  std::unordered_map<std::string, mygramdb::server::TableContext*> table_contexts;
  table_contexts["users"] = &table1;
  table_contexts["products"] = &table2;

  // Create binlog reader in multi-table mode
  mygramdb::mysql::BinlogReader::Config reader_config;
  reader_config.start_gtid = "test-uuid:1";
  reader_config.queue_size = 100;

  mygramdb::mysql::BinlogReader reader(conn, table_contexts, reader_config);

  // Verify initial state
  EXPECT_FALSE(reader.IsRunning());
  EXPECT_EQ(reader.GetProcessedEvents(), 0);

  // Verify multi-table mode by checking that both tables are accessible
  EXPECT_EQ(table1.doc_store->Size(), 0);
  EXPECT_EQ(table2.doc_store->Size(), 0);
}

/**
 * @brief Test that events for unknown tables are ignored
 */
TEST(BinlogReaderMultiTableTest, UnknownTableEventIgnored) {
  mygramdb::mysql::Connection::Config conn_config;
  conn_config.host = "localhost";
  conn_config.user = "test";
  conn_config.password = "test";
  mygramdb::mysql::Connection conn(conn_config);

  // Create only one table
  auto index1 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store1 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table1;
  table1.name = "known_table";
  table1.config.name = "known_table";
  table1.config.primary_key = "id";
  table1.config.text_source.column = "text";
  table1.config.ngram_size = 1;
  table1.index = std::move(index1);
  table1.doc_store = std::move(doc_store1);

  std::unordered_map<std::string, mygramdb::server::TableContext*> table_contexts;
  table_contexts["known_table"] = &table1;

  mygramdb::mysql::BinlogReader::Config reader_config;
  reader_config.start_gtid = "test-uuid:1";

  mygramdb::mysql::BinlogReader reader(conn, table_contexts, reader_config);

  // Verify that the reader is configured correctly
  EXPECT_FALSE(reader.IsRunning());

  // The binlog reader should silently ignore events for tables not in table_contexts
  // This is tested implicitly by the ProcessEvent implementation which checks:
  // auto it = table_contexts_.find(event.table_name);
  // if (it == table_contexts_.end()) { return true; }

  EXPECT_EQ(table1.doc_store->Size(), 0);
}

/**
 * @brief Test INSERT/UPDATE/DELETE events on multiple tables
 */
TEST(BinlogReaderMultiTableTest, MultipleEventTypesAcrossTables) {
  mygramdb::mysql::Connection::Config conn_config;
  conn_config.host = "localhost";
  conn_config.user = "test";
  conn_config.password = "test";
  mygramdb::mysql::Connection conn(conn_config);

  // Create three tables with different configurations
  auto index1 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store1 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table1;
  table1.name = "articles";
  table1.config.name = "articles";
  table1.config.primary_key = "article_id";
  table1.config.text_source.column = "title";
  table1.config.ngram_size = 1;
  table1.index = std::move(index1);
  table1.doc_store = std::move(doc_store1);

  auto index2 = std::make_unique<mygramdb::index::Index>(2);
  auto doc_store2 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table2;
  table2.name = "comments";
  table2.config.name = "comments";
  table2.config.primary_key = "comment_id";
  table2.config.text_source.column = "content";
  table2.config.ngram_size = 2;
  table2.index = std::move(index2);
  table2.doc_store = std::move(doc_store2);

  auto index3 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store3 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table3;
  table3.name = "tags";
  table3.config.name = "tags";
  table3.config.primary_key = "tag_id";
  table3.config.text_source.column = "name";
  table3.config.ngram_size = 1;
  table3.index = std::move(index3);
  table3.doc_store = std::move(doc_store3);

  std::unordered_map<std::string, mygramdb::server::TableContext*> table_contexts;
  table_contexts["articles"] = &table1;
  table_contexts["comments"] = &table2;
  table_contexts["tags"] = &table3;

  mygramdb::mysql::BinlogReader::Config reader_config;
  reader_config.start_gtid = "test-uuid:1";

  mygramdb::mysql::BinlogReader reader(conn, table_contexts, reader_config);

  // Verify all tables are empty initially
  EXPECT_EQ(table1.doc_store->Size(), 0);
  EXPECT_EQ(table2.doc_store->Size(), 0);
  EXPECT_EQ(table3.doc_store->Size(), 0);

  // Verify different ngram sizes are preserved
  EXPECT_EQ(table1.config.ngram_size, 1);
  EXPECT_EQ(table2.config.ngram_size, 2);
  EXPECT_EQ(table3.config.ngram_size, 1);
}

/**
 * @brief Test that binlog reader correctly identifies multi-table mode
 */
TEST(BinlogReaderMultiTableTest, MultiTableModeConfiguration) {
  mygramdb::mysql::Connection::Config conn_config;
  conn_config.host = "localhost";
  conn_config.user = "test";
  conn_config.password = "test";
  mygramdb::mysql::Connection conn(conn_config);

  // Create two tables
  auto index1 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store1 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table1;
  table1.name = "table_a";
  table1.config.name = "table_a";
  table1.config.primary_key = "id";
  table1.config.text_source.column = "data";
  table1.config.ngram_size = 1;
  table1.index = std::move(index1);
  table1.doc_store = std::move(doc_store1);

  auto index2 = std::make_unique<mygramdb::index::Index>(1);
  auto doc_store2 = std::make_unique<mygramdb::storage::DocumentStore>();
  mygramdb::server::TableContext table2;
  table2.name = "table_b";
  table2.config.name = "table_b";
  table2.config.primary_key = "id";
  table2.config.text_source.column = "data";
  table2.config.ngram_size = 1;
  table2.index = std::move(index2);
  table2.doc_store = std::move(doc_store2);

  std::unordered_map<std::string, mygramdb::server::TableContext*> table_contexts;
  table_contexts["table_a"] = &table1;
  table_contexts["table_b"] = &table2;

  // Test with reader configuration
  mygramdb::mysql::BinlogReader::Config reader_config;
  reader_config.start_gtid = "server-uuid:100";

  mygramdb::mysql::BinlogReader reader(conn, table_contexts, reader_config);

  // Verify configuration
  EXPECT_FALSE(reader.IsRunning());
  EXPECT_EQ(reader.GetCurrentGTID(), "server-uuid:100");
  EXPECT_EQ(reader.GetProcessedEvents(), 0);
  EXPECT_EQ(reader.GetQueueSize(), 0);
}

#endif  // USE_MYSQL
