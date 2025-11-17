/**
 * Test snapshot save and load with id < 10000 filter
 */

#include <spdlog/spdlog.h>

#include <iostream>

#include "config/config.h"
#include "index/index.h"
#include "mysql/connection.h"
#include "storage/document_store.h"
#include "storage/snapshot_builder.h"

int main() {
  spdlog::set_level(spdlog::level::info);
  spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");

  // MySQL connection config
  mygramdb::mysql::Connection::Config mysql_config;
  mysql_config.host = "127.0.0.1";
  mysql_config.port = 3306;
  mysql_config.user = "root";
  mysql_config.password = "solaris10";
  mysql_config.database = "test";

  // Table config
  mygramdb::config::TableConfig table_config;
  table_config.name = "threads";
  table_config.primary_key = "id";
  table_config.text_source.column = "name";
  table_config.ngram_size = 2;

  // Required filters (data existence conditions)
  mygramdb::config::RequiredFilterConfig req_filter1;
  req_filter1.name = "enabled";
  req_filter1.type = "int";
  req_filter1.op = "=";
  req_filter1.value = "1";
  table_config.required_filters.push_back(req_filter1);

  mygramdb::config::RequiredFilterConfig req_filter2;
  req_filter2.name = "id";
  req_filter2.type = "int";
  req_filter2.op = "<";
  req_filter2.value = "10000";
  table_config.required_filters.push_back(req_filter2);

  // Optional filter config (for search-time filtering)
  mygramdb::config::FilterConfig filter;
  filter.name = "comic_type_id";
  filter.type = "int";
  filter.dict_compress = true;
  filter.bitmap_index = true;
  table_config.filters.push_back(filter);

  std::cout << "\n=== Phase 1: Build and Save Snapshot ===" << std::endl;

  // Create index and document store
  auto index = std::make_unique<mygramdb::index::Index>(table_config.ngram_size);
  auto doc_store = std::make_unique<mygramdb::storage::DocumentStore>();

  // Connect to MySQL
  auto mysql_conn = std::make_unique<mygramdb::mysql::Connection>(mysql_config);
  if (!mysql_conn->Connect()) {
    std::cerr << "Failed to connect to MySQL: " << mysql_conn->GetLastError() << std::endl;
    return 1;
  }
  std::cout << "✓ Connected to MySQL" << std::endl;

  // Build snapshot
  mygramdb::storage::SnapshotBuilder snapshot_builder(*mysql_conn, *index, *doc_store, table_config);

  auto result = snapshot_builder.Build([](const auto& progress) {
    if (progress.processed_rows % 5000 == 0 && progress.processed_rows > 0) {
      std::cout << "  Processed " << progress.processed_rows << " rows (" << progress.rows_per_second << " rows/s)"
                << std::endl;
    }
  });

  if (!result) {
    std::cerr << "Failed to build snapshot: " << result.error().message() << std::endl;
    return 1;
  }

  uint64_t original_rows = snapshot_builder.GetProcessedRows();
  std::string snapshot_gtid = snapshot_builder.GetSnapshotGTID();
  std::cout << "✓ Snapshot built: " << original_rows << " rows" << std::endl;
  std::cout << "✓ Snapshot GTID: " << snapshot_gtid << std::endl;

  // Test GetDocId for a known document
  auto test_doc_id = doc_store->GetDocId("100");
  if (test_doc_id) {
    std::cout << "✓ Test document found: id=100 -> doc_id=" << test_doc_id.value() << std::endl;
  }

  // Save to disk
  std::string index_file = "/tmp/mygramdb_index_test.dat";
  std::string docstore_file = "/tmp/mygramdb_docstore_test.dat";

  if (!index->SaveToFile(index_file)) {
    std::cerr << "Failed to save index" << std::endl;
    return 1;
  }
  std::cout << "✓ Index saved to " << index_file << std::endl;

  if (!doc_store->SaveToFile(docstore_file, snapshot_gtid)) {
    std::cerr << "Failed to save document store" << std::endl;
    return 1;
  }
  std::cout << "✓ Document store saved to " << docstore_file << std::endl;

  std::cout << "\n=== Phase 2: Load from Disk ===" << std::endl;

  // Create new empty index and document store
  auto index2 = std::make_unique<mygramdb::index::Index>(table_config.ngram_size);
  auto doc_store2 = std::make_unique<mygramdb::storage::DocumentStore>();

  // Load from disk
  if (!index2->LoadFromFile(index_file)) {
    std::cerr << "Failed to load index" << std::endl;
    return 1;
  }
  std::cout << "✓ Index loaded from " << index_file << std::endl;

  std::string loaded_gtid;
  if (!doc_store2->LoadFromFile(docstore_file, &loaded_gtid)) {
    std::cerr << "Failed to load document store" << std::endl;
    return 1;
  }
  std::cout << "✓ Document store loaded from " << docstore_file << std::endl;
  std::cout << "✓ Loaded GTID: " << loaded_gtid << std::endl;

  // Verify GTID
  if (snapshot_gtid != loaded_gtid) {
    std::cerr << "✗ GTID mismatch!" << std::endl;
    std::cerr << "  Original: " << snapshot_gtid << std::endl;
    std::cerr << "  Loaded: " << loaded_gtid << std::endl;
    return 1;
  }
  std::cout << "✓ GTID matches: " << loaded_gtid << std::endl;

  // Verify document lookup
  auto loaded_test_doc_id = doc_store2->GetDocId("100");
  if (!loaded_test_doc_id) {
    std::cerr << "✗ Test document not found in loaded store!" << std::endl;
    return 1;
  }
  if (test_doc_id.value() != loaded_test_doc_id.value()) {
    std::cerr << "✗ Document ID mismatch!" << std::endl;
    std::cerr << "  Original: " << test_doc_id.value() << std::endl;
    std::cerr << "  Loaded: " << loaded_test_doc_id.value() << std::endl;
    return 1;
  }
  std::cout << "✓ Document lookup matches: id=100 -> doc_id=" << loaded_test_doc_id.value() << std::endl;

  // Verify document outside range is not present
  auto outside_doc_id = doc_store2->GetDocId("20000");
  if (outside_doc_id) {
    std::cerr << "✗ Document id=20000 should not be in snapshot (id < 10000 filter)!" << std::endl;
    return 1;
  }
  std::cout << "✓ Document id=20000 correctly not in snapshot (filtered out)" << std::endl;

  std::cout << "\n=== ALL TESTS PASSED ===" << std::endl;
  std::cout << "Snapshot with 'id < 10000' filter:" << std::endl;
  std::cout << "  - Saved " << original_rows << " rows" << std::endl;
  std::cout << "  - GTID preserved: " << loaded_gtid << std::endl;
  std::cout << "  - Data integrity verified" << std::endl;

  return 0;
}
