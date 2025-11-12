/**
 * @file document_store.h
 * @brief Document store for primary key mapping and filter columns
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace mygramdb::storage {

using DocId = uint32_t;  // Supports up to 4B documents (aligned with index::DocId)

/**
 * @brief Filter value types
 *
 * Supports multiple types for memory efficiency:
 * - std::monostate: NULL value
 * - bool: BOOLEAN/TINYINT(1) (1 byte)
 * - int8_t: TINYINT (-128 to 127)
 * - uint8_t: TINYINT UNSIGNED (0 to 255)
 * - int16_t: SMALLINT (-32768 to 32767)
 * - uint16_t: SMALLINT UNSIGNED (0 to 65535)
 * - int32_t: INT/MEDIUMINT (-2B to 2B)
 * - uint32_t: INT UNSIGNED (0 to 4B)
 * - int64_t: BIGINT
 * - uint64_t: DATETIME/TIMESTAMP (epoch timestamp)
 * - double: FLOAT/DOUBLE
 * - std::string: VARCHAR/TEXT
 */
using FilterValue = std::variant<std::monostate,  // NULL value
                                 bool,            // BOOLEAN/TINYINT(1)
                                 int8_t,          // TINYINT
                                 uint8_t,         // TINYINT UNSIGNED
                                 int16_t,         // SMALLINT
                                 uint16_t,        // SMALLINT UNSIGNED
                                 int32_t,         // INT/MEDIUMINT
                                 uint32_t,        // INT UNSIGNED
                                 int64_t,         // BIGINT
                                 uint64_t,        // DATETIME/TIMESTAMP (epoch timestamp)
                                 std::string,     // VARCHAR/TEXT
                                 double           // FLOAT/DOUBLE
                                 >;

/**
 * @brief Document metadata
 */
struct Document {
  DocId doc_id = 0;
  std::string primary_key;
  std::unordered_map<std::string, FilterValue> filters;
};

/**
 * @brief Document store
 *
 * Manages DocID <-> Primary Key mapping and filter columns
 */
class DocumentStore {
 public:
  DocumentStore() = default;
  ~DocumentStore() = default;

  // Non-copyable and non-movable (due to std::shared_mutex)
  DocumentStore(const DocumentStore&) = delete;
  DocumentStore& operator=(const DocumentStore&) = delete;
  DocumentStore(DocumentStore&&) = delete;
  DocumentStore& operator=(DocumentStore&&) = delete;

  /**
   * @brief Document item for batch addition
   */
  struct DocumentItem {
    std::string primary_key;
    std::unordered_map<std::string, FilterValue> filters;
  };

  /**
   * @brief Add document
   *
   * @param primary_key Primary key from MySQL
   * @param filters Filter column values
   * @return Assigned DocID
   */
  DocId AddDocument(const std::string& primary_key, const std::unordered_map<std::string, FilterValue>& filters = {});

  /**
   * @brief Add multiple documents (batch operation, thread-safe)
   *
   * This method is optimized for bulk insertions during snapshot builds.
   * It processes documents in a single lock acquisition for better performance.
   *
   * @param documents Vector of documents to add
   * @return Vector of assigned DocIDs (same order as input)
   * @note This method is thread-safe
   */
  std::vector<DocId> AddDocumentBatch(const std::vector<DocumentItem>& documents);

  /**
   * @brief Update document
   *
   * @param doc_id Document ID
   * @param filters New filter values
   * @return true if document exists
   */
  bool UpdateDocument(DocId doc_id, const std::unordered_map<std::string, FilterValue>& filters);

  /**
   * @brief Remove document
   *
   * @param doc_id Document ID
   * @return true if document existed
   */
  bool RemoveDocument(DocId doc_id);

  /**
   * @brief Get document by DocID
   *
   * @param doc_id Document ID
   * @return Document if exists
   */
  [[nodiscard]] std::optional<Document> GetDocument(DocId doc_id) const;

  /**
   * @brief Get DocID by primary key
   *
   * @param primary_key Primary key
   * @return DocID if exists
   */
  [[nodiscard]] std::optional<DocId> GetDocId(const std::string& primary_key) const;

  /**
   * @brief Get primary key by DocID
   *
   * @param doc_id Document ID
   * @return Primary key if exists
   */
  [[nodiscard]] std::optional<std::string> GetPrimaryKey(DocId doc_id) const;

  /**
   * @brief Get filter value
   *
   * @param doc_id Document ID
   * @param filter_name Filter column name
   * @return Filter value if exists
   */
  [[nodiscard]] std::optional<FilterValue> GetFilterValue(DocId doc_id, const std::string& filter_name) const;

  /**
   * @brief Filter documents by value
   *
   * @param filter_name Filter column name
   * @param value Filter value
   * @return Vector of matching DocIDs
   */
  [[nodiscard]] std::vector<DocId> FilterByValue(const std::string& filter_name, const FilterValue& value) const;

  /**
   * @brief Get all document IDs
   *
   * @return Sorted vector of all document IDs
   */
  [[nodiscard]] std::vector<DocId> GetAllDocIds() const;

  /**
   * @brief Check if a filter column exists in any document
   *
   * This method is useful for validating ORDER BY and FILTER clauses.
   * Returns true if at least one document has the specified filter column.
   *
   * @param filter_name Filter column name
   * @return true if the column exists in at least one document
   */
  [[nodiscard]] bool HasFilterColumn(const std::string& filter_name) const;

  /**
   * @brief Get total document count
   */
  [[nodiscard]] size_t Size() const { return doc_id_to_pk_.size(); }

  /**
   * @brief Get memory usage estimate
   */
  [[nodiscard]] size_t MemoryUsage() const;

  /**
   * @brief Clear all documents
   */
  void Clear();

  /**
   * @brief Serialize document store to file
   * @param filepath Output file path
   * @param replication_gtid Optional GTID position for replication (empty if not using replication)
   * @return true if successful
   */
  [[nodiscard]] bool SaveToFile(const std::string& filepath, const std::string& replication_gtid = "") const;

  /**
   * @brief Deserialize document store from file
   * @param filepath Input file path
   * @param replication_gtid Output parameter for GTID position (empty if snapshot has no GTID)
   * @return true if successful
   */
  [[nodiscard]] bool LoadFromFile(const std::string& filepath, std::string* replication_gtid = nullptr);

 private:
  // Next DocID to assign
  DocId next_doc_id_ = 1;

  // DocID -> Primary Key mapping
  std::unordered_map<DocId, std::string> doc_id_to_pk_;

  // Primary Key -> DocID mapping (reverse index)
  std::unordered_map<std::string, DocId> pk_to_doc_id_;

  // DocID -> Filter values
  std::unordered_map<DocId, std::unordered_map<std::string, FilterValue>> doc_filters_;

  // Mutex for thread-safe access (shared for reads, exclusive for writes)
  mutable std::shared_mutex mutex_;
};

}  // namespace mygramdb::storage
