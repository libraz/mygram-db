/**
 * @file document_store.h
 * @brief Document store for primary key mapping and filter columns
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <optional>
#include <variant>

namespace mygramdb {
namespace storage {

using DocId = uint64_t;

/**
 * @brief Filter value types
 */
using FilterValue = std::variant<int64_t, std::string, double>;

/**
 * @brief Document metadata
 */
struct Document {
  DocId doc_id;
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

  /**
   * @brief Add document
   *
   * @param primary_key Primary key from MySQL
   * @param filters Filter column values
   * @return Assigned DocID
   */
  DocId AddDocument(const std::string& primary_key,
                    const std::unordered_map<std::string, FilterValue>& filters = {});

  /**
   * @brief Update document
   *
   * @param doc_id Document ID
   * @param filters New filter values
   * @return true if document exists
   */
  bool UpdateDocument(DocId doc_id,
                      const std::unordered_map<std::string, FilterValue>& filters);

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
  std::optional<Document> GetDocument(DocId doc_id) const;

  /**
   * @brief Get DocID by primary key
   *
   * @param primary_key Primary key
   * @return DocID if exists
   */
  std::optional<DocId> GetDocId(const std::string& primary_key) const;

  /**
   * @brief Get primary key by DocID
   *
   * @param doc_id Document ID
   * @return Primary key if exists
   */
  std::optional<std::string> GetPrimaryKey(DocId doc_id) const;

  /**
   * @brief Get filter value
   *
   * @param doc_id Document ID
   * @param filter_name Filter column name
   * @return Filter value if exists
   */
  std::optional<FilterValue> GetFilterValue(DocId doc_id,
                                            const std::string& filter_name) const;

  /**
   * @brief Filter documents by value
   *
   * @param filter_name Filter column name
   * @param value Filter value
   * @return Vector of matching DocIDs
   */
  std::vector<DocId> FilterByValue(const std::string& filter_name,
                                   const FilterValue& value) const;

  /**
   * @brief Get total document count
   */
  size_t Size() const { return doc_id_to_pk_.size(); }

  /**
   * @brief Get memory usage estimate
   */
  size_t MemoryUsage() const;

  /**
   * @brief Clear all documents
   */
  void Clear();

 private:
  // Next DocID to assign
  DocId next_doc_id_ = 1;

  // DocID -> Primary Key mapping
  std::unordered_map<DocId, std::string> doc_id_to_pk_;

  // Primary Key -> DocID mapping (reverse index)
  std::unordered_map<std::string, DocId> pk_to_doc_id_;

  // DocID -> Filter values
  std::unordered_map<DocId, std::unordered_map<std::string, FilterValue>> doc_filters_;
};

}  // namespace storage
}  // namespace mygramdb
