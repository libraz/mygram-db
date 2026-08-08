/**
 * @file document_store.h
 * @brief Document store for primary key mapping and filter columns
 */

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_set>
#include <variant>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "types/doc_id.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/hash_utils.h"

namespace mygramdb::storage {

class FilterIndex;  // Forward declaration (defined in filter_index.h)

using mygram::utils::Error;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

// DocId is now defined in types/doc_id.h and re-exported via namespace

/**
 * @brief TIME value representation
 *
 * Stores MySQL TIME type as seconds since midnight
 * Range: -3020399 to 3020399 (-838:59:59 to 838:59:59)
 */
struct TimeValue {
  int64_t seconds;  // Seconds since midnight (can be negative)

  bool operator==(const TimeValue& other) const { return seconds == other.seconds; }
  bool operator!=(const TimeValue& other) const { return seconds != other.seconds; }
  bool operator<(const TimeValue& other) const { return seconds < other.seconds; }
  bool operator<=(const TimeValue& other) const { return seconds <= other.seconds; }
  bool operator>(const TimeValue& other) const { return seconds > other.seconds; }
  bool operator>=(const TimeValue& other) const { return seconds >= other.seconds; }
};

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
 * - uint64_t: BIGINT UNSIGNED (legacy dumps may also contain non-negative datetime values)
 * - TimeValue: TIME (seconds since midnight, -3020399 to 3020399)
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
                                 uint64_t,        // BIGINT UNSIGNED (or legacy non-negative DATETIME/TIMESTAMP)
                                 TimeValue,       // TIME (seconds since midnight)
                                 std::string,     // VARCHAR/TEXT
                                 double           // FLOAT/DOUBLE
                                 >;

/**
 * @brief Document metadata
 */
/// Filter map type with transparent hash for heterogeneous lookup (string_view → no allocation)
using FilterMap = absl::flat_hash_map<std::string, FilterValue, mygram::utils::TransparentStringHash,
                                      mygram::utils::TransparentStringEqual>;

/**
 * @brief Document metadata
 */
struct Document {
  DocId doc_id = 0;
  std::string primary_key;
  FilterMap filters;
};

/// Interned filter-column name, valid within one DocumentStore.
using FilterColumnId = uint32_t;

/**
 * @brief One document's filter values, keyed by interned column
 *
 * Holding a FilterMap per document costs one hash map plus one owned column-name
 * string per (document, column) pair — 10 million rows with 5 filter columns
 * means 10 million hash maps and 50 million column-name strings for a set of
 * names the table configuration fixes at a handful. The names are interned once
 * per store and each document keeps only a small vector sorted by column id,
 * which for the usual handful of columns is also faster to scan than to hash.
 */
struct DocumentFilterValues {
  std::vector<std::pair<FilterColumnId, FilterValue>> entries;  ///< Sorted by column id

  [[nodiscard]] const FilterValue* Find(FilterColumnId column_id) const {
    const auto iterator = std::lower_bound(entries.begin(), entries.end(), column_id,
                                           [](const auto& entry, FilterColumnId id) { return entry.first < id; });
    if (iterator == entries.end() || iterator->first != column_id) {
      return nullptr;
    }
    return &iterator->second;
  }
};

/**
 * @brief Document store
 *
 * Manages DocID <-> Primary Key mapping and filter columns
 */
class DocumentStore {
 public:
  struct NormalizedTextEntry {
    DocId doc_id;
    std::string text;
  };

  using NormalizedTextChunkVisitor = std::function<bool(const std::vector<NormalizedTextEntry>&)>;

  /// Receives the position in the requested DocID list, the DocID, and its
  /// normalized text — nullptr when the document has no stored text. Return
  /// false to stop iteration.
  using SelectedNormalizedTextVisitor = std::function<bool(size_t, DocId, const std::string*)>;

  /// Documents whose text is materialized per lock acquisition when verifying a
  /// candidate set. Bounds peak copy size independently of the candidate count.
  static constexpr size_t kSelectedNormalizedTextChunkSize = 1024;

  DocumentStore();
  ~DocumentStore();

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
    FilterMap filters;
    std::string normalized_text;  ///< Normalized text for n-gram verification
    std::string original_text;    ///< Original source text for user-facing highlights
  };

  /**
   * @brief Add document (insert-or-ignore semantic)
   *
   * If a document with the same primary key already exists, the existing DocId
   * is returned without modifying the document. The existing document's text
   * and filters are NOT updated. No error is raised for duplicates.
   *
   * @param primary_key Primary key from MySQL
   * @param filters Filter column values
   * @return Expected<DocId, Error> Assigned DocID (or existing DocID if duplicate primary key),
   *         or error (e.g., DocID exhausted)
   */
  [[nodiscard]] Expected<DocId, Error> AddDocument(std::string_view primary_key, const FilterMap& filters = {},
                                                   std::string_view normalized_text = "",
                                                   std::string_view original_text = "");

  /**
   * @brief Add multiple documents (batch operation, thread-safe, insert-or-ignore semantic)
   *
   * This method is optimized for bulk insertions during snapshot builds.
   * It processes documents in a single lock acquisition for better performance.
   *
   * If a document with a duplicate primary key is encountered, the existing DocId
   * is placed in the result vector at the corresponding position without modifying
   * the existing document. No error is raised for duplicates.
   *
   * @param documents Vector of documents to add
   * @param existing_doc_ids_out If non-null, populated with DocIds of documents
   *        that already existed (duplicates). Caller can use this to skip indexing.
   * @return Expected<std::vector<DocId>, Error> Vector of assigned DocIDs (same order as input,
   *         existing DocIDs for duplicates) or error (e.g., DocID exhausted mid-batch)
   * @note This method is thread-safe
   */
  [[nodiscard]] Expected<std::vector<DocId>, Error> AddDocumentBatch(
      const std::vector<DocumentItem>& documents, std::unordered_set<DocId>* existing_doc_ids_out = nullptr);

  /**
   * @brief Update document
   *
   * @param doc_id Document ID
   * @param filters New filter values
   * @return true if document exists
   */
  bool UpdateDocument(DocId doc_id, const FilterMap& filters);

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
  [[nodiscard]] std::optional<DocId> GetDocId(std::string_view primary_key) const;

  /**
   * @brief Get primary key by DocID
   *
   * @param doc_id Document ID
   * @return Primary key if exists
   */
  [[nodiscard]] std::optional<std::string> GetPrimaryKey(DocId doc_id) const;

  /**
   * @brief Get documents by multiple IDs in a single lock acquisition
   *
   * @param doc_ids Vector of document IDs to retrieve
   * @return Vector of optional documents (nullopt for non-existent IDs)
   */
  [[nodiscard]] std::vector<std::optional<Document>> GetDocumentsBatch(const std::vector<DocId>& doc_ids) const;

  /**
   * @brief Get primary keys for multiple DocIDs (batch operation)
   *
   * Optimized for retrieving many primary keys in a single lock acquisition.
   * Missing documents will have `std::nullopt` in the result. A present empty
   * string is a valid primary key and remains distinguishable from a missing
   * document.
   *
   * @param doc_ids Vector of document IDs
   * @return Vector of primary keys (`std::nullopt` if doc_id not found)
   */
  [[nodiscard]] std::vector<std::optional<std::string>> GetPrimaryKeysBatch(const std::vector<DocId>& doc_ids) const;

  /**
   * @brief Get filter value
   *
   * @param doc_id Document ID
   * @param filter_name Filter column name
   * @return Filter value if exists
   */
  [[nodiscard]] std::optional<FilterValue> GetFilterValue(DocId doc_id, std::string_view filter_name) const;

  /**
   * @brief Filter documents by value
   *
   * @param filter_name Filter column name
   * @param value Filter value
   * @return Vector of matching DocIDs
   */
  [[nodiscard]] std::vector<DocId> FilterByValue(std::string_view filter_name, const FilterValue& value) const;

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
  [[nodiscard]] bool HasFilterColumn(std::string_view filter_name) const;

  /**
   * @brief Resolve a filter column name using case-insensitive matching
   *
   * Returns the stored column key when exactly one case-insensitive match exists.
   * Exact matches are preferred. Ambiguous case-only duplicates return nullopt.
   *
   * @param filter_name Requested filter column name
   * @return Stored filter column name if resolvable
   */
  [[nodiscard]] std::optional<std::string> ResolveFilterColumnName(std::string_view filter_name) const;

  /**
   * @brief Get total document count (thread-safe)
   */
  [[nodiscard]] size_t Size() const {
    std::shared_lock lock(mutex_);
    return doc_id_to_pk_.size();
  }

  /**
   * @brief Get filter values for multiple DocIDs in a single lock acquisition
   *
   * @param doc_ids Vector of document IDs
   * @param column Filter column name
   * @return Vector of filter values (nullopt if doc_id or column not found)
   */
  [[nodiscard]] std::vector<std::optional<FilterValue>> GetFilterValuesBatch(const std::vector<DocId>& doc_ids,
                                                                             const std::string& column) const;

  /**
   * @brief Get filter values for multiple columns in a single lock acquisition
   *
   * More efficient than calling GetFilterValuesBatch once per column when
   * multiple filter columns are needed for the same set of documents.
   *
   * @param doc_ids Vector of document IDs
   * @param columns Vector of filter column names
   * @return Vector of per-column results; outer index = column, inner index = doc_id.
   *         Each element is nullopt if doc_id or column not found for that document.
   */
  [[nodiscard]] std::vector<std::vector<std::optional<FilterValue>>> GetFilterValuesBatchMultiColumn(
      const std::vector<DocId>& doc_ids, const std::vector<std::string>& columns) const;

  /**
   * @brief Enable or disable storing normalized text for documents
   *
   * When disabled, AddDocument and AddDocumentBatch skip populating doc_texts_,
   * saving memory when verify_text is not needed. Enabled by default.
   *
   * @param enabled Whether to store normalized text
   */
  void SetStoreTexts(bool enabled) { store_texts_.store(enabled, std::memory_order_relaxed); }

  /**
   * @brief Check if normalized text storage is enabled
   * @return true if AddDocument stores normalized text
   */
  [[nodiscard]] bool IsStoreTextsEnabled() const { return store_texts_.load(std::memory_order_relaxed); }

  /**
   * @brief Whether primary-key order is known to match DocID allocation order.
   *
   * SEARCH Top-N can use Index DocID order as primary-key order only while this
   * remains true. Non-numeric keys, out-of-order inserts, deletes, and loads
   * that violate the invariant clear the flag.
   */
  [[nodiscard]] bool IsPrimaryKeyDocIdOrderValid() const;

  /**
   * @brief Set normalized text for a document (for n-gram verification)
   *
   * @param doc_id Document ID
   * @param text Normalized text to store
   */
  void SetNormalizedText(DocId doc_id, std::string_view text);

  /**
   * @brief Get normalized text for a document (for n-gram verification)
   *
   * @param doc_id Document ID
   * @return Normalized text if stored, std::nullopt otherwise
   */
  [[nodiscard]] std::optional<std::string> GetNormalizedText(DocId doc_id) const;

  /**
   * @brief Get normalized text for multiple documents (batch operation)
   *
   * Optimized for retrieving many normalized texts in a single lock acquisition.
   *
   * @param doc_ids Vector of document IDs
   * @return Vector of optional normalized texts (nullopt if doc_id not found or text not stored)
   */
  [[nodiscard]] std::vector<std::optional<std::string>> GetNormalizedTextBatch(const std::vector<DocId>& doc_ids) const;

  /**
   * @brief Visit the normalized text of specific documents in bounded chunks
   *
   * GetNormalizedTextBatch() copies every requested document's text before the
   * first comparison runs, so verifying or scoring a large candidate set
   * duplicates a large fraction of the corpus. This materializes one bounded
   * chunk at a time and runs the visitor with the store mutex released, so peak
   * copy size depends on the chunk size rather than the candidate count.
   *
   * @param doc_ids Documents to visit, in the caller's order
   * @param max_doc_ids_per_chunk Documents copied per lock acquisition (0 = no visit)
   * @param visitor Callback invoked once per document
   */
  void VisitNormalizedTextsFor(const std::vector<DocId>& doc_ids, size_t max_doc_ids_per_chunk,
                               const SelectedNormalizedTextVisitor& visitor) const;

  /**
   * @brief Visit stored normalized texts in bounded DocID chunks
   *
   * Each chunk owns at most @p max_doc_ids_per_chunk texts and the visitor runs
   * without holding the store mutex. Returning false stops iteration.
   * The DocID high-water mark is captured when iteration starts.
   *
   * @param max_doc_ids_per_chunk Maximum DocID range copied per lock acquisition
   * @param visitor Callback invoked for each non-empty chunk
   */
  void VisitNormalizedTextChunks(size_t max_doc_ids_per_chunk, const NormalizedTextChunkVisitor& visitor) const;

  /// Set original, non-normalized source text for user-facing highlights.
  void SetOriginalText(DocId doc_id, std::string_view text);

  /// Get original, non-normalized source text for one document.
  [[nodiscard]] std::optional<std::string> GetOriginalText(DocId doc_id) const;

  /// Batch-get original, non-normalized source text in DocId order.
  [[nodiscard]] std::vector<std::optional<std::string>> GetOriginalTextBatch(const std::vector<DocId>& doc_ids) const;

  /// Get a snapshot of the filter index (thread-safe, caller holds shared_ptr)
  [[nodiscard]] std::shared_ptr<const FilterIndex> GetFilterIndex() const;

  /**
   * @brief Get memory usage estimate.
   *
   * WARNING: O(N) complexity -- iterates all document maps under a shared lock,
   * blocking writers for the entire scan duration. Callers should avoid invoking
   * this on hot paths or at high frequency. Suitable for periodic health/metrics
   * endpoints (e.g., once every 30-60 seconds).
   */
  [[nodiscard]] size_t MemoryUsage() const;

  /**
   * @brief Clear all documents
   */
  void Clear();

  /**
   * @brief Atomically swap in validated state while retaining the old state.
   *
   * The previous live data is left in @p loaded, allowing dump/SYNC callers
   * to roll back a larger transaction if a subsequent component fails. The
   * target store_texts setting is preserved.
   */
  void ReplaceWithLoaded(DocumentStore& loaded);

  /**
   * @brief Compact internal data structures to reclaim memory
   *
   * After many insertions and deletions, the internal hash maps may
   * have excess bucket capacity. This method rehashes the maps to
   * reduce memory usage.
   */
  void Compact();

  /**
   * @brief Serialize document store to file
   * @param filepath Output file path
   * @param replication_gtid Optional GTID position for replication (empty if not using replication)
   * @return Expected<void, Error> Success or error with details
   */
  [[nodiscard]] Expected<void, Error> SaveToFile(const std::string& filepath,
                                                 const std::string& replication_gtid = "") const;

  /**
   * @brief Serialize document store to output stream
   * @param output_stream Output stream
   * @param replication_gtid Optional GTID position for replication (empty if not using replication)
   * @return Expected<void, Error> Success or error with details
   */
  [[nodiscard]] Expected<void, Error> SaveToStream(std::ostream& output_stream,
                                                   const std::string& replication_gtid = "") const;

  /**
   * @brief Deserialize document store from file
   * @param filepath Input file path
   * @param replication_gtid Output parameter for GTID position (empty if snapshot has no GTID)
   * @return Expected<void, Error> Success or error with details
   */
  [[nodiscard]] Expected<void, Error> LoadFromFile(const std::string& filepath,
                                                   std::string* replication_gtid = nullptr);

  /**
   * @brief Deserialize document store from input stream
   * @param input_stream Input stream
   * @param replication_gtid Output parameter for GTID position (empty if snapshot has no GTID)
   * @return Expected<void, Error> Success or error with details
   */
  [[nodiscard]] Expected<void, Error> LoadFromStream(std::istream& input_stream,
                                                     std::string* replication_gtid = nullptr);

 protected:
  /// @brief Get the next DocID value (for testing subclasses)
  [[nodiscard]] DocId GetNextDocId() const { return next_doc_id_; }

  /// @brief Set the next DocID value (for testing subclasses)
  void SetNextDocId(DocId value) { next_doc_id_ = value; }

 private:
  friend struct DumpLoadAccess;

  // Next DocID to assign
  DocId next_doc_id_ = 1;
  // DocID -> Primary Key mapping
  absl::flat_hash_map<DocId, std::string> doc_id_to_pk_;

  // Primary Key -> DocID mapping (reverse index)
  // Uses absl::flat_hash_map with transparent hash for heterogeneous lookup.
  // This allows GetDocId(std::string_view) without creating temporary std::string
  absl::flat_hash_map<std::string, DocId, mygram::utils::TransparentStringHash, mygram::utils::TransparentStringEqual>
      pk_to_doc_id_;

  // DocID -> Filter values, keyed by interned column id. FilterMap stays the
  // interface type for producers and consumers; only the stored form is interned.
  absl::flat_hash_map<DocId, DocumentFilterValues> doc_filters_;

  // Interned filter-column names. Ids are dense indices into filter_column_names_
  // and are never reused, so a stored DocumentFilterValues stays valid for the
  // lifetime of the store.
  std::vector<std::string> filter_column_names_;
  absl::flat_hash_map<std::string, FilterColumnId, mygram::utils::TransparentStringHash,
                      mygram::utils::TransparentStringEqual>
      filter_column_ids_;

  /// Intern a column name, assigning a new id when it is seen for the first time.
  /// @note Caller must hold mutex_ exclusively
  FilterColumnId InternFilterColumnLocked(std::string_view column_name);

  /// Look up an already-interned column name.
  /// @note Caller must hold mutex_ at least in shared mode
  [[nodiscard]] std::optional<FilterColumnId> FindFilterColumnLocked(std::string_view column_name) const;

  /// Convert a caller-supplied FilterMap into the interned stored form.
  /// @note Caller must hold mutex_ exclusively
  [[nodiscard]] DocumentFilterValues InternFilterMapLocked(const FilterMap& filters);

  /// Rebuild a FilterMap from the interned stored form.
  /// @note Caller must hold mutex_ at least in shared mode
  [[nodiscard]] FilterMap MaterializeFiltersLocked(const DocumentFilterValues& values) const;

  // DocID -> Normalized text (for n-gram post-filter verification)
  absl::flat_hash_map<DocId, std::string> doc_texts_;
  // DocID -> Original source text (for user-facing highlights)
  absl::flat_hash_map<DocId, std::string> original_texts_;

  // Whether to store normalized text in doc_texts_ (disabled saves memory when verify_text is off).
  // memory_order_relaxed is correct here because this flag is set during initialization
  // before worker threads start. If runtime toggling is ever needed, upgrade to
  // memory_order_release/acquire.
  std::atomic<bool> store_texts_{true};

  bool primary_key_doc_id_order_valid_ = true;
  std::optional<uint64_t> last_numeric_primary_key_;

  // Bitmap-based filter index for fast EQ/NE filter evaluation
  // Uses shared_ptr so readers can hold a lifetime-safe snapshot while Clear()
  // or LoadFrom() swaps in a new index. Per-document writes mutate the current
  // FilterIndex under mutex_, and FilterIndex provides its own internal
  // synchronization for bitmap-level readers.
  std::shared_ptr<FilterIndex> filter_index_;

  // Mutex for thread-safe access (shared for reads, exclusive for writes)
  mutable std::shared_mutex mutex_;

  /// Serialize all documents to an output stream (called by SaveToFile and SaveToStream)
  /// @return true if all writes succeeded, false on stream error
  bool SerializeDocuments(std::ostream& out, const std::string& replication_gtid) const;

  /// Deserialize all documents from an input stream (called by LoadFromFile and LoadFromStream)
  /// @param context Identifier for error messages (e.g., filepath or "stream")
  Expected<void, Error> DeserializeDocuments(std::istream& in, std::string* replication_gtid,
                                             const std::string& context);

  void RecordPrimaryKeyForDocIdOrder(std::string_view primary_key);
  void RecomputePrimaryKeyDocIdOrderLocked();
};

}  // namespace mygramdb::storage
