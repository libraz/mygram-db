/**
 * @file mygramclient_c.h
 * @brief C API wrapper for MygramDB client library
 *
 * This header provides a C-compatible interface for the MygramDB client library,
 * suitable for use with FFI bindings (node-gyp, ctypes, etc.).
 *
 * All functions return 0 on success, non-zero on error.
 * Use mygramclient_get_last_error() to retrieve error messages.
 *
 * A handle may be shared by threads. Connection lifecycle calls and commands
 * are serialized; disconnect waits for an in-flight command and does not
 * cancel it. mygramclient_destroy() must not race with another handle call.
 * Error-state access is synchronized and mygramclient_get_last_error() returns
 * a thread-local snapshot that remains valid until that thread calls it again.
 * On failure, writable pointer out-parameters are set to NULL and numeric
 * out-parameters are set to zero. No C++ exception crosses this C ABI:
 * unexpected failures are translated to the documented failure return, safe
 * output values, and last_error when a client handle is available.
 *
 * Note: This is a C API header, so typedef is used instead of using declarations
 * for C compatibility. The modernize-use-using check is disabled for this file.
 */

#pragma once

// NOLINTBEGIN(modernize-use-using) - C API requires typedef for C compatibility

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

/**
 * @brief Opaque handle to MygramDB client
 */
typedef struct MygramClient_C MygramClient_C;

/**
 * @brief Client configuration
 */
typedef struct {
  const char* host;           // Server hostname (default: "127.0.0.1")
  uint16_t port;              // Server port (default: 11016)
  uint32_t timeout_ms;        // Connection timeout in milliseconds (default: 5000)
  uint32_t recv_buffer_size;  // Receive buffer size (default: 65536)
} MygramClientConfig_C;

#define MYGRAMCLIENT_CONFIG_V2_VERSION 2U

/**
 * @brief Size/versioned client configuration.
 *
 * Callers must set struct_size to sizeof(MygramClientConfigV2_C) (or the size
 * known to an older caller) and version to MYGRAMCLIENT_CONFIG_V2_VERSION.
 * Fields beyond struct_size are ignored, preserving ABI compatibility when
 * this structure grows.
 */
typedef struct {
  uint32_t struct_size;             // Size known by the caller
  uint32_t version;                 // MYGRAMCLIENT_CONFIG_V2_VERSION
  const char* host;                 // TCP host (default: "127.0.0.1")
  uint16_t port;                    // TCP port (default: 11016)
  uint32_t timeout_ms;              // Per-request timeout (default: 5000)
  uint32_t recv_buffer_size;        // Receive buffer size (default: 65536)
  const char* unix_socket_path;     // UDS path; takes precedence over TCP when non-NULL
  uint32_t dump_save_timeout_ms;    // Async DUMP SAVE deadline (0 = client default)
  uint64_t max_response_bytes;      // Maximum response frame size (0 = client default 64 MiB)
  uint32_t connect_timeout_ms;      // Connect deadline (0 = timeout_ms)
  uint32_t dump_load_timeout_ms;    // DUMP LOAD deadline (0 = client default)
  uint32_t dump_verify_timeout_ms;  // DUMP VERIFY deadline (0 = client default)
  uint32_t optimize_timeout_ms;     // OPTIMIZE deadline (0 = client default)
} MygramClientConfigV2_C;

/**
 * @brief Search result
 */
typedef struct {
  char** primary_keys;   // Array of primary key strings
  size_t count;          // Number of results
  uint64_t total_count;  // Total matching documents (may exceed count)
} MygramSearchResult_C;

/**
 * @brief Facet result
 */
typedef struct {
  char** values;         // Array of facet values
  uint64_t* counts;      // Array of facet counts, aligned with values
  size_t count;          // Number of facet values in this page
  uint64_t total_count;  // Total distinct values before OFFSET/LIMIT
} MygramFacetResult_C;

/**
 * @brief Search result with highlight snippets
 */
typedef struct {
  char** primary_keys;   // Array of primary key strings
  char** snippets;       // Array of highlight snippets, aligned with primary_keys
  size_t count;          // Number of results
  uint64_t total_count;  // Total matching documents (may exceed count)
} MygramSearchResultWithHighlights_C;

typedef enum {
  MYGRAM_FILTER_EQ = 0,
  MYGRAM_FILTER_NE = 1,
  MYGRAM_FILTER_GT = 2,
  MYGRAM_FILTER_GTE = 3,
  MYGRAM_FILTER_LT = 4,
  MYGRAM_FILTER_LTE = 5,
} MygramFilterOp_C;

typedef enum {
  MYGRAM_QUERY_LITERAL = 0,
  MYGRAM_QUERY_BOOLEAN = 1,
} MygramQueryMode_C;

typedef struct {
  const char* key;
  MygramFilterOp_C op;
  const char* value;
} MygramFilter_C;

typedef struct {
  // Size known by the caller. The prefix through offset is mandatory; fields
  // beyond struct_size are ignored and receive library defaults so future
  // appended fields remain ABI-compatible with older callers.
  uint32_t struct_size;
  uint32_t limit;   // 0 = server default
  uint32_t offset;  // 0 = first result
  const char* const* and_terms;
  size_t and_count;
  const char* const* not_terms;
  size_t not_count;
  const MygramFilter_C* filters;
  size_t filter_count;
  const char* sort_column;
  int sort_desc;
  uint32_t fuzzy_distance;  // 0 = disabled; otherwise 1 or 2
  int highlight;
  const char* highlight_open_tag;
  const char* highlight_close_tag;
  uint32_t highlight_snippet_length;  // 0 = server default
  uint32_t highlight_max_fragments;   // 0 = server default
  MygramQueryMode_C query_mode;       // Literal by default; boolean sends query as an expression
} MygramSearchOptions_C;

/**
 * @brief Document with fields
 */
typedef struct {
  char* primary_key;    // Document primary key
  char** field_keys;    // Array of field keys
  char** field_values;  // Array of field values
  size_t field_count;   // Number of fields
} MygramDocument_C;

/**
 * @brief Server information
 */
typedef struct {
  char* version;
  uint64_t uptime_seconds;
  uint64_t total_requests;
  uint64_t active_connections;
  uint64_t index_size_bytes;
  uint64_t doc_count;
  char** tables;       // Array of table names
  size_t table_count;  // Number of tables
} MygramServerInfo_C;

/**
 * @brief Create a new MygramDB client
 *
 * @param config Client configuration
 * @return Client handle, or NULL on error
 */
MygramClient_C* mygramclient_create(const MygramClientConfig_C* config);

/** Create a client with the size/versioned configuration. */
MygramClient_C* mygramclient_create_v2(const MygramClientConfigV2_C* config);

/**
 * @brief Destroy a MygramDB client and free resources
 *
 * @param client Client handle
 */
void mygramclient_destroy(MygramClient_C* client);

/**
 * @brief Connect to MygramDB server
 *
 * @param client Client handle
 * @return 0 on success, -1 on error
 */
int mygramclient_connect(MygramClient_C* client);

/**
 * @brief Disconnect from server
 *
 * @param client Client handle
 */
void mygramclient_disconnect(MygramClient_C* client);

/**
 * @brief Check if connected to server
 *
 * @param client Client handle
 * @return 1 if connected, 0 otherwise
 */
int mygramclient_is_connected(const MygramClient_C* client);

/**
 * @brief Search for documents
 *
 * @param client Client handle
 * @param table Table name
 * @param query Search query text
 * @param limit Maximum number of results (0 for default)
 * @param offset Result offset for pagination
 * @param result Output search results (caller must free with mygramclient_free_search_result)
 * @return 0 on success, -1 on error
 */
int mygramclient_search(MygramClient_C* client, const char* table, const char* query, uint32_t limit, uint32_t offset,
                        MygramSearchResult_C** result);

/** Execute a boolean/grouped expression verbatim. */
int mygramclient_search_raw(MygramClient_C* client, const char* table, const char* raw_query, uint32_t limit,
                            uint32_t offset, MygramSearchResult_C** result);

/**
 * @brief Search for documents and return highlight snippets
 *
 * @param client Client handle
 * @param table Table name
 * @param query Search query text
 * @param limit Maximum number of results (0 for default)
 * @param offset Result offset for pagination
 * @param result Output search results (caller must free with mygramclient_free_search_result_with_highlights)
 * @return 0 on success, -1 on error
 */
int mygramclient_search_with_highlights(MygramClient_C* client, const char* table, const char* query, uint32_t limit,
                                        uint32_t offset, MygramSearchResultWithHighlights_C** result);

/** Execute a boolean/grouped expression verbatim and return highlights. */
int mygramclient_search_raw_with_highlights(MygramClient_C* client, const char* table, const char* raw_query,
                                            uint32_t limit, uint32_t offset,
                                            MygramSearchResultWithHighlights_C** result);

/** Search through the complete typed FILTER/FUZZY/HIGHLIGHT surface. */
int mygramclient_search_with_options(MygramClient_C* client, const char* table, const char* query,
                                     const MygramSearchOptions_C* options, MygramSearchResultWithHighlights_C** result);

/**
 * @brief Search for documents with AND/NOT/FILTER clauses and return highlight snippets
 *
 * @param client Client handle
 * @param table Table name
 * @param query Search query text
 * @param limit Maximum number of results (0 for default)
 * @param offset Result offset for pagination
 * @param and_terms Array of AND terms (can be NULL)
 * @param and_count Number of AND terms
 * @param not_terms Array of NOT terms (can be NULL)
 * @param not_count Number of NOT terms
 * @param filter_keys Array of filter keys (can be NULL)
 * @param filter_values Array of filter values (can be NULL)
 * @param filter_count Number of filters
 * @param sort_column Column name for SORT clause (can be NULL for primary key)
 * @param sort_desc Sort descending (0 = ascending, 1 = descending, default 1)
 * @param result Output search results (caller must free with mygramclient_free_search_result_with_highlights)
 * @return 0 on success, -1 on error
 */
int mygramclient_search_with_highlights_advanced(MygramClient_C* client, const char* table, const char* query,
                                                 uint32_t limit, uint32_t offset, const char** and_terms,
                                                 size_t and_count, const char** not_terms, size_t not_count,
                                                 const char** filter_keys, const char** filter_values,
                                                 size_t filter_count, const char* sort_column, int sort_desc,
                                                 MygramSearchResultWithHighlights_C** result);

/**
 * @brief Search for documents with AND/NOT/FILTER clauses
 *
 * @param client Client handle
 * @param table Table name
 * @param query Search query text
 * @param limit Maximum number of results (0 for default)
 * @param offset Result offset for pagination
 * @param and_terms Array of AND terms (can be NULL)
 * @param and_count Number of AND terms
 * @param not_terms Array of NOT terms (can be NULL)
 * @param not_count Number of NOT terms
 * @param filter_keys Array of filter keys (can be NULL)
 * @param filter_values Array of filter values (can be NULL)
 * @param filter_count Number of filters
 * @param sort_column Column name for SORT clause (can be NULL for primary key)
 * @param sort_desc Sort descending (0 = ascending, 1 = descending, default 1)
 * @param result Output search results (caller must free with mygramclient_free_search_result)
 * @return 0 on success, -1 on error
 */
int mygramclient_search_advanced(MygramClient_C* client, const char* table, const char* query, uint32_t limit,
                                 uint32_t offset, const char** and_terms, size_t and_count, const char** not_terms,
                                 size_t not_count, const char** filter_keys, const char** filter_values,
                                 size_t filter_count, const char* sort_column, int sort_desc,
                                 MygramSearchResult_C** result);

/**
 * @brief Count matching documents
 *
 * @param client Client handle
 * @param table Table name
 * @param query Search query text
 * @param count Output count
 * @return 0 on success, -1 on error
 */
int mygramclient_count(MygramClient_C* client, const char* table, const char* query, uint64_t* count);

/**
 * @brief Count matching documents with AND/NOT/FILTER clauses
 *
 * @param client Client handle
 * @param table Table name
 * @param query Search query text
 * @param and_terms Array of AND terms (can be NULL)
 * @param and_count Number of AND terms
 * @param not_terms Array of NOT terms (can be NULL)
 * @param not_count Number of NOT terms
 * @param filter_keys Array of filter keys (can be NULL)
 * @param filter_values Array of filter values (can be NULL)
 * @param filter_count Number of filters
 * @param count Output count
 * @return 0 on success, -1 on error
 */
int mygramclient_count_advanced(MygramClient_C* client, const char* table, const char* query, const char** and_terms,
                                size_t and_count, const char** not_terms, size_t not_count, const char** filter_keys,
                                const char** filter_values, size_t filter_count, uint64_t* count);

/**
 * @brief Count matching documents by facet column value
 *
 * @param client Client handle
 * @param table Table name
 * @param column Facet column name
 * @param query Optional search query text (can be empty)
 * @param limit Maximum number of facet values (0 for no explicit limit)
 * @param result Output facet values (caller must free with mygramclient_free_facet_result)
 * @return 0 on success, -1 on error
 */
int mygramclient_facet(MygramClient_C* client, const char* table, const char* column, const char* query, uint32_t limit,
                       MygramFacetResult_C** result);

/**
 * @brief Count matching documents by facet value with pagination
 *
 * @param client Client handle
 * @param table Table name
 * @param column Facet column name
 * @param query Optional search query text (can be empty)
 * @param limit Maximum number of facet values (0 for server default)
 * @param offset Number of facet values to skip before applying limit
 * @param result Output facet page and total count
 * @return 0 on success, -1 on error
 */
int mygramclient_facet_paged(MygramClient_C* client, const char* table, const char* column, const char* query,
                             uint32_t limit, uint32_t offset, MygramFacetResult_C** result);

/**
 * @brief Count matching documents by facet column value with AND/NOT/FILTER clauses
 *
 * @param client Client handle
 * @param table Table name
 * @param column Facet column name
 * @param query Optional search query text (can be empty)
 * @param limit Maximum number of facet values (0 for no explicit limit)
 * @param and_terms Array of AND terms (can be NULL)
 * @param and_count Number of AND terms
 * @param not_terms Array of NOT terms (can be NULL)
 * @param not_count Number of NOT terms
 * @param filter_keys Array of filter keys (can be NULL)
 * @param filter_values Array of filter values (can be NULL)
 * @param filter_count Number of filters
 * @param result Output facet values (caller must free with mygramclient_free_facet_result)
 * @return 0 on success, -1 on error
 */
int mygramclient_facet_advanced(MygramClient_C* client, const char* table, const char* column, const char* query,
                                uint32_t limit, const char** and_terms, size_t and_count, const char** not_terms,
                                size_t not_count, const char** filter_keys, const char** filter_values,
                                size_t filter_count, MygramFacetResult_C** result);

/**
 * @brief Facet pagination with AND/NOT/FILTER clauses
 *
 * This is the paginated counterpart of mygramclient_facet_advanced.
 */
int mygramclient_facet_advanced_paged(MygramClient_C* client, const char* table, const char* column, const char* query,
                                      uint32_t limit, uint32_t offset, const char** and_terms, size_t and_count,
                                      const char** not_terms, size_t not_count, const char** filter_keys,
                                      const char** filter_values, size_t filter_count, MygramFacetResult_C** result);

/**
 * @brief Get document by primary key
 *
 * @param client Client handle
 * @param table Table name
 * @param primary_key Primary key value
 * @param doc Output document (caller must free with mygramclient_free_document)
 * @return 0 on success, -1 on error
 */
int mygramclient_get(MygramClient_C* client, const char* table, const char* primary_key, MygramDocument_C** doc);

/**
 * @brief Get server information
 *
 * @param client Client handle
 * @param info Output server info (caller must free with mygramclient_free_server_info)
 * @return 0 on success, -1 on error
 */
int mygramclient_info(MygramClient_C* client, MygramServerInfo_C** info);

/**
 * @brief Get server configuration
 *
 * @param client Client handle
 * @param config_str Output configuration string (caller must free with mygramclient_free_string)
 * @return 0 on success, -1 on error
 */
int mygramclient_get_config(MygramClient_C* client, char** config_str);

int mygramclient_set_variable(MygramClient_C* client, const char* name, const char* value);
int mygramclient_show_variables(MygramClient_C* client, const char* like_pattern, char** response);
int mygramclient_cache_clear(MygramClient_C* client, const char* table);
int mygramclient_cache_stats(MygramClient_C* client, char** response);
int mygramclient_cache_enable(MygramClient_C* client);
int mygramclient_cache_disable(MygramClient_C* client);
int mygramclient_optimize(MygramClient_C* client, const char* table, char** response);
int mygramclient_sync(MygramClient_C* client, const char* table, char** response);
int mygramclient_sync_status(MygramClient_C* client, char** response);
int mygramclient_sync_stop(MygramClient_C* client, const char* table, char** response);
int mygramclient_dump_info(MygramClient_C* client, const char* filepath, char** response);
int mygramclient_dump_status(MygramClient_C* client, char** response);
int mygramclient_dump_verify(MygramClient_C* client, const char* filepath, char** response);

/**
 * @brief Save snapshot to disk
 *
 * @param client Client handle
 * @param filepath Optional filepath (NULL for default)
 * @param saved_path Output saved filepath (caller must free with mygramclient_free_string)
 * @return 0 on success, -1 on error
 */
int mygramclient_save(MygramClient_C* client, const char* filepath, char** saved_path);

/**
 * @brief Load snapshot from disk
 *
 * @param client Client handle
 * @param filepath Snapshot filepath
 * @param loaded_path Output loaded filepath (caller must free with mygramclient_free_string)
 * @return 0 on success, -1 on error
 */
int mygramclient_load(MygramClient_C* client, const char* filepath, char** loaded_path);

/**
 * @brief Replication status (C-compatible mirror of ReplicationStatus)
 */
typedef struct {
  int running;                // 1 if replication is active, 0 otherwise
  char* gtid;                 // Current GTID position (caller must NOT free directly;
                              // free the whole struct via mygramclient_free_replication_status)
  uint64_t processed_events;  // Number of binlog events processed since start
  uint64_t queue_size;        // Pending events waiting to be applied
  char* status_str;           // Raw status string ("running", "stopped", ...)
} MygramReplicationStatus_C;

/**
 * @brief Get replication status
 *
 * @param client Client handle
 * @param status Output replication status (caller must free with mygramclient_free_replication_status)
 * @return 0 on success, -1 on error
 *
 * @note A structured C API for SEARCH debug info is not provided in this
 *       release; callers that need debug fields should use
 *       mygramclient_send_command() and parse the raw "# DEBUG" block.
 *       A typed wrapper can be added when there is concrete demand from
 *       FFI consumers.
 */
int mygramclient_replication_status(MygramClient_C* client, MygramReplicationStatus_C** status);

/**
 * @brief Free replication status struct
 *
 * @param status Replication status to free
 */
void mygramclient_free_replication_status(MygramReplicationStatus_C* status);

/**
 * @brief Stop replication
 *
 * @param client Client handle
 * @return 0 on success, -1 on error
 */
int mygramclient_replication_stop(MygramClient_C* client);

/**
 * @brief Start replication
 *
 * @param client Client handle
 * @return 0 on success, -1 on error
 */
int mygramclient_replication_start(MygramClient_C* client);

/**
 * @brief Enable debug mode
 *
 * @param client Client handle
 * @return 0 on success, -1 on error
 */
int mygramclient_debug_on(MygramClient_C* client);

/**
 * @brief Disable debug mode
 *
 * @param client Client handle
 * @return 0 on success, -1 on error
 */
int mygramclient_debug_off(MygramClient_C* client);

/**
 * @brief Send a raw command to the server
 *
 * This is a generic function that allows sending any command to the server
 * and receiving the raw response. Useful for custom commands or future
 * protocol extensions.
 *
 * @param client Client handle
 * @param command Command string (without \\r\\n terminator)
 * @param response Output response string (caller must free with mygramclient_free_string)
 * @return 0 on success, -1 on error
 */
int mygramclient_send_command(MygramClient_C* client, const char* command, char** response);

/**
 * @brief Get last error message
 *
 * @param client Client handle
 * @return Error message string (do not free)
 */
const char* mygramclient_get_last_error(const MygramClient_C* client);

/**
 * @brief Get last error code
 *
 * Returns the numeric MygramDB error code (for example, client errors use the 7000 range).
 * Returns 1 (kUnknown) for an invalid client handle.
 *
 * @param client Client handle
 * @return Last error code, or 0 after successful operations that clear the error state
 */
int mygramclient_get_last_error_code(const MygramClient_C* client);

/**
 * @brief Free search result
 *
 * @param result Search result to free
 */
void mygramclient_free_search_result(MygramSearchResult_C* result);

/**
 * @brief Free search result with highlight snippets
 *
 * @param result Search result to free
 */
void mygramclient_free_search_result_with_highlights(MygramSearchResultWithHighlights_C* result);

/**
 * @brief Free facet result
 *
 * @param result Facet result to free
 */
void mygramclient_free_facet_result(MygramFacetResult_C* result);

/**
 * @brief Free document
 *
 * @param doc Document to free
 */
void mygramclient_free_document(MygramDocument_C* doc);

/**
 * @brief Free server info
 *
 * @param info Server info to free
 */
void mygramclient_free_server_info(MygramServerInfo_C* info);

/**
 * @brief Free string
 *
 * @param str String to free
 */
void mygramclient_free_string(char* str);

/**
 * @brief Parsed search expression components
 *
 * Note: the optional_terms / optional_count fields are deprecated and
 * are always emitted as NULL / 0 since the implicit-AND parser change.
 * They are retained here for ABI compatibility only — new code should
 * use and_terms (required terms) and not_terms (excluded terms).
 */
typedef struct {
  char* main_term;        // Main search term (first required or optional term)
  char** and_terms;       // Array of additional required terms (AND)
  size_t and_count;       // Number of AND terms
  char** not_terms;       // Array of excluded terms (NOT)
  size_t not_count;       // Number of NOT terms
  char** optional_terms;  // Deprecated: always NULL since the implicit-AND change.
                          // Reserved for ABI compatibility.
  size_t optional_count;  // Deprecated: always 0 since the implicit-AND change.
                          // Reserved for ABI compatibility.
} MygramParsedExpression_C;

/**
 * @brief Parse web-style search expression
 *
 * Parses expressions like "+golang -old tutorial" into structured components.
 *
 * Supported syntax:
 * - `+term` - Required term (AND)
 * - `-term` - Excluded term (NOT)
 * - `term` - Required term (implicit AND)
 * - `"phrase"` - Quoted phrase
 * - `OR` - Logical OR operator
 * - `()` - Grouping
 *
 * Examples:
 * - `golang tutorial` → main_term="golang", and_terms=["tutorial"]
 * - `+golang -old` → main_term="golang", and_terms=[], not_terms=["old"]
 * - `+golang +tutorial -old` → main_term="golang", and_terms=["tutorial"], not_terms=["old"]
 *
 * @param expression Web-style search expression
 * @param parsed Output parsed expression (caller must free with mygramclient_free_parsed_expression)
 * Complex expressions that combine OR/grouping with other required or
 * excluded terms cannot be represented by MygramParsedExpression_C and return
 * -1 instead of silently dropping part of the expression.
 *
 * @return 0 on success, -1 on syntax, allocation, or representation error
 */
int mygramclient_parse_search_expression(const char* expression, MygramParsedExpression_C** parsed);

/**
 * @brief Parse a search expression and return a concrete failure diagnostic.
 *
 * On failure, diagnostic receives an allocated message when possible. The
 * caller must release it with mygramclient_free_string(). Both output pointers
 * are initialized to NULL before validation.
 */
int mygramclient_parse_search_expression_ex(const char* expression, MygramParsedExpression_C** parsed,
                                            char** diagnostic);

/**
 * @brief Convert web-style input to a lossless raw server expression.
 *
 * The returned string is suitable for mygramclient_search_raw() and must be
 * released with mygramclient_free_string().
 */
int mygramclient_convert_search_expression(const char* expression, char** converted);

/**
 * @brief Convert a search expression and return a concrete failure diagnostic.
 *
 * The diagnostic ownership contract matches
 * mygramclient_parse_search_expression_ex().
 */
int mygramclient_convert_search_expression_ex(const char* expression, char** converted, char** diagnostic);

/**
 * @brief Free parsed expression
 *
 * @param parsed Parsed expression to free
 */
void mygramclient_free_parsed_expression(MygramParsedExpression_C* parsed);

#ifdef __cplusplus
}
#endif

// NOLINTEND(modernize-use-using)
