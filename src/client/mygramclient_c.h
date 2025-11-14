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

/**
 * @brief Search result
 */
typedef struct {
  char** primary_keys;   // Array of primary key strings
  size_t count;          // Number of results
  uint64_t total_count;  // Total matching documents (may exceed count)
} MygramSearchResult_C;

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
 * @brief Get last error message
 *
 * @param client Client handle
 * @return Error message string (do not free)
 */
const char* mygramclient_get_last_error(const MygramClient_C* client);

/**
 * @brief Free search result
 *
 * @param result Search result to free
 */
void mygramclient_free_search_result(MygramSearchResult_C* result);

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

#ifdef __cplusplus
}
#endif

// NOLINTEND(modernize-use-using)
