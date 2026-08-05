/**
 * @file mygramclient_c.cpp
 * @brief C API wrapper implementation
 *
 * This file implements a C API wrapper which requires manual memory management
 * and uses C naming conventions. All related warnings are suppressed for the entire file.
 */

// NOLINTBEGIN(readability-identifier-naming, cppcoreguidelines-owning-memory,
// cppcoreguidelines-no-malloc, readability-implicit-bool-conversion)

#include "client/mygramclient_c.h"

#include <cstdlib>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "client/mygramclient.h"
#ifdef MYGRAMCLIENT_C_TEST_HOOKS
#include "client/mygramclient_c_testing.h"
#endif
#include "client/search_expression.h"
#include "utils/error.h"

using namespace mygramdb::client;
using mygram::utils::ErrorCode;

// Opaque handle structure
struct MygramClient_C {
  std::unique_ptr<MygramClient> client;
  mutable std::mutex error_mutex;
  std::string last_error;
  int last_error_code = static_cast<int>(ErrorCode::kSuccess);
};

// Helper: Allocate C string copy
// cppcoreguidelines-no-malloc)
static char* strdup_safe(const std::string& str) {
#ifdef MYGRAMCLIENT_C_TEST_HOOKS
  if (mygramdb::client::testing::ShouldFailCStringAllocation()) {
    return nullptr;
  }
#endif
  char* result = static_cast<char*>(malloc(str.size() + 1));
  if (result != nullptr) {
    std::memcpy(result, str.c_str(), str.size() + 1);
  }
  return result;
}

// Helper: Convert vector<string> to char** array
// Returns nullptr on empty input. On allocation failure, frees all prior
// entries and returns nullptr.
// cppcoreguidelines-no-malloc)
static char** string_vector_to_c_array(const std::vector<std::string>& vec) {
  if (vec.empty()) {
    return nullptr;
  }

  auto** result = static_cast<char**>(malloc(sizeof(char*) * vec.size()));
  if (result == nullptr) {
    return nullptr;
  }

  for (size_t i = 0; i < vec.size(); ++i) {
    result[i] = strdup_safe(vec[i]);
    if (result[i] == nullptr) {
      // Free all previously allocated entries
      for (size_t j = 0; j < i; ++j) {
        free(result[j]);
      }
      free(result);
      return nullptr;
    }
  }

  return result;
}

static bool string_vector_to_c_array_checked(const std::vector<std::string>& vec, char*** out) {
  if (out == nullptr) {
    return false;
  }
  *out = nullptr;
  if (vec.empty()) {
    return true;
  }
  char** converted = string_vector_to_c_array(vec);
  if (converted == nullptr) {
    return false;
  }
  *out = converted;
  return true;
}

// Helper: Free char** array
static void free_c_string_array(char** array, size_t count) {
  if (array == nullptr) {
    return;
  }

  for (size_t i = 0; i < count; ++i) {
    free(array[i]);
  }
  free(array);
}

static void set_last_error(MygramClient_C* client, const std::string& message,
                           ErrorCode code = ErrorCode::kClientCommandFailed) {
  if (client == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> lock(client->error_mutex);
  client->last_error = message;
  client->last_error_code = static_cast<int>(code);
}

static void set_last_error(MygramClient_C* client, const mygram::utils::Error& error) {
  if (client == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> lock(client->error_mutex);
  client->last_error = error.to_string();
  client->last_error_code = static_cast<int>(error.code());
}

static void clear_last_error(MygramClient_C* client) {
  if (client == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> lock(client->error_mutex);
  client->last_error.clear();
  client->last_error_code = static_cast<int>(ErrorCode::kSuccess);
}

static int invalid_argument(MygramClient_C* client, const char* message) {
  set_last_error(client, message, ErrorCode::kClientInvalidArgument);
  return -1;
}

/**
 * @brief Convert a C string array (char* const*) into std::vector<std::string>.
 *
 * Defensively skips NULL pointer entries. Returns an empty vector when @p arr
 * is NULL or @p count is zero.
 */
static std::vector<std::string> CArrayToVector(const char* const* arr, size_t count) {
  std::vector<std::string> result;
  if (arr == nullptr || count == 0) {
    return result;
  }
  result.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    if (arr[i] != nullptr) {
      result.emplace_back(arr[i]);
    }
  }
  return result;
}

static bool CArrayContainsNull(const char* const* arr, size_t count) {
  if (arr == nullptr) {
    return count != 0;
  }
  for (size_t i = 0; i < count; ++i) {
    if (arr[i] == nullptr) {
      return true;
    }
  }
  return false;
}

static bool CFilterArraysContainNull(const char* const* keys, const char* const* values, size_t count) {
  return CArrayContainsNull(keys, count) || CArrayContainsNull(values, count);
}

/**
 * @brief Convert paired C key/value arrays into a vector of pairs.
 *
 * Skips entries where either key or value is NULL. Returns an empty vector
 * when @p count is zero or either array pointer is NULL.
 */
static std::vector<std::pair<std::string, std::string>> CFilterArraysToVector(const char* const* keys,
                                                                              const char* const* values, size_t count) {
  std::vector<std::pair<std::string, std::string>> result;
  if (keys == nullptr || values == nullptr || count == 0) {
    return result;
  }
  result.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    if (keys[i] != nullptr && values[i] != nullptr) {
      result.emplace_back(keys[i], values[i]);
    }
  }
  return result;
}

/**
 * @brief Run a void-returning C++ client method and translate to C return code.
 *
 * Validates the client handle, invokes @p fn with a reference to the wrapped
 * MygramClient, stores any error message in last_error, and returns 0/-1.
 */
template <typename Fn>
static int ForwardVoid(MygramClient_C* client, Fn&& fn) {
  if (client == nullptr || client->client == nullptr) {
    return invalid_argument(client, "Invalid argument: client must not be NULL");
  }
  auto result = fn(*client->client);
  if (!result) {
    set_last_error(client, result.error());
    return -1;
  }
  clear_last_error(client);
  return 0;
}

template <typename Fn>
static int ForwardString(MygramClient_C* client, char** out, Fn&& fn) {
  if (out != nullptr) {
    *out = nullptr;
  }
  if (client == nullptr || client->client == nullptr || out == nullptr) {
    return invalid_argument(client, "Invalid argument: client and output must not be NULL");
  }
  auto result = fn(*client->client);
  if (!result) {
    set_last_error(client, result.error());
    return -1;
  }
  *out = strdup_safe(*result);
  if (*out == nullptr) {
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return -1;
  }
  clear_last_error(client);
  return 0;
}

static MygramClient_C* CreateClient(ClientConfig config) {
  try {
    auto client_c = std::make_unique<MygramClient_C>();
    client_c->client = std::make_unique<MygramClient>(std::move(config));
    return client_c.release();
  } catch (...) {
    return nullptr;
  }
}

static bool V2FieldAvailable(uint32_t struct_size, size_t offset, size_t field_size) {
  return static_cast<size_t>(struct_size) >= offset + field_size;
}

static void record_c_api_exception(MygramClient_C* client, const char* message) noexcept {
  if (client == nullptr) {
    return;
  }
  try {
    set_last_error(client, message, ErrorCode::kClientCommandFailed);
  } catch (...) {
    // Even recording the error can allocate. Preserve the C boundary first;
    // callers still receive the operation's safe failure value.
  }
}

template <typename Result, typename Fn>
static Result c_api_guard(MygramClient_C* client, Result failure, Fn&& fn) noexcept {
  try {
#ifdef MYGRAMCLIENT_C_TEST_HOOKS
    mygramdb::client::testing::ThrowOnCApiEntryIfRequested();
#endif
    return fn();
  } catch (const std::exception& exception) {
    record_c_api_exception(client, exception.what());
  } catch (...) {
    record_c_api_exception(client, "Unknown exception in C API");
  }
  return failure;
}

template <typename Fn>
static void c_api_guard_void(MygramClient_C* client, Fn&& fn) noexcept {
  try {
#ifdef MYGRAMCLIENT_C_TEST_HOOKS
    mygramdb::client::testing::ThrowOnCApiEntryIfRequested();
#endif
    fn();
  } catch (const std::exception& exception) {
    record_c_api_exception(client, exception.what());
  } catch (...) {
    record_c_api_exception(client, "Unknown exception in C API");
  }
}

// Keep the existing implementations readable and route every exported symbol
// through the uniform wrappers defined at the end of this translation unit.
namespace {

#define mygramclient_create mygramclient_create_impl
#define mygramclient_create_v2 mygramclient_create_v2_impl
#define mygramclient_destroy mygramclient_destroy_impl
#define mygramclient_connect mygramclient_connect_impl
#define mygramclient_disconnect mygramclient_disconnect_impl
#define mygramclient_is_connected mygramclient_is_connected_impl
#define mygramclient_search mygramclient_search_impl
#define mygramclient_search_raw mygramclient_search_raw_impl
#define mygramclient_search_with_highlights mygramclient_search_with_highlights_impl
#define mygramclient_search_raw_with_highlights mygramclient_search_raw_with_highlights_impl
#define mygramclient_search_with_options mygramclient_search_with_options_impl
#define mygramclient_search_with_highlights_advanced mygramclient_search_with_highlights_advanced_impl
#define mygramclient_search_advanced mygramclient_search_advanced_impl
#define mygramclient_count mygramclient_count_impl
#define mygramclient_count_advanced mygramclient_count_advanced_impl
#define mygramclient_facet mygramclient_facet_impl
#define mygramclient_facet_advanced mygramclient_facet_advanced_impl
#define mygramclient_get mygramclient_get_impl
#define mygramclient_info mygramclient_info_impl
#define mygramclient_get_config mygramclient_get_config_impl
#define mygramclient_set_variable mygramclient_set_variable_impl
#define mygramclient_show_variables mygramclient_show_variables_impl
#define mygramclient_cache_clear mygramclient_cache_clear_impl
#define mygramclient_cache_stats mygramclient_cache_stats_impl
#define mygramclient_cache_enable mygramclient_cache_enable_impl
#define mygramclient_cache_disable mygramclient_cache_disable_impl
#define mygramclient_optimize mygramclient_optimize_impl
#define mygramclient_sync mygramclient_sync_impl
#define mygramclient_sync_status mygramclient_sync_status_impl
#define mygramclient_sync_stop mygramclient_sync_stop_impl
#define mygramclient_dump_info mygramclient_dump_info_impl
#define mygramclient_dump_status mygramclient_dump_status_impl
#define mygramclient_dump_verify mygramclient_dump_verify_impl
#define mygramclient_save mygramclient_save_impl
#define mygramclient_load mygramclient_load_impl
#define mygramclient_replication_status mygramclient_replication_status_impl
#define mygramclient_free_replication_status mygramclient_free_replication_status_impl
#define mygramclient_replication_stop mygramclient_replication_stop_impl
#define mygramclient_replication_start mygramclient_replication_start_impl
#define mygramclient_debug_on mygramclient_debug_on_impl
#define mygramclient_debug_off mygramclient_debug_off_impl
#define mygramclient_send_command mygramclient_send_command_impl
#define mygramclient_get_last_error mygramclient_get_last_error_impl
#define mygramclient_get_last_error_code mygramclient_get_last_error_code_impl
#define mygramclient_free_search_result mygramclient_free_search_result_impl
#define mygramclient_free_search_result_with_highlights mygramclient_free_search_result_with_highlights_impl
#define mygramclient_free_facet_result mygramclient_free_facet_result_impl
#define mygramclient_free_document mygramclient_free_document_impl
#define mygramclient_free_server_info mygramclient_free_server_info_impl
#define mygramclient_free_string mygramclient_free_string_impl
#define mygramclient_parse_search_expression mygramclient_parse_search_expression_impl
#define mygramclient_convert_search_expression mygramclient_convert_search_expression_impl
#define mygramclient_free_parsed_expression mygramclient_free_parsed_expression_impl

int mygramclient_search_advanced_impl(MygramClient_C* client, const char* table, const char* query, uint32_t limit,
                                      uint32_t offset, const char** and_terms, size_t and_count, const char** not_terms,
                                      size_t not_count, const char** filter_keys, const char** filter_values,
                                      size_t filter_count, const char* sort_column, int sort_desc,
                                      MygramSearchResult_C** result);
int mygramclient_search_with_highlights_advanced_impl(MygramClient_C* client, const char* table, const char* query,
                                                      uint32_t limit, uint32_t offset, const char** and_terms,
                                                      size_t and_count, const char** not_terms, size_t not_count,
                                                      const char** filter_keys, const char** filter_values,
                                                      size_t filter_count, const char* sort_column, int sort_desc,
                                                      MygramSearchResultWithHighlights_C** result);
int mygramclient_count_advanced_impl(MygramClient_C* client, const char* table, const char* query,
                                     const char** and_terms, size_t and_count, const char** not_terms, size_t not_count,
                                     const char** filter_keys, const char** filter_values, size_t filter_count,
                                     uint64_t* count);
int mygramclient_facet_advanced_impl(MygramClient_C* client, const char* table, const char* column, const char* query,
                                     uint32_t limit, const char** and_terms, size_t and_count, const char** not_terms,
                                     size_t not_count, const char** filter_keys, const char** filter_values,
                                     size_t filter_count, MygramFacetResult_C** result);
void mygramclient_free_parsed_expression_impl(MygramParsedExpression_C* parsed);

MygramClient_C* mygramclient_create(const MygramClientConfig_C* config) {
  if (config == nullptr) {
    return nullptr;
  }

  ClientConfig cpp_config;
  cpp_config.host = (config->host != nullptr) ? config->host : "127.0.0.1";
  cpp_config.port = config->port != 0 ? config->port : static_cast<uint16_t>(mygramdb::config::defaults::kTcpPort);
  cpp_config.timeout_ms = config->timeout_ms != 0 ? config->timeout_ms : 5000;
  cpp_config.recv_buffer_size = config->recv_buffer_size != 0 ? config->recv_buffer_size : 65536;
  return CreateClient(std::move(cpp_config));
}

MygramClient_C* mygramclient_create_v2(const MygramClientConfigV2_C* config) {
  if (config == nullptr) {
    return nullptr;
  }
  constexpr size_t kMinimumSize = offsetof(MygramClientConfigV2_C, recv_buffer_size) + sizeof(uint32_t);
  // struct_size is the only field callers are required to make readable
  // before compatibility can be established. Never inspect version or any
  // later member until this prefix proves it is present.
  if (config->struct_size < kMinimumSize) {
    return nullptr;
  }
  if (config->version != MYGRAMCLIENT_CONFIG_V2_VERSION) {
    return nullptr;
  }

  ClientConfig cpp_config;
  if (config->host != nullptr) {
    cpp_config.host = config->host;
  }
  if (config->port != 0) {
    cpp_config.port = config->port;
  }
  if (config->timeout_ms != 0) {
    cpp_config.timeout_ms = config->timeout_ms;
  }
  if (config->recv_buffer_size != 0) {
    cpp_config.recv_buffer_size = config->recv_buffer_size;
  }
  if (V2FieldAvailable(config->struct_size, offsetof(MygramClientConfigV2_C, unix_socket_path),
                       sizeof(config->unix_socket_path)) &&
      config->unix_socket_path != nullptr) {
    cpp_config.unix_socket_path = config->unix_socket_path;
  }
  if (V2FieldAvailable(config->struct_size, offsetof(MygramClientConfigV2_C, dump_save_timeout_ms),
                       sizeof(config->dump_save_timeout_ms)) &&
      config->dump_save_timeout_ms != 0) {
    cpp_config.dump_save_timeout_ms = config->dump_save_timeout_ms;
  }
  if (V2FieldAvailable(config->struct_size, offsetof(MygramClientConfigV2_C, max_response_bytes),
                       sizeof(config->max_response_bytes)) &&
      config->max_response_bytes != 0) {
    cpp_config.max_response_bytes = config->max_response_bytes;
  }

  return CreateClient(std::move(cpp_config));
}

void mygramclient_destroy(MygramClient_C* client) {
  delete client;
}

int mygramclient_connect(MygramClient_C* client) {
  if (client == nullptr || client->client == nullptr) {
    return invalid_argument(client, "Invalid argument: client must not be NULL");
  }

  auto result = client->client->Connect();
  if (!result) {
    set_last_error(client, result.error());
    return -1;
  }

  clear_last_error(client);
  return 0;
}

void mygramclient_disconnect(MygramClient_C* client) {
  if (client != nullptr && client->client != nullptr) {
    client->client->Disconnect();
  }
}

int mygramclient_is_connected(const MygramClient_C* client) {
  if (client == nullptr || client->client == nullptr) {
    return 0;
  }

  return client->client->IsConnected() ? 1 : 0;
}

static bool CopySearchResponseStrings(MygramClient_C* client, const SearchResponse& response, char**& primary_keys,
                                      char*** snippets) {
  primary_keys = nullptr;
  if (snippets != nullptr) {
    *snippets = nullptr;
  }
  if (response.results.empty()) {
    return true;
  }

  primary_keys = static_cast<char**>(calloc(response.results.size(), sizeof(char*)));
  if (primary_keys == nullptr) {
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return false;
  }
  if (snippets != nullptr) {
    *snippets = static_cast<char**>(calloc(response.results.size(), sizeof(char*)));
    if (*snippets == nullptr) {
      free(primary_keys);
      primary_keys = nullptr;
      set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
      return false;
    }
  }

  for (size_t index = 0; index < response.results.size(); ++index) {
    primary_keys[index] = strdup_safe(response.results[index].primary_key);
    if (snippets != nullptr) {
      (*snippets)[index] = strdup_safe(response.results[index].snippet);
    }
    if (primary_keys[index] == nullptr || (snippets != nullptr && (*snippets)[index] == nullptr)) {
      free_c_string_array(primary_keys, response.results.size());
      primary_keys = nullptr;
      if (snippets != nullptr) {
        free_c_string_array(*snippets, response.results.size());
        *snippets = nullptr;
      }
      set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
      return false;
    }
  }
  return true;
}

static int CopySearchResult(MygramClient_C* client, const SearchResponse& response, MygramSearchResult_C** result) {
  *result = nullptr;
  std::unique_ptr<MygramSearchResult_C, decltype(&std::free)> result_c(
      static_cast<MygramSearchResult_C*>(calloc(1, sizeof(MygramSearchResult_C))), &std::free);
  if (result_c == nullptr) {
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return -1;
  }
  result_c->count = response.results.size();
  result_c->total_count = response.total_count;
  if (!CopySearchResponseStrings(client, response, result_c->primary_keys, nullptr)) {
    return -1;
  }
  *result = result_c.release();
  clear_last_error(client);
  return 0;
}

static int CopyHighlightedSearchResult(MygramClient_C* client, const SearchResponse& response,
                                       MygramSearchResultWithHighlights_C** result) {
  *result = nullptr;
  std::unique_ptr<MygramSearchResultWithHighlights_C, decltype(&std::free)> result_c(
      static_cast<MygramSearchResultWithHighlights_C*>(calloc(1, sizeof(MygramSearchResultWithHighlights_C))),
      &std::free);
  if (result_c == nullptr) {
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return -1;
  }
  result_c->count = response.results.size();
  result_c->total_count = response.total_count;
  if (!CopySearchResponseStrings(client, response, result_c->primary_keys, &result_c->snippets)) {
    return -1;
  }
  *result = result_c.release();
  clear_last_error(client);
  return 0;
}

int mygramclient_search(MygramClient_C* client, const char* table, const char* query, uint32_t limit, uint32_t offset,
                        MygramSearchResult_C** result) {
  return mygramclient_search_advanced(client, table, query, limit, offset, nullptr, 0, nullptr, 0, nullptr, nullptr, 0,
                                      nullptr, 1, result);  // Default sort_desc = 1 (descending)
}

int mygramclient_search_raw(MygramClient_C* client, const char* table, const char* raw_query, uint32_t limit,
                            uint32_t offset, MygramSearchResult_C** result) {
  if (result != nullptr) {
    *result = nullptr;
  }
  if (client == nullptr || client->client == nullptr || table == nullptr || raw_query == nullptr || result == nullptr) {
    return invalid_argument(client, "Invalid argument: client, table, raw_query, and result must not be NULL");
  }
  auto response = client->client->SearchRaw(table, raw_query, limit, offset);
  if (!response) {
    set_last_error(client, response.error());
    return -1;
  }
  return CopySearchResult(client, *response, result);
}

int mygramclient_search_with_highlights(MygramClient_C* client, const char* table, const char* query, uint32_t limit,
                                        uint32_t offset, MygramSearchResultWithHighlights_C** result) {
  return mygramclient_search_with_highlights_advanced(client, table, query, limit, offset, nullptr, 0, nullptr, 0,
                                                      nullptr, nullptr, 0, nullptr, 1, result);
}

int mygramclient_search_raw_with_highlights(MygramClient_C* client, const char* table, const char* raw_query,
                                            uint32_t limit, uint32_t offset,
                                            MygramSearchResultWithHighlights_C** result) {
  if (result != nullptr) {
    *result = nullptr;
  }
  if (client == nullptr || client->client == nullptr || table == nullptr || raw_query == nullptr || result == nullptr) {
    return invalid_argument(client, "Invalid argument: client, table, raw_query, and result must not be NULL");
  }
  auto response = client->client->SearchRawWithHighlights(table, raw_query, limit, offset);
  if (!response) {
    set_last_error(client, response.error());
    return -1;
  }
  return CopyHighlightedSearchResult(client, *response, result);
}

int mygramclient_search_with_options(MygramClient_C* client, const char* table, const char* query,
                                     const MygramSearchOptions_C* options,
                                     MygramSearchResultWithHighlights_C** result) {
  if (result != nullptr) {
    *result = nullptr;
  }
  if (client == nullptr || client->client == nullptr || table == nullptr || query == nullptr || options == nullptr ||
      result == nullptr) {
    return invalid_argument(client, "Invalid argument: client, table, query, options, and result must not be NULL");
  }
  constexpr size_t kMinimumSize = offsetof(MygramSearchOptions_C, offset) + sizeof(uint32_t);
  if (options->struct_size < kMinimumSize) {
    return invalid_argument(client, "Invalid argument: search options struct_size is too small");
  }

  const auto field_available = [options](size_t offset, size_t size) {
    return V2FieldAvailable(options->struct_size, offset, size);
  };
  const auto and_terms = field_available(offsetof(MygramSearchOptions_C, and_count), sizeof(options->and_count))
                             ? options->and_terms
                             : nullptr;
  const size_t and_count = and_terms != nullptr ? options->and_count : 0;
  const auto not_terms = field_available(offsetof(MygramSearchOptions_C, not_count), sizeof(options->not_count))
                             ? options->not_terms
                             : nullptr;
  const size_t not_count = not_terms != nullptr ? options->not_count : 0;
  const auto filters = field_available(offsetof(MygramSearchOptions_C, filter_count), sizeof(options->filter_count))
                           ? options->filters
                           : nullptr;
  const size_t filter_count = filters != nullptr ? options->filter_count : 0;

  if ((field_available(offsetof(MygramSearchOptions_C, and_count), sizeof(options->and_count)) &&
       options->and_count > 0 && options->and_terms == nullptr) ||
      (field_available(offsetof(MygramSearchOptions_C, not_count), sizeof(options->not_count)) &&
       options->not_count > 0 && options->not_terms == nullptr) ||
      (field_available(offsetof(MygramSearchOptions_C, filter_count), sizeof(options->filter_count)) &&
       options->filter_count > 0 && options->filters == nullptr)) {
    return invalid_argument(client, "Invalid argument: option array is NULL while its count is non-zero");
  }
  if (CArrayContainsNull(and_terms, and_count) || CArrayContainsNull(not_terms, not_count)) {
    return invalid_argument(client, "Invalid argument: option term arrays must not contain NULL");
  }

  SearchOptions cpp_options;
  cpp_options.limit = options->limit;
  cpp_options.offset = options->offset;
  cpp_options.and_terms = CArrayToVector(and_terms, and_count);
  cpp_options.not_terms = CArrayToVector(not_terms, not_count);
  if (field_available(offsetof(MygramSearchOptions_C, sort_column), sizeof(options->sort_column)) &&
      options->sort_column != nullptr) {
    cpp_options.sort_column = options->sort_column;
  }
  if (field_available(offsetof(MygramSearchOptions_C, sort_desc), sizeof(options->sort_desc))) {
    cpp_options.sort_desc = options->sort_desc != 0;
  }
  if (field_available(offsetof(MygramSearchOptions_C, fuzzy_distance), sizeof(options->fuzzy_distance)) &&
      options->fuzzy_distance != 0) {
    cpp_options.fuzzy_distance = options->fuzzy_distance;
  }
  if (field_available(offsetof(MygramSearchOptions_C, highlight), sizeof(options->highlight)) &&
      options->highlight != 0) {
    HighlightOptions highlight;
    if (field_available(offsetof(MygramSearchOptions_C, highlight_open_tag), sizeof(options->highlight_open_tag)) &&
        options->highlight_open_tag != nullptr) {
      highlight.open_tag = options->highlight_open_tag;
    }
    if (field_available(offsetof(MygramSearchOptions_C, highlight_close_tag), sizeof(options->highlight_close_tag)) &&
        options->highlight_close_tag != nullptr) {
      highlight.close_tag = options->highlight_close_tag;
    }
    if (field_available(offsetof(MygramSearchOptions_C, highlight_snippet_length),
                        sizeof(options->highlight_snippet_length))) {
      highlight.snippet_length = options->highlight_snippet_length;
    }
    if (field_available(offsetof(MygramSearchOptions_C, highlight_max_fragments),
                        sizeof(options->highlight_max_fragments))) {
      highlight.max_fragments = options->highlight_max_fragments;
    }
    cpp_options.highlight = std::move(highlight);
  }
  cpp_options.filters.reserve(filter_count);
  for (size_t i = 0; i < filter_count; ++i) {
    const auto& filter = filters[i];
    if (filter.key == nullptr || filter.value == nullptr || filter.op < MYGRAM_FILTER_EQ ||
        filter.op > MYGRAM_FILTER_LTE) {
      return invalid_argument(client, "Invalid argument: malformed typed filter");
    }
    cpp_options.filters.push_back({filter.key, static_cast<FilterOp>(filter.op), filter.value});
  }

  auto response = client->client->Search(table, query, cpp_options);
  if (!response) {
    set_last_error(client, response.error());
    return -1;
  }
  return CopyHighlightedSearchResult(client, *response, result);
}

int mygramclient_search_with_highlights_advanced(MygramClient_C* client, const char* table, const char* query,
                                                 uint32_t limit, uint32_t offset, const char** and_terms,
                                                 size_t and_count, const char** not_terms, size_t not_count,
                                                 const char** filter_keys, const char** filter_values,
                                                 size_t filter_count, const char* sort_column, int sort_desc,
                                                 MygramSearchResultWithHighlights_C** result) {
  if (result != nullptr) {
    *result = nullptr;
  }
  if (client == nullptr || client->client == nullptr || table == nullptr || query == nullptr || result == nullptr) {
    return invalid_argument(client, "Invalid argument: client, table, query, and result must not be NULL");
  }

  if (and_count > 0 && and_terms == nullptr) {
    set_last_error(client, "Invalid argument: and_terms is NULL but and_count > 0", ErrorCode::kClientInvalidArgument);
    return -1;
  }
  if (not_count > 0 && not_terms == nullptr) {
    set_last_error(client, "Invalid argument: not_terms is NULL but not_count > 0", ErrorCode::kClientInvalidArgument);
    return -1;
  }
  if (filter_count > 0 && (filter_keys == nullptr || filter_values == nullptr)) {
    set_last_error(client, "Invalid argument: filter_keys or filter_values is NULL but filter_count > 0",
                   ErrorCode::kClientInvalidArgument);
    return -1;
  }
  if (CArrayContainsNull(and_terms, and_count) || CArrayContainsNull(not_terms, not_count) ||
      CFilterArraysContainNull(filter_keys, filter_values, filter_count)) {
    return invalid_argument(client, "Invalid argument: term and filter arrays must not contain NULL");
  }

  auto and_terms_vec = CArrayToVector(and_terms, and_count);
  auto not_terms_vec = CArrayToVector(not_terms, not_count);
  auto filters_vec = CFilterArraysToVector(filter_keys, filter_values, filter_count);
  std::string sort_column_str = sort_column != nullptr ? sort_column : "";

  auto search_result = client->client->SearchWithHighlights(table, query, limit, offset, and_terms_vec, not_terms_vec,
                                                            filters_vec, sort_column_str, sort_desc != 0);
  if (!search_result) {
    set_last_error(client, search_result.error());
    return -1;
  }

  return CopyHighlightedSearchResult(client, *search_result, result);
}

int mygramclient_search_advanced(MygramClient_C* client, const char* table, const char* query, uint32_t limit,
                                 uint32_t offset, const char** and_terms, size_t and_count, const char** not_terms,
                                 size_t not_count, const char** filter_keys, const char** filter_values,
                                 size_t filter_count, const char* sort_column, int sort_desc,
                                 MygramSearchResult_C** result) {
  if (result != nullptr) {
    *result = nullptr;
  }
  if (client == nullptr || client->client == nullptr || table == nullptr || query == nullptr || result == nullptr) {
    return invalid_argument(client, "Invalid argument: client, table, query, and result must not be NULL");
  }

  // Reject the (count > 0, ptr == nullptr) combination for each array argument.
  // Without this guard, the conversion loops below dereference a NULL pointer.
  if (and_count > 0 && and_terms == nullptr) {
    set_last_error(client, "Invalid argument: and_terms is NULL but and_count > 0", ErrorCode::kClientInvalidArgument);
    return -1;
  }
  if (not_count > 0 && not_terms == nullptr) {
    set_last_error(client, "Invalid argument: not_terms is NULL but not_count > 0", ErrorCode::kClientInvalidArgument);
    return -1;
  }
  if (filter_count > 0 && (filter_keys == nullptr || filter_values == nullptr)) {
    set_last_error(client, "Invalid argument: filter_keys or filter_values is NULL but filter_count > 0",
                   ErrorCode::kClientInvalidArgument);
    return -1;
  }
  if (CArrayContainsNull(and_terms, and_count) || CArrayContainsNull(not_terms, not_count) ||
      CFilterArraysContainNull(filter_keys, filter_values, filter_count)) {
    return invalid_argument(client, "Invalid argument: term and filter arrays must not contain NULL");
  }

  // Convert C arrays to C++ vectors
  auto and_terms_vec = CArrayToVector(and_terms, and_count);
  auto not_terms_vec = CArrayToVector(not_terms, not_count);
  auto filters_vec = CFilterArraysToVector(filter_keys, filter_values, filter_count);

  std::string sort_column_str = sort_column != nullptr ? sort_column : "";

  auto search_result = client->client->Search(table, query, limit, offset, and_terms_vec, not_terms_vec, filters_vec,
                                              sort_column_str, sort_desc != 0);

  if (!search_result) {
    set_last_error(client, search_result.error());
    return -1;
  }

  return CopySearchResult(client, *search_result, result);
}

int mygramclient_count(MygramClient_C* client, const char* table, const char* query, uint64_t* count) {
  return mygramclient_count_advanced(client, table, query, nullptr, 0, nullptr, 0, nullptr, nullptr, 0, count);
}

int mygramclient_count_advanced(MygramClient_C* client, const char* table, const char* query, const char** and_terms,
                                size_t and_count, const char** not_terms, size_t not_count, const char** filter_keys,
                                const char** filter_values, size_t filter_count, uint64_t* count) {
  if (count != nullptr) {
    *count = 0;
  }
  if (client == nullptr || client->client == nullptr || table == nullptr || query == nullptr || count == nullptr) {
    return invalid_argument(client, "Invalid argument: client, table, query, and count must not be NULL");
  }

  // Reject the (count > 0, ptr == nullptr) combination for each array argument.
  if (and_count > 0 && and_terms == nullptr) {
    set_last_error(client, "Invalid argument: and_terms is NULL but and_count > 0", ErrorCode::kClientInvalidArgument);
    return -1;
  }
  if (not_count > 0 && not_terms == nullptr) {
    set_last_error(client, "Invalid argument: not_terms is NULL but not_count > 0", ErrorCode::kClientInvalidArgument);
    return -1;
  }
  if (filter_count > 0 && (filter_keys == nullptr || filter_values == nullptr)) {
    set_last_error(client, "Invalid argument: filter_keys or filter_values is NULL but filter_count > 0",
                   ErrorCode::kClientInvalidArgument);
    return -1;
  }
  if (CArrayContainsNull(and_terms, and_count) || CArrayContainsNull(not_terms, not_count) ||
      CFilterArraysContainNull(filter_keys, filter_values, filter_count)) {
    return invalid_argument(client, "Invalid argument: term and filter arrays must not contain NULL");
  }

  // Convert C arrays to C++ vectors
  auto and_terms_vec = CArrayToVector(and_terms, and_count);
  auto not_terms_vec = CArrayToVector(not_terms, not_count);
  auto filters_vec = CFilterArraysToVector(filter_keys, filter_values, filter_count);

  auto count_result = client->client->Count(table, query, and_terms_vec, not_terms_vec, filters_vec);

  if (!count_result) {
    set_last_error(client, count_result.error());
    return -1;
  }

  auto& resp = *count_result;
  *count = resp.count;

  clear_last_error(client);
  return 0;
}

int mygramclient_facet(MygramClient_C* client, const char* table, const char* column, const char* query, uint32_t limit,
                       MygramFacetResult_C** result) {
  return mygramclient_facet_advanced(client, table, column, query, limit, nullptr, 0, nullptr, 0, nullptr, nullptr, 0,
                                     result);
}

int mygramclient_facet_advanced(MygramClient_C* client, const char* table, const char* column, const char* query,
                                uint32_t limit, const char** and_terms, size_t and_count, const char** not_terms,
                                size_t not_count, const char** filter_keys, const char** filter_values,
                                size_t filter_count, MygramFacetResult_C** result) {
  if (result != nullptr) {
    *result = nullptr;
  }
  if (client == nullptr || client->client == nullptr || table == nullptr || column == nullptr || query == nullptr ||
      result == nullptr) {
    return invalid_argument(client, "Invalid argument: client, table, column, query, and result must not be NULL");
  }

  if (and_count > 0 && and_terms == nullptr) {
    set_last_error(client, "Invalid argument: and_terms is NULL but and_count > 0", ErrorCode::kClientInvalidArgument);
    return -1;
  }
  if (not_count > 0 && not_terms == nullptr) {
    set_last_error(client, "Invalid argument: not_terms is NULL but not_count > 0", ErrorCode::kClientInvalidArgument);
    return -1;
  }
  if (filter_count > 0 && (filter_keys == nullptr || filter_values == nullptr)) {
    set_last_error(client, "Invalid argument: filter_keys or filter_values is NULL but filter_count > 0",
                   ErrorCode::kClientInvalidArgument);
    return -1;
  }
  if (CArrayContainsNull(and_terms, and_count) || CArrayContainsNull(not_terms, not_count) ||
      CFilterArraysContainNull(filter_keys, filter_values, filter_count)) {
    return invalid_argument(client, "Invalid argument: term and filter arrays must not contain NULL");
  }

  auto and_terms_vec = CArrayToVector(and_terms, and_count);
  auto not_terms_vec = CArrayToVector(not_terms, not_count);
  auto filters_vec = CFilterArraysToVector(filter_keys, filter_values, filter_count);

  auto facet_result = client->client->Facet(table, column, query, limit, and_terms_vec, not_terms_vec, filters_vec);
  if (!facet_result) {
    set_last_error(client, facet_result.error());
    return -1;
  }

  const auto& resp = *facet_result;
  auto* result_c = static_cast<MygramFacetResult_C*>(malloc(sizeof(MygramFacetResult_C)));
  if (result_c == nullptr) {
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return -1;
  }

  result_c->count = resp.facets.size();
  result_c->values = nullptr;
  result_c->counts = nullptr;

  if (!resp.facets.empty()) {
    result_c->values = static_cast<char**>(malloc(sizeof(char*) * resp.facets.size()));
    result_c->counts = static_cast<uint64_t*>(malloc(sizeof(uint64_t) * resp.facets.size()));
    if (result_c->values == nullptr || result_c->counts == nullptr) {
      free(result_c->values);
      free(result_c->counts);
      free(result_c);
      set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
      return -1;
    }
  }

  for (size_t i = 0; i < resp.facets.size(); ++i) {
    result_c->values[i] = strdup_safe(resp.facets[i].value);
    if (result_c->values[i] == nullptr) {
      for (size_t j = 0; j < i; ++j) {
        free(result_c->values[j]);
      }
      free(result_c->values);
      free(result_c->counts);
      free(result_c);
      set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
      return -1;
    }
    result_c->counts[i] = resp.facets[i].count;
  }

  *result = result_c;
  clear_last_error(client);
  return 0;
}

int mygramclient_get(MygramClient_C* client, const char* table, const char* primary_key, MygramDocument_C** doc) {
  if (doc != nullptr) {
    *doc = nullptr;
  }
  if (client == nullptr || client->client == nullptr || table == nullptr || primary_key == nullptr || doc == nullptr) {
    return invalid_argument(client, "Invalid argument: client, table, primary_key, and doc must not be NULL");
  }

  auto get_result = client->client->Get(table, primary_key);

  if (!get_result) {
    set_last_error(client, get_result.error());
    return -1;
  }

  auto& document = *get_result;

  auto* doc_c = static_cast<MygramDocument_C*>(malloc(sizeof(MygramDocument_C)));
  if (doc_c == nullptr) {
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return -1;
  }

  doc_c->primary_key = strdup_safe(document.primary_key);
  if (doc_c->primary_key == nullptr) {
    free(doc_c);
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return -1;
  }
  doc_c->field_count = document.fields.size();

  if (doc_c->field_count > 0) {
    doc_c->field_keys = static_cast<char**>(malloc(sizeof(char*) * doc_c->field_count));
    doc_c->field_values = static_cast<char**>(malloc(sizeof(char*) * doc_c->field_count));

    if (doc_c->field_keys == nullptr || doc_c->field_values == nullptr) {
      free(doc_c->primary_key);
      free(doc_c->field_keys);
      free(doc_c->field_values);
      free(doc_c);
      set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
      return -1;
    }

    for (size_t i = 0; i < doc_c->field_count; ++i) {
      doc_c->field_keys[i] = strdup_safe(document.fields[i].first);
      if (doc_c->field_keys[i] == nullptr) {
        // Free all previously allocated keys and values
        for (size_t j = 0; j < i; ++j) {
          free(doc_c->field_keys[j]);
          free(doc_c->field_values[j]);
        }
        free(doc_c->field_keys);
        free(doc_c->field_values);
        free(doc_c->primary_key);
        free(doc_c);
        set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
        return -1;
      }
      doc_c->field_values[i] = strdup_safe(document.fields[i].second);
      if (doc_c->field_values[i] == nullptr) {
        // Free current key and all previously allocated keys/values
        free(doc_c->field_keys[i]);
        for (size_t j = 0; j < i; ++j) {
          free(doc_c->field_keys[j]);
          free(doc_c->field_values[j]);
        }
        free(doc_c->field_keys);
        free(doc_c->field_values);
        free(doc_c->primary_key);
        free(doc_c);
        set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
        return -1;
      }
    }
  } else {
    doc_c->field_keys = nullptr;
    doc_c->field_values = nullptr;
  }

  *doc = doc_c;
  clear_last_error(client);
  return 0;
}

int mygramclient_info(MygramClient_C* client, MygramServerInfo_C** info) {
  if (info != nullptr) {
    *info = nullptr;
  }
  if (client == nullptr || client->client == nullptr || info == nullptr) {
    return invalid_argument(client, "Invalid argument: client and info must not be NULL");
  }

  auto info_result = client->client->Info();

  if (!info_result) {
    set_last_error(client, info_result.error());
    return -1;
  }

  auto& server_info = *info_result;

  auto* info_c = static_cast<MygramServerInfo_C*>(malloc(sizeof(MygramServerInfo_C)));
  if (info_c == nullptr) {
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return -1;
  }

  info_c->version = strdup_safe(server_info.version);
  if (info_c->version == nullptr) {
    free(info_c);
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return -1;
  }
  info_c->uptime_seconds = server_info.uptime_seconds;
  info_c->total_requests = server_info.total_requests;
  info_c->active_connections = server_info.active_connections;
  info_c->index_size_bytes = server_info.index_size_bytes;
  info_c->doc_count = server_info.doc_count;
  info_c->table_count = 0;
  info_c->tables = nullptr;
  if (!string_vector_to_c_array_checked(server_info.tables, &info_c->tables)) {
    free(info_c->version);
    free(info_c);
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return -1;
  }
  info_c->table_count = server_info.tables.size();

  *info = info_c;
  clear_last_error(client);
  return 0;
}

int mygramclient_get_config(MygramClient_C* client, char** config_str) {
  return ForwardString(client, config_str, [](MygramClient& c) { return c.GetConfig(); });
}

int mygramclient_set_variable(MygramClient_C* client, const char* name, const char* value) {
  if (name == nullptr || value == nullptr) {
    return invalid_argument(client, "Invalid argument: name and value must not be NULL");
  }
  return ForwardVoid(client, [name, value](MygramClient& c) { return c.SetVariable(name, value); });
}

int mygramclient_show_variables(MygramClient_C* client, const char* like_pattern, char** response) {
  std::string pattern = like_pattern != nullptr ? like_pattern : "";
  return ForwardString(client, response, [&pattern](MygramClient& c) { return c.ShowVariables(pattern); });
}

int mygramclient_cache_clear(MygramClient_C* client, const char* table) {
  std::string table_name = table != nullptr ? table : "";
  return ForwardVoid(client, [&table_name](MygramClient& c) { return c.CacheClear(table_name); });
}

int mygramclient_cache_stats(MygramClient_C* client, char** response) {
  return ForwardString(client, response, [](MygramClient& c) { return c.CacheStats(); });
}

int mygramclient_cache_enable(MygramClient_C* client) {
  return ForwardVoid(client, [](MygramClient& c) { return c.CacheEnable(); });
}

int mygramclient_cache_disable(MygramClient_C* client) {
  return ForwardVoid(client, [](MygramClient& c) { return c.CacheDisable(); });
}

int mygramclient_optimize(MygramClient_C* client, const char* table, char** response) {
  std::string table_name = table != nullptr ? table : "";
  return ForwardString(client, response, [&table_name](MygramClient& c) { return c.Optimize(table_name); });
}

int mygramclient_sync(MygramClient_C* client, const char* table, char** response) {
  if (table == nullptr) {
    if (response != nullptr) {
      *response = nullptr;
    }
    return invalid_argument(client, "Invalid argument: table must not be NULL");
  }
  return ForwardString(client, response, [table](MygramClient& c) { return c.Sync(table); });
}

int mygramclient_sync_status(MygramClient_C* client, char** response) {
  return ForwardString(client, response, [](MygramClient& c) { return c.SyncStatus(); });
}

int mygramclient_sync_stop(MygramClient_C* client, const char* table, char** response) {
  std::string table_name = table != nullptr ? table : "";
  return ForwardString(client, response, [&table_name](MygramClient& c) { return c.SyncStop(table_name); });
}

int mygramclient_dump_info(MygramClient_C* client, const char* filepath, char** response) {
  if (filepath == nullptr) {
    if (response != nullptr) {
      *response = nullptr;
    }
    return invalid_argument(client, "Invalid argument: filepath must not be NULL");
  }
  return ForwardString(client, response, [filepath](MygramClient& c) { return c.DumpInfo(filepath); });
}

int mygramclient_dump_status(MygramClient_C* client, char** response) {
  return ForwardString(client, response, [](MygramClient& c) { return c.DumpStatus(); });
}

int mygramclient_dump_verify(MygramClient_C* client, const char* filepath, char** response) {
  if (filepath == nullptr) {
    if (response != nullptr) {
      *response = nullptr;
    }
    return invalid_argument(client, "Invalid argument: filepath must not be NULL");
  }
  return ForwardString(client, response, [filepath](MygramClient& c) { return c.DumpVerify(filepath); });
}

int mygramclient_save(MygramClient_C* client, const char* filepath, char** saved_path) {
  if (saved_path != nullptr) {
    *saved_path = nullptr;
  }
  if (client == nullptr || client->client == nullptr || saved_path == nullptr) {
    return invalid_argument(client, "Invalid argument: client and saved_path must not be NULL");
  }

  std::string filepath_str = filepath != nullptr ? filepath : "";
  auto save_result = client->client->Save(filepath_str);

  if (!save_result) {
    set_last_error(client, save_result.error());
    return -1;
  }

  *saved_path = strdup_safe(*save_result);
  if (*saved_path == nullptr) {
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return -1;
  }
  clear_last_error(client);
  return 0;
}

int mygramclient_load(MygramClient_C* client, const char* filepath, char** loaded_path) {
  if (loaded_path != nullptr) {
    *loaded_path = nullptr;
  }
  if (client == nullptr || client->client == nullptr || filepath == nullptr || loaded_path == nullptr) {
    return invalid_argument(client, "Invalid argument: client, filepath, and loaded_path must not be NULL");
  }

  auto load_result = client->client->Load(filepath);

  if (!load_result) {
    set_last_error(client, load_result.error());
    return -1;
  }

  *loaded_path = strdup_safe(*load_result);
  if (*loaded_path == nullptr) {
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return -1;
  }
  clear_last_error(client);
  return 0;
}

int mygramclient_replication_status(MygramClient_C* client, MygramReplicationStatus_C** status) {
  if (status != nullptr) {
    *status = nullptr;
  }
  if (client == nullptr || client->client == nullptr || status == nullptr) {
    return invalid_argument(client, "Invalid argument: client and status must not be NULL");
  }

  auto repl_result = client->client->GetReplicationStatus();
  if (!repl_result) {
    set_last_error(client, repl_result.error());
    return -1;
  }

  const auto& repl = *repl_result;

  auto* out = static_cast<MygramReplicationStatus_C*>(malloc(sizeof(MygramReplicationStatus_C)));
  if (out == nullptr) {
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return -1;
  }

  // Initialize all pointer fields to NULL up front so partial failure
  // below leaves the struct in a state safe to free.
  out->gtid = nullptr;
  out->status_str = nullptr;
  out->running = repl.running ? 1 : 0;
  out->processed_events = repl.processed_events;
  out->queue_size = repl.queue_size;

  out->gtid = strdup_safe(repl.gtid);
  if (out->gtid == nullptr) {
    free(out);
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return -1;
  }

  out->status_str = strdup_safe(repl.status_str);
  if (out->status_str == nullptr) {
    free(out->gtid);
    free(out);
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return -1;
  }

  *status = out;
  clear_last_error(client);
  return 0;
}

void mygramclient_free_replication_status(MygramReplicationStatus_C* status) {
  if (status == nullptr) {
    return;
  }
  free(status->gtid);
  free(status->status_str);
  free(status);
}

int mygramclient_replication_stop(MygramClient_C* client) {
  return ForwardVoid(client, [](MygramClient& c) { return c.StopReplication(); });
}

int mygramclient_replication_start(MygramClient_C* client) {
  return ForwardVoid(client, [](MygramClient& c) { return c.StartReplication(); });
}

int mygramclient_debug_on(MygramClient_C* client) {
  return ForwardVoid(client, [](MygramClient& c) { return c.EnableDebug(); });
}

int mygramclient_debug_off(MygramClient_C* client) {
  return ForwardVoid(client, [](MygramClient& c) { return c.DisableDebug(); });
}

int mygramclient_send_command(MygramClient_C* client, const char* command, char** response) {
  if (response != nullptr) {
    *response = nullptr;
  }
  if (client == nullptr || client->client == nullptr || command == nullptr || response == nullptr) {
    return invalid_argument(client, "Invalid argument: client, command, and response must not be NULL");
  }

  auto result = client->client->SendCommand(command);
  if (!result) {
    set_last_error(client, result.error());
    return -1;
  }

  *response = strdup_safe(*result);
  if (*response == nullptr) {
    set_last_error(client, "Memory allocation failed", ErrorCode::kClientCommandFailed);
    return -1;
  }
  clear_last_error(client);
  return 0;
}

const char* mygramclient_get_last_error(const MygramClient_C* client) {
  if (client == nullptr) {
    return "Invalid client handle";
  }
  thread_local std::string error_snapshot;
  try {
    std::lock_guard<std::mutex> lock(client->error_mutex);
    error_snapshot = client->last_error;
    return error_snapshot.c_str();
  } catch (...) {
    return "Unable to copy client error";
  }
}

int mygramclient_get_last_error_code(const MygramClient_C* client) {
  if (client == nullptr) {
    return static_cast<int>(ErrorCode::kUnknown);
  }

  std::lock_guard<std::mutex> lock(client->error_mutex);
  return client->last_error_code;
}

void mygramclient_free_search_result(MygramSearchResult_C* result) {
  if (result == nullptr) {
    return;
  }

  free_c_string_array(result->primary_keys, result->count);
  free(result);
}

void mygramclient_free_search_result_with_highlights(MygramSearchResultWithHighlights_C* result) {
  if (result == nullptr) {
    return;
  }

  free_c_string_array(result->primary_keys, result->count);
  free_c_string_array(result->snippets, result->count);
  free(result);
}

void mygramclient_free_facet_result(MygramFacetResult_C* result) {
  if (result == nullptr) {
    return;
  }

  free_c_string_array(result->values, result->count);
  free(result->counts);
  free(result);
}

void mygramclient_free_document(MygramDocument_C* doc) {
  if (doc == nullptr) {
    return;
  }

  free(doc->primary_key);
  free_c_string_array(doc->field_keys, doc->field_count);
  free_c_string_array(doc->field_values, doc->field_count);
  free(doc);
}

void mygramclient_free_server_info(MygramServerInfo_C* info) {
  if (info == nullptr) {
    return;
  }

  free(info->version);
  free_c_string_array(info->tables, info->table_count);
  free(info);
}

void mygramclient_free_string(char* str) {
  free(str);
}

int mygramclient_parse_search_expression(const char* expression, MygramParsedExpression_C** parsed) {
  if (parsed != nullptr) {
    *parsed = nullptr;
  }
  if (expression == nullptr || parsed == nullptr) {
    return -1;
  }

  auto simplified = SimplifySearchExpression(expression);
  if (!simplified) {
    return -1;
  }

  // Allocate result
  auto* result = static_cast<MygramParsedExpression_C*>(malloc(sizeof(MygramParsedExpression_C)));
  if (result == nullptr) {
    return -1;
  }
  result->main_term = nullptr;
  result->and_terms = nullptr;
  result->and_count = 0;
  result->not_terms = nullptr;
  result->not_count = 0;
  result->optional_terms = nullptr;
  result->optional_count = 0;

  // Copy main term
  result->main_term = strdup_safe(simplified->main_term);
  if (result->main_term == nullptr) {
    free(result);
    return -1;
  }

  // Copy AND terms
  if (!string_vector_to_c_array_checked(simplified->and_terms, &result->and_terms)) {
    mygramclient_free_parsed_expression(result);
    return -1;
  }
  result->and_count = simplified->and_terms.size();

  // Copy NOT terms
  if (!string_vector_to_c_array_checked(simplified->not_terms, &result->not_terms)) {
    mygramclient_free_parsed_expression(result);
    return -1;
  }
  result->not_count = simplified->not_terms.size();

  // optional_terms / optional_count are deprecated since the implicit-AND
  // parser change: every parsed term is now classified as required (AND)
  // or excluded (NOT). Emit NULL / 0 explicitly so consumers do not need
  // to defensively check string_vector_to_c_array's empty-input behavior.
  result->optional_terms = nullptr;
  result->optional_count = 0;

  *parsed = result;
  return 0;
}

int mygramclient_convert_search_expression(const char* expression, char** converted) {
  if (converted != nullptr) {
    *converted = nullptr;
  }
  if (expression == nullptr || converted == nullptr) {
    return -1;
  }
  auto result = ConvertSearchExpression(expression);
  if (!result) {
    return -1;
  }
  *converted = strdup_safe(*result);
  return *converted == nullptr ? -1 : 0;
}

void mygramclient_free_parsed_expression(MygramParsedExpression_C* parsed) {
  if (parsed == nullptr) {
    return;
  }

  free(parsed->main_term);
  free_c_string_array(parsed->and_terms, parsed->and_count);
  free_c_string_array(parsed->not_terms, parsed->not_count);
  free_c_string_array(parsed->optional_terms, parsed->optional_count);
  free(parsed);
}

}  // namespace

#undef mygramclient_create
#undef mygramclient_create_v2
#undef mygramclient_destroy
#undef mygramclient_connect
#undef mygramclient_disconnect
#undef mygramclient_is_connected
#undef mygramclient_search
#undef mygramclient_search_raw
#undef mygramclient_search_with_highlights
#undef mygramclient_search_raw_with_highlights
#undef mygramclient_search_with_options
#undef mygramclient_search_with_highlights_advanced
#undef mygramclient_search_advanced
#undef mygramclient_count
#undef mygramclient_count_advanced
#undef mygramclient_facet
#undef mygramclient_facet_advanced
#undef mygramclient_get
#undef mygramclient_info
#undef mygramclient_get_config
#undef mygramclient_set_variable
#undef mygramclient_show_variables
#undef mygramclient_cache_clear
#undef mygramclient_cache_stats
#undef mygramclient_cache_enable
#undef mygramclient_cache_disable
#undef mygramclient_optimize
#undef mygramclient_sync
#undef mygramclient_sync_status
#undef mygramclient_sync_stop
#undef mygramclient_dump_info
#undef mygramclient_dump_status
#undef mygramclient_dump_verify
#undef mygramclient_save
#undef mygramclient_load
#undef mygramclient_replication_status
#undef mygramclient_free_replication_status
#undef mygramclient_replication_stop
#undef mygramclient_replication_start
#undef mygramclient_debug_on
#undef mygramclient_debug_off
#undef mygramclient_send_command
#undef mygramclient_get_last_error
#undef mygramclient_get_last_error_code
#undef mygramclient_free_search_result
#undef mygramclient_free_search_result_with_highlights
#undef mygramclient_free_facet_result
#undef mygramclient_free_document
#undef mygramclient_free_server_info
#undef mygramclient_free_string
#undef mygramclient_parse_search_expression
#undef mygramclient_convert_search_expression
#undef mygramclient_free_parsed_expression

#define DEFINE_C_INT_WRAPPER(name, client_expr, call_args, ...)                   \
  int name(__VA_ARGS__) {                                                         \
    return c_api_guard(client_expr, -1, [&]() { return name##_impl call_args; }); \
  }

#define DEFINE_C_INT_POINTER_OUT_WRAPPER(name, client_expr, out, call_args, ...)  \
  int name(__VA_ARGS__) {                                                         \
    if (out != nullptr) {                                                         \
      *out = nullptr;                                                             \
    }                                                                             \
    return c_api_guard(client_expr, -1, [&]() { return name##_impl call_args; }); \
  }

extern "C" {

MygramClient_C* mygramclient_create(const MygramClientConfig_C* config) {
  return c_api_guard<MygramClient_C*>(nullptr, nullptr, [&]() { return mygramclient_create_impl(config); });
}

MygramClient_C* mygramclient_create_v2(const MygramClientConfigV2_C* config) {
  return c_api_guard<MygramClient_C*>(nullptr, nullptr, [&]() { return mygramclient_create_v2_impl(config); });
}

void mygramclient_destroy(MygramClient_C* client) {
  c_api_guard_void(nullptr, [&]() { mygramclient_destroy_impl(client); });
}

DEFINE_C_INT_WRAPPER(mygramclient_connect, client, (client), MygramClient_C* client)

void mygramclient_disconnect(MygramClient_C* client) {
  c_api_guard_void(client, [&]() { mygramclient_disconnect_impl(client); });
}

int mygramclient_is_connected(const MygramClient_C* client) {
  return c_api_guard(const_cast<MygramClient_C*>(client), 0, [&]() { return mygramclient_is_connected_impl(client); });
}

DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_search, client, result, (client, table, query, limit, offset, result),
                                 MygramClient_C* client, const char* table, const char* query, uint32_t limit,
                                 uint32_t offset, MygramSearchResult_C** result)

DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_search_raw, client, result,
                                 (client, table, raw_query, limit, offset, result), MygramClient_C* client,
                                 const char* table, const char* raw_query, uint32_t limit, uint32_t offset,
                                 MygramSearchResult_C** result)

DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_search_with_highlights, client, result,
                                 (client, table, query, limit, offset, result), MygramClient_C* client,
                                 const char* table, const char* query, uint32_t limit, uint32_t offset,
                                 MygramSearchResultWithHighlights_C** result)

DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_search_raw_with_highlights, client, result,
                                 (client, table, raw_query, limit, offset, result), MygramClient_C* client,
                                 const char* table, const char* raw_query, uint32_t limit, uint32_t offset,
                                 MygramSearchResultWithHighlights_C** result)

DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_search_with_options, client, result,
                                 (client, table, query, options, result), MygramClient_C* client, const char* table,
                                 const char* query, const MygramSearchOptions_C* options,
                                 MygramSearchResultWithHighlights_C** result)

DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_search_with_highlights_advanced, client, result,
                                 (client, table, query, limit, offset, and_terms, and_count, not_terms, not_count,
                                  filter_keys, filter_values, filter_count, sort_column, sort_desc, result),
                                 MygramClient_C* client, const char* table, const char* query, uint32_t limit,
                                 uint32_t offset, const char** and_terms, size_t and_count, const char** not_terms,
                                 size_t not_count, const char** filter_keys, const char** filter_values,
                                 size_t filter_count, const char* sort_column, int sort_desc,
                                 MygramSearchResultWithHighlights_C** result)

DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_search_advanced, client, result,
                                 (client, table, query, limit, offset, and_terms, and_count, not_terms, not_count,
                                  filter_keys, filter_values, filter_count, sort_column, sort_desc, result),
                                 MygramClient_C* client, const char* table, const char* query, uint32_t limit,
                                 uint32_t offset, const char** and_terms, size_t and_count, const char** not_terms,
                                 size_t not_count, const char** filter_keys, const char** filter_values,
                                 size_t filter_count, const char* sort_column, int sort_desc,
                                 MygramSearchResult_C** result)

int mygramclient_count(MygramClient_C* client, const char* table, const char* query, uint64_t* count) {
  if (count != nullptr) {
    *count = 0;
  }
  return c_api_guard(client, -1, [&]() { return mygramclient_count_impl(client, table, query, count); });
}

int mygramclient_count_advanced(MygramClient_C* client, const char* table, const char* query, const char** and_terms,
                                size_t and_count, const char** not_terms, size_t not_count, const char** filter_keys,
                                const char** filter_values, size_t filter_count, uint64_t* count) {
  if (count != nullptr) {
    *count = 0;
  }
  return c_api_guard(client, -1, [&]() {
    return mygramclient_count_advanced_impl(client, table, query, and_terms, and_count, not_terms, not_count,
                                            filter_keys, filter_values, filter_count, count);
  });
}

DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_facet, client, result, (client, table, column, query, limit, result),
                                 MygramClient_C* client, const char* table, const char* column, const char* query,
                                 uint32_t limit, MygramFacetResult_C** result)

DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_facet_advanced, client, result,
                                 (client, table, column, query, limit, and_terms, and_count, not_terms, not_count,
                                  filter_keys, filter_values, filter_count, result),
                                 MygramClient_C* client, const char* table, const char* column, const char* query,
                                 uint32_t limit, const char** and_terms, size_t and_count, const char** not_terms,
                                 size_t not_count, const char** filter_keys, const char** filter_values,
                                 size_t filter_count, MygramFacetResult_C** result)

DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_get, client, doc, (client, table, primary_key, doc),
                                 MygramClient_C* client, const char* table, const char* primary_key,
                                 MygramDocument_C** doc)

DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_info, client, info, (client, info), MygramClient_C* client,
                                 MygramServerInfo_C** info)

DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_get_config, client, config_str, (client, config_str),
                                 MygramClient_C* client, char** config_str)

DEFINE_C_INT_WRAPPER(mygramclient_set_variable, client, (client, name, value), MygramClient_C* client, const char* name,
                     const char* value)

DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_show_variables, client, response, (client, like_pattern, response),
                                 MygramClient_C* client, const char* like_pattern, char** response)

DEFINE_C_INT_WRAPPER(mygramclient_cache_clear, client, (client, table), MygramClient_C* client, const char* table)
DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_cache_stats, client, response, (client, response), MygramClient_C* client,
                                 char** response)
DEFINE_C_INT_WRAPPER(mygramclient_cache_enable, client, (client), MygramClient_C* client)
DEFINE_C_INT_WRAPPER(mygramclient_cache_disable, client, (client), MygramClient_C* client)
DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_optimize, client, response, (client, table, response),
                                 MygramClient_C* client, const char* table, char** response)
DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_sync, client, response, (client, table, response), MygramClient_C* client,
                                 const char* table, char** response)
DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_sync_status, client, response, (client, response), MygramClient_C* client,
                                 char** response)
DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_sync_stop, client, response, (client, table, response),
                                 MygramClient_C* client, const char* table, char** response)
DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_dump_info, client, response, (client, filepath, response),
                                 MygramClient_C* client, const char* filepath, char** response)
DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_dump_status, client, response, (client, response), MygramClient_C* client,
                                 char** response)
DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_dump_verify, client, response, (client, filepath, response),
                                 MygramClient_C* client, const char* filepath, char** response)
DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_save, client, saved_path, (client, filepath, saved_path),
                                 MygramClient_C* client, const char* filepath, char** saved_path)
DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_load, client, loaded_path, (client, filepath, loaded_path),
                                 MygramClient_C* client, const char* filepath, char** loaded_path)
DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_replication_status, client, status, (client, status),
                                 MygramClient_C* client, MygramReplicationStatus_C** status)

void mygramclient_free_replication_status(MygramReplicationStatus_C* status) {
  c_api_guard_void(nullptr, [&]() { mygramclient_free_replication_status_impl(status); });
}

DEFINE_C_INT_WRAPPER(mygramclient_replication_stop, client, (client), MygramClient_C* client)
DEFINE_C_INT_WRAPPER(mygramclient_replication_start, client, (client), MygramClient_C* client)
DEFINE_C_INT_WRAPPER(mygramclient_debug_on, client, (client), MygramClient_C* client)
DEFINE_C_INT_WRAPPER(mygramclient_debug_off, client, (client), MygramClient_C* client)
DEFINE_C_INT_POINTER_OUT_WRAPPER(mygramclient_send_command, client, response, (client, command, response),
                                 MygramClient_C* client, const char* command, char** response)

const char* mygramclient_get_last_error(const MygramClient_C* client) {
  return c_api_guard<const char*>(const_cast<MygramClient_C*>(client), "C API exception",
                                  [&]() { return mygramclient_get_last_error_impl(client); });
}

int mygramclient_get_last_error_code(const MygramClient_C* client) {
  return c_api_guard(const_cast<MygramClient_C*>(client), static_cast<int>(ErrorCode::kClientCommandFailed),
                     [&]() { return mygramclient_get_last_error_code_impl(client); });
}

void mygramclient_free_search_result(MygramSearchResult_C* result) {
  c_api_guard_void(nullptr, [&]() { mygramclient_free_search_result_impl(result); });
}

void mygramclient_free_search_result_with_highlights(MygramSearchResultWithHighlights_C* result) {
  c_api_guard_void(nullptr, [&]() { mygramclient_free_search_result_with_highlights_impl(result); });
}

void mygramclient_free_facet_result(MygramFacetResult_C* result) {
  c_api_guard_void(nullptr, [&]() { mygramclient_free_facet_result_impl(result); });
}

void mygramclient_free_document(MygramDocument_C* doc) {
  c_api_guard_void(nullptr, [&]() { mygramclient_free_document_impl(doc); });
}

void mygramclient_free_server_info(MygramServerInfo_C* info) {
  c_api_guard_void(nullptr, [&]() { mygramclient_free_server_info_impl(info); });
}

void mygramclient_free_string(char* str) {
  c_api_guard_void(nullptr, [&]() { mygramclient_free_string_impl(str); });
}

int mygramclient_parse_search_expression(const char* expression, MygramParsedExpression_C** parsed) {
  if (parsed != nullptr) {
    *parsed = nullptr;
  }
  return c_api_guard(static_cast<MygramClient_C*>(nullptr), -1,
                     [&]() { return mygramclient_parse_search_expression_impl(expression, parsed); });
}

int mygramclient_convert_search_expression(const char* expression, char** converted) {
  if (converted != nullptr) {
    *converted = nullptr;
  }
  return c_api_guard(static_cast<MygramClient_C*>(nullptr), -1,
                     [&]() { return mygramclient_convert_search_expression_impl(expression, converted); });
}

void mygramclient_free_parsed_expression(MygramParsedExpression_C* parsed) {
  c_api_guard_void(nullptr, [&]() { mygramclient_free_parsed_expression_impl(parsed); });
}

}  // extern "C"

#undef DEFINE_C_INT_WRAPPER
#undef DEFINE_C_INT_POINTER_OUT_WRAPPER

// NOLINTEND(readability-identifier-naming, cppcoreguidelines-owning-memory,
// cppcoreguidelines-no-malloc, readability-implicit-bool-conversion)
