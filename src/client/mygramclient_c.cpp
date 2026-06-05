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

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "client/mygramclient.h"
#include "client/search_expression.h"

using namespace mygramdb::client;

// Opaque handle structure
struct MygramClient_C {
  std::unique_ptr<MygramClient> client;
  std::string last_error;
};

// Helper: Allocate C string copy
// cppcoreguidelines-no-malloc)
static char* strdup_safe(const std::string& str) {
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
    return -1;
  }
  auto result = fn(*client->client);
  if (!result) {
    client->last_error = result.error().to_string();
    return -1;
  }
  return 0;
}

MygramClient_C* mygramclient_create(const MygramClientConfig_C* config) {
  if (config == nullptr) {
    return nullptr;
  }

  // Use unique_ptr so any exception during MygramClient construction frees
  // the wrapper without leaking memory. release() transfers ownership to the
  // caller only after every fallible step has succeeded.
  std::unique_ptr<MygramClient_C> client_c;
  try {
    client_c = std::make_unique<MygramClient_C>();

    ClientConfig cpp_config;
    cpp_config.host = (config->host != nullptr) ? config->host : "127.0.0.1";
    cpp_config.port = config->port != 0 ? config->port : static_cast<uint16_t>(mygramdb::config::defaults::kTcpPort);
    cpp_config.timeout_ms = config->timeout_ms != 0 ? config->timeout_ms : 5000;
    cpp_config.recv_buffer_size = config->recv_buffer_size != 0 ? config->recv_buffer_size : 65536;

    client_c->client = std::make_unique<MygramClient>(cpp_config);
  } catch (...) {
    return nullptr;
  }

  return client_c.release();
}

void mygramclient_destroy(MygramClient_C* client) {
  delete client;
}

int mygramclient_connect(MygramClient_C* client) {
  if (client == nullptr || client->client == nullptr) {
    return -1;
  }

  auto result = client->client->Connect();
  if (!result) {
    client->last_error = result.error().to_string();
    return -1;
  }

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

int mygramclient_search(MygramClient_C* client, const char* table, const char* query, uint32_t limit, uint32_t offset,
                        MygramSearchResult_C** result) {
  return mygramclient_search_advanced(client, table, query, limit, offset, nullptr, 0, nullptr, 0, nullptr, nullptr, 0,
                                      nullptr, 1, result);  // Default sort_desc = 1 (descending)
}

int mygramclient_search_advanced(MygramClient_C* client, const char* table, const char* query, uint32_t limit,
                                 uint32_t offset, const char** and_terms, size_t and_count, const char** not_terms,
                                 size_t not_count, const char** filter_keys, const char** filter_values,
                                 size_t filter_count, const char* sort_column, int sort_desc,
                                 MygramSearchResult_C** result) {
  if (client == nullptr || client->client == nullptr || table == nullptr || query == nullptr || result == nullptr) {
    return -1;
  }

  // Reject the (count > 0, ptr == nullptr) combination for each array argument.
  // Without this guard, the conversion loops below dereference a NULL pointer.
  if (and_count > 0 && and_terms == nullptr) {
    client->last_error = "Invalid argument: and_terms is NULL but and_count > 0";
    return -1;
  }
  if (not_count > 0 && not_terms == nullptr) {
    client->last_error = "Invalid argument: not_terms is NULL but not_count > 0";
    return -1;
  }
  if (filter_count > 0 && (filter_keys == nullptr || filter_values == nullptr)) {
    client->last_error = "Invalid argument: filter_keys or filter_values is NULL but filter_count > 0";
    return -1;
  }

  // Convert C arrays to C++ vectors
  auto and_terms_vec = CArrayToVector(and_terms, and_count);
  auto not_terms_vec = CArrayToVector(not_terms, not_count);
  auto filters_vec = CFilterArraysToVector(filter_keys, filter_values, filter_count);

  std::string sort_column_str = sort_column != nullptr ? sort_column : "";

  auto search_result = client->client->Search(table, query, limit, offset, and_terms_vec, not_terms_vec, filters_vec,
                                              sort_column_str, sort_desc != 0);

  if (!search_result) {
    client->last_error = search_result.error().to_string();
    return -1;
  }

  auto& resp = *search_result;

  auto* result_c = static_cast<MygramSearchResult_C*>(malloc(sizeof(MygramSearchResult_C)));
  if (result_c == nullptr) {
    client->last_error = "Memory allocation failed";
    return -1;
  }

  result_c->count = resp.results.size();
  result_c->total_count = resp.total_count;

  // Allocate array for primary keys
  result_c->primary_keys = static_cast<char**>(malloc(sizeof(char*) * resp.results.size()));
  if (result_c->primary_keys == nullptr) {
    free(result_c);
    client->last_error = "Memory allocation failed";
    return -1;
  }

  for (size_t i = 0; i < resp.results.size(); ++i) {
    result_c->primary_keys[i] = strdup_safe(resp.results[i].primary_key);
    if (result_c->primary_keys[i] == nullptr) {
      // Free all previously allocated entries
      for (size_t j = 0; j < i; ++j) {
        free(result_c->primary_keys[j]);
      }
      free(result_c->primary_keys);
      free(result_c);
      client->last_error = "Memory allocation failed";
      return -1;
    }
  }

  *result = result_c;
  return 0;
}

int mygramclient_count(MygramClient_C* client, const char* table, const char* query, uint64_t* count) {
  return mygramclient_count_advanced(client, table, query, nullptr, 0, nullptr, 0, nullptr, nullptr, 0, count);
}

int mygramclient_count_advanced(MygramClient_C* client, const char* table, const char* query, const char** and_terms,
                                size_t and_count, const char** not_terms, size_t not_count, const char** filter_keys,
                                const char** filter_values, size_t filter_count, uint64_t* count) {
  if (client == nullptr || client->client == nullptr || table == nullptr || query == nullptr || count == nullptr) {
    return -1;
  }

  // Reject the (count > 0, ptr == nullptr) combination for each array argument.
  if (and_count > 0 && and_terms == nullptr) {
    client->last_error = "Invalid argument: and_terms is NULL but and_count > 0";
    return -1;
  }
  if (not_count > 0 && not_terms == nullptr) {
    client->last_error = "Invalid argument: not_terms is NULL but not_count > 0";
    return -1;
  }
  if (filter_count > 0 && (filter_keys == nullptr || filter_values == nullptr)) {
    client->last_error = "Invalid argument: filter_keys or filter_values is NULL but filter_count > 0";
    return -1;
  }

  // Convert C arrays to C++ vectors
  auto and_terms_vec = CArrayToVector(and_terms, and_count);
  auto not_terms_vec = CArrayToVector(not_terms, not_count);
  auto filters_vec = CFilterArraysToVector(filter_keys, filter_values, filter_count);

  auto count_result = client->client->Count(table, query, and_terms_vec, not_terms_vec, filters_vec);

  if (!count_result) {
    client->last_error = count_result.error().to_string();
    return -1;
  }

  auto& resp = *count_result;
  *count = resp.count;

  return 0;
}

int mygramclient_get(MygramClient_C* client, const char* table, const char* primary_key, MygramDocument_C** doc) {
  if (client == nullptr || client->client == nullptr || table == nullptr || primary_key == nullptr || doc == nullptr) {
    return -1;
  }

  auto get_result = client->client->Get(table, primary_key);

  if (!get_result) {
    client->last_error = get_result.error().to_string();
    return -1;
  }

  auto& document = *get_result;

  auto* doc_c = static_cast<MygramDocument_C*>(malloc(sizeof(MygramDocument_C)));
  if (doc_c == nullptr) {
    client->last_error = "Memory allocation failed";
    return -1;
  }

  doc_c->primary_key = strdup_safe(document.primary_key);
  doc_c->field_count = document.fields.size();

  if (doc_c->field_count > 0) {
    doc_c->field_keys = static_cast<char**>(malloc(sizeof(char*) * doc_c->field_count));
    doc_c->field_values = static_cast<char**>(malloc(sizeof(char*) * doc_c->field_count));

    if (doc_c->field_keys == nullptr || doc_c->field_values == nullptr) {
      free(doc_c->primary_key);
      free(doc_c->field_keys);
      free(doc_c->field_values);
      free(doc_c);
      client->last_error = "Memory allocation failed";
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
        client->last_error = "Memory allocation failed";
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
        client->last_error = "Memory allocation failed";
        return -1;
      }
    }
  } else {
    doc_c->field_keys = nullptr;
    doc_c->field_values = nullptr;
  }

  *doc = doc_c;
  return 0;
}

int mygramclient_info(MygramClient_C* client, MygramServerInfo_C** info) {
  if (client == nullptr || client->client == nullptr || info == nullptr) {
    return -1;
  }

  auto info_result = client->client->Info();

  if (!info_result) {
    client->last_error = info_result.error().to_string();
    return -1;
  }

  auto& server_info = *info_result;

  auto* info_c = static_cast<MygramServerInfo_C*>(malloc(sizeof(MygramServerInfo_C)));
  if (info_c == nullptr) {
    client->last_error = "Memory allocation failed";
    return -1;
  }

  info_c->version = strdup_safe(server_info.version);
  info_c->uptime_seconds = server_info.uptime_seconds;
  info_c->total_requests = server_info.total_requests;
  info_c->active_connections = server_info.active_connections;
  info_c->index_size_bytes = server_info.index_size_bytes;
  info_c->doc_count = server_info.doc_count;
  info_c->table_count = server_info.tables.size();
  info_c->tables = string_vector_to_c_array(server_info.tables);

  *info = info_c;
  return 0;
}

int mygramclient_get_config(MygramClient_C* client, char** config_str) {
  if (client == nullptr || client->client == nullptr || config_str == nullptr) {
    return -1;
  }

  auto config_result = client->client->GetConfig();

  if (!config_result) {
    client->last_error = config_result.error().to_string();
    return -1;
  }

  *config_str = strdup_safe(*config_result);
  return 0;
}

int mygramclient_save(MygramClient_C* client, const char* filepath, char** saved_path) {
  if (client == nullptr || client->client == nullptr || saved_path == nullptr) {
    return -1;
  }

  std::string filepath_str = filepath != nullptr ? filepath : "";
  auto save_result = client->client->Save(filepath_str);

  if (!save_result) {
    client->last_error = save_result.error().to_string();
    return -1;
  }

  *saved_path = strdup_safe(*save_result);
  return 0;
}

int mygramclient_load(MygramClient_C* client, const char* filepath, char** loaded_path) {
  if (client == nullptr || client->client == nullptr || filepath == nullptr || loaded_path == nullptr) {
    return -1;
  }

  auto load_result = client->client->Load(filepath);

  if (!load_result) {
    client->last_error = load_result.error().to_string();
    return -1;
  }

  *loaded_path = strdup_safe(*load_result);
  return 0;
}

int mygramclient_replication_status(MygramClient_C* client, MygramReplicationStatus_C** status) {
  if (client == nullptr || client->client == nullptr || status == nullptr) {
    return -1;
  }

  auto repl_result = client->client->GetReplicationStatus();
  if (!repl_result) {
    client->last_error = repl_result.error().to_string();
    return -1;
  }

  const auto& repl = *repl_result;

  auto* out = static_cast<MygramReplicationStatus_C*>(malloc(sizeof(MygramReplicationStatus_C)));
  if (out == nullptr) {
    client->last_error = "Memory allocation failed";
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
    client->last_error = "Memory allocation failed";
    return -1;
  }

  out->status_str = strdup_safe(repl.status_str);
  if (out->status_str == nullptr) {
    free(out->gtid);
    free(out);
    client->last_error = "Memory allocation failed";
    return -1;
  }

  *status = out;
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
  if (client == nullptr || client->client == nullptr || command == nullptr || response == nullptr) {
    return -1;
  }

  auto result = client->client->SendCommand(command);
  if (!result) {
    client->last_error = result.error().to_string();
    return -1;
  }

  *response = strdup_safe(*result);
  return 0;
}

const char* mygramclient_get_last_error(const MygramClient_C* client) {
  if (client == nullptr) {
    return "Invalid client handle";
  }

  return client->last_error.c_str();
}

void mygramclient_free_search_result(MygramSearchResult_C* result) {
  if (result == nullptr) {
    return;
  }

  free_c_string_array(result->primary_keys, result->count);
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
  if (expression == nullptr || parsed == nullptr) {
    return -1;
  }

  // Parse using SimplifySearchExpression
  std::string main_term;
  std::vector<std::string> and_terms;
  std::vector<std::string> not_terms;

  if (!SimplifySearchExpression(expression, main_term, and_terms, not_terms)) {
    return -1;
  }

  // Allocate result
  auto* result = static_cast<MygramParsedExpression_C*>(malloc(sizeof(MygramParsedExpression_C)));
  if (result == nullptr) {
    return -1;
  }

  // Copy main term
  result->main_term = strdup_safe(main_term);

  // Copy AND terms
  result->and_count = and_terms.size();
  result->and_terms = string_vector_to_c_array(and_terms);

  // Copy NOT terms
  result->not_count = not_terms.size();
  result->not_terms = string_vector_to_c_array(not_terms);

  // optional_terms / optional_count are deprecated since the implicit-AND
  // parser change: every parsed term is now classified as required (AND)
  // or excluded (NOT). Emit NULL / 0 explicitly so consumers do not need
  // to defensively check string_vector_to_c_array's empty-input behavior.
  result->optional_terms = nullptr;
  result->optional_count = 0;

  *parsed = result;
  return 0;
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

// NOLINTEND(readability-identifier-naming, cppcoreguidelines-owning-memory,
// cppcoreguidelines-no-malloc, readability-implicit-bool-conversion)
