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

MygramClient_C* mygramclient_create(const MygramClientConfig_C* config) {
  if (config == nullptr) {
    return nullptr;
  }

  auto* client_c = new MygramClient_C();

  ClientConfig cpp_config;
  cpp_config.host = (config->host != nullptr) ? config->host : "127.0.0.1";
  cpp_config.port = config->port != 0 ? config->port : 11016;
  cpp_config.timeout_ms = config->timeout_ms != 0 ? config->timeout_ms : 5000;
  cpp_config.recv_buffer_size = config->recv_buffer_size != 0 ? config->recv_buffer_size : 65536;

  client_c->client = std::make_unique<MygramClient>(cpp_config);

  return client_c;
}

void mygramclient_destroy(MygramClient_C* client) {
  delete client;
}

int mygramclient_connect(MygramClient_C* client) {
  if (client == nullptr || client->client == nullptr) {
    return -1;
  }

  auto err = client->client->Connect();
  if (err) {
    client->last_error = *err;
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
                                      nullptr, 0, result);
}

int mygramclient_search_advanced(MygramClient_C* client, const char* table, const char* query, uint32_t limit,
                                 uint32_t offset, const char** and_terms, size_t and_count, const char** not_terms,
                                 size_t not_count, const char** filter_keys, const char** filter_values,
                                 size_t filter_count, const char* order_by, int order_desc,
                                 MygramSearchResult_C** result) {
  if (client == nullptr || client->client == nullptr || table == nullptr || query == nullptr || result == nullptr) {
    return -1;
  }

  // Convert C arrays to C++ vectors
  std::vector<std::string> and_terms_vec;
  for (size_t i = 0; i < and_count; ++i) {
    if (and_terms[i] != nullptr) {
      and_terms_vec.emplace_back(and_terms[i]);
    }
  }

  std::vector<std::string> not_terms_vec;
  for (size_t i = 0; i < not_count; ++i) {
    if (not_terms[i] != nullptr) {
      not_terms_vec.emplace_back(not_terms[i]);
    }
  }

  std::vector<std::pair<std::string, std::string>> filters_vec;
  for (size_t i = 0; i < filter_count; ++i) {
    if (filter_keys[i] != nullptr && filter_values[i] != nullptr) {
      filters_vec.emplace_back(filter_keys[i], filter_values[i]);
    }
  }

  std::string order_by_str = order_by != nullptr ? order_by : "";

  auto search_result = client->client->Search(table, query, limit, offset, and_terms_vec, not_terms_vec, filters_vec,
                                              order_by_str, order_desc != 0);

  if (auto* err = std::get_if<Error>(&search_result)) {
    client->last_error = err->message;
    return -1;
  }

  auto resp = std::get<SearchResponse>(search_result);

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

  // Convert C arrays to C++ vectors
  std::vector<std::string> and_terms_vec;
  for (size_t i = 0; i < and_count; ++i) {
    if (and_terms[i] != nullptr) {
      and_terms_vec.emplace_back(and_terms[i]);
    }
  }

  std::vector<std::string> not_terms_vec;
  for (size_t i = 0; i < not_count; ++i) {
    if (not_terms[i] != nullptr) {
      not_terms_vec.emplace_back(not_terms[i]);
    }
  }

  std::vector<std::pair<std::string, std::string>> filters_vec;
  for (size_t i = 0; i < filter_count; ++i) {
    if (filter_keys[i] != nullptr && filter_values[i] != nullptr) {
      filters_vec.emplace_back(filter_keys[i], filter_values[i]);
    }
  }

  auto count_result = client->client->Count(table, query, and_terms_vec, not_terms_vec, filters_vec);

  if (auto* err = std::get_if<Error>(&count_result)) {
    client->last_error = err->message;
    return -1;
  }

  auto resp = std::get<CountResponse>(count_result);
  *count = resp.count;

  return 0;
}

int mygramclient_get(MygramClient_C* client, const char* table, const char* primary_key, MygramDocument_C** doc) {
  if (client == nullptr || client->client == nullptr || table == nullptr || primary_key == nullptr || doc == nullptr) {
    return -1;
  }

  auto get_result = client->client->Get(table, primary_key);

  if (auto* err = std::get_if<Error>(&get_result)) {
    client->last_error = err->message;
    return -1;
  }

  auto document = std::get<Document>(get_result);

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
      doc_c->field_values[i] = strdup_safe(document.fields[i].second);
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

  if (auto* err = std::get_if<Error>(&info_result)) {
    client->last_error = err->message;
    return -1;
  }

  auto server_info = std::get<ServerInfo>(info_result);

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

  if (auto* err = std::get_if<Error>(&config_result)) {
    client->last_error = err->message;
    return -1;
  }

  *config_str = strdup_safe(std::get<std::string>(config_result));
  return 0;
}

int mygramclient_save(MygramClient_C* client, const char* filepath, char** saved_path) {
  if (client == nullptr || client->client == nullptr || saved_path == nullptr) {
    return -1;
  }

  std::string filepath_str = filepath != nullptr ? filepath : "";
  auto save_result = client->client->Save(filepath_str);

  if (auto* err = std::get_if<Error>(&save_result)) {
    client->last_error = err->message;
    return -1;
  }

  *saved_path = strdup_safe(std::get<std::string>(save_result));
  return 0;
}

int mygramclient_load(MygramClient_C* client, const char* filepath, char** loaded_path) {
  if (client == nullptr || client->client == nullptr || filepath == nullptr || loaded_path == nullptr) {
    return -1;
  }

  auto load_result = client->client->Load(filepath);

  if (auto* err = std::get_if<Error>(&load_result)) {
    client->last_error = err->message;
    return -1;
  }

  *loaded_path = strdup_safe(std::get<std::string>(load_result));
  return 0;
}

int mygramclient_replication_stop(MygramClient_C* client) {
  if (client == nullptr || client->client == nullptr) {
    return -1;
  }

  auto err = client->client->StopReplication();
  if (err) {
    client->last_error = *err;
    return -1;
  }

  return 0;
}

int mygramclient_replication_start(MygramClient_C* client) {
  if (client == nullptr || client->client == nullptr) {
    return -1;
  }

  auto err = client->client->StartReplication();
  if (err) {
    client->last_error = *err;
    return -1;
  }

  return 0;
}

int mygramclient_debug_on(MygramClient_C* client) {
  if (client == nullptr || client->client == nullptr) {
    return -1;
  }

  auto err = client->client->EnableDebug();
  if (err) {
    client->last_error = *err;
    return -1;
  }

  return 0;
}

int mygramclient_debug_off(MygramClient_C* client) {
  if (client == nullptr || client->client == nullptr) {
    return -1;
  }

  auto err = client->client->DisableDebug();
  if (err) {
    client->last_error = *err;
    return -1;
  }

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

// NOLINTEND(readability-identifier-naming, cppcoreguidelines-owning-memory,
// cppcoreguidelines-no-malloc, readability-implicit-bool-conversion)
