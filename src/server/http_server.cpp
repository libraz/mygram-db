/**
 * @file http_server.cpp
 * @brief HTTP server implementation
 */

#include "server/http_server.h"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <sstream>
#include <chrono>
#include <variant>

// Fix for httplib missing NI_MAXHOST on some platforms
#ifndef NI_MAXHOST
#define NI_MAXHOST 1025
#endif

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#endif

using json = nlohmann::json;

namespace mygramdb {
namespace server {

HttpServer::HttpServer(HttpServerConfig config, index::Index& index,
                       storage::DocumentStore& doc_store, int ngram_size,
                       const config::Config* full_config,
#ifdef USE_MYSQL
                       mysql::BinlogReader* binlog_reader
#else
                       void* binlog_reader
#endif
                       )
    : config_(config),
      index_(index),
      doc_store_(doc_store),
      ngram_size_(ngram_size),
      full_config_(full_config),
      binlog_reader_(binlog_reader) {
  server_ = std::make_unique<httplib::Server>();

  // Set timeouts
  server_->set_read_timeout(config_.read_timeout_sec, 0);
  server_->set_write_timeout(config_.write_timeout_sec, 0);

  // Setup routes
  SetupRoutes();

  // Setup CORS if enabled
  if (config_.enable_cors) {
    SetupCors();
  }
}

HttpServer::~HttpServer() {
  Stop();
}

void HttpServer::SetupRoutes() {
  // POST /{table}/search - Full-text search
  server_->Post(R"(/(\w+)/search)", [this](const httplib::Request& req, httplib::Response& res) {
    HandleSearch(req, res);
  });

  // GET /{table}/:id - Get document by ID
  server_->Get(R"(/(\w+)/(\d+))", [this](const httplib::Request& req, httplib::Response& res) {
    HandleGet(req, res);
  });

  // GET /info - Server information
  server_->Get("/info", [this](const httplib::Request& req, httplib::Response& res) {
    HandleInfo(req, res);
  });

  // GET /health - Health check
  server_->Get("/health", [this](const httplib::Request& req, httplib::Response& res) {
    HandleHealth(req, res);
  });

  // GET /config - Configuration
  server_->Get("/config", [this](const httplib::Request& req, httplib::Response& res) {
    HandleConfig(req, res);
  });

  // GET /replication/status - Replication status
  server_->Get("/replication/status", [this](const httplib::Request& req, httplib::Response& res) {
    HandleReplicationStatus(req, res);
  });
}

void HttpServer::SetupCors() {
  // CORS preflight
  server_->Options(".*", [](const httplib::Request& /*req*/, httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", "*");
    res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.set_header("Access-Control-Allow-Headers", "Content-Type");
    res.status = 204;
  });

  // Add CORS headers to all responses
  server_->set_post_routing_handler([](const httplib::Request& /*req*/, httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", "*");
  });
}

bool HttpServer::Start() {
  if (running_) {
    last_error_ = "Server already running";
    return false;
  }

  start_time_ = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();

  // Start server in separate thread
  server_thread_ = std::make_unique<std::thread>([this]() {
    spdlog::info("Starting HTTP server on {}:{}", config_.bind, config_.port);
    running_ = true;

    if (!server_->listen(config_.bind.c_str(), config_.port)) {
      last_error_ = "Failed to bind to " + config_.bind + ":" + std::to_string(config_.port);
      spdlog::error("{}", last_error_);
      running_ = false;
      return;
    }
  });

  // Wait a bit for server to start
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  if (!running_) {
    if (server_thread_ && server_thread_->joinable()) {
      server_thread_->join();
    }
    return false;
  }

  spdlog::info("HTTP server started successfully on {}:{}", config_.bind, config_.port);
  return true;
}

void HttpServer::Stop() {
  if (!running_) {
    return;
  }

  spdlog::info("Stopping HTTP server...");
  running_ = false;

  if (server_) {
    server_->stop();
  }

  if (server_thread_ && server_thread_->joinable()) {
    server_thread_->join();
  }

  spdlog::info("HTTP server stopped");
}

void HttpServer::HandleSearch(const httplib::Request& req, httplib::Response& res) {
  total_requests_++;

  try {
    // Extract table name from URL
    std::string table = req.matches[1];

    // Parse JSON body
    json body;
    try {
      body = json::parse(req.body);
    } catch (const json::parse_error& e) {
      SendError(res, 400, "Invalid JSON: " + std::string(e.what()));
      return;
    }

    // Validate required field
    if (!body.contains("q")) {
      SendError(res, 400, "Missing required field: q");
      return;
    }

    // Build query string for QueryParser
    std::ostringstream query_str;
    query_str << "SEARCH " << table << " " << body["q"].get<std::string>();

    // Add filters
    if (body.contains("filters") && body["filters"].is_object()) {
      for (auto& [key, val] : body["filters"].items()) {
        query_str << " FILTER " << key << "=";
        if (val.is_string()) {
          query_str << val.get<std::string>();
        } else {
          query_str << val.dump();
        }
      }
    }

    // Add limit
    if (body.contains("limit")) {
      query_str << " LIMIT " << body["limit"].get<int>();
    }

    // Add offset
    if (body.contains("offset")) {
      query_str << " OFFSET " << body["offset"].get<int>();
    }

    // Parse and execute query
    auto query = query_parser_.Parse(query_str.str());
    if (!query.IsValid()) {
      SendError(res, 400, "Invalid query: " + query_parser_.GetError());
      return;
    }

    // Perform search (AND search for all terms)
    std::vector<std::string> search_terms;
    if (!query.search_text.empty()) {
      search_terms.push_back(query.search_text);
    }
    search_terms.insert(search_terms.end(), query.and_terms.begin(), query.and_terms.end());

    auto results = index_.SearchAnd(search_terms);

    // Build JSON response
    json response;
    response["count"] = results.size();
    response["limit"] = query.limit;
    response["offset"] = query.offset;

    json results_array = json::array();
    for (const auto& doc_id : results) {
      auto doc = doc_store_.GetDocument(doc_id);
      if (doc) {
        json doc_obj;
        doc_obj["doc_id"] = doc->doc_id;
        doc_obj["primary_key"] = doc->primary_key;

        // Add filters
        if (!doc->filters.empty()) {
          json filters_obj;
          for (const auto& [key, val] : doc->filters) {
            // Convert FilterValue to JSON
            std::visit([&](auto&& arg) {
              using T = std::decay_t<decltype(arg)>;
              if constexpr (std::is_same_v<T, int64_t>) {
                filters_obj[key] = arg;
              } else if constexpr (std::is_same_v<T, double>) {
                filters_obj[key] = arg;
              } else if constexpr (std::is_same_v<T, std::string>) {
                filters_obj[key] = arg;
              } else if constexpr (std::is_same_v<T, int64_t>) {
                filters_obj[key] = arg;
              }
            }, val);
          }
          doc_obj["filters"] = filters_obj;
        }

        results_array.push_back(doc_obj);
      }
    }
    response["results"] = results_array;

    SendJson(res, 200, response);

  } catch (const std::exception& e) {
    SendError(res, 500, "Internal error: " + std::string(e.what()));
  }
}

void HttpServer::HandleGet(const httplib::Request& req, httplib::Response& res) {
  total_requests_++;

  try {
    // Extract table name and ID from URL
    std::string table = req.matches[1];
    std::string id_str = req.matches[2];

    // Parse ID
    uint64_t doc_id = 0;
    try {
      doc_id = std::stoull(id_str);
    } catch (const std::exception& e) {
      SendError(res, 400, "Invalid document ID");
      return;
    }

    // Get document
    auto doc = doc_store_.GetDocument(doc_id);
    if (!doc) {
      SendError(res, 404, "Document not found");
      return;
    }

    // Build JSON response
    json response;
    response["doc_id"] = doc->doc_id;
    response["primary_key"] = doc->primary_key;

    if (!doc->filters.empty()) {
      json filters_obj;
      for (const auto& [key, val] : doc->filters) {
        // Convert FilterValue to JSON
        std::visit([&](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, int64_t>) {
            filters_obj[key] = arg;
          } else if constexpr (std::is_same_v<T, double>) {
            filters_obj[key] = arg;
          } else if constexpr (std::is_same_v<T, std::string>) {
            filters_obj[key] = arg;
          }
        }, val);
      }
      response["filters"] = filters_obj;
    }

    SendJson(res, 200, response);

  } catch (const std::exception& e) {
    SendError(res, 500, "Internal error: " + std::string(e.what()));
  }
}

void HttpServer::HandleInfo(const httplib::Request& /*req*/, httplib::Response& res) {
  total_requests_++;

  try {
    json response;
    response["server"] = "MygramDB";
    response["version"] = "1.0.0";
    response["uptime_seconds"] = std::chrono::duration_cast<std::chrono::seconds>(
                                      std::chrono::system_clock::now().time_since_epoch())
                                      .count() -
                                  start_time_;
    response["total_requests"] = total_requests_.load();
    response["document_count"] = doc_store_.Size();
    response["ngram_size"] = ngram_size_;

    SendJson(res, 200, response);

  } catch (const std::exception& e) {
    SendError(res, 500, "Internal error: " + std::string(e.what()));
  }
}

void HttpServer::HandleHealth(const httplib::Request& /*req*/, httplib::Response& res) {
  total_requests_++;

  json response;
  response["status"] = "ok";
  response["timestamp"] = std::chrono::duration_cast<std::chrono::seconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();

  SendJson(res, 200, response);
}

void HttpServer::HandleConfig(const httplib::Request& /*req*/, httplib::Response& res) {
  total_requests_++;

  if (!full_config_) {
    SendError(res, 500, "Configuration not available");
    return;
  }

  try {
    json response;

    // MySQL config
    json mysql_obj;
    mysql_obj["host"] = full_config_->mysql.host;
    mysql_obj["port"] = full_config_->mysql.port;
    mysql_obj["database"] = full_config_->mysql.database;
    mysql_obj["user"] = full_config_->mysql.user;
    response["mysql"] = mysql_obj;

    // API config
    json api_obj;
    api_obj["tcp"]["bind"] = full_config_->api.tcp.bind;
    api_obj["tcp"]["port"] = full_config_->api.tcp.port;
    api_obj["http"]["enable"] = full_config_->api.http.enable;
    api_obj["http"]["bind"] = full_config_->api.http.bind;
    api_obj["http"]["port"] = full_config_->api.http.port;
    response["api"] = api_obj;

    // Replication config
    json repl_obj;
    repl_obj["enable"] = full_config_->replication.enable;
    repl_obj["server_id"] = full_config_->replication.server_id;
    response["replication"] = repl_obj;

    SendJson(res, 200, response);

  } catch (const std::exception& e) {
    SendError(res, 500, "Internal error: " + std::string(e.what()));
  }
}

void HttpServer::HandleReplicationStatus(const httplib::Request& /*req*/, httplib::Response& res) {
  total_requests_++;

#ifdef USE_MYSQL
  if (!binlog_reader_) {
    SendError(res, 503, "Replication not configured");
    return;
  }

  try {
    json response;
    response["enabled"] = binlog_reader_->IsRunning();
    response["current_gtid"] = binlog_reader_->GetCurrentGTID();

    SendJson(res, 200, response);

  } catch (const std::exception& e) {
    SendError(res, 500, "Internal error: " + std::string(e.what()));
  }
#else
  SendError(res, 503, "MySQL replication not compiled");
#endif
}

void HttpServer::SendJson(httplib::Response& res, int status_code, const nlohmann::json& body) {
  res.status = status_code;
  res.set_content(body.dump(), "application/json");
}

void HttpServer::SendError(httplib::Response& res, int status_code, const std::string& message) {
  json error_obj;
  error_obj["error"] = message;
  SendJson(res, status_code, error_obj);
}

}  // namespace server
}  // namespace mygramdb
