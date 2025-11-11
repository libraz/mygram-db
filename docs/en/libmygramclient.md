# libmygramclient - MygramDB Client Library

## Overview

libmygramclient is a C/C++ client library for connecting to and querying MygramDB servers. It provides both a modern C++ API and a C API suitable for bindings (e.g., node-gyp).

## Features

- Full support for all MygramDB protocol commands
- Modern C++17 API with RAII and type safety
- C API for easy integration with other languages
- Thread-safe connection management
- Both static and shared library builds
- Full Unicode support including 4-byte UTF-8 characters (emojis: 😀🎉👍)

## Building

The library is built automatically with MygramDB:

```bash
make
```

This creates:
- `build/lib/libmygramclient.a` - Static library
- `build/lib/libmygramclient.dylib` (macOS) or `libmygramclient.so` (Linux) - Shared library

## Installation

```bash
sudo make install
```

This installs:
- Headers to `/usr/local/include/mygramdb/`
- Libraries to `/usr/local/lib/`

## C++ API

### Basic Usage

```cpp
#include <mygramdb/mygramclient.h>
#include <iostream>

using namespace mygramdb::client;

int main() {
    // Configure client
    ClientConfig config;
    config.host = "localhost";
    config.port = 11211;
    config.timeout_ms = 5000;

    // Create client
    MygramClient client(config);

    // Connect
    if (auto err = client.Connect()) {
        std::cerr << "Connection failed: " << *err << std::endl;
        return 1;
    }

    // Search
    auto result = client.Search("articles", "hello world", 100);
    if (auto* err = std::get_if<Error>(&result)) {
        std::cerr << "Search failed: " << err->message << std::endl;
        return 1;
    }

    auto resp = std::get<SearchResponse>(result);
    std::cout << "Found " << resp.total_count << " results\n";
    for (const auto& doc : resp.results) {
        std::cout << "  - " << doc.primary_key << "\n";
    }

    return 0;
}
```

### Compiling with libmygramclient

```bash
g++ -std=c++17 -o myapp myapp.cpp -lmygramclient
```

### Advanced Search

```cpp
// Search with AND, NOT, and FILTER
std::vector<std::string> and_terms = {"AI"};
std::vector<std::string> not_terms = {"old"};
std::vector<std::pair<std::string, std::string>> filters = {
    {"status", "active"},
    {"category", "tech"}
};

auto result = client.Search(
    "articles",           // table
    "technology",         // query
    50,                   // limit
    0,                    // offset
    and_terms,            // AND terms
    not_terms,            // NOT terms
    filters,              // filters
    "created_at",         // order by column
    true                  // descending
);
```

### Count Query

```cpp
auto result = client.Count("articles", "hello");
if (auto* resp = std::get_if<CountResponse>(&result)) {
    std::cout << "Total matches: " << resp->count << "\n";
}
```

### Get Document

```cpp
auto result = client.Get("articles", "12345");
if (auto* doc = std::get_if<Document>(&result)) {
    std::cout << "Primary key: " << doc->primary_key << "\n";
    for (const auto& [key, value] : doc->fields) {
        std::cout << "  " << key << " = " << value << "\n";
    }
}
```

### Server Info

```cpp
auto result = client.Info();
if (auto* info = std::get_if<ServerInfo>(&result)) {
    std::cout << "Version: " << info->version << "\n";
    std::cout << "Documents: " << info->doc_count << "\n";
    std::cout << "Uptime: " << info->uptime_seconds << "s\n";
}
```

### Debug Mode

```cpp
// Enable debug mode
client.EnableDebug();

auto result = client.Search("articles", "hello", 10);
if (auto* resp = std::get_if<SearchResponse>(&result)) {
    if (resp->debug) {
        std::cout << "Query time: " << resp->debug->query_time_ms << "ms\n";
        std::cout << "Candidates: " << resp->debug->candidates << "\n";
    }
}

// Disable debug mode
client.DisableDebug();
```

### Search Expression Parser

The library includes a web-style search expression parser that converts Google-like search syntax into MygramDB query format.

#### Syntax

- `term1 term2` - Multiple terms with implicit AND (both must appear)
- `"phrase"` - Quoted phrase (exact match with spaces)
- `+term` - Explicitly required term (same as non-prefixed)
- `-term` - Excluded term (must NOT appear in results)
- `term1 OR term2` - Logical OR between terms
- `(expr)` - Grouping with parentheses
- Full-width space (`　`) is supported as a delimiter (useful for Japanese text)

#### Examples

```cpp
#include <mygramdb/search_expression.h>

using namespace mygramdb::client;

// Implicit AND (space-separated)
auto result = ParseSearchExpression("golang tutorial");
auto expr = std::get<SearchExpression>(result);
// expr.required_terms = ["golang", "tutorial"]

// Quoted phrase search
auto result = ParseSearchExpression("\"machine learning\" tutorial");
auto expr = std::get<SearchExpression>(result);
// expr.required_terms = ["\"machine learning\"", "tutorial"]

// Excluded terms
auto result = ParseSearchExpression("golang -old -deprecated");
auto expr = std::get<SearchExpression>(result);
// expr.required_terms = ["golang"]
// expr.excluded_terms = ["old", "deprecated"]

// OR expression
auto result = ParseSearchExpression("python OR ruby");
auto expr = std::get<SearchExpression>(result);
// expr.raw_expression = "python OR ruby"

// Complex expression with quotes
auto result = ParseSearchExpression("\"deep learning\" +(tutorial OR guide) -old");
auto expr = std::get<SearchExpression>(result);
// expr.required_terms = ["\"deep learning\"", "(tutorial OR guide)"]
// expr.excluded_terms = ["old"]

// Full-width space (Japanese)
auto result = ParseSearchExpression("機械学習　チュートリアル");
auto expr = std::get<SearchExpression>(result);
// expr.required_terms = ["機械学習", "チュートリアル"]

// Emoji search (4-byte UTF-8 characters)
auto result = ParseSearchExpression("😀 tutorial -😢");
auto expr = std::get<SearchExpression>(result);
// expr.required_terms = ["😀", "tutorial"]
// expr.excluded_terms = ["😢"]
```

#### Converting to Query String

```cpp
// Convert directly to QueryAST-compatible string
auto result = ConvertSearchExpression("+golang -old");
if (result.index() == 0) {
    std::string query = std::get<0>(result);
    // query = "golang AND NOT old"

    // Use with MygramClient
    auto search_result = client.Search("articles", query, 100);
}
```

#### Expression Examples

| Input | Output Query | Description |
|-------|-------------|-------------|
| `golang tutorial` | `golang AND tutorial` | Implicit AND - both terms required |
| `"machine learning"` | `"machine learning"` | Exact phrase search |
| `golang -old` | `golang AND NOT old` | Must have "golang", must not have "old" |
| `python OR ruby` | `(python OR ruby)` | Either "python" or "ruby" |
| `"deep learning" tutorial` | `"deep learning" AND tutorial` | Phrase and term |
| `golang +(tutorial OR guide)` | `golang AND (tutorial OR guide)` | "golang" AND either "tutorial" or "guide" |
| `AI machine -learning` | `AI AND machine AND NOT learning` | Must have "AI" and "machine", exclude "learning" |
| `機械学習　チュートリアル` | `機械学習 AND チュートリアル` | Full-width space delimiter |
| `😀 tutorial -😢` | `😀 AND tutorial AND NOT 😢` | Emoji search (4-byte UTF-8) |

#### Simplified API (Backward Compatible)

For simple use cases without OR/grouping:

```cpp
std::string main_term;
std::vector<std::string> and_terms;
std::vector<std::string> not_terms;

bool success = SimplifySearchExpression(
    "+golang +tutorial -old",
    main_term,
    and_terms,
    not_terms
);

if (success) {
    // main_term = "golang"
    // and_terms = ["tutorial"]
    // not_terms = ["old"]
}
```

**Note:** Complex expressions with OR and parentheses will lose their semantic meaning when using `SimplifySearchExpression()`.

## C API

### Basic Usage

```c
#include <mygramdb/mygramclient_c.h>
#include <stdio.h>

int main() {
    // Configure client
    MygramClientConfig_C config = {
        .host = "localhost",
        .port = 11211,
        .timeout_ms = 5000,
        .recv_buffer_size = 65536
    };

    // Create client
    MygramClient_C* client = mygramclient_create(&config);
    if (!client) {
        fprintf(stderr, "Failed to create client\n");
        return 1;
    }

    // Connect
    if (mygramclient_connect(client) != 0) {
        fprintf(stderr, "Connection failed: %s\n",
                mygramclient_get_last_error(client));
        mygramclient_destroy(client);
        return 1;
    }

    // Search
    MygramSearchResult_C* result = NULL;
    if (mygramclient_search(client, "articles", "hello", 100, 0, &result) == 0) {
        printf("Found %llu results (showing %zu):\n",
               result->total_count, result->count);
        for (size_t i = 0; i < result->count; i++) {
            printf("  - %s\n", result->primary_keys[i]);
        }
        mygramclient_free_search_result(result);
    } else {
        fprintf(stderr, "Search failed: %s\n",
                mygramclient_get_last_error(client));
    }

    // Cleanup
    mygramclient_disconnect(client);
    mygramclient_destroy(client);

    return 0;
}
```

### Compiling C Programs

```bash
gcc -o myapp myapp.c -lmygramclient
```

### Advanced Search (C API)

```c
const char* and_terms[] = {"AI"};
const char* not_terms[] = {"old"};
const char* filter_keys[] = {"status", "category"};
const char* filter_values[] = {"active", "tech"};

MygramSearchResult_C* result = NULL;
int ret = mygramclient_search_advanced(
    client,
    "articles",           // table
    "technology",         // query
    50,                   // limit
    0,                    // offset
    and_terms, 1,         // AND terms
    not_terms, 1,         // NOT terms
    filter_keys, filter_values, 2,  // filters
    "created_at",         // order by
    1,                    // descending
    &result
);

if (ret == 0) {
    // Process results
    mygramclient_free_search_result(result);
}
```

## Node.js Bindings Example

Using node-gyp with the C API:

```javascript
// binding.gyp
{
  "targets": [{
    "target_name": "mygramdb",
    "sources": [ "src/mygramdb_node.cpp" ],
    "include_dirs": [
      "/usr/local/include",
      "<!(node -p \"require('node-addon-api').include_dir\")"
    ],
    "libraries": [
      "-L/usr/local/lib",
      "-lmygramclient"
    ],
    "cflags!": [ "-fno-exceptions" ],
    "cflags_cc!": [ "-fno-exceptions" ],
    "defines": [ "NAPI_DISABLE_CPP_EXCEPTIONS" ]
  }]
}
```

```cpp
// src/mygramdb_node.cpp (simplified example)
#include <napi.h>
#include <mygramdb/mygramclient_c.h>

Napi::Value Search(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    // Get parameters
    std::string table = info[0].As<Napi::String>();
    std::string query = info[1].As<Napi::String>();
    uint32_t limit = info[2].As<Napi::Number>().Uint32Value();

    // Create and connect client
    MygramClientConfig_C config = {
        .host = "localhost",
        .port = 11211,
        .timeout_ms = 5000,
        .recv_buffer_size = 65536
    };

    MygramClient_C* client = mygramclient_create(&config);
    if (mygramclient_connect(client) != 0) {
        Napi::Error::New(env, mygramclient_get_last_error(client)).ThrowAsJavaScriptException();
        mygramclient_destroy(client);
        return env.Null();
    }

    // Search
    MygramSearchResult_C* result = NULL;
    if (mygramclient_search(client, table.c_str(), query.c_str(), limit, 0, &result) != 0) {
        Napi::Error::New(env, mygramclient_get_last_error(client)).ThrowAsJavaScriptException();
        mygramclient_destroy(client);
        return env.Null();
    }

    // Convert to JavaScript array
    Napi::Array jsResults = Napi::Array::New(env, result->count);
    for (size_t i = 0; i < result->count; i++) {
        jsResults[i] = Napi::String::New(env, result->primary_keys[i]);
    }

    // Cleanup
    mygramclient_free_search_result(result);
    mygramclient_disconnect(client);
    mygramclient_destroy(client);

    return jsResults;
}

Napi::Object Init(Napi::Env env, Napi::Object exports) {
    exports.Set("search", Napi::Function::New(env, Search));
    return exports;
}

NODE_API_MODULE(mygramdb, Init)
```

## API Reference

### C++ API Classes

#### ClientConfig
- `host` - Server hostname (default: "127.0.0.1")
- `port` - Server port (default: 11211)
- `timeout_ms` - Connection timeout (default: 5000)
- `recv_buffer_size` - Receive buffer size (default: 65536)

#### SearchResponse
- `results` - Vector of SearchResult
- `total_count` - Total matching documents
- `debug` - Optional debug information

#### Error
- `message` - Error message string

### C API Functions

See `mygramclient_c.h` for full function documentation.

Key functions:
- `mygramclient_create()` - Create client
- `mygramclient_connect()` - Connect to server
- `mygramclient_search()` - Simple search
- `mygramclient_search_advanced()` - Advanced search with filters
- `mygramclient_count()` - Count matches
- `mygramclient_get()` - Get document by key
- `mygramclient_free_*()` - Free result structures

## Thread Safety

The MygramClient class manages a single TCP connection and is **not thread-safe**. For multi-threaded applications, create one client instance per thread or use proper synchronization.

## Error Handling

### C++ API

Functions return `std::variant<T, Error>` where:
- Success: Contains the result type T
- Failure: Contains an Error with a message

Use `std::get_if<Error>` to check for errors:

```cpp
auto result = client.Search(...);
if (auto* err = std::get_if<Error>(&result)) {
    // Handle error: err->message
} else {
    auto resp = std::get<SearchResponse>(result);
    // Use resp
}
```

### C API

Functions return 0 on success, -1 on error. Use `mygramclient_get_last_error()` to retrieve the error message.

## License

MIT License (see LICENSE file)
