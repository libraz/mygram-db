/**
 * @file mygram-cli.cpp
 * @brief Command-line client for MygramDB (redis-cli style)
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

// Try to use readline if available
#ifdef HAVE_READLINE
#include <readline/readline.h>
#include <readline/history.h>
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define USE_READLINE 1
#endif

namespace {

#ifdef USE_READLINE
// Command list for tab completion
// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-avoid-non-const-global-variables)
const char* command_list[] = {
  "SEARCH", "COUNT", "GET", "INFO", "SAVE", "LOAD", "CONFIG",
  "REPLICATION", "DEBUG", "quit", "exit", "help", nullptr
};

/**
 * @brief Command name generator for readline completion
 * @param text Partial text to complete
 * @param state 0 for first call, non-zero for subsequent calls
 * @return Completed command name or nullptr
 */
char* CommandGenerator(const char* text, int state) {
  static int list_index;
  static int len;
  const char* name = nullptr;

  if (state == 0) {
    list_index = 0;
    len = static_cast<int>(strlen(text));
  }

  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
  while ((name = command_list[list_index++]) != nullptr) {
    if (strncasecmp(name, text, len) == 0) {
      // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
      return strdup(name);
    }
  }

  return nullptr;
}

/**
 * @brief Parse the input line and extract tokens
 * @param line Input line to parse
 * @return Vector of tokens
 */
std::vector<std::string> ParseTokens(const std::string& line) {
  std::vector<std::string> tokens;
  std::istringstream iss(line);
  std::string token;
  while (iss >> token) {
    tokens.push_back(token);
  }
  return tokens;
}

/**
 * @brief Generator for keyword completions
 * @param keywords List of keywords to complete from
 * @param text Partial text to complete
 * @param state 0 for first call, non-zero for subsequent calls
 * @return Completed keyword or nullptr
 */
char* KeywordGenerator(const std::vector<std::string>& keywords, const char* text, int state) {
  static size_t list_index;
  static int len;

  if (state == 0) {
    list_index = 0;
    len = static_cast<int>(strlen(text));
  }

  while (list_index < keywords.size()) {
    const std::string& name = keywords[list_index++];
    if (len == 0 || strncasecmp(name.c_str(), text, len) == 0) {
      // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
      return strdup(name.c_str());
    }
  }

  return nullptr;
}

/**
 * @brief Wrapper for keyword generator with static storage
 */
static std::vector<std::string> current_keywords;

/**
 * @brief Global storage for table names fetched from server
 */
static std::vector<std::string> available_tables;

char* KeywordGeneratorWrapper(const char* text, int state) {
  return KeywordGenerator(current_keywords, text, state);
}

/**
 * @brief Completion function for readline
 * @param text Text to complete
 * @param start Start position in line buffer
 * @param end End position in line buffer (unused)
 * @return Array of possible completions
 */
char** CommandCompletion(const char* text, int start, int /* end */) {
  // Disable default filename completion
  rl_attempted_completion_over = 1;

  // Get the full line buffer up to the cursor
  std::string line(rl_line_buffer, start);
  std::vector<std::string> tokens = ParseTokens(line);

  // First word: complete command name
  if (tokens.empty()) {
    return rl_completion_matches(text, CommandGenerator);
  }

  std::string command = tokens[0];
  // Convert to uppercase for comparison
  for (char& c : command) {
    c = static_cast<char>(toupper(c));
  }

  size_t token_count = tokens.size();

  // Helper: find if a keyword exists in the token list
  auto has_keyword = [&tokens](const std::string& keyword) {
    for (const auto& token : tokens) {
      std::string upper_token = token;
      for (char& c : upper_token) {
        c = static_cast<char>(toupper(c));
      }
      if (upper_token == keyword) {
        return true;
      }
    }
    return false;
  };

  // Helper: get the previous token (uppercase)
  auto get_prev_token = [&tokens]() -> std::string {
    if (tokens.size() >= 2) {
      std::string prev = tokens[tokens.size() - 1];
      for (char& c : prev) {
        c = static_cast<char>(toupper(c));
      }
      return prev;
    }
    return "";
  };

  // SEARCH <table> <text> [AND/OR/NOT <term>] [FILTER <col=val>] [ORDER [BY] [ASC|DESC]] [LIMIT <n>] [OFFSET <n>]
  if (command == "SEARCH") {
    std::string prev = get_prev_token();

    // Special handling for ORDER BY clause
    if (prev == "ORDER") {
      // After ORDER: suggest BY, ASC, DESC (shorthand)
      current_keywords = {"BY", "ASC", "DESC"};
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    if (prev == "BY" && has_keyword("ORDER")) {
      // After ORDER BY: suggest ASC, DESC, or <column_name>
      current_keywords = {"ASC", "DESC", "<column_name>"};
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }

    if (token_count == 1) {
      // After SEARCH: suggest table names from server
      if (!available_tables.empty()) {
        current_keywords = available_tables;
      } else {
        current_keywords = {"<table_name>"};
      }
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    if (token_count == 2) {
      // After table name: suggest search text hint
      current_keywords = {"<search_text>"};
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    // After search text: suggest optional keywords
    current_keywords = {"AND", "OR", "NOT", "FILTER", "ORDER", "LIMIT", "OFFSET"};
    return rl_completion_matches(text, KeywordGeneratorWrapper);
  }

  // COUNT <table> <text> [NOT <term>] [FILTER <col=val>]
  if (command == "COUNT") {
    if (token_count == 1) {
      // After COUNT: suggest table names from server
      if (!available_tables.empty()) {
        current_keywords = available_tables;
      } else {
        current_keywords = {"<table_name>"};
      }
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    if (token_count == 2) {
      current_keywords = {"<search_text>"};
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    current_keywords = {"NOT", "FILTER"};
    return rl_completion_matches(text, KeywordGeneratorWrapper);
  }

  // GET <table> <primary_key>
  if (command == "GET") {
    if (token_count == 1) {
      // After GET: suggest table names from server
      if (!available_tables.empty()) {
        current_keywords = available_tables;
      } else {
        current_keywords = {"<table_name>"};
      }
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    if (token_count == 2) {
      current_keywords = {"<primary_key>"};
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    return nullptr;
  }

  // SAVE [filename]
  if (command == "SAVE") {
    if (token_count == 1) {
      // Enable filename completion for SAVE
      rl_attempted_completion_over = 0;
      return nullptr;
    }
    return nullptr;
  }

  // LOAD <filename>
  if (command == "LOAD") {
    if (token_count == 1) {
      // Enable filename completion for LOAD
      rl_attempted_completion_over = 0;
      return nullptr;
    }
    return nullptr;
  }

  // REPLICATION STATUS|STOP|START
  if (command == "REPLICATION") {
    if (token_count == 1) {
      current_keywords = {"STATUS", "STOP", "START"};
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    return nullptr;
  }

  // DEBUG ON|OFF
  if (command == "DEBUG") {
    if (token_count == 1) {
      current_keywords = {"ON", "OFF"};
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    return nullptr;
  }

  // INFO, CONFIG: no arguments
  if (command == "INFO" || command == "CONFIG") {
    return nullptr;
  }

  return nullptr;
}
#endif

struct Config {
  std::string host = "127.0.0.1";
  uint16_t port = 11211;
  bool interactive = true;
  int retry_count = 0;       // Number of retries (0 = no retry)
  int retry_interval = 3;    // Seconds between retries
};

class MygramClient {
 public:
  MygramClient(Config config) : config_(std::move(config)) {}

  // Non-copyable (manages socket file descriptor)
  MygramClient(const MygramClient&) = delete;
  MygramClient& operator=(const MygramClient&) = delete;

  // Movable (default)
  MygramClient(MygramClient&&) = default;
  MygramClient& operator=(MygramClient&&) = default;

  ~MygramClient() {
    Disconnect();
  }

  bool Connect() {
    int attempts = 0;
    int max_attempts = 1 + config_.retry_count;

    while (attempts < max_attempts) {
      if (attempts > 0) {
        std::cerr << "\nRetrying in " << config_.retry_interval << " seconds... (attempt "
                  << (attempts + 1) << "/" << max_attempts << ")\n";
        sleep(config_.retry_interval);
      }

      sock_ = socket(AF_INET, SOCK_STREAM, 0);
      if (sock_ < 0) {
        std::cerr << "Failed to create socket: " << strerror(errno) << '\n';
        attempts++;
        continue;
      }

      struct sockaddr_in server_addr{};
      server_addr.sin_family = AF_INET;
      server_addr.sin_port = htons(config_.port);

      if (inet_pton(AF_INET, config_.host.c_str(), &server_addr.sin_addr) <= 0) {
        std::cerr << "Invalid address: " << config_.host << '\n';
        close(sock_);
        sock_ = -1;
        return false;  // Don't retry invalid address
      }

      if (connect(sock_, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        int saved_errno = errno;
        std::cerr << "Connection failed: " << strerror(saved_errno) << '\n';

        // Provide helpful hints based on error type
        if (saved_errno == ECONNREFUSED) {
          std::cerr << "\nPossible reasons:\n";
          std::cerr << "  1. MygramDB server is not running\n";
          std::cerr << "  2. Server is still initializing (building initial index from MySQL)\n";
          std::cerr << "  3. Wrong port (check config.yaml - default is 11211)\n";
          std::cerr << "\nTo check server status:\n";
          std::cerr << "  ps aux | grep mygramdb\n";
          std::cerr << "  lsof -i -P | grep LISTEN | grep " << config_.port << "\n";
          std::cerr << "\nFor large datasets, initial index build may take 10-30 minutes.\n";
          std::cerr << "Server will start accepting connections after initialization completes.\n";
        } else if (saved_errno == ETIMEDOUT) {
          std::cerr << "\nServer is not responding. Check if the server is running and network is accessible.\n";
        } else if (saved_errno == ENETUNREACH || saved_errno == EHOSTUNREACH) {
          std::cerr << "\nNetwork is unreachable. Check hostname and network connectivity.\n";
        }

        close(sock_);
        sock_ = -1;

        // Only retry on ECONNREFUSED (server not ready yet)
        if (saved_errno != ECONNREFUSED) {
          return false;
        }

        attempts++;
        continue;
      }

      // Connected successfully
      if (attempts > 0) {
        std::cerr << "\nConnected successfully after " << attempts << " retry(ies)!\n\n";
      }
      return true;
    }

    return false;
  }

  void Disconnect() {
    if (sock_ >= 0) {
      close(sock_);
      sock_ = -1;
    }
  }

  [[nodiscard]] bool IsConnected() const { return sock_ >= 0; }

#ifdef USE_READLINE
  /**
   * @brief Fetch table names from server INFO command
   * Updates global available_tables vector
   */
  void FetchTableNames() {
    if (!IsConnected()) {
      return;
    }

    std::string response = SendCommand("INFO");

    // Parse response to extract table names
    // Look for line: "tables: table1,table2,table3"
    size_t pos = response.find("tables: ");
    if (pos != std::string::npos) {
      pos += 8;  // Skip "tables: "
      size_t end_pos = response.find("\r\n", pos);
      if (end_pos == std::string::npos) {
        end_pos = response.find("\n", pos);
      }
      if (end_pos != std::string::npos) {
        std::string tables_str = response.substr(pos, end_pos - pos);

        // Split by comma
        available_tables.clear();
        std::istringstream iss(tables_str);
        std::string table;
        while (std::getline(iss, table, ',')) {
          // Trim whitespace
          table.erase(0, table.find_first_not_of(" \t\r\n"));
          table.erase(table.find_last_not_of(" \t\r\n") + 1);
          if (!table.empty()) {
            available_tables.push_back(table);
          }
        }
      }
    }
  }
#endif

  [[nodiscard]] std::string SendCommand(const std::string& command) const {
    if (!IsConnected()) {
      return "(error) Not connected";
    }

    // Send command with \r\n
    std::string msg = command + "\r\n";
    ssize_t sent = send(sock_, msg.c_str(), msg.length(), 0);
    if (sent < 0) {
      return "(error) Failed to send command: " + std::string(strerror(errno));
    }

    // Receive response
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    char buffer[65536];
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    ssize_t received = recv(sock_, buffer, sizeof(buffer) - 1, 0);
    if (received <= 0) {
      if (received == 0) {
        return "(error) Connection closed by server";
      }
      return "(error) Failed to receive response: " + std::string(strerror(errno));
    }

    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
    buffer[received] = '\0';
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    std::string response(buffer);

    // Remove trailing \r\n
    if (response.size() >= 2 &&
        response[response.size()-2] == '\r' &&
        response[response.size()-1] == '\n') {
      response = response.substr(0, response.size() - 2);
    }

    return response;
  }

  void RunInteractive() {
    std::cout << "mygram-cli " << config_.host << ":" << config_.port << '\n';
#ifdef USE_READLINE
    std::cout << "Type 'quit' or 'exit' to exit, 'help' for help" << '\n';
    std::cout << "Use TAB for context-aware command completion" << '\n';
#else
    std::cout << "Type 'quit' or 'exit' to exit, 'help' for help" << std::endl;
#endif
    std::cout << '\n';

#ifdef USE_READLINE
    // Fetch table names from server for tab completion
    FetchTableNames();

    // Setup readline completion
    rl_attempted_completion_function = CommandCompletion;
#endif

    while (true) {
      // Read command
      std::string line;

#ifdef USE_READLINE
      // Use readline for better line editing and history
      std::string prompt = config_.host + ":" + std::to_string(config_.port) + "> ";
      char* input = readline(prompt.c_str());

      if (input == nullptr) {
        // EOF (Ctrl-D)
        std::cout << '\n';
        break;
      }

      line = input;

      // Trim whitespace
      line.erase(0, line.find_first_not_of(" \t\r\n"));
      line.erase(line.find_last_not_of(" \t\r\n") + 1);

      // Add to history if non-empty
      if (!line.empty()) {
        add_history(input);
      }

      // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
      free(input);
#else
      // Fallback to std::getline
      std::cout << config_.host << ":" << config_.port << "> ";
      std::cout.flush();

      if (!std::getline(std::cin, line)) {
        break;  // EOF
      }

      // Trim whitespace
      line.erase(0, line.find_first_not_of(" \t\r\n"));
      line.erase(line.find_last_not_of(" \t\r\n") + 1);
#endif

      if (line.empty()) {
        continue;
      }

      // Check for exit commands
      if (line == "quit" || line == "exit") {
        std::cout << "Bye!" << '\n';
        break;
      }

      // Check for help
      if (line == "help") {
        PrintHelp();
        continue;
      }

      // Send command and print response
      std::string response = SendCommand(line);
      PrintResponse(response);
    }
  }

  void RunSingleCommand(const std::string& command) const {
    std::string response = SendCommand(command);
    PrintResponse(response);
  }

 private:
  static void PrintHelp() {
    std::cout << "Available commands:" << '\n';
    std::cout << "  SEARCH <table> <text> [(AND|OR|NOT) <term>...] [FILTER <col=val>...]" << '\n';
    std::cout << "         [ORDER [BY] <col>|ASC|DESC] [LIMIT <n>] [OFFSET <n>]" << '\n';
    std::cout << "  COUNT <table> <text> [(AND|OR|NOT) <term>...] [FILTER <col=val>...]" << '\n';
    std::cout << "  GET <table> <primary_key>" << '\n';
    std::cout << "  INFO - Show server statistics" << '\n';
    std::cout << "  CONFIG - Show current configuration" << '\n';
    std::cout << "  SAVE [filename] - Save snapshot to disk" << '\n';
    std::cout << "  LOAD <filename> - Load snapshot from disk" << '\n';
    std::cout << "  REPLICATION STATUS - Show replication status" << '\n';
    std::cout << "  REPLICATION STOP - Stop replication" << '\n';
    std::cout << "  REPLICATION START - Start replication" << '\n';
    std::cout << "  DEBUG ON - Enable debug mode (shows query execution details)" << '\n';
    std::cout << "  DEBUG OFF - Disable debug mode" << '\n';
    std::cout << '\n';
    std::cout << "Query syntax examples:" << '\n';
    std::cout << "  SEARCH threads golang                          # Simple search" << '\n';
    std::cout << "  SEARCH threads (golang OR python) AND tutorial # Boolean query" << '\n';
    std::cout << "  SEARCH threads golang ORDER DESC LIMIT 10      # With sorting" << '\n';
    std::cout << "  SEARCH threads golang ORDER BY created_at ASC  # Sort by column" << '\n';
    std::cout << '\n';
    std::cout << "Other commands:" << '\n';
    std::cout << "  quit/exit - Exit the client" << '\n';
    std::cout << "  help - Show this help" << '\n';
  }

  // NOLINTNEXTLINE(readability-function-cognitive-complexity)
  static void PrintResponse(const std::string& response) {
    // Parse response type
    if (response.find("OK RESULTS") == 0) {
      // SEARCH response: OK RESULTS <count> [<id1> <id2> ...] [DEBUG ...]
      std::istringstream iss(response);
      std::string status;
      std::string results;
      size_t count = 0;
      iss >> status >> results >> count;

      std::vector<std::string> ids;
      std::string token;
      std::string debug_info;

      while (iss >> token) {
        if (token == "DEBUG") {
          // Read rest of stream as debug info
          std::string rest;
          std::getline(iss, rest);
          debug_info = rest;
          break;
        }
        ids.push_back(token);
      }

      std::cout << "(" << count << " results";
      if (!ids.empty()) {
        std::cout << ", showing " << ids.size() << ")";
      } else {
        std::cout << ")";
      }
      std::cout << '\n';

      for (size_t i = 0; i < ids.size(); ++i) {
        std::cout << (i + 1) << ") " << ids[i] << '\n';
      }

      // Print debug info if present
      if (!debug_info.empty()) {
        std::cout << "\n[DEBUG INFO]" << debug_info << '\n';
      }
    } else if (response.find("OK COUNT") == 0) {
      // COUNT response: OK COUNT <n> [DEBUG ...]
      std::istringstream iss(response);
      std::string status;
      std::string count_str;
      uint64_t count = 0;
      iss >> status >> count_str >> count;

      std::cout << "(integer) " << count << '\n';

      // Check for debug info
      std::string token;
      if (iss >> token && token == "DEBUG") {
        std::string rest;
        std::getline(iss, rest);
        std::cout << "\n[DEBUG INFO]" << rest << '\n';
      }
    } else if (response.find("OK DEBUG_ON") == 0) {
      std::cout << "Debug mode enabled" << '\n';
    } else if (response.find("OK DEBUG_OFF") == 0) {
      std::cout << "Debug mode disabled" << '\n';
    } else if (response.find("OK DOC") == 0) {
      // GET response: OK DOC <primary_key> [<filter=value>...]
      std::cout << response.substr(3) << '\n';  // Remove "OK "
    } else if (response.find("OK INFO") == 0) {
      // INFO response: OK INFO\r\n<key>: <value>\r\n...\r\nEND
      // Simply print the formatted response (already has nice formatting from server)
      std::string info = response.substr(8);  // Remove "OK INFO\r\n"

      // Replace \r\n with actual newlines for display
      size_t pos = 0;
      while ((pos = info.find("\\r\\n", pos)) != std::string::npos) {
        info.replace(pos, 4, "\n");
        pos += 1;
      }

      // Handle actual \r\n sequences
      pos = 0;
      while ((pos = info.find("\r\n", pos)) != std::string::npos) {
        info.replace(pos, 2, "\n");
        pos += 1;
      }

      std::cout << info << '\n';
    } else if (response.find("OK SAVED") == 0) {
      // SAVE response: OK SAVED <filepath>
      std::string filepath = response.substr(9);  // Remove "OK SAVED "
      std::cout << "Snapshot saved to: " << filepath << '\n';
    } else if (response.find("OK LOADED") == 0) {
      // LOAD response: OK LOADED <filepath>
      std::string filepath = response.substr(10);  // Remove "OK LOADED "
      std::cout << "Snapshot loaded from: " << filepath << '\n';
    } else if (response.find("OK REPLICATION_STOPPED") == 0) {
      std::cout << "Replication stopped successfully" << '\n';
    } else if (response.find("OK REPLICATION_STARTED") == 0) {
      std::cout << "Replication started successfully" << '\n';
    } else if (response.find("OK REPLICATION") == 0) {
      // REPLICATION STATUS response: OK REPLICATION\r\n<key>: <value>\r\n...END
      std::string info = response.substr(15);  // Remove "OK REPLICATION\r\n"

      // Replace \\r\\n with actual newlines for display
      size_t pos = 0;
      while ((pos = info.find(R"(\\r\\n)", pos)) != std::string::npos) {
        info.replace(pos, 4, "\\n");
        pos += 1;
      }

      // Handle actual \\r\\n sequences
      pos = 0;
      while ((pos = info.find("\\r\\n", pos)) != std::string::npos) {
        info.replace(pos, 2, "\\n");
        pos += 1;
      }

      std::cout << info << '\n';
    } else if (response.find("ERROR") == 0) {
      // Error response
      std::cout << "(error) " << response.substr(6) << '\n';  // Remove "ERROR "
    } else {
      // Unknown response
      std::cout << response << '\n';
    }
  }

  Config config_;
  int sock_{-1};
};

void PrintUsage(const char* program_name) {
  std::cout << "Usage: " << program_name << " [OPTIONS] [COMMAND]" << '\n';
  std::cout << '\n';
  std::cout << "Options:" << '\n';
  std::cout << "  -h HOST         Server hostname (default: 127.0.0.1)" << '\n';
  std::cout << "  -p PORT         Server port (default: 11211)" << '\n';
  std::cout << "  --retry N       Retry connection N times if refused (default: 0)" << '\n';
  std::cout << "  --wait-ready    Keep retrying until server is ready (max 100 attempts)" << '\n';
  std::cout << "  --help          Show this help" << '\n';
  std::cout << '\n';
  std::cout << "Examples:" << '\n';
  std::cout << "  " << program_name << "                          # Interactive mode" << '\n';
  std::cout << "  " << program_name << " -h localhost -p 11211    # Connect to specific server"
            << '\n';
  std::cout << "  " << program_name << " --retry 5 INFO           # Retry 5 times if server not ready" << '\n';
  std::cout << "  " << program_name << " --wait-ready INFO        # Wait until server is ready" << '\n';
  std::cout << "  " << program_name << " SEARCH articles hello    # Execute single command" << '\n';
}

}  // namespace

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
int main(int argc, char* argv[]) {
  Config config;
  std::vector<std::string> command_args;

  // Parse arguments
  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);

    if (arg == "--help") {
      PrintUsage(argv[0]);
      return 0;
    }
    if (arg == "-h") {
      if (i + 1 < argc) {
        config.host = argv[++i];
      } else {
        std::cerr << "Error: -h requires an argument" << '\n';
        return 1;
      }
    } else if (arg == "-p") {
      if (i + 1 < argc) {
        config.port = static_cast<uint16_t>(std::stoi(argv[++i]));
      } else {
        std::cerr << "Error: -p requires an argument" << '\n';
        return 1;
      }
    } else if (arg == "--retry") {
      if (i + 1 < argc) {
        config.retry_count = std::stoi(argv[++i]);
        if (config.retry_count < 0) {
          std::cerr << "Error: --retry value must be non-negative" << '\n';
          return 1;
        }
      } else {
        std::cerr << "Error: --retry requires an argument" << '\n';
        return 1;
      }
    } else if (arg == "--wait-ready") {
      config.retry_count = 100;  // Max 100 retries = ~5 minutes
    } else {
      // Assume remaining args are a command
      for (int j = i; j < argc; ++j) {
        command_args.emplace_back(argv[j]);
      }
      config.interactive = false;
      break;
    }
  }

  // Create client and connect
  MygramClient client(config);
  if (!client.Connect()) {
    return 1;
  }

  // Run interactive or single command mode
  if (config.interactive) {
    client.RunInteractive();
  } else {
    // Build command from args
    std::ostringstream command;
    for (size_t i = 0; i < command_args.size(); ++i) {
      if (i > 0) {
        command << " ";
      }
      command << command_args[i];
    }
    client.RunSingleCommand(command.str());
  }

  return 0;
}
