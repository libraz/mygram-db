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
  "REPLICATION", "quit", "exit", "help", nullptr
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
 * @brief Completion function for readline
 * @param text Text to complete
 * @param start Start position in line buffer
 * @param end End position in line buffer (unused)
 * @return Array of possible completions
 */
char** CommandCompletion(const char* text, int start, int /* end */) {
  // Only complete first word (command name)
  if (start == 0) {
    return rl_completion_matches(text, CommandGenerator);
  }
  return nullptr;
}
#endif

struct Config {
  std::string host = "127.0.0.1";
  uint16_t port = 11211;
  bool interactive = true;
};

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class MygramClient {
 public:
  MygramClient(Config config) : config_(std::move(config)) {}

  ~MygramClient() {
    Disconnect();
  }

  bool Connect() {
    sock_ = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_ < 0) {
      std::cerr << "Failed to create socket: " << strerror(errno) << '\n';
      return false;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(config_.port);

    if (inet_pton(AF_INET, config_.host.c_str(), &server_addr.sin_addr) <= 0) {
      std::cerr << "Invalid address: " << config_.host << '\n';
      close(sock_);
      sock_ = -1;
      return false;
    }

    if (connect(sock_, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
      std::cerr << "Connection failed: " << strerror(errno) << '\n';
      close(sock_);
      sock_ = -1;
      return false;
    }

    return true;
  }

  void Disconnect() {
    if (sock_ >= 0) {
      close(sock_);
      sock_ = -1;
    }
  }

  [[nodiscard]] bool IsConnected() const { return sock_ >= 0; }

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
    std::string response(buffer);

    // Remove trailing \r\n
    if (response.size() >= 2 &&
        response[response.size()-2] == '\r' &&
        response[response.size()-1] == '\n') {
      response = response.substr(0, response.size() - 2);
    }

    return response;
  }

  void RunInteractive() const {
    std::cout << "mygram-cli " << config_.host << ":" << config_.port << '\n';
#ifdef USE_READLINE
    std::cout << "Type 'quit' or 'exit' to exit, 'help' for help (tab completion enabled)" << '\n';
#else
    std::cout << "Type 'quit' or 'exit' to exit, 'help' for help" << std::endl;
#endif
    std::cout << '\n';

#ifdef USE_READLINE
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
    std::cout
        << "  SEARCH <table> <text> [NOT <term>...] [FILTER <col=val>...] [LIMIT <n>] [OFFSET <n>]"
        << '\n';
    std::cout << "  COUNT <table> <text> [NOT <term>...] [FILTER <col=val>...]" << '\n';
    std::cout << "  GET <table> <primary_key>" << '\n';
    std::cout << "  INFO - Show server statistics" << '\n';
    std::cout << "  CONFIG - Show current configuration" << '\n';
    std::cout << "  SAVE [filename] - Save snapshot to disk" << '\n';
    std::cout << "  LOAD <filename> - Load snapshot from disk" << '\n';
    std::cout << "  REPLICATION STATUS - Show replication status" << '\n';
    std::cout << "  REPLICATION STOP - Stop replication" << '\n';
    std::cout << "  REPLICATION START - Start replication" << '\n';
    std::cout << "  quit/exit - Exit the client" << '\n';
    std::cout << "  help - Show this help" << '\n';
  }

  // NOLINTNEXTLINE(readability-function-cognitive-complexity)
  static void PrintResponse(const std::string& response) {
    // Parse response type
    if (response.find("OK RESULTS") == 0) {
      // SEARCH response: OK RESULTS <count> [<id1> <id2> ...]
      std::istringstream iss(response);
      std::string status;
      std::string results;
      size_t count = 0;
      iss >> status >> results >> count;

      std::vector<std::string> ids;
      std::string doc_id;
      while (iss >> doc_id) {
        ids.push_back(doc_id);
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
    } else if (response.find("OK COUNT") == 0) {
      // COUNT response: OK COUNT <n>
      std::istringstream iss(response);
      std::string status;
      std::string count_str;
      uint64_t count = 0;
      iss >> status >> count_str >> count;
      std::cout << "(integer) " << count << '\n';
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
  std::cout << "  -h HOST    Server hostname (default: 127.0.0.1)" << '\n';
  std::cout << "  -p PORT    Server port (default: 11211)" << '\n';
  std::cout << "  --help     Show this help" << '\n';
  std::cout << '\n';
  std::cout << "Examples:" << '\n';
  std::cout << "  " << program_name << "                          # Interactive mode" << '\n';
  std::cout << "  " << program_name << " -h localhost -p 11211    # Connect to specific server"
            << '\n';
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
