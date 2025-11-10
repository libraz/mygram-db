/**
 * @file mygram-cli.cpp
 * @brief Command-line client for MygramDB (redis-cli style)
 */

#include <iostream>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sstream>
#include <vector>

namespace {

struct Config {
  std::string host = "127.0.0.1";
  uint16_t port = 11211;
  bool interactive = true;
};

class MygramClient {
 public:
  MygramClient(const Config& config) : config_(config), sock_(-1) {}

  ~MygramClient() {
    Disconnect();
  }

  bool Connect() {
    sock_ = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_ < 0) {
      std::cerr << "Failed to create socket: " << strerror(errno) << std::endl;
      return false;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(config_.port);

    if (inet_pton(AF_INET, config_.host.c_str(), &server_addr.sin_addr) <= 0) {
      std::cerr << "Invalid address: " << config_.host << std::endl;
      close(sock_);
      sock_ = -1;
      return false;
    }

    if (connect(sock_, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
      std::cerr << "Connection failed: " << strerror(errno) << std::endl;
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

  bool IsConnected() const {
    return sock_ >= 0;
  }

  std::string SendCommand(const std::string& command) {
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
    char buffer[65536];
    ssize_t received = recv(sock_, buffer, sizeof(buffer) - 1, 0);
    if (received <= 0) {
      if (received == 0) {
        return "(error) Connection closed by server";
      }
      return "(error) Failed to receive response: " + std::string(strerror(errno));
    }

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

  void RunInteractive() {
    std::cout << "mygram-cli " << config_.host << ":" << config_.port << std::endl;
    std::cout << "Type 'quit' or 'exit' to exit, 'help' for help" << std::endl;
    std::cout << std::endl;

    while (true) {
      // Print prompt
      std::cout << config_.host << ":" << config_.port << "> ";
      std::cout.flush();

      // Read command
      std::string line;
      if (!std::getline(std::cin, line)) {
        break;  // EOF
      }

      // Trim whitespace
      line.erase(0, line.find_first_not_of(" \t\r\n"));
      line.erase(line.find_last_not_of(" \t\r\n") + 1);

      if (line.empty()) {
        continue;
      }

      // Check for exit commands
      if (line == "quit" || line == "exit") {
        std::cout << "Bye!" << std::endl;
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

  void RunSingleCommand(const std::string& command) {
    std::string response = SendCommand(command);
    PrintResponse(response);
  }

 private:
  void PrintHelp() {
    std::cout << "Available commands:" << std::endl;
    std::cout << "  SEARCH <table> <text> [NOT <term>...] [FILTER <col=val>...] [LIMIT <n>] [OFFSET <n>]" << std::endl;
    std::cout << "  COUNT <table> <text> [NOT <term>...] [FILTER <col=val>...]" << std::endl;
    std::cout << "  GET <table> <primary_key>" << std::endl;
    std::cout << "  quit/exit - Exit the client" << std::endl;
    std::cout << "  help - Show this help" << std::endl;
  }

  void PrintResponse(const std::string& response) {
    // Parse response type
    if (response.find("OK RESULTS") == 0) {
      // SEARCH response: OK RESULTS <count> [<id1> <id2> ...]
      std::istringstream iss(response);
      std::string ok, results;
      size_t count;
      iss >> ok >> results >> count;

      std::vector<std::string> ids;
      std::string id;
      while (iss >> id) {
        ids.push_back(id);
      }

      std::cout << "(" << count << " results";
      if (!ids.empty()) {
        std::cout << ", showing " << ids.size() << ")";
      } else {
        std::cout << ")";
      }
      std::cout << std::endl;

      for (size_t i = 0; i < ids.size(); ++i) {
        std::cout << (i + 1) << ") " << ids[i] << std::endl;
      }
    } else if (response.find("OK COUNT") == 0) {
      // COUNT response: OK COUNT <n>
      std::istringstream iss(response);
      std::string ok, count_str;
      uint64_t count;
      iss >> ok >> count_str >> count;
      std::cout << "(integer) " << count << std::endl;
    } else if (response.find("OK DOC") == 0) {
      // GET response: OK DOC <primary_key> [<filter=value>...]
      std::cout << response.substr(3) << std::endl;  // Remove "OK "
    } else if (response.find("ERROR") == 0) {
      // Error response
      std::cout << "(error) " << response.substr(6) << std::endl;  // Remove "ERROR "
    } else {
      // Unknown response
      std::cout << response << std::endl;
    }
  }

  Config config_;
  int sock_;
};

void PrintUsage(const char* program_name) {
  std::cout << "Usage: " << program_name << " [OPTIONS] [COMMAND]" << std::endl;
  std::cout << std::endl;
  std::cout << "Options:" << std::endl;
  std::cout << "  -h HOST    Server hostname (default: 127.0.0.1)" << std::endl;
  std::cout << "  -p PORT    Server port (default: 11211)" << std::endl;
  std::cout << "  --help     Show this help" << std::endl;
  std::cout << std::endl;
  std::cout << "Examples:" << std::endl;
  std::cout << "  " << program_name << "                          # Interactive mode" << std::endl;
  std::cout << "  " << program_name << " -h localhost -p 11211    # Connect to specific server" << std::endl;
  std::cout << "  " << program_name << " SEARCH articles hello    # Execute single command" << std::endl;
}

}  // namespace

int main(int argc, char* argv[]) {
  Config config;
  std::vector<std::string> command_args;

  // Parse arguments
  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);

    if (arg == "--help") {
      PrintUsage(argv[0]);
      return 0;
    } else if (arg == "-h") {
      if (i + 1 < argc) {
        config.host = argv[++i];
      } else {
        std::cerr << "Error: -h requires an argument" << std::endl;
        return 1;
      }
    } else if (arg == "-p") {
      if (i + 1 < argc) {
        config.port = static_cast<uint16_t>(std::stoi(argv[++i]));
      } else {
        std::cerr << "Error: -p requires an argument" << std::endl;
        return 1;
      }
    } else {
      // Assume remaining args are a command
      for (int j = i; j < argc; ++j) {
        command_args.push_back(argv[j]);
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
      if (i > 0) command << " ";
      command << command_args[i];
    }
    client.RunSingleCommand(command.str());
  }

  return 0;
}
