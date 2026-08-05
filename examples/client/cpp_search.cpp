#include <mygramdb/mygramclient.h>

#include <cstdlib>
#include <iostream>
#include <string>

int main(int argc, char** argv) {
  if (argc != 5) {
    std::cerr << "Usage: " << argv[0] << " HOST PORT TABLE QUERY\n";
    return 2;
  }

  mygramdb::client::ClientConfig config;
  config.host = argv[1];
  config.port = static_cast<uint16_t>(std::strtoul(argv[2], nullptr, 10));

  mygramdb::client::MygramClient client(config);
  auto connected = client.Connect();
  if (!connected) {
    std::cerr << "Connect failed: " << connected.error().message() << '\n';
    return 1;
  }

  auto response = client.Search(argv[3], argv[4]);
  if (!response) {
    std::cerr << "Search failed: " << response.error().message() << '\n';
    return 1;
  }

  std::cout << "total=" << response->total_count << '\n';
  for (const auto& document : response->results) {
    std::cout << document.primary_key << '\n';
  }
  return 0;
}
