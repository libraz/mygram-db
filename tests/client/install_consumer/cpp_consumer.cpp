#include <mygramdb/mygramclient.h>
#include <mygramdb/search_expression.h>

int main() {
  mygramdb::client::ClientConfig config;
  if (config.port != 11016) {
    return 1;
  }

  auto query = mygramdb::client::ConvertSearchExpression("alpha beta");
  if (!query || *query != "alpha AND beta") {
    return 2;
  }

  mygramdb::client::MygramClient client(config);
  auto facet = client.Facet("app.articles", "category", "", 10, {}, {}, {}, 5);
  if (facet) {
    return 3;
  }
  return client.IsConnected() ? 4 : 0;
}
