#include <mygramdb/mygramclient_c.h>

#include <string.h>

int main(void) {
  const MygramClientConfig_C config = {
      .host = "127.0.0.1",
      .port = 11016,
      .timeout_ms = 100,
      .recv_buffer_size = 1024,
  };

  MygramClient_C* client = mygramclient_create(&config);
  if (client == NULL) {
    return 1;
  }

  MygramFacetResult_C* facet = NULL;
  if (mygramclient_facet_paged(client, "app.articles", "category", "", 10, 5, &facet) != -1 || facet != NULL) {
    mygramclient_destroy(client);
    return 2;
  }

  MygramParsedExpression_C* parsed = NULL;
  char* diagnostic = NULL;
  if (mygramclient_parse_search_expression_ex("+", &parsed, &diagnostic) != -1 || parsed != NULL ||
      diagnostic == NULL || strcmp(diagnostic, "Expected term after '+'") != 0) {
    mygramclient_free_string(diagnostic);
    mygramclient_destroy(client);
    return 3;
  }
  mygramclient_free_string(diagnostic);
  mygramclient_destroy(client);
  return 0;
}
