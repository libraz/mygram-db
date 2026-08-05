#include <mygramdb/mygramclient_c.h>

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char** argv) {
  if (argc != 5) {
    fprintf(stderr, "Usage: %s HOST PORT TABLE QUERY\n", argv[0]);
    return 2;
  }

  MygramClientConfig_C config = {0};
  config.host = argv[1];
  config.port = (uint16_t)strtoul(argv[2], NULL, 10);
  MygramClient_C* client = mygramclient_create(&config);
  if (client == NULL) {
    fprintf(stderr, "Client creation failed\n");
    return 1;
  }
  if (mygramclient_connect(client) != 0) {
    fprintf(stderr, "Connect failed: %s\n", mygramclient_get_last_error(client));
    mygramclient_destroy(client);
    return 1;
  }

  MygramSearchResult_C* response = NULL;
  if (mygramclient_search(client, argv[3], argv[4], 0, 0, &response) != 0) {
    fprintf(stderr, "Search failed: %s\n", mygramclient_get_last_error(client));
    mygramclient_destroy(client);
    return 1;
  }

  printf("total=%" PRIu64 "\n", response->total_count);
  for (size_t index = 0; index < response->count; ++index) {
    printf("%s\n", response->primary_keys[index]);
  }

  mygramclient_free_search_result(response);
  mygramclient_disconnect(client);
  mygramclient_destroy(client);
  return 0;
}
