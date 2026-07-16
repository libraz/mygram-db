#include <stddef.h>
#include <stdint.h>

#include "client/mygramclient_c.h"

int main(void) {
  MygramClientConfigV2_C config = {0};
  config.struct_size = (uint32_t)sizeof(config);
  config.version = MYGRAMCLIENT_CONFIG_V2_VERSION;
  config.host = "127.0.0.1";
  config.port = 11016;
  config.timeout_ms = 100;
  config.dump_save_timeout_ms = 600000;

  MygramClient_C* client = mygramclient_create_v2(&config);
  if (client == NULL) {
    return 1;
  }
  if (mygramclient_search_raw(NULL, "db.table", "alpha AND beta", 10, 0, NULL) != -1) {
    mygramclient_destroy(client);
    return 2;
  }
  mygramclient_destroy(client);

  MygramClientConfigV2_C short_config = {0};
  short_config.struct_size = (uint32_t)offsetof(MygramClientConfigV2_C, unix_socket_path);
  short_config.version = MYGRAMCLIENT_CONFIG_V2_VERSION;
  short_config.host = "127.0.0.1";
  short_config.port = 11016;
  client = mygramclient_create_v2(&short_config);
  if (client == NULL) {
    return 3;
  }
  mygramclient_destroy(client);
  return 0;
}
