/**
 * @file event_multiplexer.cpp
 * @brief Factory for the platform-appropriate EventMultiplexer backend.
 *
 * The individual backend implementations live next to this file
 * (`epoll_multiplexer.cpp`, `kqueue_multiplexer.cpp`). Each one is compiled
 * only on its target platform; this factory picks the right one at
 * compile time.
 */

#include "server/reactor/event_multiplexer.h"

#if defined(__linux__)
#include "server/reactor/epoll_multiplexer.h"
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
#include "server/reactor/kqueue_multiplexer.h"
#endif

namespace mygramdb::server::reactor {

std::unique_ptr<EventMultiplexer> CreateEventMultiplexer() {
#if defined(__linux__)
  return std::make_unique<EpollMultiplexer>();
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
  return std::make_unique<KqueueMultiplexer>();
#else
  return nullptr;
#endif
}

}  // namespace mygramdb::server::reactor
