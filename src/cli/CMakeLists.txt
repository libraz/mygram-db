add_executable(mygram-cli
  mygram-cli.cpp
)

# No external dependencies needed for the CLI client
# It's a simple TCP client using POSIX sockets

install(TARGETS mygram-cli
  RUNTIME DESTINATION bin
)
