# Installation Guide

This guide provides detailed instructions for building and installing MygramDB.

## Prerequisites

- C++17 compatible compiler (GCC 7+, Clang 5+)
- CMake 3.15+
- MySQL client library (libmysqlclient)
- ICU library (libicu)

### Installing Dependencies

#### Ubuntu/Debian

```bash
sudo apt-get update
sudo apt-get install -y pkg-config libmysqlclient-dev libicu-dev cmake g++
```

#### macOS

```bash
brew install cmake mysql-client@8.4 icu4c pkg-config
```

## Building from Source

### Using Makefile (Recommended)

```bash
# Clone repository
git clone https://github.com/libraz/mygram-db.git
cd mygram-db

# Build
make

# Run tests
make test

# Clean build
make clean

# Other useful commands
make help      # Show all available commands
make rebuild   # Clean and rebuild
make format    # Format code with clang-format
```

### Using CMake Directly

```bash
# Create build directory
mkdir build && cd build

# Configure and build
cmake ..
cmake --build .

# Run tests
ctest
```

## Installing Binaries

### System-wide Installation

Install to `/usr/local` (default location):

```bash
sudo make install
```

This will install:
- Binaries: `/usr/local/bin/mygramdb`, `/usr/local/bin/mygram-cli`
- Config sample: `/usr/local/etc/mygramdb/config.yaml.example`
- Documentation: `/usr/local/share/doc/mygramdb/`

### Custom Installation Location

Install to a custom directory:

```bash
make PREFIX=/opt/mygramdb install
```

### Uninstalling

To remove installed files:

```bash
sudo make uninstall
```

## Running Tests

Using Makefile:

```bash
make test
```

Or using CTest directly:

```bash
cd build
ctest --output-on-failure
```

Current test coverage: **169 tests, 100% passing**

### Integration Tests

All unit tests run without requiring a MySQL server connection. Integration tests that require a MySQL server are separated and disabled by default.

To run integration tests:

```bash
# Set environment variables for MySQL connection
export MYSQL_HOST=127.0.0.1
export MYSQL_USER=root
export MYSQL_PASSWORD=your_password
export MYSQL_DATABASE=test
export ENABLE_MYSQL_INTEGRATION_TESTS=1

# Run integration tests
./build/bin/mysql_connection_integration_test
```

## Build Options

You can configure CMake options when using Makefile:

```bash
# Enable AddressSanitizer
make CMAKE_OPTIONS="-DENABLE_ASAN=ON" configure

# Enable ThreadSanitizer
make CMAKE_OPTIONS="-DENABLE_TSAN=ON" configure

# Disable tests
make CMAKE_OPTIONS="-DBUILD_TESTS=OFF" configure
```

## Verifying Installation

After installation, verify the binaries are accessible:

```bash
# Check server binary
mygramdb --help

# Check CLI client
mygram-cli --help
```

## Running as a Service (systemd)

MygramDB **refuses to run as root** for security reasons. You must run it as a non-privileged user.

### 1. Create a dedicated user

```bash
sudo useradd -r -s /bin/false mygramdb
```

### 2. Create required directories

```bash
sudo mkdir -p /etc/mygramdb /var/lib/mygramdb/dumps
sudo chown -R mygramdb:mygramdb /var/lib/mygramdb
```

### 3. Copy configuration file

```bash
sudo cp examples/config.yaml /etc/mygramdb/config.yaml
sudo chown mygramdb:mygramdb /etc/mygramdb/config.yaml
sudo chmod 600 /etc/mygramdb/config.yaml  # Protect credentials
```

### 4. Install systemd service

```bash
sudo cp support/systemd/mygramdb.service /etc/systemd/system/
sudo systemctl daemon-reload
```

### 5. Start and enable service

```bash
# Start service
sudo systemctl start mygramdb

# Check status
sudo systemctl status mygramdb

# Enable auto-start on boot
sudo systemctl enable mygramdb

# View logs
sudo journalctl -u mygramdb -f
```

## Running Manually (Daemon Mode)

For manual operation or traditional init systems, you can use the `-d` / `--daemon` option:

```bash
# Run as daemon (background process)
sudo -u mygramdb mygramdb -d -c /etc/mygramdb/config.yaml

# Check if running
ps aux | grep mygramdb

# Stop (send SIGTERM)
pkill -TERM mygramdb
```

**Note**: When running as daemon, all output is redirected to `/dev/null`. Configure file-based logging in your configuration if needed.

## Security Notes

- **Root execution blocked**: MygramDB will refuse to start if run as root
- **Recommended approach**: Use systemd `User=` and `Group=` directives (see `support/systemd/mygramdb.service`)
- **Docker**: Already configured to run as non-root user `mygramdb`
- **File permissions**: Configuration files should be readable only by the mygramdb user (mode 600)
- **Daemon mode**: Use `-d` / `--daemon` for traditional init systems or manual background execution

## Next Steps

After successful installation:

1. See [Configuration Guide](configuration.md) to set up your config file
2. See [Replication Guide](replication.md) to configure MySQL replication
3. Run `mygramdb -c config.yaml` as a non-root user or via systemd
