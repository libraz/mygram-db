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

## Next Steps

After successful installation:

1. See [Configuration Guide](configuration.md) to set up your config file
2. See [Replication Guide](replication.md) to configure MySQL replication
3. Run `mygramdb -c config.yaml` (or `mygramdb config.yaml`) to start the server
