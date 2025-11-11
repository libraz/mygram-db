# MygramDB Makefile
# Convenience wrapper for CMake build system

.PHONY: help build test clean rebuild install uninstall format configure run

# Build directory
BUILD_DIR := build

# Install prefix (can be overridden: make PREFIX=/opt/mygramdb install)
PREFIX ?= /usr/local

# Default target
.DEFAULT_GOAL := build

help:
	@echo "MygramDB Build System"
	@echo ""
	@echo "Available targets:"
	@echo "  make build     - Build the project (default)"
	@echo "  make test      - Run all tests"
	@echo "  make clean     - Clean build directory"
	@echo "  make rebuild   - Clean and rebuild"
	@echo "  make install   - Install binaries and files"
	@echo "  make uninstall - Uninstall binaries and files"
	@echo "  make format    - Format code with clang-format"
	@echo "  make configure - Configure CMake (for changing options)"
	@echo "  make run       - Build and run mygramdb"
	@echo "  make help      - Show this help message"
	@echo ""
	@echo "Examples:"
	@echo "  make                    # Build the project"
	@echo "  make test               # Run tests"
	@echo "  make install            # Install to $(PREFIX) (default: /usr/local)"
	@echo "  make PREFIX=/opt/mygramdb install  # Install to custom location"
	@echo "  make CMAKE_OPTIONS=\"-DENABLE_ASAN=ON\" configure  # Enable AddressSanitizer"
	@echo "  make CMAKE_OPTIONS=\"-DBUILD_TESTS=OFF\" configure # Disable tests"

# Configure CMake
configure:
	@mkdir -p $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake -DCMAKE_INSTALL_PREFIX=$(PREFIX) $(CMAKE_OPTIONS) ..

# Build the project
build: configure
	@echo "Building MygramDB..."
	$(MAKE) -C $(BUILD_DIR) -j$$(nproc)
	@echo "Build complete!"

# Run tests
test: build
	@echo "Running tests..."
	cd $(BUILD_DIR) && ctest --output-on-failure
	@echo "Tests complete!"

# Clean build directory
clean:
	@echo "Cleaning build directory..."
	rm -rf $(BUILD_DIR)
	@echo "Clean complete!"

# Rebuild from scratch
rebuild: clean build

# Install binaries and files
install: build
	@echo "Installing MygramDB to $(PREFIX)..."
	$(MAKE) -C $(BUILD_DIR) install
	@echo ""
	@echo "Installation complete!"
	@echo "  Binaries:      $(PREFIX)/bin/mygramdb, $(PREFIX)/bin/mygram-cli"
	@echo "  Config examples:"
	@echo "    - $(PREFIX)/etc/mygramdb/config.yaml.example (full options)"
	@echo "    - $(PREFIX)/etc/mygramdb/config-minimal.yaml (minimal setup)"
	@echo "  Documentation: $(PREFIX)/share/doc/mygramdb/"
	@echo ""
	@echo "To run: $(PREFIX)/bin/mygramdb -c config.yaml"

# Uninstall
uninstall:
	@echo "Uninstalling MygramDB from $(PREFIX)..."
	rm -f $(PREFIX)/bin/mygramdb
	rm -f $(PREFIX)/bin/mygram-cli
	rm -rf $(PREFIX)/etc/mygramdb
	rm -rf $(PREFIX)/share/doc/mygramdb
	@echo "Uninstall complete!"

# Format code with clang-format
format:
	@echo "Formatting code..."
	@find src tests -name "*.cpp" -o -name "*.h" | xargs clang-format -i
	@echo "Format complete!"

# Build and run mygramdb
run: build
	@echo "Running MygramDB..."
	$(BUILD_DIR)/bin/mygramdb

# Quick test (useful during development)
quick-test: build
	@echo "Running quick test..."
	cd $(BUILD_DIR) && ctest --output-on-failure -R "StringUtils|Config"
