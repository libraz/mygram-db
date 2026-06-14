# Development Support Tools

This directory contains development tools and scripts.

## File List

### Docker Linux Testing

Tools for macOS developers to detect build issues in Linux (CI) environments before they occur.

- **Dockerfile** - Development Docker image that reproduces the CI environment (Ubuntu 22.04)

**Usage:**
```bash
# Run all checks before committing (recommended)
make docker-ci-check

# Interactive shell
make docker-dev-shell

# Individual checks
make docker-build-linux
make docker-test-linux
make docker-lint-linux
```

**Details:** [Linux Testing Guide](https://mygramdb.libraz.net/docs/linux-testing)

### Code Quality Tools

- **run-clang-tidy.sh** - Script to run clang-tidy

**Usage:**
```bash
make lint
```

## Related Documentation

- [Linux Testing Guide](https://mygramdb.libraz.net/docs/linux-testing) - Linux CI Testing Guide
- [Development Guide](https://mygramdb.libraz.net/docs/development) - Development Environment Setup Guide
