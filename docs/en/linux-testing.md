# Linux CI Testing with Docker

## Overview

This guide explains how to test your code in the same Linux environment used by GitHub Actions CI, catching platform-specific issues (like missing headers or compiler differences) **before** pushing to CI.

## Problem

When developing on macOS, certain issues only appear in Linux environments:
- **Missing header includes** (e.g., `<cstdint>`, `<climits>`)
- **Compiler differences** (Clang vs GCC behavior)
- **Library version differences**
- **Path handling differences**

These issues often only get detected in CI, leading to failed builds after pushing.

## Solution

We provide Docker-based Linux testing that mirrors the GitHub Actions CI environment (Ubuntu 22.04 with the same dependencies).

## Quick Start

### 1. Run Full CI Checks (Recommended before git push)

```bash
make docker-ci-check
```

This runs the complete CI pipeline:
1. Code formatting check
2. Build (with same flags as CI)
3. Clang-tidy linting
4. All tests

**Use this before every push** to catch CI failures early.

### 2. Run Individual Steps

```bash
# Build only
make docker-build-linux

# Test only
make docker-test-linux

# Lint only
make docker-lint-linux

# Format check only
make docker-format-check-linux
```

### 3. Interactive Development Shell

```bash
make docker-dev-shell
```

This gives you an interactive bash shell inside the Linux container where you can run commands manually:

```bash
# Inside the container
make build
make test
make lint
./build/bin/mygramdb --help
```

## How It Works

### Docker Image: `support/dev/Dockerfile`

The `support/dev/Dockerfile` creates a Linux development environment that **exactly matches** the GitHub Actions CI environment:

- **Base**: Ubuntu 22.04
- **Compiler**: GCC (same as CI)
- **Tools**: clang-format-18, clang-tidy-18, ccache
- **Dependencies**: libmysqlclient-dev, libicu-dev, etc.

### Makefile Targets

All Linux testing targets follow the pattern `docker-*-linux`:

| Target | Description |
|--------|-------------|
| `docker-dev-build` | Build the Linux Docker image (auto-run by other targets) |
| `docker-dev-shell` | Interactive shell in Linux container |
| `docker-build-linux` | Build project in Linux (mimics CI) |
| `docker-test-linux` | Run tests in Linux |
| `docker-lint-linux` | Run clang-tidy in Linux |
| `docker-format-check-linux` | Check formatting in Linux |
| `docker-clean-linux` | Clean build in Linux |
| `docker-ci-check` | Run all CI checks (recommended) |

## Recommended Workflow

### Option 1: Pre-push Check (Recommended)

```bash
# After making changes
make format              # Format code (macOS)
make build              # Quick build check (macOS)
make docker-ci-check    # Full Linux CI check before push
git commit -m "..."
git push
```

### Option 2: Continuous Testing

If you're working on Linux-specific issues or want to test frequently:

```bash
# Terminal 1: Keep building in Linux
make docker-build-linux

# Terminal 2: Keep testing in Linux
make docker-test-linux

# Or use interactive shell for faster iteration
make docker-dev-shell
# Inside container: make build && make test
```

### Option 3: Debug Specific CI Failures

If CI failed with a specific error:

```bash
# Reproduce the exact CI environment
make docker-dev-shell

# Inside container, run the failing step
cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON -DENABLE_COVERAGE=ON -DUSE_ICU=ON -DUSE_MYSQL=ON ..
make -j$(nproc)
make lint

# Fix the issue, exit, and verify
exit
make docker-ci-check
```

## Performance Tips

### 1. Image Caching

The Docker image is cached after the first build. Subsequent runs are much faster:

- **First run**: ~5-10 minutes (downloads and installs LLVM, dependencies)
- **Subsequent runs**: ~10-30 seconds (only builds your code)

### 2. Incremental Builds

The `build/` directory is mounted as a volume, so builds are incremental:

```bash
# First build: full compilation
make docker-build-linux

# Second build: only changed files
make docker-build-linux  # Much faster!
```

### 3. Parallel Testing

Tests run in parallel using all available CPU cores:

```bash
make docker-test-linux  # Uses $(nproc) cores
```

### 4. ccache

The container uses ccache to speed up compilation. This is shared across builds.

## Troubleshooting

### Issue: "permission denied" errors

The container runs as root, but files are owned by your user. If you encounter permission issues:

```bash
# Clean build directory
make docker-clean-linux

# Or manually
sudo rm -rf build/
```

### Issue: Docker image is outdated

If dependencies changed (e.g., new LLVM version, new libraries):

```bash
# Rebuild the development image
docker build -f support/dev/Dockerfile -t mygramdb-dev:latest --no-cache .
```

### Issue: Disk space

Docker images can consume disk space. To clean up:

```bash
# Remove development image
docker rmi mygramdb-dev:latest

# Clean all unused Docker resources
docker system prune -a
```

### Issue: Slow builds

If builds are slow even with caching:

```bash
# Check ccache stats
make docker-dev-shell
ccache --show-stats

# Increase ccache size if needed (inside container)
ccache --set-config=max_size=1G
```

## CI Environment Details

The `support/dev/Dockerfile` environment matches `.github/workflows/ci.yml`:

| Component | Version |
|-----------|---------|
| OS | Ubuntu 22.04 |
| Compiler | GCC (default) + Clang 18 |
| CMake | Latest from apt |
| clang-format | 18 |
| clang-tidy | 18 |
| MySQL Client | libmysqlclient-dev |
| ICU | libicu-dev |
| Readline | libreadline-dev |
| Coverage | lcov |
| Cache | ccache |

## Integration with Git Hooks

You can add a pre-push hook to automatically run CI checks:

```bash
# .git/hooks/pre-push
#!/bin/bash
echo "Running CI checks before push..."
make docker-ci-check
if [ $? -ne 0 ]; then
    echo "CI checks failed. Push aborted."
    exit 1
fi
```

```bash
chmod +x .git/hooks/pre-push
```

## Advanced Usage

### Custom CMake Options

```bash
# Build with different options
docker run --rm -v $(pwd):/workspace -w /workspace mygramdb-dev:latest \
    bash -c "mkdir -p build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_ASAN=ON .. && \
    make -j$(nproc)"
```

### Run Specific Tests

```bash
docker run --rm -v $(pwd):/workspace -w /workspace mygramdb-dev:latest \
    bash -c "cd build && ctest -R 'MyTest.*' --verbose"
```

### Generate Coverage Report

```bash
make docker-dev-shell
# Inside container
cd build
make coverage
# View coverage/html/index.html
```

## Comparison: macOS vs Linux Testing

| Aspect | macOS (`make build`) | Linux (`make docker-build-linux`) |
|--------|----------------------|-----------------------------------|
| Speed | Fast (native) | Slower (containerized) |
| Environment | Your macOS | CI environment (Ubuntu 22.04) |
| Use case | Quick iteration | Pre-push verification |
| Compiler | Clang (macOS) | GCC (Linux) |
| Headers | More permissive | Stricter (catches missing includes) |

**Recommendation**: Use both!
- macOS for rapid development
- Linux for pre-push verification

## See Also

- [GitHub Actions CI Workflow](../.github/workflows/ci.yml)
- [Docker Compose Setup](../docker-compose.yml)
- [Development Guide](./development.md)
