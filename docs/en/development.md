# Development Environment Setup

This guide helps you set up the development environment for MygramDB with real-time linting and auto-formatting (like ESLint/Prettier for JavaScript).

## Prerequisites

Before starting development, install the required tools.

### macOS

```bash
# Install Homebrew (if not already installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install development tools
brew install cmake
brew install llvm@18  # Version 18 required for consistent formatting
brew install mysql-client@8.4
brew install icu4c

# Create symlinks to use LLVM 18 by default
# IMPORTANT: This ensures consistent code formatting with CI
ln -sf /opt/homebrew/opt/llvm@18/bin/clang-format /opt/homebrew/bin/clang-format
ln -sf /opt/homebrew/opt/llvm@18/bin/clang-tidy /opt/homebrew/bin/clang-tidy
ln -sf /opt/homebrew/opt/llvm@18/bin/clangd /opt/homebrew/bin/clangd
```

### Linux (Ubuntu/Debian)

```bash
# Update package list
sudo apt-get update

# Install basic development tools
sudo apt-get install -y \
  cmake \
  build-essential \
  libmysqlclient-dev \
  libicu-dev \
  pkg-config \
  wget \
  lsb-release \
  software-properties-common \
  gnupg

# Install LLVM/Clang 18 (required for consistent formatting)
# IMPORTANT: Version 18 is required to match CI environment
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 18

# Install clang-format, clang-tidy, and clangd version 18
sudo apt-get install -y clang-format-18 clang-tidy-18 clangd-18

# Set version 18 as default
sudo update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-18 100
sudo update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-18 100
sudo update-alternatives --install /usr/bin/clangd clangd /usr/bin/clangd-18 100
```

## Verify Installation

```bash
# Check if tools are installed
cmake --version        # Should show 3.15+
clang-format --version # Should show 18.x.x
clang-tidy --version   # Should show 18.x.x
clangd --version       # Should show 18.x.x (optional, for LSP)
```

**Why version 18?** Different versions of clang-format produce different formatting results. We standardize on version 18 to ensure consistency between local development and CI.

## Build Project

```bash
# Build the project (generates compile_commands.json)
make

# Run tests
make test

# Format code
make format
```

After building, `compile_commands.json` will be generated in the `build/` directory. This file is required for clangd and clang-tidy to work correctly.

## VS Code Setup

### Required Extensions

Open VS Code and install the recommended extensions:

1. **C/C++** (`ms-vscode.cpptools`) - IntelliSense and debugging
2. **CMake Tools** (`ms-vscode.cmake-tools`) - CMake integration
3. **Error Lens** (`usernamehw.errorlens`) - Inline error display (like ESLint)

### Optional Extension (for best experience)

4. **clangd** (`llvm-vs-code-extensions.vscode-clangd`) - Fast LSP for real-time linting

## Enable Linting

After building the project, enable linting in VS Code:

### Option A: Using C/C++ Extension (Simpler)

Edit `.vscode/settings.json` and change:

```json
"C_Cpp.codeAnalysis.clangTidy.enabled": true,
"C_Cpp.codeAnalysis.clangTidy.useBuildPath": true,
```

Reload VS Code window (Cmd/Ctrl + Shift + P → "Developer: Reload Window")

### Option B: Using clangd (Faster, ESLint-like experience)

1. Make sure clangd extension is installed
2. Edit `.vscode/settings.json` and uncomment the clangd section (OPTION 1)
3. Comment out the C/C++ Extension section (OPTION 2)
4. Reload VS Code window

## Features Enabled

After setup, you'll have:

- ✅ **Real-time linting** - clang-tidy checks as you type
- ✅ **Auto-formatting** - Code formatted on save (Google C++ Style)
- ✅ **Inline errors** - Errors displayed inline with Error Lens
- ✅ **Code completion** - IntelliSense for C++17
- ✅ **Quick fixes** - Auto-fix available issues on save

## Coding Standards

This project follows the **Google C++ Style Guide** with specific configurations:

### Code Formatting

- Base style: Google
- Column limit: 120 characters
- Indentation: 2 spaces (no tabs)
- Pointer alignment: Left (`int* ptr`, not `int *ptr`)
- Braces: Attach style (`if (x) {`)

Run formatting tools:

```bash
make format        # Auto-format all code
make format-check  # Check formatting (CI mode - fails if not formatted)
make lint          # Run clang-tidy static analysis (takes time)
```

### Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Classes | CamelCase | `DocumentStore`, `Index` |
| Functions | CamelCase | `AddDocument()`, `GetPrimaryKey()` |
| Variables | lower_case | `doc_id`, `primary_key` |
| Constants | kCamelCase | `kMaxConnections`, `kDefaultPort` |
| Member variables | lower_case_ | `next_doc_id_`, `term_postings_` |
| Namespaces | lower_case | `mygramdb::index` |

### Documentation Comments

Use Doxygen-style comments for all public APIs:

```cpp
/**
 * @brief Brief description of what the function does
 *
 * @param param_name Description of parameter
 * @return Description of return value
 */
ReturnType FunctionName(Type param_name);
```

### Suppressing clang-tidy Warnings

When necessary, suppress warnings using NOLINT comments:

```cpp
// ✅ Good: Use NOLINTNEXTLINE before the problematic line
// NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
snprintf(buf, sizeof(buf), "%d", value);

// ✅ Good: Inline NOLINT (keep comments within 120 chars)
char buf[32];  // NOLINT(cppcoreguidelines-avoid-c-arrays)

// ❌ Bad: Multi-line NOLINT (not recognized by clang-tidy)
snprintf(buf, sizeof(buf), "%d",
         value);  // NOLINT(cppcoreguidelines-pro-type-vararg)

// ✅ Good: File-level suppression for pervasive issues
// NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
// ... code requiring pointer arithmetic ...
// NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
```

**Important:** NOLINT comments must be on a single line. Multi-line comments are not recognized.

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

## Troubleshooting

### "clangd: Unable to find compile_commands.json"

**Solution:** Run `make` to build the project first.

### "clang-tidy: error while loading shared libraries"

**Solution:** Install clang-tidy version 18:

```bash
# macOS
brew install llvm@18
ln -sf /opt/homebrew/opt/llvm@18/bin/clang-tidy /opt/homebrew/bin/clang-tidy

# Linux
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 18
sudo apt-get install -y clang-tidy-18
sudo update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-18 100
```

### "Many errors in VS Code after opening project"

**Solution:**
1. Make sure project is built: `make`
2. Check if `build/compile_commands.json` exists
3. Reload VS Code window: Cmd/Ctrl + Shift + P → "Developer: Reload Window"

### "clang-format not working"

**Solution:** Install clang-format version 18:

```bash
# macOS
brew install llvm@18
ln -sf /opt/homebrew/opt/llvm@18/bin/clang-format /opt/homebrew/bin/clang-format

# Linux
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 18
sudo apt-get install -y clang-format-18
sudo update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-18 100
```

Verify the version:

```bash
clang-format --version  # Should show 18.x.x
```

## Quick Start Checklist

- [ ] Install cmake
- [ ] Install LLVM 18 (clang-format-18, clang-tidy-18, clangd-18)
- [ ] Verify clang-format --version shows 18.x.x
- [ ] Install MySQL client library
- [ ] Install ICU library
- [ ] Run `make` to build project
- [ ] Verify `build/compile_commands.json` exists
- [ ] Install VS Code extensions (C/C++, CMake Tools, Error Lens)
- [ ] Enable clang-tidy in `.vscode/settings.json`
- [ ] Reload VS Code window

## Next Steps

Once setup is complete:

1. Open any `.cpp` or `.h` file
2. Make a small change (e.g., add extra spaces)
3. Save the file → Auto-formatting should work
4. Try adding code that violates style guide → Should see warnings

## Development Priorities

When implementing features or making changes, prioritize in this order:

1. **Performance**: Optimize for speed and low latency
2. **Memory Efficiency**: Minimize memory footprint
3. **Maintainability**: Write clean, testable code

For detailed coding guidelines, see [CLAUDE.md](../../CLAUDE.md) in the project root.

## Linux CI Testing (macOS Developers)

If you're developing on macOS, certain issues (missing headers, compiler differences) only appear in Linux environments and fail in CI.

**Solution:** Test your code in the same Linux environment used by GitHub Actions CI **before pushing**.

```bash
# Run full CI checks (recommended before git push)
make docker-ci-check
```

This runs:
1. Code formatting check
2. Build (with same flags as CI)
3. Clang-tidy linting
4. All tests

**Individual checks:**

```bash
make docker-build-linux       # Build only
make docker-test-linux        # Test only
make docker-lint-linux        # Lint only
make docker-format-check-linux # Format check only
```

**Interactive shell:**

```bash
make docker-dev-shell         # Enter Linux container for debugging
```

See [Linux Testing Guide](./linux-testing.md) for details.

## My Development Workflow (Optional Reference)

This is my personal workflow for maintaining code quality. **You don't need to follow this exactly** - the main requirement is that your PR passes CI checks.

### What I typically do before committing:

1. **Write code** following Google C++ Style Guide
2. **Run `make format`** to auto-format code
3. **Run `make lint`** to catch issues early (slow, but worth it)
4. **Run `make test`** to ensure all tests pass
5. **Run `make docker-ci-check`** to verify Linux compatibility (macOS only)

### CI Requirements

The following are automatically checked by CI:
- ✅ Code formatting (clang-format)
- ✅ Static analysis (clang-tidy)
- ✅ All tests pass
- ✅ No compiler warnings
- ✅ Linux build compatibility

**You can rely on CI to check these if you prefer.** Just make sure your PR passes all CI checks before merging.
