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
brew install llvm  # Includes clang, clang-tidy, clang-format, clangd
brew install mysql-client@8.4
brew install icu4c

# Add LLVM to PATH (optional, for using latest clangd)
echo 'export PATH="/opt/homebrew/opt/llvm/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### Linux (Ubuntu/Debian)

```bash
# Update package list
sudo apt-get update

# Install development tools
sudo apt-get install -y cmake
sudo apt-get install -y clang clang-tidy clang-format clangd
sudo apt-get install -y libmysqlclient-dev
sudo apt-get install -y libicu-dev
sudo apt-get install -y pkg-config
```

## Verify Installation

```bash
# Check if tools are installed
cmake --version        # Should show 3.15+
clang++ --version      # Should show C++17 support
clang-tidy --version   # For linting
clang-format --version # For formatting
clangd --version       # For LSP (optional)
```

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
- Column limit: 100 characters
- Indentation: 2 spaces (no tabs)
- Pointer alignment: Left (`int* ptr`, not `int *ptr`)
- Braces: Attach style (`if (x) {`)

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

**Solution:** Install clang-tidy:

```bash
# macOS
brew install llvm

# Linux
sudo apt-get install clang-tidy
```

### "Many errors in VS Code after opening project"

**Solution:**
1. Make sure project is built: `make`
2. Check if `build/compile_commands.json` exists
3. Reload VS Code window: Cmd/Ctrl + Shift + P → "Developer: Reload Window"

### "clang-format not working"

**Solution:**

```bash
# macOS
brew install clang-format

# Linux
sudo apt-get install clang-format
```

Verify the path in `.vscode/settings.json` matches your installation:

```bash
which clang-format  # Check the actual path
```

## Quick Start Checklist

- [ ] Install cmake, clang, clang-tidy, clang-format
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

## Contributing

Before submitting changes:

- [ ] Code follows Google C++ Style Guide
- [ ] All comments and documentation are in English
- [ ] Doxygen comments added for public APIs
- [ ] Unit tests added/updated
- [ ] All tests pass (`make test`)
- [ ] Code formatted with clang-format (`make format`)
- [ ] No compiler warnings
- [ ] Documentation updated (if needed)
