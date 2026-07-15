# Contributing to MygramDB

Thank you for your interest in contributing to MygramDB. Contributions, bug reports, documentation fixes, and small reproducible test cases are welcome.

## How to Contribute

1. **Fork the repository**
2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes**
4. **Test your changes**
   ```bash
   make test
   ```
5. **Commit your changes**
   ```bash
   git commit -m "Add: your feature description"
   ```
6. **Push to your fork**
   ```bash
   git push origin feature/your-feature-name
   ```
7. **Open a Pull Request**

## Pull Request Guidelines

- **Tests**: Ensure all tests pass (`make test`)
- **Description**: Clearly describe what your PR does and why
- **Scope**: Keep each PR focused on one feature, bug fix, or documentation improvement

## Code Quality

**Required check: tests must pass**

```bash
make test  # Required before opening a PR
```

CI also runs formatting and linting checks. These are currently advisory:
- **Formatting** (clang-format): Runs in CI, warnings only
- **Linting** (clang-tidy): Runs in CI, warnings only

If style issues appear, maintainers may adjust them during review. Passing tests and providing a clear change description are the main expectations.

## Development Setup

For detailed development environment setup, see [Development Guide](https://mygramdb.libraz.net/docs/development).

**Quick start:**
```bash
# Install dependencies (macOS example)
brew install cmake llvm@18 mysql-client icu4c

# Build project
make

# Run tests
make test
```

## Reporting Issues

- Check whether the issue already exists
- Provide clear reproduction steps or a minimal example
- Include relevant logs, versions, configuration snippets, and system information

## Questions?

Open an issue for questions, design discussion, or usage problems that are not covered by the documentation.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
