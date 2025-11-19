# Contributing to MygramDB

Thank you for your interest in contributing to MygramDB! We welcome contributions from the community.

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
- **One feature per PR**: Keep changes focused and atomic

## Code Quality

**Only requirement: Tests must pass**

```bash
make test  # This is the only required check
```

CI also runs formatting and linting checks, but these are **not blocking**:
- **Formatting** (clang-format): Runs in CI, warnings only
- **Linting** (clang-tidy): Runs in CI, warnings only

If there are style issues, the maintainer will fix them. You don't need to worry about formatting or linting.

## Development Setup

For detailed development environment setup, see [Development Guide](docs/en/development.md).

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

- Check if the issue already exists
- Provide clear reproduction steps
- Include relevant logs and system information

## Questions?

Feel free to open an issue for questions or discussions.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
