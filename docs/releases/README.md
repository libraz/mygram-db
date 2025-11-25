# Release Notes

This directory contains detailed release notes for each version of MygramDB.

## Available Versions

- [v1.3.5](v1.3.5.md) - Latest release (2025-11-26) - Parallel Query Performance (QPS 372)
- [v1.3.4](v1.3.4.md) - 2025-11-25 - Log Rotation & Testing Improvements
- [v1.3.3](v1.3.3.md) - 2025-11-25 - DATETIME2/server_id Bug Fixes
- [v1.3.2](v1.3.2.md) - 2025-11-25 - Critical Binlog Parsing Fixes
- [v1.3.1](v1.3.1.md) - 2025-11-24 - Critical Bug Fixes
- [v1.3.0](v1.3.0.md) - 2025-11-22 - Temporal Data Types & Timezone Support ⚠️ BREAKING CHANGES
- [v1.2.5](v1.2.5.md) - 2025-11-22 - Log Noise Reduction
- [v1.2.4](v1.2.4.md) - 2025-11-21 - Critical Bug Fix
- [v1.2.3](v1.2.3.md) - 2025-11-20
- [v1.2.2](v1.2.2.md) - 2025-11-20
- [v1.2.1](v1.2.1.md) - 2025-11-19
- [v1.2.0](v1.2.0.md) - 2025-11-19
- [v1.1.0](v1.1.0.md) - Production-ready release (2025-11-17)
- v1.0.0 - Initial release (2025-11-13)

## Quick Links

- [CHANGELOG.md](../../CHANGELOG.md) - Concise version history (Keep a Changelog format)
- [GitHub Releases](https://github.com/libraz/mygram-db/releases) - Download binaries and RPM packages
- [Latest Release](https://github.com/libraz/mygram-db/releases/latest) - Most recent version

## Format

Each release note file includes:

- **Overview**: Major features and improvements
- **Breaking Changes**: Migration steps for incompatible changes
- **New Features**: Detailed feature descriptions
- **Bug Fixes**: Issue resolutions
- **Performance Improvements**: Optimization details
- **Documentation Updates**: New or updated guides
- **Migration Guide**: Step-by-step upgrade instructions

## Versioning

MygramDB follows [Semantic Versioning](https://semver.org/):

- **MAJOR** (X.0.0): Incompatible API changes
- **MINOR** (0.X.0): New features (backward compatible)
- **PATCH** (0.0.X): Bug fixes (backward compatible)

## Contributing

When creating a new release:

1. Create detailed release notes in `docs/releases/vX.Y.Z.md`
2. Update `CHANGELOG.md` in project root with concise summary
3. Tag the release: `git tag -a vX.Y.Z -m "Release vX.Y.Z"`
4. Push the tag: `git push origin vX.Y.Z`

The CI/CD pipeline will automatically:
- Build RPM packages for x86_64 and aarch64
- Create GitHub Release with installation instructions
- Attach RPM packages and release notes as assets
