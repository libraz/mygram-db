# RPM Build System

This directory contains scripts and configurations for building MygramDB RPM packages for RHEL/AlmaLinux/Rocky Linux 9.

## Multi-Architecture Support

The build system supports both x86_64 (Intel/AMD) and aarch64 (ARM64) architectures.

## Local Development

### Prerequisites

- Docker with buildx support
- Git

### Building RPM Packages

#### Build for current architecture:
```bash
cd support/rpm
./test-rpm-build.sh
```

#### Build for specific architecture:
```bash
cd support/rpm
./test-rpm-build.sh x86_64    # Build for x86_64
./test-rpm-build.sh aarch64   # Build for aarch64 (ARM64)
```

#### Build for all architectures:
```bash
cd support/rpm
./test-rpm-build.sh --all
```

Generated packages will be in `dist/rpm/`:
- `mygramdb-{version}-1.el9.{arch}.rpm` - Main package
- `mygramdb-debuginfo-{version}-1.el9.{arch}.rpm` - Debug symbols
- `mygramdb-debugsource-{version}-1.el9.{arch}.rpm` - Debug sources
- `mygramdb-devel-{version}-1.el9.{arch}.rpm` - Development headers
- `mygramdb-{version}-1.el9.src.rpm` - Source RPM

### Testing RPM Installation

After building, test the package in a clean AlmaLinux 9 container:

For x86_64:
```bash
docker run --rm -it --platform linux/amd64 \
  -v $(git rev-parse --show-toplevel)/dist:/dist \
  almalinux:9 bash

# Inside container:
dnf install -y https://dev.mysql.com/get/mysql84-community-release-el9-1.noarch.rpm
dnf module disable -y mysql
dnf install -y /dist/rpm/mygramdb-*.x86_64.rpm
mygramdb --version
```

For aarch64:
```bash
docker run --rm -it --platform linux/arm64 \
  -v $(git rev-parse --show-toplevel)/dist:/dist \
  almalinux:9 bash

# Inside container:
dnf install -y https://dev.mysql.com/get/mysql84-community-release-el9-1.noarch.rpm
dnf module disable -y mysql
dnf install -y /dist/rpm/mygramdb-*.aarch64.rpm
mygramdb --version
```

## CI/CD Integration

The GitHub Actions workflow (`.github/workflows/release.yml`) automatically builds RPM packages for both architectures when you push a version tag:

```bash
git tag v1.2.3
git push origin v1.2.3
```

This will:
1. Build RPM packages for x86_64 and aarch64 in parallel
2. Test installation on AlmaLinux 9 for both architectures
3. Upload artifacts to GitHub Actions
4. Create a GitHub Release with all packages attached

### Testing Workflows Locally with act

You can test GitHub Actions workflows locally using [act](https://github.com/nektos/act):

#### Install act
```bash
# macOS
brew install act

# Linux
curl https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash
```

#### Test workflow syntax (dry-run)
```bash
# Check workflow syntax without running
act -n -j build-rpm

# List all available jobs
act -l
```

#### Test specific architecture build
```bash
# Test x86_64 build (note: act doesn't fully support matrix jobs)
act -j build-rpm --env GITHUB_REF=refs/tags/v1.1.1 --matrix arch:x86_64

# Test aarch64 build
act -j build-rpm --env GITHUB_REF=refs/tags/v1.1.1 --matrix arch:aarch64
```

**Note**: Full workflow execution with act can be slow due to Docker-in-Docker and QEMU emulation. For actual RPM builds, use `./test-rpm-build.sh` instead. See [.github/ACT_TESTING.md](.github/ACT_TESTING.md) for detailed act usage.

## Build Process

### Docker-based Build

1. **Base Image**: Rocky Linux 9
2. **Build Dependencies**:
   - Oracle MySQL 8.0 repository and development libraries
   - CMake, GCC, and build tools
   - libicu-devel, readline-devel for dependencies
3. **Build Type**: Static linking for minimal runtime dependencies
4. **RPM Structure**:
   - Uses standard rpmbuild directory structure
   - Spec file: `mygramdb.spec`
   - Includes systemd service unit

### Multi-Architecture Strategy

The build system uses Docker buildx with QEMU emulation to build for different architectures:

- **x86_64**: Native build on GitHub Actions (linux/amd64)
- **aarch64**: Emulated build using QEMU (linux/arm64)

### Caching

GitHub Actions workflow uses GitHub Actions cache for Docker layers to speed up builds:
- Cache key includes architecture for isolation
- Significantly reduces build time on subsequent runs

## Files

- `Dockerfile.rpmbuild` - Docker image for RPM build environment
- `mygramdb.spec` - RPM specification file
- `test-rpm-build.sh` - Local build and test script (multi-arch support)
- `build-rpm.sh` - Legacy single-arch script (deprecated)
- `README.md` - This file

## Package Contents

The RPM package includes:

- **Binaries**:
  - `/usr/bin/mygramdb` - Main server binary
  - `/usr/bin/mygram-cli` - CLI client tool

- **Configuration**:
  - `/etc/mygramdb/config.yaml.example` - Example configuration

- **Systemd Service**:
  - `/usr/lib/systemd/system/mygramdb.service` - Service unit file

- **Data Directory**:
  - `/var/lib/mygramdb` - Runtime data directory (owned by mygramdb user)

- **User/Group**:
  - Creates `mygramdb` system user and group

## Troubleshooting

### Build fails with "platform not supported"

Ensure Docker has QEMU support enabled:
```bash
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
```

### RPM installation fails with missing dependencies

Verify MySQL repository is installed:
```bash
dnf install -y https://dev.mysql.com/get/mysql84-community-release-el9-1.noarch.rpm
dnf module disable -y mysql
```

### Binary has "not found" libraries

Check library dependencies:
```bash
ldd /usr/bin/mygramdb
```

All dependencies should be satisfied by the RPM's `Requires:` directives.

## Version Management

Package version is determined by git tags:
- Tag format: `vX.Y.Z` (e.g., `v1.2.3`)
- RPM version: `X.Y.Z` (without 'v' prefix)
- RPM release: `1` (can be incremented for packaging fixes)
- Full package name: `mygramdb-X.Y.Z-1.el9.{arch}.rpm`

## References

- [RPM Packaging Guide](https://rpm-packaging-guide.github.io/)
- [Fedora RPM Guide](https://docs.fedoraproject.org/en-US/packaging-guidelines/)
- [Docker Buildx Documentation](https://docs.docker.com/buildx/working-with-buildx/)
- [GitHub Actions Cache](https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows)
