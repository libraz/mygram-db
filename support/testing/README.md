# RPM Testing Environment

This directory contains files for testing MygramDB RPM packages in a Docker environment.

## Files

- `Dockerfile.rpm-test` - Dockerfile for building RPM test container
- `docker-compose.rpm-test.yml` - Docker Compose configuration for RPM testing
- `config-rpm-test.yaml` - MygramDB configuration file for testing

## Usage

### Build and Start Test Environment

```bash
# From project root
cd /path/to/mygram-db

# Build the RPM test image
docker-compose -f support/testing/docker-compose.rpm-test.yml build

# Start the test environment
docker-compose -f support/testing/docker-compose.rpm-test.yml up -d

# Check logs
docker-compose -f support/testing/docker-compose.rpm-test.yml logs -f
```

### Test Basic Functionality

```bash
# Trigger initial sync
docker-compose -f support/testing/docker-compose.rpm-test.yml exec mygramdb \
  mygram-cli -p 11016 SYNC articles

# Test COUNT
docker-compose -f support/testing/docker-compose.rpm-test.yml exec mygramdb \
  mygram-cli -p 11016 COUNT articles "hello"

# Test SEARCH
docker-compose -f support/testing/docker-compose.rpm-test.yml exec mygramdb \
  mygram-cli -p 11016 SEARCH articles "hello" LIMIT 10

# Test INFO
docker-compose -f support/testing/docker-compose.rpm-test.yml exec mygramdb \
  mygram-cli -p 11016 INFO articles
```

### Cleanup

```bash
# Stop and remove containers
docker-compose -f support/testing/docker-compose.rpm-test.yml down

# Remove volumes (WARNING: deletes all data)
docker-compose -f support/testing/docker-compose.rpm-test.yml down -v
```

## Test Environment Details

- **Base Image**: Rocky Linux 9
- **MySQL Version**: 8.4
- **MygramDB Port**: 11017 (mapped from container port 11016)
- **MySQL Port**: 3307 (mapped from container port 3306)

## Configuration

The test environment uses:
- MySQL 8.4 with GTID mode enabled
- Network ACL allowing all connections (0.0.0.0/0)
- Sample table: `articles` with test data

## Notes

- This environment is for testing RPM packages only
- RPM packages are automatically downloaded from GitHub Releases
- The environment uses the latest released version by default
- To test a specific version, set `MYGRAMDB_VERSION` in docker-compose.rpm-test.yml
