# MygramDB Docker Files

This directory contains Docker-related files for MygramDB.

## Directory Structure

```
support/docker/
├── README.md              # This file
├── entrypoint.sh          # Entrypoint script for configuration generation
└── mysql/
    └── init/              # MySQL initialization scripts
        └── 01-create-tables.sql  # Sample table creation
```

## Files

### entrypoint.sh

Entrypoint script that:
- Reads environment variables
- Generates MygramDB configuration file (`/etc/mygramdb/config.yaml`)
- Validates the configuration
- Starts MygramDB

**Environment Variables Supported:**
- `MYSQL_*` - MySQL connection settings
- `TABLE_*` - Table configuration
- `REPLICATION_*` - Replication settings
- `MEMORY_*` - Memory management
- `SNAPSHOT_*` - Snapshot configuration
- `API_*` - API server settings
- `LOG_*` - Logging configuration

See `.env.example` for all available variables.

### mysql/init/

MySQL initialization scripts that run when the MySQL container starts for the first time.

**01-create-tables.sql:**
- Creates sample `articles` table
- Inserts sample data for testing

To add custom initialization scripts, create new `.sql` files in this directory.
They will be executed in alphabetical order.

## Usage

### Development

```bash
# From project root
docker-compose up -d
```

### Production

```bash
# From project root
docker-compose -f docker-compose.prod.yml up -d
```

### Custom Configuration

If you need more advanced configuration, mount your own config file:

```yaml
# docker-compose.override.yml
version: '3.8'
services:
  mygramdb:
    volumes:
      - ./my-custom-config.yaml:/etc/mygramdb/config.yaml:ro
```

## Documentation

See [docs/en/docker-deployment.md](../../docs/en/docker-deployment.md) or [docs/ja/docker-deployment.md](../../docs/ja/docker-deployment.md) for detailed documentation.
