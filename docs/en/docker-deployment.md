# Docker Deployment Guide

This guide explains how to deploy MygramDB using Docker and Docker Compose.

## Quick Start

### 1. Prerequisites

- Docker 20.10+
- Docker Compose 2.0+

### 2. Setup Environment Variables

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env and configure your settings
nano .env
```

**Important:** Change the following default values in `.env`:
- `MYSQL_ROOT_PASSWORD` - MySQL root password
- `MYSQL_PASSWORD` - MySQL replication user password
- `REPLICATION_SERVER_ID` - Unique server ID for this MygramDB instance

### 3. Start Services (Development)

```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Check status
docker-compose ps
```

### 4. Stop Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (WARNING: This deletes all data)
docker-compose down -v
```

## Configuration

### Environment Variables

All configuration is done via environment variables in the `.env` file:

#### MySQL Configuration
```bash
MYSQL_HOST=mysql                    # MySQL host
MYSQL_PORT=3306                     # MySQL port
MYSQL_USER=repl_user                # MySQL user
MYSQL_PASSWORD=your_password        # MySQL password
MYSQL_DATABASE=mydb                 # Database name
MYSQL_USE_GTID=true                 # Use GTID-based replication
```

#### Table Configuration
```bash
TABLE_NAME=articles                 # Table to index
TABLE_PRIMARY_KEY=id                # Primary key column
TABLE_TEXT_COLUMN=content           # Text column to index
TABLE_NGRAM_SIZE=2                  # N-gram size for ASCII
TABLE_KANJI_NGRAM_SIZE=1            # N-gram size for CJK
```

#### Replication Configuration
```bash
REPLICATION_ENABLE=true             # Enable replication
REPLICATION_SERVER_ID=12345         # Unique server ID (IMPORTANT)
REPLICATION_START_FROM=snapshot     # Start from: snapshot, latest, or gtid=<UUID:txn>
```

#### Memory Management
```bash
MEMORY_HARD_LIMIT_MB=8192           # Hard memory limit
MEMORY_SOFT_TARGET_MB=4096          # Soft memory target
MEMORY_NORMALIZE_NFKC=true          # NFKC normalization
MEMORY_NORMALIZE_WIDTH=narrow       # Width normalization
```

#### API Server
```bash
API_BIND=0.0.0.0                    # Bind address
API_PORT=11016                      # API port
```

#### Logging
```bash
LOG_LEVEL=info                      # Log level: debug, info, warn, error
LOG_JSON=true                       # JSON log format
```

### Custom Configuration File

If you need more advanced configuration (filters, multiple tables, etc.), you can mount a custom config file:

```yaml
# docker-compose.override.yml
version: '3.8'

services:
  mygramdb:
    volumes:
      - ./my-config.yaml:/etc/mygramdb/config.yaml:ro
    environment:
      SKIP_CONFIG_GEN: "true"
    command: ["mygramdb", "-c", "/etc/mygramdb/config.yaml"]
```

Or run directly with Docker:

```bash
# Create your config file
cp examples/config-minimal.yaml my-config.yaml
# Edit my-config.yaml as needed

# Run with custom config
docker run -d --name mygramdb \
  -p 11016:11016 \
  -v $(pwd)/my-config.yaml:/etc/mygramdb/config.yaml:ro \
  -e SKIP_CONFIG_GEN=true \
  mygramdb:latest \
  mygramdb -c /etc/mygramdb/config.yaml
```

## Production Deployment

### Using Pre-built Images

```bash
# Pull the latest image from GitHub Container Registry
docker pull ghcr.io/libraz/mygram-db:latest

# Use production docker-compose file
docker-compose -f docker-compose.prod.yml up -d
```

### Environment Setup

1. Create production `.env` file:
```bash
cp .env.example .env.prod
nano .env.prod
```

2. Set production values:
```bash
# Production MySQL configuration
MYSQL_HOST=production-mysql-host
MYSQL_PORT=3306
MYSQL_USER=repl_user
MYSQL_PASSWORD=strong_secure_password_here

# Production memory settings
MEMORY_HARD_LIMIT_MB=16384
MEMORY_SOFT_TARGET_MB=8192

# Production API settings
API_PORT=11016

# Production logging
LOG_LEVEL=info
LOG_JSON=true
```

3. Start with production configuration:
```bash
docker-compose -f docker-compose.prod.yml --env-file .env.prod up -d
```

### Resource Limits

Production compose file includes resource limits:

**MySQL:**
- CPU: 2-4 cores
- Memory: 2-4 GB

**MygramDB:**
- CPU: 4-8 cores
- Memory: 10-20 GB

Adjust these in `docker-compose.prod.yml` based on your workload.

## Database Initialization

The MySQL container automatically executes scripts in `docker/mysql/init/`:

- `01-create-tables.sql` - Creates sample tables

To add your own initialization scripts:

```bash
# Create your SQL script
cat > docker/mysql/init/02-my-tables.sql <<EOF
CREATE TABLE my_table (
    id BIGINT PRIMARY KEY,
    content TEXT
);
EOF

# Restart MySQL container
docker-compose restart mysql
```

## Monitoring

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f mygramdb

# Last 100 lines
docker-compose logs --tail=100 mygramdb
```

### Health Checks

```bash
# Check service health
docker-compose ps

# Manual health check
docker exec mygramdb pgrep -x mygramdb
```

### Metrics

MygramDB exposes metrics on the API port. Access via:

```bash
curl http://localhost:11016/metrics
```

## Backup and Restore

### Backup

```bash
# Backup MySQL data
docker exec mygramdb_mysql mysqldump -u root -p${MYSQL_ROOT_PASSWORD} mydb > backup.sql

# Backup MygramDB snapshot
docker cp mygramdb:/var/lib/mygramdb/snapshots ./backup-snapshots/
```

### Restore

```bash
# Restore MySQL data
docker exec -i mygramdb_mysql mysql -u root -p${MYSQL_ROOT_PASSWORD} mydb < backup.sql

# Restore MygramDB snapshot
docker cp ./backup-snapshots/ mygramdb:/var/lib/mygramdb/snapshots/
docker-compose restart mygramdb
```

## Troubleshooting

### Connection Issues

```bash
# Check network connectivity
docker-compose exec mygramdb ping mysql

# Check MySQL connection
docker-compose exec mygramdb mysql -h mysql -u repl_user -p${MYSQL_PASSWORD} -e "SELECT 1"
```

### Configuration Issues

```bash
# Check version
docker run --rm mygramdb:latest --version

# Show help
docker run --rm mygramdb:latest --help

# Test configuration
docker-compose exec mygramdb /usr/local/bin/entrypoint.sh test-config

# Or test with environment variables
docker run --rm -e MYSQL_HOST=testdb -e TABLE_NAME=test mygramdb:latest test-config

# View generated configuration
docker-compose exec mygramdb cat /etc/mygramdb/config.yaml
```

### Performance Issues

1. Check resource usage:
```bash
docker stats
```

2. Adjust memory limits in `.env`:
```bash
MEMORY_HARD_LIMIT_MB=16384
MEMORY_SOFT_TARGET_MB=8192
```

3. Adjust build parallelism:
```bash
BUILD_PARALLELISM=4
```

## Scaling

### Multiple MygramDB Instances

To run multiple MygramDB instances (e.g., for different tables):

```bash
# Create separate compose files for each instance
cp docker-compose.yml docker-compose.instance1.yml
cp docker-compose.yml docker-compose.instance2.yml

# Use different project names and ports
docker-compose -f docker-compose.instance1.yml -p mygramdb1 up -d
docker-compose -f docker-compose.instance2.yml -p mygramdb2 up -d
```

### Load Balancing

Use nginx or HAProxy to load balance across multiple MygramDB instances:

```nginx
upstream mygramdb_backend {
    server localhost:11016;
    server localhost:11312;
    server localhost:11313;
}

server {
    listen 80;
    location / {
        proxy_pass http://mygramdb_backend;
    }
}
```

## Security Best Practices

1. **Use strong passwords** - Change all default passwords in `.env`
2. **Network isolation** - Use Docker networks to isolate services
3. **Bind to localhost** - In production, bind MySQL to `127.0.0.1` only
4. **Enable TLS** - Use TLS for MySQL connections
5. **Regular updates** - Keep Docker images up to date
6. **Backup regularly** - Automate backups of MySQL and MygramDB snapshots

## References

- [Configuration Guide](configuration.md)
- [MySQL Replication Setup](mysql-replication.md)
- [Performance Tuning](performance-tuning.md)
