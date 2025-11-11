# Configuration Examples

This directory contains example configuration files for MygramDB.

MygramDB supports both **YAML** and **JSON** configuration formats. All configurations are automatically validated against a built-in JSON Schema at startup, ensuring that invalid settings (typos, wrong types, unknown keys) are caught immediately.

## Available Examples

### YAML Format

#### config.yaml

Complete configuration example with all available options and detailed comments.

**Use this when:**
- You want to see all available configuration options
- You need detailed explanations for each setting
- You're setting up a production environment

#### config-minimal.yaml

Minimal configuration example with only required settings.

**Use this when:**
- You want to get started quickly
- You're setting up a development environment
- You prefer to use default values

### JSON Format

#### config.json

Complete configuration example in JSON format with all available options.

**Use this when:**
- You prefer JSON over YAML
- You're integrating with JSON-based tooling
- You want strict syntax validation

#### config-minimal.json

Minimal configuration example in JSON format.

**Use this when:**
- You want to get started quickly with JSON
- You prefer JSON's strict syntax
- You're programmatically generating configurations

## Getting Started

1. Copy an example configuration file:

```bash
# YAML format - Full options
cp examples/config.yaml config.yaml

# YAML format - Minimal setup
cp examples/config-minimal.yaml config.yaml

# JSON format - Full options
cp examples/config.json config.json

# JSON format - Minimal setup
cp examples/config-minimal.json config.json
```

2. Edit the configuration file and update:
   - MySQL connection settings (host, user, password, database)
   - Table name and primary key
   - Text source column(s)
   - API server bind address and port

3. Test the configuration:

```bash
./build/bin/mygramdb -t config.yaml
```

4. Start the server:

```bash
# With YAML
./build/bin/mygramdb config.yaml

# With JSON
./build/bin/mygramdb config.json
```

## Automatic Validation

All configuration files (both YAML and JSON) are automatically validated at startup using a built-in JSON Schema. The validation checks for:

- **Required fields**: Ensures all mandatory settings are present
- **Type checking**: Validates data types (strings, numbers, booleans, etc.)
- **Value constraints**: Checks ranges, enums, and patterns
- **Unknown keys**: Detects typos and unsupported options

If validation fails, MygramDB will display a detailed error message pointing to the exact problem in your configuration file.

**Custom Schema (Advanced):**

If you need to validate against a custom schema (e.g., for testing or extending the configuration), you can use the `--schema` option:

```bash
./build/bin/mygramdb config.yaml --schema custom-schema.json
```

## Configuration Documentation

For detailed documentation on all configuration options, see:

- [English Documentation](../docs/en/configuration.md)
- [Japanese Documentation](../docs/ja/configuration.md)

## MySQL Prerequisites

Before starting MygramDB, ensure your MySQL server has:

1. **GTID mode enabled:**

```sql
SHOW VARIABLES LIKE 'gtid_mode';
-- Should show: gtid_mode = ON
```

2. **Binary log format set to ROW:**

```sql
SHOW VARIABLES LIKE 'binlog_format';
-- Should show: binlog_format = ROW
```

3. **Replication user created:**

```sql
CREATE USER 'repl_user'@'%' IDENTIFIED BY 'your_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%';
FLUSH PRIVILEGES;
```

See [Replication Guide](../docs/en/replication.md) for more details.
