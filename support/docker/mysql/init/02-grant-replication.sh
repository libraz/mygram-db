#!/bin/bash
# Grants the privileges MygramDB needs from the replication account.
#
# Runs inside the MySQL container as a docker-entrypoint-initdb.d init script,
# after the image has created MYSQL_USER. A shell script rather than plain SQL
# because the account name is configurable through the environment.
#
# Without REPLICATION CLIENT the server cannot read the binary log position, so
# MygramDB fails during startup before it ever accepts a connection.

set -e

REPL_USER=${MYSQL_USER:-repl_user}
REPL_DATABASE=${MYSQL_DATABASE:-mydb}

mysql -u root -p"${MYSQL_ROOT_PASSWORD}" <<SQL
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '${REPL_USER}'@'%';
GRANT SELECT ON \`${REPL_DATABASE}\`.* TO '${REPL_USER}'@'%';
FLUSH PRIVILEGES;
SQL

echo "Granted REPLICATION SLAVE, REPLICATION CLIENT and SELECT to '${REPL_USER}'."
