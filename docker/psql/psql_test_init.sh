#!/bin/bash
set -e

if [ -z "$POSTGRES_USER" ] || [ -z "$POSTGRES_DB" ] || [ -z "$POSTGRES_DB_TEST" ]; then
    echo "Error: Required environment variables not set"
    echo "POSTGRES_USER: ${POSTGRES_USER:-not set}"
    echo "POSTGRES_DB: ${POSTGRES_DB:-not set}"
    echo "POSTGRES_DB_TEST: ${POSTGRES_DB_TEST:-not set}"
    exit 1
fi

echo "Initializing test database '$POSTGRES_DB_TEST'..."

DB_EXISTS=$(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -tc "SELECT 1 FROM pg_database WHERE datname = '$POSTGRES_DB_TEST'" | xargs)

if [ "$DB_EXISTS" = "1" ]; then
    echo "Test database '$POSTGRES_DB_TEST' already exists."
else
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        CREATE DATABASE "$POSTGRES_DB_TEST";
        GRANT ALL PRIVILEGES ON DATABASE "$POSTGRES_DB_TEST" TO "$POSTGRES_USER";
EOSQL
    echo "Test database '$POSTGRES_DB_TEST' created successfully."
fi
