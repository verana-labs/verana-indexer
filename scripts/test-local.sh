#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="verana-test-pg"
PG_PORT="${TEST_PG_PORT:-5433}"
PG_USER="phamphong"
PG_PASSWORD="phamphong9981"
PG_DB="test"
PG_IMAGE="postgres:16-alpine"

cleanup() {
  echo ""
  echo "Tearing down test database container..."
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Starting ephemeral test database on port ${PG_PORT}..."
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER_NAME" \
  -e POSTGRES_USER="$PG_USER" \
  -e POSTGRES_PASSWORD="$PG_PASSWORD" \
  -e POSTGRES_DB="$PG_DB" \
  -p "${PG_PORT}:5432" \
  "$PG_IMAGE" >/dev/null

echo "Waiting for database to be ready..."
for _ in $(seq 1 60); do
  if docker exec "$CONTAINER_NAME" pg_isready -U "$PG_USER" >/dev/null 2>&1; then
    echo "Database is ready."
    break
  fi
  sleep 1
done

export NODE_ENV=test
export POSTGRES_HOST=localhost
export POSTGRES_PORT="$PG_PORT"
export POSTGRES_USER="$PG_USER"
export POSTGRES_PASSWORD="$PG_PASSWORD"
export POSTGRES_DB_TEST="$PG_DB"
export USE_HEIGHT_SYNC_CS=true
export USE_HEIGHT_SYNC_PERM=true
export USE_HEIGHT_SYNC_TR=true
export USE_HEIGHT_SYNC_TD=true

echo "Running migrations..."
pnpm run migrate:dev

echo "Running tests..."
pnpm run test-ci "$@"
