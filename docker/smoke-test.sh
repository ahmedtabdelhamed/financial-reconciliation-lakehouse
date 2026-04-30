#!/bin/sh
set -eu

apk add --no-cache curl postgresql-client netcat-openbsd >/dev/null

: "${POSTGRES_HOST:?POSTGRES_HOST is required}"
: "${POSTGRES_PORT:?POSTGRES_PORT is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${KAFKA_HOST:?KAFKA_HOST is required}"
: "${KAFKA_PORT:?KAFKA_PORT is required}"
: "${CONNECT_URL:?CONNECT_URL is required}"

export PGPASSWORD="$POSTGRES_PASSWORD"

psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c 'SELECT 1;' >/dev/null
nc -z -w 3 "$KAFKA_HOST" "$KAFKA_PORT"
curl -fsS "$CONNECT_URL/" >/dev/null

echo "Smoke test passed"
