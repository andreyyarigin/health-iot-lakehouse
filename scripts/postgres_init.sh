#!/usr/bin/env bash
# ============================================================
# postgres_init.sh
# Run by PostgreSQL's docker-entrypoint-initdb.d mechanism.
# Creates the Airflow metadata database alongside the default
# Iceberg catalog database configured via POSTGRES_DB env var.
# ============================================================

# AIRFLOW_POSTGRES_PASSWORD is injected by docker-compose from .env
AIRFLOW_PW="${AIRFLOW_POSTGRES_PASSWORD:-}"

if [ -z "$AIRFLOW_PW" ]; then
  echo "ERROR: AIRFLOW_POSTGRES_PASSWORD is not set" >&2
  exit 1
fi

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  DO \$\$
  BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') THEN
      CREATE USER airflow WITH PASSWORD '${AIRFLOW_PW}';
    END IF;
  END\$\$;

  SELECT 'CREATE DATABASE airflow_metadata OWNER airflow'
  WHERE NOT EXISTS (
      SELECT FROM pg_database WHERE datname = 'airflow_metadata'
  )\gexec
EOSQL
