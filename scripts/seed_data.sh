#!/usr/bin/env bash
# ============================================================
# seed_data.sh
#
# Orchestrates all Phase 1 initialisation steps:
#   1. Wait for the full stack to be healthy
#   2. Create MinIO buckets (via minio-init container)
#   3. Create Iceberg schemas via Trino SQL
#
# Usage (from repo root, after `docker compose up -d`):
#   bash scripts/seed_data.sh
#
# Options:
#   --skip-minio-init   Skip bucket creation (already done)
#   --skip-iceberg-init Skip Iceberg schema creation
# ============================================================

set -euo pipefail

SKIP_MINIO_INIT=false
SKIP_ICEBERG_INIT=false

for arg in "$@"; do
    case "$arg" in
        --skip-minio-init)   SKIP_MINIO_INIT=true ;;
        --skip-iceberg-init) SKIP_ICEBERG_INIT=true ;;
    esac
done

# ── Helpers ───────────────────────────────────────────────────
log()  { echo "[seed_data] $*"; }
error(){ echo "[seed_data] ERROR: $*" >&2; exit 1; }

wait_for_service() {
    local name="$1"
    local url="$2"
    local max_attempts="${3:-30}"
    local attempt=1

    log "Waiting for ${name} at ${url}..."
    until curl -sf "${url}" >/dev/null 2>&1; do
        if [[ ${attempt} -ge ${max_attempts} ]]; then
            error "${name} did not become healthy after ${max_attempts} attempts."
        fi
        log "  ${name} not ready yet (attempt ${attempt}/${max_attempts}) — retrying in 5s..."
        sleep 5
        ((attempt++))
    done
    log "${name} is ready."
}

# ── 1. Wait for core services ─────────────────────────────────
log "=== Phase 1: health checks ==="

wait_for_service "MinIO"        "http://localhost:9010/minio/health/live"
wait_for_service "Iceberg REST" "http://localhost:8181/v1/config"
# Trino /v1/info returns a JSON doc once the server is up
wait_for_service "Trino"        "http://localhost:8090/v1/info" 40

# ── 2. MinIO bucket creation ──────────────────────────────────
if [[ "${SKIP_MINIO_INIT}" == "false" ]]; then
    log ""
    log "=== Phase 2: MinIO bucket initialisation ==="
    if docker ps --format '{{.Names}}' | grep -q "^minio-init$"; then
        log "minio-init container already exists — checking state..."
        state=$(docker inspect --format='{{.State.Status}}' minio-init 2>/dev/null || echo "missing")
        if [[ "${state}" == "exited" ]]; then
            exit_code=$(docker inspect --format='{{.State.ExitCode}}' minio-init)
            if [[ "${exit_code}" == "0" ]]; then
                log "minio-init already completed successfully — skipping."
            else
                error "minio-init exited with code ${exit_code}. Check logs: docker logs minio-init"
            fi
        else
            log "minio-init is in state '${state}' — waiting for it to finish..."
            docker wait minio-init
        fi
    else
        log "Running minio-init container..."
        docker compose run --rm minio-init
    fi
else
    log "Skipping MinIO bucket init (--skip-minio-init)."
fi

# ── 3. Iceberg schema creation via Trino ─────────────────────
if [[ "${SKIP_ICEBERG_INIT}" == "false" ]]; then
    log ""
    log "=== Phase 3: Iceberg schema creation ==="

    # Copy the SQL file into the Trino container and execute it
    log "Copying init_iceberg_catalog.sql into Trino container..."
    docker cp "$(dirname "$0")/init_iceberg_catalog.sql" trino:/tmp/init_iceberg_catalog.sql

    log "Executing schema creation SQL via Trino CLI..."
    docker exec trino trino \
        --output-format TSV \
        --file /tmp/init_iceberg_catalog.sql

    log "Iceberg schemas created successfully."
else
    log "Skipping Iceberg schema init (--skip-iceberg-init)."
fi

# ── Done ──────────────────────────────────────────────────────
log ""
log "=== Seed complete ==="
log ""
log "  MinIO console : http://localhost:9011"
log "  Iceberg REST  : http://localhost:8181/v1/config"
log "  Trino UI      : http://localhost:8090"
log "  Airflow UI    : http://localhost:8082"
log ""
log "Verify end-to-end with:"
log "  docker exec -it lh-trino trino --execute \"SHOW SCHEMAS IN iceberg\""
