#!/usr/bin/env bash
# ============================================================
# init_minio_buckets.sh
#
# Creates the required MinIO buckets and seed prefixes for the
# health-iot-lakehouse platform.
#
# Expected environment variables (all supplied by docker-compose):
#   MINIO_ALIAS             — mc alias name (default: local)
#   MINIO_WAREHOUSE_BUCKET  — Iceberg warehouse bucket (default: warehouse)
#   MINIO_RAW_BUCKET        — Landing zone bucket (default: raw)
#
# This script is idempotent: re-running it when buckets already
# exist is safe (mc will exit 0 for existing buckets).
# ============================================================

set -euo pipefail

ALIAS="${MINIO_ALIAS:-local}"
WAREHOUSE="${MINIO_WAREHOUSE_BUCKET:-warehouse}"
RAW="${MINIO_RAW_BUCKET:-raw}"

# ── Helper ────────────────────────────────────────────────────
log() { echo "[init_minio] $*"; }

make_bucket() {
    local bucket="$1"
    if mc ls "${ALIAS}/${bucket}" >/dev/null 2>&1; then
        log "Bucket '${bucket}' already exists — skipping."
    else
        mc mb "${ALIAS}/${bucket}"
        log "Bucket '${bucket}' created."
    fi
}

# ── Create buckets ────────────────────────────────────────────
log "Creating bucket: ${WAREHOUSE}"
make_bucket "${WAREHOUSE}"

log "Creating bucket: ${RAW}"
make_bucket "${RAW}"

# ── Seed landing-zone prefix structure in the raw bucket ──────
# MinIO does not have real directories; we create placeholder
# objects (zero-byte .keep files) so the prefix hierarchy is
# visible in the console and tools that list prefixes.

seed_prefix() {
    local bucket="$1"
    local prefix="$2"
    local key="${ALIAS}/${bucket}/${prefix}/.keep"

    if mc ls "${key}" >/dev/null 2>&1; then
        log "Prefix '${prefix}/' already seeded — skipping."
    else
        echo -n "" | mc pipe "${key}"
        log "Seeded prefix '${bucket}/${prefix}/'."
    fi
}

log "Seeding landing-zone prefixes in '${RAW}' bucket..."
seed_prefix "${RAW}" "synthea"
seed_prefix "${RAW}" "wearable"

log "MinIO bucket initialisation complete."
log ""
log "Bucket layout:"
log "  s3://${WAREHOUSE}/    — Iceberg warehouse root"
log "  s3://${RAW}/          — Landing zone"
log "  s3://${RAW}/synthea/  — Synthea CSV exports"
log "  s3://${RAW}/wearable/ — Daily wearable simulator output"
