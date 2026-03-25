-- ============================================================
-- init_iceberg_catalog.sql
--
-- Creates the top-level Iceberg schemas (namespaces) used by
-- the health-iot-lakehouse platform.
--
-- Run this file via the Trino CLI after the full stack is up:
--
--   docker exec -it trino trino \
--     --catalog iceberg \
--     --file /scripts/init_iceberg_catalog.sql
--
-- Or via the seed_data.sh orchestrator script.
--
-- Schemas:
--   raw_vault      — Hubs, links, satellites (insert-only)
--   business_vault — Computed / derived satellites
--   marts          — ML feature tables and BI views
--
-- All schemas are created in the `iceberg` catalog which is
-- backed by the Iceberg REST catalog pointing at MinIO.
-- ============================================================

-- ── raw_vault ─────────────────────────────────────────────────
-- Stores the core Data Vault 2.0 entities loaded by dbt:
--   hub_patient, hub_device, hub_metric_type, hub_reading,
--   hub_alert, lnk_patient_device, lnk_device_metric,
--   lnk_reading_alert, sat_patient_demographics,
--   sat_device_spec, sat_metric_definition,
--   sat_reading_value, sat_reading_context, sat_alert_detail
create schema if not exists iceberg.raw_vault
    with (location = 's3://warehouse/raw_vault.db/');

-- ── business_vault ────────────────────────────────────────────
-- Derived / computed satellites built on top of raw_vault:
--   bsat_daily_vitals_agg, bsat_anomaly_score,
--   bsat_patient_risk_profile, bsat_data_quality_metrics
create schema if not exists iceberg.business_vault
    with (location = 's3://warehouse/business_vault.db/');

-- ── marts ─────────────────────────────────────────────────────
-- Denormalised consumption layer:
--   mart_patient_daily_features  (ML-ready wide table)
--   mart_anomaly_dashboard       (BI-ready)
create schema if not exists iceberg.marts
    with (location = 's3://warehouse/marts.db/');

-- ── Verification ──────────────────────────────────────────────
show schemas in iceberg;
