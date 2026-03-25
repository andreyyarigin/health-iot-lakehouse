# health-iot-lakehouse

Lakehouse platform for wearable health data with 24-hour health anomaly prediction.

Vitals (heart rate, SpO2, steps, skin temperature, glucose) flow from raw files to ML-ready features through a Data Vault 2.0 model on Apache Iceberg. The full stack runs locally via Docker Compose.

---

## Stack

| Layer | Technology |
|-------|-----------|
| Object storage | **MinIO** (S3-compatible) |
| Table format | **Apache Iceberg** + REST Catalog (PostgreSQL backend) |
| Compute engine | **Trino** |
| Transformations | **dbt-trino** |
| Orchestration | **Apache Airflow** (CeleryExecutor) + Redis |
| Data catalog | **OpenMetadata** |
| Data generation | **Synthea** + custom Python simulator |

---

## Architecture

![Architecture](docs/plantuml/architecture.png)

---

## Repository structure

```
health-iot-lakehouse/
├── docker-compose.yml                # Main stack
├── docker-compose.openmetadata.yml   # OpenMetadata stack
├── .env.example                      # Environment variables template
├── docs/
│   ├── ARCHITECTURE.md               # Component descriptions and data flow
│   ├── DATA_VAULT_MODEL.md           # Data Vault 2.0 entity reference
│   └── ДОКУМЕНТАЦИЯ.md               # Full documentation in Russian
├── simulator/                        # Python wearable simulator
├── synthea/output/csv/               # Pre-generated synthetic patients (10)
├── airflow/dags/                     # daily_ingest, dbt_raw_vault, dbt_business_vault
├── dbt_project/models/               # 22 models: staging → raw_vault → business_vault → marts
├── openmetadata/ingestion/           # Trino and dbt ingestion configs
├── notebooks/                        # Feature exploration, ML prototype
├── scripts/                          # Stack initialization scripts
└── trino/                            # Trino config + Iceberg connector
```

---

## ML use case

**Goal:** predict anomalous vital events (arrhythmia, hypoxemia, hypo/hyperglycemia) 24 hours ahead based on wearable data patterns.

**Features** from `mart_patient_daily_features`:
- Patient demographics: age, sex, BMI, chronic conditions
- Daily vitals: mean/std/min/max of HR, SpO2, steps, skin temperature, glucose
- Temporal patterns: 7-day rolling averages, day-over-day deltas
- Risk profile: risk tier, anomaly events in the last 30 days

**Target:** binary label `anomaly_next_24h`

**Reproducibility:** Iceberg snapshots pin the training dataset to a specific data version.

---

## Daily data flow

![Data Flow](docs/plantuml/data_flow.png)

---

## Quick start

**Requirements:** Docker Desktop (≥ 8 GB RAM), Python 3.11+

```bash
# 1. Start the main stack
cp .env.example .env
# Fill in passwords in .env, then:
docker compose up -d

# 2. Initialize schemas and seed data
bash scripts/seed_data.sh

# 3. Generate simulator data
pip install -r simulator/requirements.txt
python -m simulator.cli generate-day --date 2026-03-25

# 4. Run dbt models
cd dbt_project
TRINO_PORT=8090 dbt deps && dbt run && dbt test

# 5. Start OpenMetadata (optional, needs extra RAM)
docker compose -f docker-compose.openmetadata.yml up -d
```

---

## Service endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| **OpenMetadata** | http://localhost:8585 | admin@openmetadata.org / admin |
| **Airflow** | http://localhost:8082 | admin / admin |
| **Trino UI** | http://localhost:8090 | admin (no password) |
| **MinIO Console** | http://localhost:9011 | from .env |
| Iceberg REST | http://localhost:8181 | — |
| PostgreSQL | localhost:5433 | from .env |

---

## Data Vault model

![Data Vault Model](docs/plantuml/data_vault_model.png)

---

## Data quality

- dbt tests: range checks (HR 30–250, SpO2 70–100%), not-null, unique, referential integrity — 206 tests passing
- Freshness monitoring: devices silent >24h flagged as stale
- Audit columns on every satellite row: `load_datetime`, `record_source`, `hash_diff`
- Full lineage graph in OpenMetadata: source CSV → staging → raw vault → marts

---

## License

MIT
