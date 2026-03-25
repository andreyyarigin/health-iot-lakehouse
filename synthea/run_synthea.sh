#!/usr/bin/env bash
# run_synthea.sh — Download and run Synthea to generate synthetic patient data.
#
# Usage:
#   bash synthea/run_synthea.sh
#
# Output:
#   synthea/output/csv/patients.csv
#   synthea/output/csv/conditions.csv
#   (and other Synthea CSVs — only patients + conditions are used by the simulator)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SYNTHEA_JAR="${SCRIPT_DIR}/synthea-with-dependencies.jar"
SYNTHEA_VERSION="3.3.0"
SYNTHEA_URL="https://github.com/synthetichealth/synthea/releases/download/v${SYNTHEA_VERSION}/synthea-with-dependencies.jar"
OUTPUT_DIR="${SCRIPT_DIR}/output"
N_PATIENTS="${N_PATIENTS:-100}"
STATE="${STATE:-Massachusetts}"
CITY="${CITY:-}"  # leave empty for random cities across the state

# ── Download jar if not present ──────────────────────────────────────────────
if [[ ! -f "${SYNTHEA_JAR}" ]]; then
    echo "[synthea] Downloading Synthea ${SYNTHEA_VERSION}..."
    echo "[synthea] URL: ${SYNTHEA_URL}"

    if command -v curl &>/dev/null; then
        curl -L --progress-bar -o "${SYNTHEA_JAR}" "${SYNTHEA_URL}"
    elif command -v wget &>/dev/null; then
        wget --progress=bar:force -O "${SYNTHEA_JAR}" "${SYNTHEA_URL}"
    else
        echo "[synthea] ERROR: Neither curl nor wget found. Install one and retry." >&2
        exit 1
    fi

    echo "[synthea] Download complete: ${SYNTHEA_JAR}"
else
    echo "[synthea] Found existing jar: ${SYNTHEA_JAR}"
fi

# ── Verify Java is available ──────────────────────────────────────────────────
if ! command -v java &>/dev/null; then
    echo "[synthea] ERROR: Java not found. Install Java 17+ and retry." >&2
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -1 | grep -oE '[0-9]+' | head -1)
if [[ "${JAVA_VERSION}" -lt 17 ]]; then
    echo "[synthea] ERROR: Java 17+ required. Found version ${JAVA_VERSION}." >&2
    exit 1
fi

# ── Create output directory ───────────────────────────────────────────────────
mkdir -p "${OUTPUT_DIR}"

# ── Build Synthea command ─────────────────────────────────────────────────────
SYNTHEA_ARGS=(
    -jar "${SYNTHEA_JAR}"
    -p "${N_PATIENTS}"
    --exporter.csv.export=true
    --exporter.fhir.export=false
    --exporter.fhir_stu3.export=false
    --exporter.fhir_dstu2.export=false
    --exporter.json.export=false
    --exporter.text.export=false
    --exporter.html.export=false
    --exporter.baseDirectory="${OUTPUT_DIR}"
    "${STATE}"
)

if [[ -n "${CITY}" ]]; then
    SYNTHEA_ARGS+=("${CITY}")
fi

echo "[synthea] Generating ${N_PATIENTS} patients in ${STATE}..."
echo "[synthea] Output directory: ${OUTPUT_DIR}"
echo "[synthea] Running: java ${SYNTHEA_ARGS[*]}"
echo ""

java "${SYNTHEA_ARGS[@]}"

# ── Verify expected files ─────────────────────────────────────────────────────
CSV_DIR="${OUTPUT_DIR}/csv"
REQUIRED_FILES=("patients.csv" "conditions.csv")
ALL_OK=true

echo ""
echo "[synthea] Checking output files..."
for fname in "${REQUIRED_FILES[@]}"; do
    fpath="${CSV_DIR}/${fname}"
    if [[ -f "${fpath}" ]]; then
        ROW_COUNT=$(( $(wc -l < "${fpath}") - 1 ))  # subtract header
        echo "[synthea]   OK  ${fpath}  (${ROW_COUNT} rows)"
    else
        echo "[synthea]   MISSING  ${fpath}" >&2
        ALL_OK=false
    fi
done

if [[ "${ALL_OK}" == false ]]; then
    echo "[synthea] ERROR: Some expected files are missing." >&2
    exit 1
fi

echo ""
echo "[synthea] Done. Use these files with the wearable simulator:"
echo "  patients.csv  → ${CSV_DIR}/patients.csv"
echo "  conditions.csv → ${CSV_DIR}/conditions.csv"
echo ""
echo "[synthea] Next step:"
echo "  python -m simulator.cli generate-day --date \$(date +%Y-%m-%d)"
