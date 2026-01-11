#!/usr/bin/env bash
set -euo pipefail

# Downloads the tables used by Spider2-Snow instance "sf_bq128" into:
#   spider2-snow/resource/data/PATENTSVIEW/PATENTSVIEW/<TABLE>/
#
# Source objects live in:
#   gs://from-sf-data/sf-data/PATENTSVIEW__PATENTSVIEW/
#
# Requirements:
#   - gsutil installed and authenticated (e.g., `gcloud auth login`)
#

DATASET="PATENTSVIEW__PATENTSVIEW"
GCS_BASE="gs://from-sf-data/sf-data/${DATASET}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_BASE="${REPO_ROOT}/spider2-snow/resource/data/PATENTSVIEW/PATENTSVIEW"

TABLES=(
  "PATENT"
  "APPLICATION"
  "USPATENTCITATION"
  "CPC_CURRENT"
)

echo "GCS source:     ${GCS_BASE}"
echo "Local dest:     ${OUT_BASE}"
echo "Tables:         ${TABLES[*]}"
echo

mkdir -p "${OUT_BASE}"

gcs_glob_for_table() {
  local table="$1"
  # Avoid accidental prefix collisions like PATENT_ASSIGNEE when fetching PATENT.
  # Most exported tables follow: <TABLE>_<digit>_... .snappy.parquet
  echo "${GCS_BASE}/${table}_[0-9]*.parquet"
}

for table in "${TABLES[@]}"; do
  out_dir="${OUT_BASE}/${table}"
  mkdir -p "${out_dir}"
  gcs_glob="$(gcs_glob_for_table "${table}")"
  echo "==> Downloading ${table} (${gcs_glob})..."

  # If a table wasn't exported to GCS, don't fail the whole script.
  if ! gsutil ls "${gcs_glob}" >/dev/null 2>&1; then
    echo "    WARN: No objects matched for ${table}; skipping."
    continue
  fi

  # Note: keep the wildcard quoted so gsutil expands it (not the shell).
  # On some macOS setups, gsutil multiprocessing can fail; force a single
  # process while still allowing multi-threaded transfers.
  gsutil -m -o "GSUtil:parallel_process_count=1" cp "${gcs_glob}" "${out_dir}/"
done

echo
echo "Done."
