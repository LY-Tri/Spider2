#!/usr/bin/env bash
set -euo pipefail

# Downloads the full GITHUB_REPOS dataset export from GCS into:
#   spider2-snow/resource/data/GITHUB_REPOS/GITHUB_REPOS/
#
# Source:
#   gs://from-sf-data/sf-data/GITHUB_REPOS__GITHUB_REPOS/
#
# Requirements:
#   - gsutil installed and authenticated (e.g., `gcloud auth login`)
#
# Notes:
#   - Uses `gsutil rsync` so re-running is incremental/resumable.
#   - Forces single-process mode to avoid macOS multiprocessing issues.
#

DATASET="GITHUB_REPOS__GITHUB_REPOS"
GCS_BASE="gs://from-sf-data/sf-data/${DATASET}/"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_BASE="${REPO_ROOT}/spider2-snow/resource/data/GITHUB_REPOS/GITHUB_REPOS"

echo "GCS source:     ${GCS_BASE}"
echo "Local dest:     ${OUT_BASE}"
echo

mkdir -p "${OUT_BASE}"

# Resume-friendly mirroring (no deletes).
gsutil -m -o "GSUtil:parallel_process_count=1" rsync -r "${GCS_BASE}" "${OUT_BASE}/"

echo
echo "Done."
