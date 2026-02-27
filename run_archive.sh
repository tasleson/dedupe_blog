#!/usr/bin/env bash
set -Eeuo pipefail

# ------------------------------------------------------------
# blk-stash-pack.sh
#
# Usage:
#   ./blk-stash-pack.sh <BASE directory> <OUTPUT directory>
#
# Creates:
#   - One archive per immediate nested directory inside BASE
#   - One archive for BASE itself
#
# All archives and JSON output are written to OUTPUT directory.
# ------------------------------------------------------------

trap 'echo "ERROR: Script failed at line $LINENO" >&2' ERR

usage() {
  echo "Usage: $0 <BASE directory> <OUTPUT directory>" >&2
  exit 1
}

[[ $# -eq 2 ]] || usage

BASE="$1"
OUTDIR="$2"

# Validate BASE
if [[ ! -d "$BASE" ]]; then
  echo "ERROR: BASE '$BASE' is not a directory" >&2
  exit 1
fi

# Create output directory if needed
mkdir -p "$OUTDIR"

# Normalize paths
BASE="$(cd "$BASE" && pwd)"
OUTDIR="$(cd "$OUTDIR" && pwd)"
BASE_NAME="$(basename "$BASE")"

# Dependency check
command -v blk-stash >/dev/null 2>&1 || {
  echo "ERROR: blk-stash not found in PATH" >&2
  exit 1
}

echo "Input BASE  : $BASE"
echo "Output DIR  : $OUTDIR"
echo

# ------------------------------------------------------------
# Process each immediate nested directory
# ------------------------------------------------------------
while IFS= read -r -d '' dir; do
  dir_name="$(basename "$dir")"
  archive_name="${dir_name}_archive"
  archive_path="${OUTDIR}/${archive_name}"
  json_output="${OUTDIR}/${archive_name}.json"

  echo "----------------------------------------"
  echo "Creating archive: $archive_name"
  echo "Source directory: $dir"
  echo "Output location : $archive_path"
  echo "----------------------------------------"

  blk-stash create -a "$archive_path"

  find "$dir" -type f -print0 \
    | xargs -0 -r blk-stash pack -a "$archive_path" -j \
    | tee "$json_output"

  echo
done < <(find "$BASE" -mindepth 1 -maxdepth 1 -type d -print0)

# ------------------------------------------------------------
# Create archive for BASE itself
# ------------------------------------------------------------
echo "========================================"
echo "Creating final BASE archive"
echo "========================================"

base_archive="${BASE_NAME}_archive"
base_archive_path="${OUTDIR}/${base_archive}"
base_json="${OUTDIR}/${base_archive}.json"

blk-stash create -a "$base_archive_path"

find "$BASE" -type f -print0 \
  | xargs -0 -r blk-stash pack -a "$base_archive_path" -j \
  | tee "$base_json"

echo
echo "All archives completed successfully."

