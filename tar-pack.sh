#!/usr/bin/env bash
set -Eeuo pipefail

# ------------------------------------------------------------
# tar-pack.sh
#
# Usage:
#   ./tar-pack.sh <BASE directory> <OUTPUT directory>
#
# Creates:
#   - One compressed tar per immediate nested directory
#   - One compressed tar for BASE itself
#
# Uses pigz if available, otherwise gzip.
# ------------------------------------------------------------

trap 'echo "ERROR: Script failed at line $LINENO" >&2' ERR

usage() {
  echo "Usage: $0 <BASE directory> <OUTPUT directory>" >&2
  exit 1
}

[[ $# -eq 2 ]] || usage

BASE="$1"
OUTDIR="$2"

if [[ ! -d "$BASE" ]]; then
  echo "ERROR: BASE '$BASE' is not a directory" >&2
  exit 1
fi

mkdir -p "$OUTDIR"

# Normalize paths
BASE="$(cd "$BASE" && pwd)"
OUTDIR="$(cd "$OUTDIR" && pwd)"
BASE_NAME="$(basename "$BASE")"

# Choose compressor
if command -v pigz >/dev/null 2>&1; then
  COMPRESSOR="pigz"
  COMP_EXT="gz"
  echo "Using pigz for parallel compression"
else
  COMPRESSOR="gzip"
  COMP_EXT="gz"
  echo "Using gzip"
fi

echo "Input BASE : $BASE"
echo "Output DIR : $OUTDIR"
echo

# ------------------------------------------------------------
# Function to create compressed tar
# ------------------------------------------------------------
create_tar() {
  local src_dir="$1"
  local name="$2"
  local output_file="${OUTDIR}/${name}.tar.${COMP_EXT}"

  echo "----------------------------------------"
  echo "Creating archive: $output_file"
  echo "Source directory: $src_dir"
  echo "----------------------------------------"

  # Use -C to avoid embedding absolute paths
  tar -C "$(dirname "$src_dir")" -cf - "$(basename "$src_dir")" \
    | "$COMPRESSOR" \
    > "$output_file"

  echo
}

# ------------------------------------------------------------
# Process nested directories
# ------------------------------------------------------------
while IFS= read -r -d '' dir; do
  dir_name="$(basename "$dir")"
  create_tar "$dir" "${dir_name}_archive"
done < <(find "$BASE" -mindepth 1 -maxdepth 1 -type d -print0)

# ------------------------------------------------------------
# Create BASE archive
# ------------------------------------------------------------
echo "========================================"
echo "Creating final BASE archive"
echo "========================================"

create_tar "$BASE" "${BASE_NAME}_archive"

echo
echo "All tar archives completed successfully."
