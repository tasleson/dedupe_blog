import os
import sys
import struct
import argparse
from datasets import load_dataset
from huggingface_hub import login

# -------------------------
# CLI Arguments
# -------------------------

parser = argparse.ArgumentParser(
    description="Stream The Stack and write dedup-friendly binary chunks."
)

parser.add_argument(
    "--root",
    required=True,
    help="Root directory where output will be written (files go into ROOT/code/)"
)

parser.add_argument(
    "--progress-file",
    default="progress.txt",
    help="Path to progress file (default: progress.txt)"
)

parser.add_argument(
    "--chunk-size",
    type=int,
    default=10000,
    help="Number of records per output chunk (default: 10000)"
)

parser.add_argument(
    "--size-gb",
    type=float,
    default=10.0,
    help="Target size in gigabytes to download (default: 10.0)"
)

args = parser.parse_args()

ROOT = args.root
OUTPUT_DIR = os.path.join(ROOT, "code")
PROGRESS_FILE = args.progress_file
CHUNK_SIZE = args.chunk_size
SIZE_GB = args.size_gb
TARGET_BYTES = int(SIZE_GB * 1024 * 1024 * 1024)  # Convert GB to bytes

DATASET_NAME = "bigcode/the-stack"
HF_TOKEN = os.getenv("HF_TOKEN")

# -------------------------
# Authentication
# -------------------------

if HF_TOKEN:
    login(token=HF_TOKEN)

# -------------------------
# Setup Directories
# -------------------------

os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------------
# Resume Logic
# -------------------------

start_idx = 0
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, "r") as f:
        content = f.read().strip()
        if content:
            start_idx = int(content) + 1
            print(f"Resuming from index {start_idx}...")

# -------------------------
# Load Dataset (Streaming)
# -------------------------

print(f"Loading {DATASET_NAME} in streaming mode (target: {SIZE_GB:.1f} GB)...")

try:
    ds = load_dataset(
        DATASET_NAME,
        split="train",
        streaming=True,
        token=HF_TOKEN
    )
    print(f"Dataset loaded successfully")
except Exception as e:
    print(f"ERROR loading dataset: {e}", file=sys.stderr)
    sys.exit(1)

current_chunk = start_idx // CHUNK_SIZE
rows_in_chunk = start_idx % CHUNK_SIZE
out = None
total_bytes_written = 0

# Calculate bytes already written if resuming
if start_idx > 0:
    for chunk_idx in range(current_chunk + 1):
        chunk_path = os.path.join(OUTPUT_DIR, f"stream_{chunk_idx:05d}.bin")
        if os.path.exists(chunk_path):
            total_bytes_written += os.path.getsize(chunk_path)
    print(f"Resuming: {total_bytes_written / (1024**3):.2f} GB already written")

def open_chunk(idx):
    path = os.path.join(OUTPUT_DIR, f"stream_{idx:05d}.bin")
    return open(path, "ab")

if rows_in_chunk > 0:
    out = open_chunk(current_chunk)

# -------------------------
# Processing Loop
# -------------------------


try:
    for i, row in enumerate(ds):

        if i < start_idx:
            continue

        # Check if we've reached the target size
        if total_bytes_written >= TARGET_BYTES:
            print(f"\nTarget size reached: {total_bytes_written / (1024**3):.2f} GB >= {SIZE_GB:.1f} GB")
            print(f"Processed {i} records")
            break

        if rows_in_chunk == 0:
            out = open_chunk(current_chunk)

        content = row["content"].encode("utf-8", errors="ignore")

        # 8-byte little-endian length prefix + content
        bytes_to_write = 8 + len(content)
        out.write(struct.pack("<Q", len(content)))
        out.write(content)

        total_bytes_written += bytes_to_write
        rows_in_chunk += 1

        if rows_in_chunk >= CHUNK_SIZE:
            out.close()
            current_chunk += 1
            rows_in_chunk = 0

        if i % 100 == 0:
            with open(PROGRESS_FILE, "w") as pf:
                pf.write(str(i))

        if i % 100 == 0:
            gb_written = total_bytes_written / (1024**3)
            print(f"Progress: {i} records, {gb_written:.2f} GB / {SIZE_GB:.1f} GB ({100 * gb_written / SIZE_GB:.1f}%)")

    # Final update
    with open(PROGRESS_FILE, "w") as pf:
        pf.write(str(i))

    if out:
        out.close()

    gb_written = total_bytes_written / (1024**3)
    print(f"\nCompleted successfully: {gb_written:.2f} GB written ({i} records)")

except Exception as e:
    print(f"ERROR during processing: {e}", file=sys.stderr)
    if out:
        out.close()
    sys.exit(1)
