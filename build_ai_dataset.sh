#!/usr/bin/env bash
set -euo pipefail

# Configuration
ROOT=${1:-dataset}
VENV=".venv"
CHECKPOINT_DIR=".checkpoints"

# Trap to clean up on error
trap 'echo ""; echo "ERROR: Script failed at line $LINENO. Check the error above."; exit 1' ERR

# Check for required tools
check_required_tools() {
  local missing=()

  for tool in python3 curl wget gzip; do
    if ! command -v "$tool" &> /dev/null; then
      missing+=("$tool")
    fi
  done

  if [ ${#missing[@]} -gt 0 ]; then
    echo "ERROR: Missing required tools: ${missing[*]}"
    echo "Please install them and try again."
    exit 1
  fi
}

echo "Checking required tools..."
check_required_tools

mkdir -p "$ROOT"/{web,wiki,code,synthetic,models}
mkdir -p "$CHECKPOINT_DIR"

echo "Building dataset in: $ROOT"
echo "Checkpoints stored in: $CHECKPOINT_DIR"
echo ""

########################################
# Utility: checkpoint helpers
########################################

checkpoint() {
  touch "$CHECKPOINT_DIR/$1.done"
}

is_done() {
  [ -f "$CHECKPOINT_DIR/$1.done" ]
}

########################################
# 0. PYTHON VENV SETUP
########################################

if ! is_done venv; then
  echo "Setting up Python virtual environment..."

  if [ ! -d "$VENV" ]; then
    python3 -m venv "$VENV"
  fi

  source "$VENV/bin/activate"
  pip install --upgrade pip
  pip install datasets "huggingface_hub[cli]"

  checkpoint venv
else
  source "$VENV/bin/activate"
fi

# Detect Python version for Wikipedia extraction method
PYTHON_VERSION=$($VENV/bin/python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "Python version: $PYTHON_VERSION"

PYTHON="$VENV/bin/python"

########################################
# 0.5. BUILD RUST WIKI EXTRACTOR
########################################

WIKI_EXTRACTOR="./wiki_extract"

if [ ! -f "$WIKI_EXTRACTOR" ]; then
  echo "Building Rust Wikipedia extractor..."
  cargo build --release || {
    echo "ERROR: Failed to build wiki_extract. Install Rust: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
  }
fi

########################################
# 1. COMMON CRAWL WET
########################################

if ! is_done commoncrawl; then
  echo "Downloading Common Crawl WET..."

  # Check if combined_web.txt already exists and is large enough
  COMBINED_FILE="$ROOT/web/combined_web.txt"
  if [ -f "$COMBINED_FILE" ]; then
    FILE_SIZE=$(stat -f%z "$COMBINED_FILE" 2>/dev/null || stat -c%s "$COMBINED_FILE" 2>/dev/null)
    SIZE_GB=$((FILE_SIZE / 1024 / 1024 / 1024))

    if [ "$SIZE_GB" -ge 300 ]; then
      echo "  Found existing combined_web.txt (${SIZE_GB}GB >= 300GB), marking as complete."
      checkpoint commoncrawl
    fi
  fi
fi

if ! is_done commoncrawl; then
  CRAWL="CC-MAIN-2024-10"

  if [ ! -f wet_sample.txt ]; then
    curl -f -s \
      https://data.commoncrawl.org/crawl-data/${CRAWL}/wet.paths.gz \
      -o wet.paths.gz

    # PIPEFAIL-SAFE VERSION (NO HEAD)
    gzip -dc wet.paths.gz | sed -n '1,100p' > wet_sample.txt
    rm -f wet.paths.gz
  fi

  echo "Downloading WET files..."

  while read -r path; do
    url="https://data.commoncrawl.org/$path"
    fname=$(basename "$path")

    if [ ! -f "$fname" ]; then
      echo "  Downloading $fname..."
      wget -q -c "$url" || {
        echo "  Failed to download $fname, skipping..."
        continue
      }
    fi
  done < wet_sample.txt

  echo "Combining WET files..."

  for f in *.wet.gz; do
    # Skip if no files match
    [ -e "$f" ] || continue

    # Track individual files to avoid duplicates
    if ! is_done "wet_$(basename "$f")"; then
      echo "  Processing $f..."
      zcat "$f" >> "$ROOT/web/combined_web.txt" && checkpoint "wet_$(basename "$f")"
    fi
  done

  # Only cleanup if we have successfully processed all files
  if [ -f "$ROOT/web/combined_web.txt" ] && [ -s "$ROOT/web/combined_web.txt" ]; then
    rm -f *.wet.gz wet_sample.txt
    checkpoint commoncrawl
  else
    echo "ERROR: combined_web.txt not created or empty. Check for errors above."
    exit 1
  fi
else
  echo "Common Crawl stage already complete."
fi

########################################
# 2. WIKIPEDIA
########################################

if ! is_done wikipedia; then
  echo "Downloading Wikipedia dump..."

  WIKI_FILE="enwiki-latest-pages-articles.xml.bz2"
  WIKI_URL="https://dumps.wikimedia.org/enwiki/latest/$WIKI_FILE"

  # Check if file already exists and is large enough
  if [ -f "$WIKI_FILE" ]; then
    FILE_SIZE=$(stat -f%z "$WIKI_FILE" 2>/dev/null || stat -c%s "$WIKI_FILE" 2>/dev/null)
    SIZE_GB=$((FILE_SIZE / 1024 / 1024 / 1024))

    if [ "$SIZE_GB" -ge 20 ]; then
      echo "  Found existing $WIKI_FILE (${SIZE_GB}GB >= 20GB), skipping download."
    else
      echo "  Found $WIKI_FILE but size is only ${SIZE_GB}GB, re-downloading..."
      rm -f "$WIKI_FILE"
    fi
  fi

  if [ ! -f "$WIKI_FILE" ]; then
    echo "  Downloading Wikipedia dump (this may take a while)..."
    wget -c "$WIKI_URL" || {
      echo "ERROR: Failed to download Wikipedia dump"
      exit 1
    }
  fi

  # Verify download is complete (at least check it's not empty)
  if [ ! -s "$WIKI_FILE" ]; then
    echo "ERROR: Wikipedia dump file is empty or missing"
    exit 1
  fi

  echo "Extracting Wikipedia..."

  # Better check: look for extracted files, not just AA directory
  if [ ! -f "$CHECKPOINT_DIR/wiki_extracted.done" ]; then
    echo "  Using Rust wiki_extract for extraction..."

    # Run the Rust Wikipedia extractor
    ./"$WIKI_EXTRACTOR" --input "$WIKI_FILE" --output "$ROOT/wiki"

    if [ $? -eq 0 ]; then
      checkpoint wiki_extracted
    else
      echo "ERROR: Wikipedia extraction failed"
      exit 1
    fi
  fi

  # Verify extraction created files
  if [ -d "$ROOT/wiki" ] && [ -n "$(find "$ROOT/wiki" -type f -name 'wiki_*' 2>/dev/null)" ]; then
    checkpoint wikipedia
  else
    echo "ERROR: Wikipedia extraction failed or produced no output"
    exit 1
  fi
else
  echo "Wikipedia stage already complete."
fi

########################################
# 3. THE STACK SAMPLE
########################################

if ! is_done stack; then
  echo "Downloading The Stack sample..."

  mkdir -p "$ROOT/code"

  # Use a progress file to track resumability
  PROGRESS_FILE="$CHECKPOINT_DIR/stack_progress.txt"

  # Run code_stack.py to stream and write The Stack dataset
  $PYTHON code_stack.py \
    --root "$ROOT" \
    --progress-file "$PROGRESS_FILE" \
    --chunk-size 10000 \
    --size-gb 10

  # Check if Python script succeeded
  if [ $? -eq 0 ]; then
    checkpoint stack
  else
    echo "ERROR: The Stack download failed"
    exit 1
  fi
else
  echo "The Stack stage already complete."
fi

########################################
# 4. SYNTHETIC HOT DUPLICATES
########################################

if ! is_done hotdupes; then
  echo "Creating hot duplicate cluster..."

  # Validate dependency
  BASE_FILE="$ROOT/web/combined_web.txt"
  if [ ! -f "$BASE_FILE" ] || [ ! -s "$BASE_FILE" ]; then
    echo "ERROR: $BASE_FILE not found or empty. Common Crawl stage must complete first."
    exit 1
  fi

  mkdir -p "$ROOT/synthetic/hot_duplicates"

  if [ ! -f "$ROOT/synthetic/hot_duplicates/base.txt" ]; then
    echo "  Creating base file (100MB)..."
    head -c 100M "$BASE_FILE" > "$ROOT/synthetic/hot_duplicates/base.txt"
  fi

  echo "  Creating 100 duplicates..."
  for i in {1..10}; do
    target="$ROOT/synthetic/hot_duplicates/dup_$i.txt"
    if [ ! -f "$target" ]; then
      cp "$ROOT/synthetic/hot_duplicates/base.txt" "$target"
    fi
  done

  checkpoint hotdupes
else
  echo "Hot duplicates already created."
fi

########################################
# 5. NEAR-DUPLICATES
########################################

if ! is_done neardupes; then
  echo "Creating near-duplicate variants..."

  # Validate dependency
  BASE_DUP_FILE="$ROOT/synthetic/hot_duplicates/base.txt"
  if [ ! -f "$BASE_DUP_FILE" ] || [ ! -s "$BASE_DUP_FILE" ]; then
    echo "ERROR: $BASE_DUP_FILE not found or empty. Hot duplicates stage must complete first."
    exit 1
  fi

  mkdir -p "$ROOT/synthetic/near_dupes"

  for i in {1..5}; do
    target="$ROOT/synthetic/near_dupes/variant_$i.txt"

    if [ ! -f "$target" ]; then
      echo "  Creating variant $i..."
      cp "$BASE_DUP_FILE" "$target"

      $PYTHON <<EOF
import random
import sys

random.seed(42 + $i)

path = "$target"
try:
    data = bytearray(open(path, "rb").read())
    for _ in range(len(data)//100):
        idx = random.randint(0, len(data)-1)
        data[idx] ^= 0x01
    open(path, "wb").write(data)
except Exception as e:
    print(f"ERROR: Failed to create variant $i: {e}", file=sys.stderr)
    sys.exit(1)
EOF
      if [ $? -ne 0 ]; then
        echo "ERROR: Failed to create near-duplicate variant $i"
        exit 1
      fi
    fi
  done

  checkpoint neardupes
else
  echo "Near-duplicates already created."
fi

########################################
# 6. APPEND-ONLY LOGS
########################################

if ! is_done appendlogs; then
  echo "Creating append-only logs..."

  # Validate dependency
  WEB_FILE="$ROOT/web/combined_web.txt"
  if [ ! -f "$WEB_FILE" ] || [ ! -s "$WEB_FILE" ]; then
    echo "ERROR: $WEB_FILE not found or empty. Common Crawl stage must complete first."
    exit 1
  fi

  mkdir -p "$ROOT/synthetic/append_logs"

  for i in {1..5}; do
    target="$ROOT/synthetic/append_logs/log_$i.txt"

    if [ ! -f "$target" ]; then
      echo "  Creating log file $i..."
      cp "$WEB_FILE" "$target"
      for j in {1..10}; do
        echo "LOG ENTRY $j" >> "$target"
      done
    fi
  done

  checkpoint appendlogs
else
  echo "Append logs already created."
fi

########################################
# 7. AI MODELS
########################################

if ! is_done aimodels; then
  echo "Downloading AI models (this will take a while)..."

  mkdir -p "$ROOT/models"

  # Ensure huggingface-cli is installed
  if ! command -v huggingface-cli &> /dev/null; then
    echo "Installing huggingface-cli..."
    pip install -U "huggingface_hub[cli]"
  fi

  # Define models to download
  # Format: "repo_id|description"
  MODELS=(
    "openai/clip-vit-large-patch14|CLIP ViT-L/14"
    "bert-large-uncased|BERT Large Uncased"
  )

  # Download each model
  for model_entry in "${MODELS[@]}"; do
    IFS='|' read -r repo_id description <<< "$model_entry"

    # Convert repo_id to safe directory name
    safe_name="${repo_id//\//_}"
    model_dir="$ROOT/models/$safe_name"
    checkpoint_file="$CHECKPOINT_DIR/model_${safe_name}.done"

    # Check if already downloaded
    if [ -f "$checkpoint_file" ]; then
      echo "$description ($repo_id) already downloaded"
      continue
    fi

    echo "Downloading $description ($repo_id)..."

    # Download using huggingface-cli
    if huggingface-cli download "$repo_id" \
        --local-dir "$model_dir" \
        --local-dir-use-symlinks False \
        --resume-download \
        --exclude "*.msgpack" "*.h5" "*.ot" "*.md"; then

      # Mark as downloaded
      touch "$checkpoint_file"
      echo "Downloaded $description"
    else
      echo "Warning: Failed to download $description, skipping..."
      # Mark as done anyway to skip in future runs (may be gated/access restricted)
      touch "$checkpoint_file"
    fi
  done

  checkpoint aimodels
  echo "Model downloads complete"
else
  echo "AI models already downloaded."
fi

########################################
# COMPLETION SUMMARY
########################################

echo ""
echo "========================================="
echo "Dataset build complete!"
echo "========================================="
echo ""
echo "Total dataset size:"
du -sh "$ROOT"
echo ""
echo "Breakdown by category:"
du -sh "$ROOT"/*
echo ""
echo "File counts:"
echo "  Web files: $(find "$ROOT/web" -type f 2>/dev/null | wc -l)"
echo "  Wiki files: $(find "$ROOT/wiki" -type f 2>/dev/null | wc -l)"
echo "  Code files: $(find "$ROOT/code" -type f 2>/dev/null | wc -l)"
echo "  Model files: $(find "$ROOT/models" -type f 2>/dev/null | wc -l)"
echo "  Synthetic files: $(find "$ROOT/synthetic" -type f 2>/dev/null | wc -l)"
echo ""
echo "To restart from a specific stage, remove the checkpoint:"
echo "  rm $CHECKPOINT_DIR/<stage>.done"
echo "To fully rebuild, remove all checkpoints:"
echo "  rm -rf $CHECKPOINT_DIR"
echo "========================================="
