#!/bin/bash

# Configuration
VENV_NAME="venv"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REQUIREMENTS_FILE="requirements.txt"

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv "$PROJECT_DIR/$VENV_NAME"

# Activate virtual environment
echo "Activating virtual environment..."
source "$PROJECT_DIR/$VENV_NAME/bin/activate"

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
if [ -f "$PROJECT_DIR/$REQUIREMENTS_FILE" ]; then
    echo "Installing dependencies from $REQUIREMENTS_FILE..."
    pip install -r "$PROJECT_DIR/$REQUIREMENTS_FILE"
else
    echo "Error: $REQUIREMENTS_FILE not found. Skipping dependency installation."
    exit 1
fi

# Run Python (or your script)
echo "Entering virtual environment. Type 'deactivate' to exit."

# This python script will create a subdir checkpoints/blog
python3 generate_checkpoints.py


# Make gzip and blk-archive
tar -cf - checkpoints/blog/ | pigz > checkpoints.tar.gz

blk-stash create -a checkpoints_archive
find checkpoints/ -type f -print0 | xargs -0 blk-stash pack -a checkpoints_archive/ -j | tee checkpoints_archive_pack.json

# Dump out the sizes in bytes
du -sb checkpoints/blog/ checkpoints_archive | awk '{print $2 ": " $1 " bytes"}'
stat -c "%s bytes" checkpoints.tar.gz | sed 's/^/checkpoints.tar.gz: /'
