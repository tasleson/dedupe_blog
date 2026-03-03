#!/bin/bash

git clone https://github.com/karpathy/nanoGPT && cd nanoGPT

# Configuration
VENV_NAME="venv"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv "$PROJECT_DIR/$VENV_NAME"

# Activate virtual environment
echo "Activating virtual environment..."
source "$PROJECT_DIR/$VENV_NAME/bin/activate"

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

pip install torch numpy transformers datasets tiktoken wandb tqdm

# Run Python (or your script)
echo "Entering virtual environment. Type 'deactivate' to exit."

# preparing the data
python data/openwebtext/prepare.py


# Create the tarfile
tar -cf - data/openwebtext/train.bin | pigz > train.tar.gz

# Create archive
blk-stash create -a train_archive
blk-stash pack -a train_archive data/openwebtext/train.bin

# Dump numbers
du -sb data/openwebtext/train.bin
du -sb train.tar.gz
du -sb train_archive/

