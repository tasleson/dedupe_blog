run ./create.sh to generate the ready to train data.

This is nanoGPT scripted to prepare the openwebtext.

## Requirements

### Hardware
- >25GB free disk space
- Multi-core CPU recommended (for pigz compression)

### Software
- Python 3.x with venv support
- Git
- pigz (parallel gzip)
- blk-stash tool
- Python packages (installed automatically):
  - torch
  - numpy
  - transformers
  - datasets
  - tiktoken
  - wandb
  - tqdm

## Performance Reference
- **Runtime**: >30 minutes (depends on CPU/network speed)
- **Storage**: >25GB
