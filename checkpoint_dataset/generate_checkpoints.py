import os
import torch
import random
import numpy as np
from transformers import (AutoTokenizer, AutoModelForMaskedLM,
                          DataCollatorForLanguageModeling)
from datasets import load_dataset
from torch.utils.data import DataLoader
from torch.optim import AdamW
from tqdm import tqdm

# ---------------------------
# Configuration
# ---------------------------

MODEL_NAME = "distilbert-base-uncased"
OUTPUT_DIR = "checkpoints/blog"
EPOCHS = 10
BATCH_SIZE = 8
SEQ_LEN = 64
LEARNING_RATE = 2e-5
SEED = 42

# ---------------------------
# Deterministic setup
# ---------------------------

torch.manual_seed(SEED)
random.seed(SEED)
np.random.seed(SEED)
#torch.use_deterministic_algorithms(True)

device = torch.device("cuda")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------
# Load tokenizer + dataset
# ---------------------------

tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

dataset = load_dataset("wikitext", "wikitext-2-raw-v1")


def tokenize(examples):
    return tokenizer(
        examples["text"],
        truncation=True,
        padding=False,
        max_length=SEQ_LEN,
    )


tokenized = dataset["train"].map(tokenize,
                                 batched=True,
                                 remove_columns=["text"])

tokenized.set_format("torch")

collator = DataCollatorForLanguageModeling(tokenizer=tokenizer,
                                           mlm=True,
                                           mlm_probability=0.15)

loader = DataLoader(tokenized,
                    batch_size=BATCH_SIZE,
                    shuffle=True,
                    collate_fn=collator)

# ---------------------------
# Load model
# ---------------------------

model = AutoModelForMaskedLM.from_pretrained(MODEL_NAME)
model.to(device)

optimizer = AdamW(model.parameters(), lr=LEARNING_RATE)

# ---------------------------
# Training Loop
# ---------------------------

for epoch in range(1, EPOCHS + 1):
    model.train()
    total_loss = 0

    for batch in tqdm(loader, desc=f"Epoch {epoch}"):
        batch = {k: v.to(device) for k, v in batch.items()}

        valid_targets = (batch["labels"] != -100).sum()
        if valid_targets == 0:
            continue

        outputs = model(**batch)
        loss = outputs.loss

        logits = outputs.logits

        if not torch.isfinite(logits).all():
            print("Non-finite logits detected!")
            print("Logits min/max:", logits.min().item(), logits.max().item())
            break

        loss.backward()

        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()
        optimizer.zero_grad()

        total_loss += loss.item()

    avg_loss = total_loss / len(loader)
    print(f"Epoch {epoch} Loss: {avg_loss:.4f}")

    checkpoint_path = os.path.join(OUTPUT_DIR, f"epoch_{epoch:03d}.pt")

    # Save model + optimizer state (important for size)
    torch.save(
        {
            "epoch": epoch,
            "model_state_dict": model.state_dict(),
            "optimizer_state_dict": optimizer.state_dict(),
            "loss": avg_loss,
        },
        checkpoint_path,
        _use_new_zipfile_serialization=False)

    print(f"Saved {checkpoint_path}")

print("Done.")
