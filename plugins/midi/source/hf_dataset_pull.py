#!/usr/bin/env python3
"""
hf_repo_downloader.py

Correct downloader for HuggingFace DATASET REPO:
    mileslilly/midi_enriched

This dataset is a directory dataset with real .wav, .mid,
.csv, .yaml files stored directly in the repository.

We download these files directly using the HuggingFace Hub
file APIs (NOT load_dataset, NOT streaming).

Features:
---------
✔ Lists all files in the dataset repo
✔ Filters by extension (wav, mid, csv, yaml)
✔ Downloads files in safe batches
✔ Saves them exactly as-is in hf_repo_files/
✔ Zero RAM issues, zero shard downloads, zero Arrow files
"""

from huggingface_hub import list_repo_files, hf_hub_download
from pathlib import Path
from tqdm import tqdm
import time

# ---------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------
REPO_ID = "mileslilly/midi_enriched"
REPO_TYPE = "dataset"  # this is CRITICAL — your repo IS a dataset repo
OUT_DIR = Path("hf_repo_files")

# Download batch size
BATCH_SIZE = 10

# File types to download
TARGET_EXTS = (".wav", ".mid", ".csv", ".yaml", ".yml")


# ---------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------
def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Listing files in HuggingFace dataset repo: {REPO_ID}\n")
    files = list_repo_files(REPO_ID, repo_type=REPO_TYPE)

    print(f"Total files in repo: {len(files)}")

    # Filter only desired file types
    targets = [f for f in files if f.lower().endswith(TARGET_EXTS)]

    print(f"Matching files ({len(targets)}):")
    for f in targets[:10]:
        print(" •", f)
    if len(targets) > 10:
        print(" ...")

    print("\nStarting download...\n")

    # Batch download
    for i in range(0, len(targets), BATCH_SIZE):
        batch = targets[i:i + BATCH_SIZE]

        print(f"Batch {i}–{i + len(batch) - 1}")
        for filename in tqdm(batch):
            hf_hub_download(
                repo_id=REPO_ID,
                repo_type=REPO_TYPE,
                filename=filename,
                local_dir=OUT_DIR,
                local_dir_use_symlinks=False,  # ensures full copies
            )

        # gentle pause to avoid rate limiting
        time.sleep(1.0)

    print("\nDONE!")
    print("Files saved to:", OUT_DIR.resolve())


if __name__ == "__main__":
    main()
