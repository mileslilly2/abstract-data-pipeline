#!/usr/bin/env python3
"""
HuggingFace Sink — push local MIDI/WAV/JSON dataset to the Hub.
Place in: plugins/midi/sink/hf_push_dataset.py
"""

from huggingface_hub import HfApi, create_repo, upload_folder
from pathlib import Path
import os

# EDIT THIS
REPO_ID = "mileslilly/processing-pipeline-step-midi-video"

PLUGIN_ROOT = Path(__file__).resolve().parents[1]     # plugins/midi
DATA_DIR = PLUGIN_ROOT / "data"                       # plugins/midi/data
MANIFEST = PLUGIN_ROOT / "manifest.json"
README = PLUGIN_ROOT / "README.md"

print(f"Using plugin root: {PLUGIN_ROOT}")

api = HfApi()

# 1. Create repo if not exists
create_repo(REPO_ID, repo_type="dataset", exist_ok=True)

# 2. Upload the data directory
if DATA_DIR.exists():
    print("Uploading data/ subtree…")
    upload_folder(
        repo_id=REPO_ID,
        folder_path=str(DATA_DIR),
        path_in_repo="data",
        repo_type="dataset",
    )
else:
    print("❌ ERROR: data/ directory not found")

# 3. Upload manifest + README
files_to_upload = []
if MANIFEST.exists():
    files_to_upload.append("manifest.json")
if README.exists():
    files_to_upload.append("README.md")

if files_to_upload:
    print("Uploading manifest & README…")
    upload_folder(
        repo_id=REPO_ID,
        folder_path=str(PLUGIN_ROOT),
        path_in_repo=".",
        allow_patterns=files_to_upload,
        repo_type="dataset",
    )

print("✔ HuggingFace push complete!")
