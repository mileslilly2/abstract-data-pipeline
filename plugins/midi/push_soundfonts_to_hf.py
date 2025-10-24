#!/usr/bin/env python3
"""
push_soundfonts_to_hf.py
───────────────────────────────────────────────
Uploads all .sf2/.sf3 files from local `soundfonts/`
to a Hugging Face Datasets repo.

It does NOT generate manifests — expects:
  • soundfonts_manifest.parquet
  • soundfonts_manifest.jsonl
already created by the fetcher.
"""

from huggingface_hub import upload_folder
from pathlib import Path
import sys, time

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
REPO_ID = "mileslilly/soundfonts"
LOCAL_DIR = Path("soundfonts")
REQUIRED_FILES = [
    "soundfonts_manifest.parquet",
    "soundfonts_manifest.jsonl"
]

# ──────────────────────────────────────────────
def verify_manifests(folder: Path):
    missing = [f for f in REQUIRED_FILES if not (folder / f).exists()]
    if missing:
        print(f"❌ Missing required manifest(s): {', '.join(missing)}")
        print("Please run your fetcher first to generate them.")
        sys.exit(2)
    else:
        print(f"✅ Found required manifests in {folder}")

# ──────────────────────────────────────────────
def upload_to_hf(repo_id: str, folder: Path):
    print(f"[{time.strftime('%H:%M:%S')}] Uploading {folder} → {repo_id}")
    upload_folder(
        repo_id=repo_id,
        repo_type="dataset",
        folder_path=str(folder),
        path_in_repo="",
        commit_message=f"update soundfont dataset ({time.strftime('%Y-%m-%d')})",
        ignore_patterns=[
            "*.tmp", "*.part", "__pycache__",
            "index.json",  # avoid nested JSON confusion
            ".DS_Store",
        ],
    )
    print(f"[{time.strftime('%H:%M:%S')}] ✅ Upload complete.")
    print(f"🔗 https://huggingface.co/datasets/{repo_id}")

# ──────────────────────────────────────────────
if __name__ == "__main__":
    if not LOCAL_DIR.exists():
        print(f"❌ Missing folder: {LOCAL_DIR.resolve()}")
        sys.exit(2)

    verify_manifests(LOCAL_DIR)
    upload_to_hf(REPO_ID, LOCAL_DIR)
