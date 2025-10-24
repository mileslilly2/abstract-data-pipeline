#!/usr/bin/env python3
"""
push_soundfonts_to_hf.py
----------------------------------------
Uploads all .sf2/.sf3 files from the local `soundfonts/` directory
to a Hugging Face Datasets repository, and auto-generates an
index.json manifest for easy querying.

Requires:
    pip install huggingface_hub

Usage:
    python push_soundfonts_to_hf.py
"""

from huggingface_hub import HfApi, upload_folder
from pathlib import Path
import json, os, time

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
REPO_ID = "mileslilly/soundfonts"  # ← replace with your HF username/repo
LOCAL_DIR = Path("soundfonts")    # folder containing .sf2/.sf3 files
LICENSE = "CC-BY-4.0 or compatible"  # default license note
DESCRIPTION = (
    "A collection of free & open General-MIDI and orchestral SoundFonts "
    "mirrored for educational & generative-music research."
)

# ──────────────────────────────────────────────
# BUILD INDEX
# ──────────────────────────────────────────────
def build_index(folder: Path):
    print(f"[{time.strftime('%H:%M:%S')}] Building index for {folder}")
    entries = []
    for f in sorted(folder.glob("*.sf[23]")):
        size_mb = f.stat().st_size / (1024 * 1024)
        entries.append({
            "name": f.name,
            "size_mb": round(size_mb, 2),
            "license": LICENSE,
            "local_path": str(f),
            "updated": time.strftime("%Y-%m-%d"),
        })
    index = {
        "dataset": REPO_ID,
        "description": DESCRIPTION,
        "count": len(entries),
        "soundfonts": entries,
    }
    out = folder / "index.json"
    with open(out, "w", encoding="utf-8") as fp:
        json.dump(index, fp, indent=2)
    print(f"[{time.strftime('%H:%M:%S')}] ✓ Wrote index.json with {len(entries)} entries")
    return out

# ──────────────────────────────────────────────
# UPLOAD TO HUGGING FACE
# ──────────────────────────────────────────────
def upload_to_hf(repo_id: str, folder: Path):
    print(f"[{time.strftime('%H:%M:%S')}] Uploading {folder} → {repo_id}")
    api = HfApi()
    upload_folder(
        repo_id=repo_id,
        repo_type="dataset",
        folder_path=str(folder),
        path_in_repo="",  # upload to root
        commit_message=f"update soundfont dataset ({time.strftime('%Y-%m-%d')})",
        ignore_patterns=["*.tmp", "*.part", "__pycache__"],
    )
    print(f"[{time.strftime('%H:%M:%S')}] ✅ Upload complete.")
    print(f"Browse → https://huggingface.co/datasets/{repo_id}")

# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
if __name__ == "__main__":
    LOCAL_DIR.mkdir(exist_ok=True)
    index_path = build_index(LOCAL_DIR)
    upload_to_hf(REPO_ID, LOCAL_DIR)
