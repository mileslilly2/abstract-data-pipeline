#!/usr/bin/env python3
# fetch_push_delete_soundfonts.py
# Unified pipeline: fetch → push → delete

import subprocess, shutil, time
from pathlib import Path
from huggingface_hub import upload_folder

# ---------------- CONFIG ----------------
REPO_ID = "mileslilly/soundfonts"   # your HF dataset
LOCAL_DIR = Path("soundfonts")
LICENSE = "CC-BY-4.0 or compatible"
DESCRIPTION = (
    "A collection of free & open General-MIDI and orchestral SoundFonts "
    "mirrored for educational & generative-music research."
)

# ---------------- FUNCTIONS ----------------
def run_fetcher(sample_n=3, max_archive_mb=500):
    """Call your existing soundfont_osuosl_fetcher.py."""
    print(f"[{time.strftime('%H:%M:%S')}] 🎵 Fetching {sample_n} SoundFonts from OSUOSL …")
    subprocess.run([
        "python3", "soundfont_fetcher.py"
    ], check=True)
    print(f"[{time.strftime('%H:%M:%S')}] ✅ Fetch complete.")

def build_index(folder: Path):
    """Rebuild index.json for the dataset."""
    import json
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
    print(f"[{time.strftime('%H:%M:%S')}] 🗂️  Built index.json with {len(entries)} entries")
    return out

def push_to_hf(folder: Path):
    """Upload all soundfonts to HF dataset."""
    print(f"[{time.strftime('%H:%M:%S')}] 🚀 Uploading {folder} → {REPO_ID}")
    upload_folder(
        repo_id=REPO_ID,
        repo_type="dataset",
        folder_path=str(folder),
        path_in_repo="",  # root
        commit_message=f"Auto upload ({time.strftime('%Y-%m-%d')})",
        ignore_patterns=["*.tmp", "*.part", "__pycache__"],
    )
    print(f"[{time.strftime('%H:%M:%S')}] ✅ Upload complete.")
    print(f"🔗 https://huggingface.co/datasets/{REPO_ID}")

def cleanup(folder: Path):
    """Delete all local files after upload."""
    if folder.exists():
        shutil.rmtree(folder)
        print(f"[{time.strftime('%H:%M:%S')}] 🧹 Deleted local folder {folder}")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    start = time.time()
    run_fetcher(sample_n=3)
    index_path = build_index(LOCAL_DIR)
    push_to_hf(LOCAL_DIR)
    cleanup(LOCAL_DIR)
    print(f"[{time.strftime('%H:%M:%S')}] ✅ Done in {time.time() - start:.1f}s total")