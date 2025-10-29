#!/usr/bin/env python3
# hf_upload_waveform_specs.py
# Upload paired CSV + YAML spec files for waveform visualizations to Hugging Face

import time, shutil, sys
from pathlib import Path
from huggingface_hub import upload_folder

# ───────────── CONFIG ─────────────
HF_REPO_OUT = "mileslilly/waveform_specs"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
print(PROJECT_ROOT)
AUDIO_DIR = PROJECT_ROOT / "midi/audio_data"
print(AUDIO_DIR)
SPEC_DIR  = PROJECT_ROOT / "midi/specs"
print(SPEC_DIR)


TMP_DIR   = Path("/tmp/waveform_specs_upload")

def log(msg): 
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ───────────── HELPERS ─────────────
def prepare_upload_folder() -> Path:
    """Combine CSV + YAML outputs into one temporary folder."""
    if TMP_DIR.exists():
        shutil.rmtree(TMP_DIR)
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    csvs = sorted(AUDIO_DIR.glob("*.csv"))
    yamls = sorted(SPEC_DIR.glob("*.yaml"))

    if not csvs or not yamls:
        log("❌ No CSV or YAML files found — run generation first.")
        sys.exit(1)

    for f in csvs + yamls:
        shutil.copy(f, TMP_DIR / f.name)

    log(f"📦 Prepared {len(csvs)} CSV + {len(yamls)} YAML files in {TMP_DIR}")
    return TMP_DIR

# ───────────── MAIN ─────────────
def main():
    upload_path = prepare_upload_folder()
    log(f"⬆️ Uploading combined dataset to Hugging Face: {HF_REPO_OUT}")
    upload_folder(repo_id=HF_REPO_OUT, folder_path=str(upload_path), repo_type="dataset")
    log(f"✅ Upload complete → https://huggingface.co/datasets/{HF_REPO_OUT}")

    # Optional cleanup
    try:
        shutil.rmtree(upload_path)
        log("🧹 Cleaned up temporary upload folder.")
    except Exception as e:
        log(f"⚠️ Could not remove {upload_path}: {e}")

if __name__ == "__main__":
    main()
