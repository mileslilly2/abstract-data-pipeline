#!/usr/bin/env python3
# push_audio_specs.py
# Uploads all generated CSV + YAML spec files to a Hugging Face dataset repo.

import os, time
from pathlib import Path
from huggingface_hub import HfApi, HfFolder, whoami, upload_file
from dotenv import load_dotenv

# ─── CONFIG ───────────────────────────────────────────────
load_dotenv()
HF_TOKEN = os.getenv("HUGGINGFACE_TOKEN")
REPO_NAME = "audio_specs"       # your dataset name on HF
BASE_DIR = Path(".")
DATA_DIRS = [BASE_DIR / "audio_data", BASE_DIR / "specs"]

def log(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def main():
    if not HF_TOKEN:
        raise SystemExit("❌ Missing HUGGINGFACE_TOKEN in environment or .env file.")

    HfFolder.save_token(HF_TOKEN)
    api = HfApi()
    username = whoami(HF_TOKEN)["name"]
    repo_id = f"{username}/{REPO_NAME}"

    api.create_repo(repo_id, repo_type="dataset", private=False, exist_ok=True)
    log(f"📦 Uploading files to: https://huggingface.co/datasets/{repo_id}")

    for directory in DATA_DIRS:
        if not directory.exists():
            log(f"⚠️  Skipping missing folder: {directory}")
            continue

        for file in directory.rglob("*"):
            if not file.is_file():
                continue
            rel = file.relative_to(BASE_DIR)
            log(f"⬆️  Uploading {rel}")
            upload_file(
                path_or_fileobj=str(file),
                path_in_repo=str(rel),
                repo_id=repo_id,
                repo_type="dataset",
                commit_message=f"Add {rel.name}",
                token=HF_TOKEN
            )

    log(f"✅ Upload complete → https://huggingface.co/datasets/{repo_id}")

if __name__ == "__main__":
    main()
