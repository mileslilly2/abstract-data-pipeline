#!/usr/bin/env python3
# Source: Download waveform specs dataset from Hugging Face

from pathlib import Path
from huggingface_hub import snapshot_download

def fetch_waveform_specs(repo_id="mileslilly/waveform_specs",
                         local_dir="waveform_specs_local") -> Path:
    print(f"[SOURCE] Downloading {repo_id} → {local_dir}")
    path = snapshot_download(repo_id=repo_id, repo_type="dataset", local_dir=local_dir)
    print(f"[SOURCE] Snapshot ready at: {path}")
    return Path(path)
