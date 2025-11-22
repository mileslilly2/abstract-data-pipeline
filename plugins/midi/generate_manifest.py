#!/usr/bin/env python3
import json
from pathlib import Path

DATA_ROOT = Path("data")
manifest = {
    "version": "1.0.0",
    "description": "Machine-readable index for WAV, JSON, and CSV assets used in ADP pipelines.",
    "counts": {"csv": 0, "json": 0, "wav": 0},
    "files": []
}

# Map extension → subfolder for clarity
EXT_MAP = {
    "csv": "csv",
    "json": "json",
    "wav": "wav"
}

for ext, subdir in EXT_MAP.items():
    folder = DATA_ROOT / subdir

    if not folder.exists():
        print(f"⚠ Warning: {folder} not found. Skipping.")
        continue

    for f in folder.rglob(f"*.{ext}"):
        # Normalize path for HF (always forward slashes)
        hf_path = f.as_posix()

        manifest["files"].append({
            "type": ext,
            "name": f.name,
            "path": hf_path,          # IMPORTANT: matches exact subpath for hf_hub_download
            "size_bytes": f.stat().st_size
        })

        manifest["counts"][ext] += 1

# Write manifest.json at repo root
with open("manifest.json", "w") as fp:
    json.dump(manifest, fp, indent=2)

print("✔ manifest.json generated successfully")
print(json.dumps(manifest["counts"], indent=2))
