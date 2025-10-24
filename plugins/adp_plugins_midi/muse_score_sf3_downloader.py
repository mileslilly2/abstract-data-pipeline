#!/usr/bin/env python3
# muse_score_sf3_downloader.py
# Downloads the MuseScore_General.sf3 SoundFont from OSUOSL and logs metadata.

import time, json
from pathlib import Path
from urllib.request import urlretrieve

URL = "https://ftp.osuosl.org/pub/musescore/soundfont/MuseScore_General/MuseScore_General.sf3"
DEST_DIR = Path("soundfonts")
DEST_DIR.mkdir(exist_ok=True)
OUT_PATH = DEST_DIR / "MuseScore_General.sf3"
MANIFEST = DEST_DIR / "soundfonts_manifest.jsonl"

def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def save_manifest(entry: dict):
    with open(MANIFEST, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")

def download_soundfont():
    if OUT_PATH.exists():
        log(f"✓ {OUT_PATH.name} already exists ({OUT_PATH.stat().st_size / (1024*1024):.1f} MB)")
        return
    log(f"⬇️  Downloading MuseScore_General.sf3 …")
    urlretrieve(URL, OUT_PATH)
    size_mb = OUT_PATH.stat().st_size / (1024*1024)
    log(f"  ✓ Saved {OUT_PATH.name} ({size_mb:.1f} MB)")
    save_manifest({
        "file": str(OUT_PATH),
        "src": URL,
        "type": "sf3",
        "status": "downloaded",
        "size_mb": round(size_mb, 1)
    })

if __name__ == "__main__":
    log("MuseScore_General.sf3 downloader started …")
    download_soundfont()
    log(f"Done. File saved in: {OUT_PATH.resolve()}")
    log(f"Manifest: {MANIFEST.resolve()}")
