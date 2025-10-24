#!/usr/bin/env python3
# midi_fetcher.py
# Stream + delete MIDI fetcher that packages each dataset as a Parquet file
# and uploads to Hugging Face Datasets Hub.

import argparse, os, sys, time, json, random, shutil, tarfile, zipfile, io, requests
from pathlib import Path
from typing import Dict, Any
from huggingface_hub import HfApi, HfFolder, upload_file, whoami
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ─── CONFIG ───────────────────────────────────────────────
UA = {"User-Agent": "ADP-MIDI-Fetcher/stream-parquet-1.0"}
REQ_TIMEOUT = 60
CHUNK = 1024 * 1024
RETRIES = 3
MIDI_EXTS = (".mid", ".midi")
GLOBAL_CAP = 1000  # hard stop


LICENSE = "CC0-1.0"
DESCRIPTION = "Open MIDI datasets packaged as Parquet batches for efficient storage."

DATASETS: Dict[str, Dict[str, Any]] = {
    "groove_v1_midionly": {
        "about": "Magenta Groove (drums only)",
        "mirrors": [
            "https://storage.googleapis.com/magentadata/datasets/groove/groove-v1.0.0-midionly.zip",
        ],
        "archive_type": "zip",
    },
    "nottingham": {
        "about": "Nottingham folk/chorales",
        "mirrors": [
            "https://github.com/magenta/magenta-datasets/raw/main/nottingham/nottingham.zip",
        ],
        "archive_type": "zip",
    },
    "maestro_v3": {
        "about": "MAESTRO v3.0.0 (Magenta)",
        "mirrors": [
            "https://storage.googleapis.com/magentadata/datasets/maestro/v3.0.0/maestro-v3.0.0-midi.zip",
        ],
        "archive_type": "zip",
    },
}

# ─── HELPERS ──────────────────────────────────────────────
def log(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def head_ok(url):
    try:
        r = requests.head(url, headers=UA, allow_redirects=True, timeout=REQ_TIMEOUT)
        return r.status_code == 200
    except Exception:
        return False

def stream_download(url, out):
    with requests.get(url, headers=UA, stream=True, timeout=REQ_TIMEOUT) as r:
        r.raise_for_status()
        tmp = out.with_suffix(".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(CHUNK):
                if chunk:
                    f.write(chunk)
        tmp.replace(out)

def download(urls, out):
    if out.exists():
        log(f"↩️  Using cached {out.name}")
        return out
    for u in urls:
        log(f"→ Attempting download: {u}")
        if not head_ok(u):
            log("⚠️  HEAD check failed; skipping mirror.")
            continue
        for i in range(RETRIES):
            try:
                stream_download(u, out)
                log(f"✅ Downloaded {out.name} ({out.stat().st_size/1e6:.1f} MB)")
                return out
            except Exception as e:
                log(f"⚠️  Attempt {i+1} failed: {e}")
                time.sleep(1+i)
    raise RuntimeError("❌ All mirrors failed for " + out.name)

def extract_midis(archive: Path, kind: str, out_dir: Path, max_files: int = 100):
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    log(f"📦 Extracting {archive.name} ({kind})")
    if kind == "zip":
        with zipfile.ZipFile(archive, "r") as z:
            names = [n for n in z.namelist() if n.lower().endswith(MIDI_EXTS)]
            random.shuffle(names)
            for n in names[:max_files]:
                dest = out_dir / Path(n).name
                with z.open(n) as src, open(dest, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                paths.append(dest)
    else:
        with tarfile.open(archive, "r:gz") as t:
            names = [m for m in t.getmembers() if m.isfile() and m.name.lower().endswith(MIDI_EXTS)]
            random.shuffle(names)
            for m in names[:max_files]:
                dest = out_dir / Path(m.name).name
                with t.extractfile(m) as src, open(dest, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                paths.append(dest)
    log(f"✓ Extracted {len(paths)} files")
    return paths

def to_parquet(midi_paths, parquet_path, dataset_name):
    """Read MIDI files and write them to a Parquet file."""
    records = []
    for path in midi_paths:
        try:
            data = path.read_bytes()
            records.append({
                "filename": path.name,
                "size_kb": round(len(data)/1024, 1),
                "dataset": dataset_name,
                "midi_bytes": data,
            })
        except Exception as e:
            log(f"⚠️  Failed reading {path}: {e}")
    if not records:
        log("⚠️  No files to write.")
        return None
    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_path, compression="snappy")
    log(f"🧩 Wrote {len(df)} files → {parquet_path.name}")
    return parquet_path

def upload_to_hf_parquet(parquet_path: Path, username: str, dataset_name: str):
    """Upload parquet file to Hugging Face dataset repo."""
    HfFolder.save_token(HF_TOKEN)
    api = HfApi()
    repo_id = f"{username}/midis_parquet"
    api.create_repo(repo_id, repo_type="dataset", private=False, exist_ok=True)
    log(f"⬆️  Uploading {parquet_path.name} → {repo_id}")
    upload_file(
        path_or_fileobj=str(parquet_path),
        path_in_repo=f"{dataset_name}/{parquet_path.name}",
        repo_id=repo_id,
        repo_type="dataset",
        commit_message=f"Add {dataset_name} parquet batch",
    )
    log(f"✅ Uploaded parquet → https://huggingface.co/datasets/{repo_id}")

def cleanup(*paths):
    for p in paths:
        if p.exists():
            if p.is_dir(): shutil.rmtree(p, ignore_errors=True)
            else: p.unlink(missing_ok=True)
    log("🧹 Cleanup complete.")

# ─── PIPELINE ─────────────────────────────────────────────
def process_dataset(name, cfg, args, username, counter):
    ds_dir = Path(args.dest) / name
    arc_ext = ".zip" if cfg["archive_type"] == "zip" else ".tar.gz"
    arc_path = ds_dir / f"{name}{arc_ext}"

    try:
        arc_path.parent.mkdir(parents=True, exist_ok=True)
        arc = download(cfg["mirrors"], arc_path)
        midi_paths = extract_midis(arc, cfg["archive_type"], ds_dir, args.sample_per_dataset)
        parquet_path = ds_dir / f"{name}.parquet"
        parquet_file = to_parquet(midi_paths, parquet_path, name)
        if parquet_file:
            upload_to_hf_parquet(parquet_file, username, name)
            counter["total"] += len(midi_paths)
        cleanup(ds_dir)
    except Exception as e:
        log(f"✗ {name}: {e}")
        cleanup(ds_dir)

# ─── ENTRYPOINT ───────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Stream, pack, and upload MIDI datasets as Parquet.")
    ap.add_argument("--sample-per-dataset", type=int, default=50)
    ap.add_argument("--dest", default="data_midi")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    random.seed(args.seed)
    Path(args.dest).mkdir(exist_ok=True)
    HfFolder.save_token(HF_TOKEN)
    username = whoami(HF_TOKEN)["name"]
    log(f"👤 Using Hugging Face username: {username}")

    counter = {"total": 0}
    for name, cfg in DATASETS.items():
        if counter["total"] >= GLOBAL_CAP: break
        process_dataset(name, cfg, args, username, counter)

    log(f"✅ Finished all datasets — {counter['total']} MIDIs processed.")

if __name__ == "__main__":
    main()
