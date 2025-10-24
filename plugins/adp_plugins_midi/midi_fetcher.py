#!/usr/bin/env python3
# midi_fetcher.py
# Fetch MIDI datasets and upload raw .mid/.midi files to Hugging Face in subfolders (no Parquet).

import argparse, os, time, random, shutil, zipfile, tarfile, requests
from pathlib import Path
from typing import Dict, Any, List
from huggingface_hub import HfApi, HfFolder, upload_file, whoami
from dotenv import load_dotenv

# ─── CONFIG ───────────────────────────────────────────────
load_dotenv()
HF_TOKEN = os.getenv("HUGGINGFACE_TOKEN")  # must be set
UA = {"User-Agent": "ADP-MIDI-Fetcher/RawFiles-1.1"}
REQ_TIMEOUT = 120
CHUNK = 1024 * 1024
RETRIES = 3
MIDI_EXTS = (".mid", ".midi")
GLOBAL_CAP = 100000  # big cap; your --sample-per-dataset limits per dataset

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
def log(msg): 
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def try_head(url: str):
    """Best-effort HEAD (never blocks download)."""
    try:
        r = requests.head(url, headers=UA, allow_redirects=True, timeout=10)
        return r.status_code
    except Exception:
        return None

def stream_download(url: str, out: Path):
    """Download with GET (no HEAD gating), with .part atomic rename."""
    with requests.get(url, headers=UA, stream=True, timeout=REQ_TIMEOUT) as r:
        if r.status_code != 200:
            raise RuntimeError(f"GET {url} -> HTTP {r.status_code}")
        tmp = out.with_suffix(out.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(CHUNK):
                if chunk:
                    f.write(chunk)
        tmp.replace(out)

def download(urls: List[str], out: Path) -> Path:
    if out.exists() and out.stat().st_size > 0:
        log(f"↩️  Using cached {out.name} ({out.stat().st_size/1e6:.1f} MB)")
        return out

    last_err = None
    for u in urls:
        hs = try_head(u)
        if hs:
            log(f"ℹ️  HEAD {u} -> HTTP {hs}")
        for i in range(RETRIES):
            try:
                log(f"→ GET {u} (attempt {i+1}/{RETRIES}) …")
                stream_download(u, out)
                log(f"✅ Downloaded {out.name} ({out.stat().st_size/1e6:.1f} MB)")
                return out
            except Exception as e:
                last_err = e
                log(f"⚠️  {u} attempt {i+1} failed: {e}")
                time.sleep(1 + i)
    raise RuntimeError(f"❌ All mirrors failed for {out.name}: {last_err}")

def extract_midis(archive: Path, kind: str, out_dir: Path, max_files: int) -> List[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []
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
        mode = "r:*" if archive.suffix == ".tar" else "r:gz"
        with tarfile.open(archive, mode) as t:
            members = [m for m in t.getmembers() if m.isfile() and m.name.lower().endswith(MIDI_EXTS)]
            random.shuffle(members)
            for m in members[:max_files]:
                dest = out_dir / Path(m.name).name
                src = t.extractfile(m)
                if src:
                    with open(dest, "wb") as dst:
                        shutil.copyfileobj(src, dst)
                    paths.append(dest)
    log(f"✓ Extracted {len(paths)} MIDI files")
    return paths

def upload_midis(repo_id: str, dataset_name: str, midi_paths: List[Path]):
    api = HfApi()
    api.create_repo(repo_id, repo_type="dataset", private=False, exist_ok=True)

    uploaded = 0
    for p in midi_paths:
        rel_path = f"{dataset_name}/{p.name}"
        upload_file(
            path_or_fileobj=str(p),
            path_in_repo=rel_path,
            repo_id=repo_id,
            repo_type="dataset",
            commit_message=f"Add {rel_path}",
        )
        uploaded += 1
        if uploaded % 25 == 0:
            log(f"… {uploaded} uploaded to {repo_id}/{dataset_name}/")

    log(f"✅ Uploaded {uploaded} files → {repo_id}/{dataset_name}")

# ─── PIPELINE ─────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Fetch and upload raw MIDI files to HF (no Parquet).")
    ap.add_argument("--sample-per-dataset", type=int, default=50)
    ap.add_argument("--dest", default="data_midi")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    if not HF_TOKEN:
        raise SystemExit("HUGGINGFACE_TOKEN env var is not set.")

    random.seed(args.seed)
    root = Path(args.dest)
    root.mkdir(parents=True, exist_ok=True)

    HfFolder.save_token(HF_TOKEN)
    username = whoami(HF_TOKEN)["name"]
    repo_id = f"{username}/midi_files"
    log(f"👤 HF user: {username}")
    log(f"📦 Target repo: {repo_id}")

    total = 0
    for name, cfg in DATASETS.items():
        if total >= GLOBAL_CAP:
            break
        ds_dir = root / name
        ds_dir.mkdir(parents=True, exist_ok=True)

        ext = ".zip" if cfg["archive_type"] == "zip" else ".tar.gz"
        arc_path = ds_dir / f"{name}{ext}"

        try:
            download(cfg["mirrors"], arc_path)
            midi_paths = extract_midis(arc_path, cfg["archive_type"], ds_dir, args.sample_per_dataset)
            if not midi_paths:
                log(f"⚠️  No MIDIs extracted for {name}")
            else:
                upload_midis(repo_id, name, midi_paths)
                total += len(midi_paths)
        except Exception as e:
            log(f"✗ {name} failed: {e}")
        finally:
            # clean local temp files so reruns are clean
            shutil.rmtree(ds_dir, ignore_errors=True)

    log(f"✅ Done. Uploaded {total} MIDI files total.")
    log(f"🔗 https://huggingface.co/datasets/{repo_id}")

if __name__ == "__main__":
    main()
