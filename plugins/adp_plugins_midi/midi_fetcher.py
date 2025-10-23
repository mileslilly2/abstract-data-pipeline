#!/usr/bin/env python3
# midi_fetcher.py
# Unified MIDI dataset fetcher / sampler
# - Modes: sample | batch
# - Hard global cap: 1000 MIDIs total
# - Works with multiple open datasets
# - Mirror fallback, retries, size limit, idempotent

import argparse, os, sys, time, json, random, shutil, tarfile, zipfile
from pathlib import Path
from typing import List, Dict, Any, Optional
import requests

# ---------------- CONFIG ----------------
UA = {"User-Agent": "ADP-MIDI-Fetcher/2.2"}
REQ_TIMEOUT = 60
CHUNK = 1024 * 1024
RETRIES = 3
MIDI_EXTS = (".mid", ".midi")
GLOBAL_CAP = 5000   # stop after this many extracted MIDIs

DATASETS: Dict[str, Dict[str, Any]] = {
    # --- Small reliable sets ---
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
    "jsb_chorales": {
        "about": "Bach Chorales (JSB)",
        "mirrors": [
            "https://github.com/czhuang/JSB-Chorales-dataset/archive/refs/heads/master.zip",
        ],
        "archive_type": "zip",
    },
    "musicnet_midis": {
        "about": "MusicNet reference MIDIs (Zenodo)",
        "mirrors": [
            "https://zenodo.org/records/5120004/files/musicnet_midis.tar.gz",
        ],
        "archive_type": "tar.gz",
    },
    # --- Medium / large sets ---
    "lakh_midi_sample": {
        "about": "Lakh MIDI (sample subset)",
        "mirrors": [
            "https://huggingface.co/datasets/colinraffel/lakh-midi/resolve/main/midifiles_sample.zip",
        ],
        "archive_type": "zip",
    },
    "maestro_v3": {
        "about": "MAESTRO v3.0.0 (Magenta)",
        "mirrors": [
            "https://huggingface.co/datasets/magenta/maestro-v3/resolve/main/maestro-v3.0.0-midi.zip",
            "https://storage.googleapis.com/magentadata/datasets/maestro/v3.0.0/maestro-v3.0.0-midi.zip",
        ],
        "archive_type": "zip",
    },
    # --- Community mirrors (optional) ---
    "nes_music_db": {
        "about": "NES Music Database (converted)",
        "mirrors": [
            "https://archive.org/download/NESMusicDatabase/NESMusicDatabase-MIDI.zip",
        ],
        "archive_type": "zip",
    },
}

# --------------- HELPERS ----------------
def log(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)
def head_ok(url):
    try:
        r = requests.head(url, headers=UA, allow_redirects=True, timeout=REQ_TIMEOUT)
        return r.status_code == 200
    except Exception:
        return False
def stream_download(url, out, max_bytes):
    with requests.get(url, headers=UA, stream=True, timeout=REQ_TIMEOUT) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length") or 0)
        if max_bytes and total and total > max_bytes:
            raise RuntimeError(f"Too large ({total/1e6:.1f} MB > limit)")
        tmp = out.with_suffix(".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(CHUNK):
                if chunk: f.write(chunk)
        tmp.replace(out)

def download(urls, out, max_bytes):
    if out.exists(): return out
    for u in urls:
        log(f"→ {u}")
        if not head_ok(u): continue
        for i in range(RETRIES):
            try:
                stream_download(u, out, max_bytes)
                log(f"✓ {out.name} downloaded ({out.stat().st_size/1e6:.1f} MB)")
                return out
            except Exception as e:
                log(f"⚠️  {e}")
                time.sleep(1+i)
    raise RuntimeError("All mirrors failed")

def list_midis(path, typ):
    if typ == "zip":
        with zipfile.ZipFile(path) as z:
            return [n for n in z.namelist() if n.lower().endswith(MIDI_EXTS)]
    else:
        with tarfile.open(path, "r:gz") as t:
            return [m.name for m in t.getmembers() if m.isfile() and m.name.lower().endswith(MIDI_EXTS)]

def extract_subset(path, typ, dest, chosen):
    count = 0
    if typ == "zip":
        with zipfile.ZipFile(path) as z:
            for name in chosen:
                with z.open(name) as s, open(dest/Path(name).name,"wb") as f:
                    shutil.copyfileobj(s, f, CHUNK)
                count += 1
    else:
        with tarfile.open(path, "r:gz") as t:
            for name in chosen:
                m = t.getmember(name)
                with t.extractfile(m) as s, open(dest/Path(name).name,"wb") as f:
                    shutil.copyfileobj(s, f, CHUNK)
                count += 1
    return count

# --------------- MAIN ----------------
def run_dataset(name, cfg, dest, args, counter):
    if counter["total"] >= GLOBAL_CAP:
        log(f"🚫 Cap reached ({GLOBAL_CAP}), skipping {name}")
        return
    ds_dir = dest/name
    ds_dir.mkdir(parents=True, exist_ok=True)
    ext = ".zip" if cfg["archive_type"]=="zip" else ".tar.gz"
    arc = ds_dir/f"{name}{ext}"

    try:
        arc = download(cfg["mirrors"], arc, args.max_bytes)
        files = list_midis(arc, cfg["archive_type"])
        if not files:
            log(f"⚠️ No MIDIs found in {name}")
            return
        if args.mode=="sample":
            random.shuffle(files)
            files = files[:args.sample_per_dataset]
        remaining = GLOBAL_CAP - counter["total"]
        files = files[:remaining]
        n = extract_subset(arc, cfg["archive_type"], ds_dir, files)
        counter["total"] += n
        log(f"✓ {name}: {n} extracted (total={counter['total']})")
    except Exception as e:
        log(f"✗ {name}: {e}")

def parse_size(s):
    if not s: return None
    s=s.lower()
    mult={"kb":1024,"mb":1024**2,"gb":1024**3}
    for k,v in mult.items():
        if s.endswith(k): return int(float(s[:-len(k)])*v)
    return int(s)

def main():
    ap=argparse.ArgumentParser(description="Download/sample public MIDI datasets")
    ap.add_argument("--mode",choices=["sample","batch"],default="sample")
    ap.add_argument("--sample-per-dataset",type=int,default=50)
    ap.add_argument("--dest",default="data_midi")
    ap.add_argument("--max-bytes",default=None)
    ap.add_argument("--only",nargs="*")
    ap.add_argument("--skip",nargs="*")
    ap.add_argument("--seed",type=int,default=42)
    args=ap.parse_args()
    random.seed(args.seed)
    args.max_bytes=parse_size(args.max_bytes)
    dest=Path(args.dest); dest.mkdir(exist_ok=True)
    selected=list(DATASETS.keys())
    if args.only: selected=[k for k in selected if k in args.only]
    if args.skip: selected=[k for k in selected if k not in args.skip]
    counter={"total":0}
    for name in selected:
        if counter["total"]>=GLOBAL_CAP: break
        run_dataset(name, DATASETS[name], dest, args, counter)
    log(f"✅ Done — {counter['total']} MIDIs total (limit {GLOBAL_CAP}).")

if __name__=="__main__":
    main()
