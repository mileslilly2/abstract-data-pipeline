#!/usr/bin/env python3
# run_waveform_to_sound_videos.py
# Robust runner:
# - finds .wav in snapshot (regex, case-insensitive) or local plugins/viz_videos
# - locates viz2video.py automatically (searches several candidate locations)
# - runs viz2video.py with its parent dir as cwd so relative resources work
# - moves generated mp4s to sound_videos and uploads to HF

import os
import re
import sys
import time
import shutil
import subprocess
from pathlib import Path
from typing import Optional
from tqdm import tqdm
from huggingface_hub import snapshot_download, upload_folder, HfApi

# ---------- CONFIG ----------
DATASET_IN = "mileslilly/waveform_specs"
DATASET_OUT = "mileslilly/sound_videos"
LOCAL_SPECS_DIR = Path("waveform_specs_local")   # snapshot download target
LOCAL_OUT_DIR = Path("sound_videos")             # where final mp4s are collected
LOCAL_WAV_DIR = Path("plugins/viz_videos")       # local fallback directory for .wav files (if any)
VIZ_SCRIPT_NAME = "viz2video.py"

WAV_FILENAME_PATTERN = re.compile(r'\.wav$', re.IGNORECASE)

# ---------- HELPERS ----------
def scan_snapshot_for_wavs(snapshot_dir: Path):
    return [p for p in snapshot_dir.rglob("*") if p.is_file() and WAV_FILENAME_PATTERN.search(p.name)]

def core_from_spec_stem(stem: str) -> str:
    s = re.sub(r'_(?:audio_)?(?:waveform|beats|energy|pitch_curve|spectrogram|tempo)$', '', stem, flags=re.IGNORECASE)
    return s

def find_matching_wav_in_list(core: str, wav_paths):
    core_re = re.compile(re.escape(core), re.IGNORECASE)
    for p in wav_paths:
        if core_re.search(p.name):
            return p
    return None

def find_matching_wav(snapshot_dir: Path, spec_stem: str) -> Optional[Path]:
    core = core_from_spec_stem(spec_stem)
    snapshot_wavs = scan_snapshot_for_wavs(snapshot_dir)
    match = find_matching_wav_in_list(core, snapshot_wavs)
    if match:
        return match
    # fallback local plugins/viz_videos
    if LOCAL_WAV_DIR.exists():
        local_wavs = [p for p in LOCAL_WAV_DIR.rglob("*") if p.is_file() and WAV_FILENAME_PATTERN.search(p.name)]
        match = find_matching_wav_in_list(core, local_wavs)
        if match:
            return match
    return None

def locate_viz_script(repo_dir: Path) -> Optional[Path]:
    """
    Try to locate viz2video.py:
    - repo_dir / 'plugins/viz_videos/viz2video.py'
    - './plugins/viz_videos/viz2video.py' relative to CWD
    - any file named viz2video.py under repo_dir (scan)
    - any file named viz2video.py under project root (walk up)
    Returns Path or None.
    """
    # Candidate 1: inside downloaded snapshot repo (if the repo contains the plugin)
    candidate = repo_dir / "plugins" / "viz_videos" / VIZ_SCRIPT_NAME
    if candidate.exists():
        return candidate

    # Candidate 2: relative to current working directory (common)
    candidate2 = Path.cwd() / "plugins" / "viz_videos" / VIZ_SCRIPT_NAME
    if candidate2.exists():
        return candidate2

    # Candidate 3: scan the snapshot for viz2video.py
    found = list(repo_dir.rglob(VIZ_SCRIPT_NAME))
    if found:
        return found[0]

    # Candidate 4: search upward from cwd for a plugins/viz_videos/viz2video.py
    cur = Path.cwd()
    for _ in range(6):
        cand = cur / "plugins" / "viz_videos" / VIZ_SCRIPT_NAME
        if cand.exists():
            return cand
        cur = cur.parent

    # Candidate 5: global scan within project root (last resort - may be slow)
    # use repo_dir parent as project root if repo_dir is inside project
    project_root = repo_dir.parent if repo_dir.parent.exists() else Path.cwd()
    found_global = list(project_root.rglob(VIZ_SCRIPT_NAME))
    if found_global:
        return found_global[0]

    return None

# ---------- MAIN ----------
def main():
    print(f"[INFO] Downloading dataset snapshot: {DATASET_IN}")
    repo_path_str = snapshot_download(repo_id=DATASET_IN, repo_type="dataset", local_dir=str(LOCAL_SPECS_DIR))
    repo_dir = Path(repo_path_str)
    print(f"[OK] Snapshot at: {repo_dir}")

    LOCAL_OUT_DIR.mkdir(parents=True, exist_ok=True)

    # locate viz renderer early so we can report issues before long runs
    viz_script = locate_viz_script(repo_dir)
    if viz_script:
        print(f"[INFO] Viz renderer found at: {viz_script.resolve()}")
    else:
        print(f"❌ Viz renderer '{VIZ_SCRIPT_NAME}' not found in expected locations.")
        print("    Searched snapshot, cwd, and project tree. Please ensure viz2video.py exists.")
        # don't exit immediately — we might want to only skip rendering but continue scanning to confirm wavs
        # exit now because without renderer nothing will be produced:
        sys.exit(1)

    specs = sorted(repo_dir.rglob("*.yaml"))
    print(f"[INFO] Found {len(specs)} YAML specs in snapshot.\n")

    # Pre-scan snapshot WAVs once for efficiency and logging
    snapshot_wavs = scan_snapshot_for_wavs(repo_dir)
    print(f"[INFO] Snapshot contains {len(snapshot_wavs)} WAV-like files (case-insensitive).\n")

    matched_count = 0
    processed_count = 0

    for spec_path in tqdm(specs, desc="Processing specs"):
        processed_count += 1
        spec_stem = spec_path.stem
        match = find_matching_wav(repo_dir, spec_stem)

        if not match:
            print(f"❌ No .wav found for spec: {spec_stem}")
            continue

        matched_count += 1
        try:
            rel = match.relative_to(repo_dir)
            print(f"🎵 Found .wav for {spec_stem}: snapshot/{rel}")
        except Exception:
            print(f"🎵 Found .wav for {spec_stem}: {match}")

        print(f"🎬 Running viz2video for spec: {spec_path.name}")

        # run viz2video.py with its parent directory as cwd so relative paths inside it resolve
        try:
            result = subprocess.run(
                ["python3", str(viz_script.resolve()), "--spec", str(spec_path)],
                capture_output=True,
                text=True,
                cwd=str(viz_script.parent),
                check=False
            )
            if result.stdout:
                print(f"[viz2video stdout]\n{result.stdout}")
            if result.stderr:
                print(f"[viz2video stderr]\n{result.stderr}")
            if result.returncode != 0:
                print(f"⚠️ viz2video exited with code {result.returncode} for {spec_path.name}")
                continue
        except Exception as e:
            print(f"⚠️ Exception while running viz2video for {spec_path.name}: {e}")
            continue

        # collect mp4 outputs (prefer *_with_audio.mp4)
        mp4_candidates = list(Path(".").glob("*_with_audio.mp4")) or list(Path(".").glob("*.mp4"))
        if not mp4_candidates:
            print(f"⚠️ No mp4 output found after rendering {spec_stem}")
            continue

        latest = max(mp4_candidates, key=lambda p: p.stat().st_mtime)
        dest = LOCAL_OUT_DIR / latest.name
        shutil.move(str(latest), dest)
        print(f"✅ Saved video to {dest}\n")

    # summary
    print(f"\n[SUMMARY] Processed {processed_count} specs, matched {matched_count} wavs.")

    # upload if any videos produced
    videos = list(LOCAL_OUT_DIR.glob("*.mp4"))
    if not videos:
        print("❌ No videos were produced. Nothing to upload.")
        return

    print(f"[UPLOAD] Uploading {len(videos)} videos to {DATASET_OUT}")
    api = HfApi()
    api.create_repo(repo_id=DATASET_OUT, repo_type="dataset", exist_ok=True)
    upload_folder(
        folder_path=str(LOCAL_OUT_DIR),
        path_in_repo="videos",
        repo_id=DATASET_OUT,
        repo_type="dataset",
        commit_message=f"Upload {len(videos)} videos on {time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    print(f"🚀 Upload complete: https://huggingface.co/datasets/{DATASET_OUT}")

    # cleanup
    print("\n🧹 Cleaning up local snapshot and output folder...")
    for folder in [LOCAL_SPECS_DIR, LOCAL_OUT_DIR]:
        try:
            shutil.rmtree(folder, ignore_errors=True)
            print(f"🗑️ Deleted {folder}")
        except Exception as e:
            print(f"⚠️ Failed to delete {folder}: {e}")

    print("✅ Done.")

if __name__ == "__main__":
    main()
