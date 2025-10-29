#!/usr/bin/env python3
# hf_build_waveform_specs.py
# Pull .wav files from Hugging Face → run generate_audio_specs.py → upload specs

import subprocess, tempfile, time, sys
from pathlib import Path
from huggingface_hub import list_repo_files, hf_hub_download, upload_folder

# ──────────────── CONFIG ────────────────
HF_REPO_SRC = "mileslilly/midi_enriched"
HF_REPO_OUT = "mileslilly/waveform_specs"

# 🔧 Resolve absolute paths explicitly (no relative guessing)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
GEN_SPEC = PROJECT_ROOT / "midi/transform/generate_audio_specs.py"
TMP = Path("/tmp/waveform_specs_build")
SPEC_DIR = PROJECT_ROOT / "plugins/midi/specs"

TMP.mkdir(exist_ok=True, parents=True)
SPEC_DIR.mkdir(exist_ok=True, parents=True)

def log(msg): 
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def main(limit: int = 10):
    if not GEN_SPEC.exists():
        sys.exit(f"❌ generate_audio_specs.py not found at {GEN_SPEC}")

    log(f"📦 Listing files from Hugging Face dataset: {HF_REPO_SRC}")
    all_files = list_repo_files(HF_REPO_SRC, repo_type="dataset")

    wav_files = [f for f in all_files if f.endswith('.wav')]
    log(f"🎧 Found {len(wav_files)} .wav files in dataset.")

    if not wav_files:
        log("❌ No .wav files found.")
        return

    for i, wav_remote in enumerate(wav_files[:limit]):
        tmpdir = Path(tempfile.mkdtemp(dir=TMP))
        log(f"[{i}] Downloading {wav_remote} …")
        wav_local = Path(hf_hub_download(
            repo_id=HF_REPO_SRC,
            filename=wav_remote,
            local_dir=tmpdir,
            repo_type="dataset"
        ))

        log(f"🎨 Generating specs for {wav_local.name}")
        try:
            result = subprocess.run(
                [
                    sys.executable,  # ← uses the same interpreter as you’re running this with
                    str(GEN_SPEC),
                    "--dataset", str(wav_local),
                    "--modes", "waveform", "energy"
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            log(result.stdout)
        except subprocess.CalledProcessError as e:
            log(f"⚠️ Spec generation failed for {wav_local.name}")
            log(e.stderr)
            continue

    log(f"⬆️ Uploading generated specs and CSVs to Hugging Face: {HF_REPO_OUT}")
    upload_folder(repo_id=HF_REPO_OUT, folder_path=str(SPEC_DIR), repo_type="dataset")
    log(f"✅ Upload complete → https://huggingface.co/datasets/{HF_REPO_OUT}")

if __name__ == "__main__":
    main(limit=10)
