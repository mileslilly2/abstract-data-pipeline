#!/usr/bin/env python3
# hf_build_waveform_specs.py
# Pull .wav files from Hugging Face → generate CSV+YAML specs → upload paired dataset

import subprocess, tempfile, time, sys, shutil
from pathlib import Path
from huggingface_hub import list_repo_files, hf_hub_download, upload_folder

# ──────────────── CONFIG ────────────────
HF_REPO_SRC = "mileslilly/midi_enriched"
HF_REPO_OUT = "mileslilly/waveform_specs"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GEN_SPEC = PROJECT_ROOT / "midi/transform/generate_audio_specs.py"
TMP = Path("/tmp/waveform_specs_build")
AUDIO_DIR = PROJECT_ROOT / "plugins/midi/audio_data"
SPEC_DIR = PROJECT_ROOT / "plugins/midi/specs"

TMP.mkdir(exist_ok=True, parents=True)
AUDIO_DIR.mkdir(parents=True, exist_ok=True)
SPEC_DIR.mkdir(parents=True, exist_ok=True)

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

    # Clean out previous artifacts to avoid old uploads
    for d in [AUDIO_DIR, SPEC_DIR]:
        for f in d.glob("*"):
            if f.is_file(): f.unlink()

    # Process each .wav
    for i, wav_remote in enumerate(wav_files[:limit]):
        tmpdir = Path(tempfile.mkdtemp(dir=TMP))
        log(f"[{i}] Downloading {wav_remote} …")
        wav_local = Path(hf_hub_download(
            repo_id=HF_REPO_SRC,
            filename=wav_remote,
            local_dir=tmpdir,
            repo_type="dataset"
        ))

        log(f"🎨 Generating CSV+YAML specs for {wav_local.name}")
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    str(GEN_SPEC.resolve()),
                    "--dataset", str(wav_local)
           
                ],
                check=True,
                capture_output=True,
                text=True,
                cwd=PROJECT_ROOT
            )
            log(result.stdout)
        except subprocess.CalledProcessError as e:
            log(f"⚠️ Spec generation failed for {wav_local.name}")
            log(e.stderr)
            continue

    # Verify that YAMLs and CSVs match
    csvs = sorted(AUDIO_DIR.glob("*.csv"))
    yamls = sorted(SPEC_DIR.glob("*.yaml"))
    log(f"✅ Generated {len(csvs)} CSV and {len(yamls)} YAML files")

    if not csvs or not yamls:
        log("❌ Nothing to upload — check generation output.")
        return

    # Create a temporary combined folder for upload
    combined_dir = TMP / "upload_pair"
    if combined_dir.exists():
        shutil.rmtree(combined_dir)
    combined_dir.mkdir()

    for f in csvs + yamls:
        shutil.copy(f, combined_dir / f.name)

    log(f"⬆️ Uploading {len(list(combined_dir.glob('*')))} paired files to Hugging Face …")
    upload_folder(repo_id=HF_REPO_OUT, folder_path=str(combined_dir), repo_type="dataset")

    log(f"✅ Upload complete → https://huggingface.co/datasets/{HF_REPO_OUT}")

if __name__ == "__main__":
    main(limit=10)
