#!/usr/bin/env python3
# hf_midi_enrich_and_push_full_idempotent.py
# Build MIDI → WAV → CSV → spec triplets and push to Hugging Face dataset
# Idempotent: skips already-enriched items based on manifest presence

import tempfile, random, subprocess, yaml, soundfile as sf, librosa, numpy as np, pandas as pd, time
from pathlib import Path
from huggingface_hub import upload_folder, list_repo_files, hf_hub_download

# ─────────── CONFIG ───────────
HF_REPO_MIDI = "mileslilly/midi_files"
HF_REPO_SF   = "mileslilly/soundfonts"
HF_REPO_OUT  = "mileslilly/midi_enriched"

TMP = Path("/tmp/midi_enrich")
TMP.mkdir(exist_ok=True)

SR = 22050
HOP = 512

# ─────────── HELPERS ───────────
def log(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def list_files(repo_id, ext=None):
    """List files from a Hugging Face dataset repo."""
    files = list_repo_files(repo_id, repo_type="dataset")
    if ext:
        files = [f for f in files if f.endswith(ext)]
    return files

def run_fluidsynth(midi, sf3, wav_out):
    """Render a MIDI to WAV using FluidSynth."""
    cmd = ["fluidsynth", "-ni", str(sf3), str(midi), "-F", str(wav_out), "-r", "44100"]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return wav_out

def compute_waveform(y, sr):
    t = np.arange(len(y)) / sr
    return pd.DataFrame({"time": t, "amplitude": y})

def write_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    log(f"💾 Wrote CSV → {path}")

def write_spec(csv_path: Path, title: str):
    """Generate YAML spec for viz2video."""
    spec = {
        "chart_type": "audio_waveform",
        "data": str(csv_path.name),
        "time": "time",
        "value": "amplitude",
        "title": title,
        "palette": "Blues",
        "fps": 24,
        "out": f"out/{csv_path.stem}.mp4"
    }
    yaml_path = csv_path.with_suffix(".yaml")
    with open(yaml_path, "w") as f:
        yaml.safe_dump(spec, f)
    log(f"🧾 Spec → {yaml_path}")
    return yaml_path

def build_manifest(mid, wav, csv, yaml_file, sf3):
    """Write a YAML manifest summarizing this MIDI triplet."""
    import yaml
    info = sf.info(str(wav))
    manifest = {
        "id": mid.stem,
        "midi": mid.name,
        "wav": wav.name,
        "csv": csv.name,
        "spec": yaml_file.name,
        "soundfont": sf3.name,
        "samplerate": info.samplerate,
        "duration_sec": round(info.duration, 2)
    }
    path = wav.with_suffix(".manifest.yaml")
    with open(path, "w") as f:
        yaml.dump(manifest, f)
    log(f"📄 Manifest → {path}")
    return path

# ─────────── MAIN PIPELINE ───────────
def main():
    log("📥 Listing Hugging Face dataset assets …")
    midi_files = list_files(HF_REPO_MIDI, ".mid")
    sf3_files  = [f for f in list_files(HF_REPO_SF, ".sf3")] or list_files(HF_REPO_SF, ".sf2")
    enriched_files = list_files(HF_REPO_OUT, ".manifest.yaml")

    if not midi_files or not sf3_files:
        raise RuntimeError("Missing MIDI or SoundFont files on HF Hub.")

    done_ids = {Path(f).stem.replace(".manifest", "") for f in enriched_files}
    log(f"🎶 Found {len(midi_files)} MIDIs, {len(sf3_files)} SoundFonts, {len(done_ids)} already processed")

    for mid_remote in midi_files:
        mid_id = Path(mid_remote).stem
        if mid_id in done_ids:
            log(f"⏭️ Skipping already enriched: {mid_id}")
            continue

        sf3_remote = random.choice(sf3_files)

        with tempfile.TemporaryDirectory(dir=TMP) as tmpdir:
            tmpdir = Path(tmpdir)
            mid_local = Path(hf_hub_download(HF_REPO_MIDI, mid_remote, cache_dir=tmpdir, repo_type="dataset"))
            sf3_local = Path(hf_hub_download(HF_REPO_SF, sf3_remote, cache_dir=tmpdir, repo_type="dataset"))

            wav_local = tmpdir / f"{mid_local.stem}.wav"
            log(f"[🎧] Rendering {mid_local.name} → {wav_local.name}")
            run_fluidsynth(mid_local, sf3_local, wav_local)

            # Compute CSV + spec
            y, sr = librosa.load(wav_local, sr=SR, mono=True)
            df = compute_waveform(y, sr)
            csv_local = tmpdir / f"{mid_local.stem}.csv"
            write_csv(df, csv_local)
            yaml_local = write_spec(csv_local, mid_local.stem.replace("_", " ").title())

            manifest_local = build_manifest(mid_local, wav_local, csv_local, yaml_local, sf3_local)

            # Copy into TMP root for upload
            for f in [mid_local, wav_local, csv_local, yaml_local, manifest_local]:
                dest = TMP / f.name
                dest.write_bytes(f.read_bytes())

    log("⬆️ Uploading enriched dataset to Hugging Face …")
    upload_folder(repo_id=HF_REPO_OUT, folder_path=str(TMP), repo_type="dataset")
    log(f"✅ Upload complete → https://huggingface.co/datasets/{HF_REPO_OUT}")

if __name__ == "__main__":
    main()
