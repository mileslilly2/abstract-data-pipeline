#!/usr/bin/env python3
# generate_audio_specs.py
# Convert .wav files (local or HF dataset) → CSV + YAML specs for viz2video.py

import argparse, os, sys, time, json, librosa, librosa.display, numpy as np, pandas as pd, yaml
from pathlib import Path
from datasets import load_dataset
import soundfile as sf  # ✅ for validation

# ─────────────────────────── CONFIG ─────────────────────────────
ROOT = Path(__file__).resolve().parents[2] if "plugins" in str(Path(__file__).resolve()) else Path(".")
AUDIO_DIR = ROOT / "plugins/midi/audio_data"
SPEC_DIR  = ROOT / "plugins/midi/specs"
SR = 22050  # target sample rate
MAX_SAMPLES = 5
AUDIO_DIR.mkdir(exist_ok=True, parents=True)
SPEC_DIR.mkdir(exist_ok=True, parents=True)

def log(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ─────────────────────────── HELPERS ────────────────────────────
def load_audio(path_or_bytes):
    """Load a WAV file from path or bytes → mono, resampled"""
    if isinstance(path_or_bytes, (str, Path)):
        y, sr = librosa.load(path_or_bytes, sr=SR, mono=True)
    else:
        import io
        y, sr = sf.read(io.BytesIO(path_or_bytes))
        if len(y.shape) > 1: y = np.mean(y, axis=1)
        if sr != SR: y = librosa.resample(y, orig_sr=sr, target_sr=SR)
    return y, SR

def write_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    log(f"💾 Wrote CSV → {path}")

def write_spec(csv_path: Path, mode: str, title: str):
    """Generate YAML spec for viz2video.py (audio_waveform renderer)."""
    chart_type = "audio_waveform" if mode in ["waveform", "energy", "pitch", "tempo"] else "choropleth"
    value_map = {
        "waveform": "amplitude",
        "energy": "rms",
        "spectrogram": "intensity",
        "beats": "onset_strength",
        "tempo": "tempo",
        "pitch": "frequency"
    }

    spec = {
        "chart_type": chart_type,
        "data": str(csv_path),
        "time": "time",
        "value": value_map.get(mode, "value"),
        "title": title,
        "palette": "Blues" if mode in ["waveform", "energy"] else "Reds",
        "fps": 24,
        "out": f"out/{csv_path.stem}.mp4"
    }

    out_yaml = SPEC_DIR / f"{csv_path.stem}.yaml"
    if out_yaml.exists():
        log(f"⏩ Spec already exists, skipping: {out_yaml.name}")
        return
    with open(out_yaml, "w", encoding="utf-8") as f:
        yaml.safe_dump(spec, f, sort_keys=False)
    log(f"🧾 Spec written → {out_yaml}")

# ─────────────────────────── COMPUTATIONS ─────────────────────────
def compute_waveform(y, sr):
    t = np.arange(len(y)) / sr
    return pd.DataFrame({"time": t, "amplitude": y})

def compute_energy(y, sr, hop=512):
    rms = librosa.feature.rms(y=y, hop_length=hop)[0]
    t = librosa.frames_to_time(np.arange(len(rms)), sr=sr, hop_length=hop)
    return pd.DataFrame({"time": t, "rms": rms})

# ─────────────────────────── PIPELINE ─────────────────────────────
def process_audio(path: Path, modes: list[str]):
    """Validate and process WAV into CSV/spec."""
    try:
        with sf.SoundFile(path) as f:
            if f.format != "WAV":
                log(f"⚠️ Skipping non-WAV file: {path}")
                return
    except Exception as e:
        log(f"⚠️ Cannot open {path}: {e}")
        return

    try:
        y, sr = load_audio(path)
    except Exception as e:
        log(f"⚠️ librosa failed to load {path}: {e}")
        return

    base = path.stem
    for mode in modes:
        try:
            csv_path = AUDIO_DIR / f"{base}_{mode}.csv"
            if csv_path.exists():
                log(f"⏩ CSV already exists, skipping: {csv_path.name}")
                continue

            log(f"🎨 Processing {base} in mode={mode}")
            if mode == "waveform":
                df = compute_waveform(y, sr)
            elif mode == "energy":
                df = compute_energy(y, sr)
            else:
                log(f"⚠️ Unknown mode: {mode}")
                continue

            write_csv(df, csv_path)
            write_spec(csv_path, mode, f"{base.replace('_', ' ').title()} ({mode.title()})")
        except Exception as e:
            log(f"⚠️ Error during {mode} generation for {path}: {e}")

# ─────────────────────────── MAIN ─────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Generate CSV+specs for viz2video from audio.")
    ap.add_argument("--dataset", help="Hugging Face dataset or local folder with .wav files", required=False)
    ap.add_argument("--modes", nargs="+", default=["waveform"], help="Modes to generate")
    ap.add_argument("--limit", type=int, default=5)
    args = ap.parse_args()

    if args.dataset and args.dataset.endswith(".wav"):
        paths = [Path(args.dataset)]
    elif args.dataset and Path(args.dataset).exists():
        paths = list(Path(args.dataset).glob("*.wav"))
    else:
        paths = list((ROOT / "plugins/midi/audio_out").glob("*.wav"))

    if not paths:
        log("❌ No audio files found.")
        sys.exit(1)

    for path in paths[:args.limit]:
        process_audio(path, args.modes)

    log("✅ Done! Specs ready for viz2video.py.")

if __name__ == "__main__":
    main()
