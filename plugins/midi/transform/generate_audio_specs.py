#!/usr/bin/env python3
# generate_audio_specs.py
# Convert .wav files → CSV + YAML specs in viz2video format
# Produces unified spec schema (like econ bar_race specs)

import argparse, os, sys, time, librosa, numpy as np, pandas as pd, yaml, soundfile as sf
from pathlib import Path

# ─────────────────────────── CONFIG ─────────────────────────────
ROOT = Path(__file__).resolve().parents[2] if "plugins" in str(Path(__file__).resolve()) else Path(".")
AUDIO_DIR = ROOT / "plugins/midi/audio_data"
SPEC_DIR = ROOT / "plugins/midi/specs"
AUDIO_DIR.mkdir(parents=True, exist_ok=True)
SPEC_DIR.mkdir(parents=True, exist_ok=True)

SR = 22050  # target sample rate
FPS = 24
BITRATE = "8M"
WIDTH, HEIGHT = 1080, 1920
DPI = 150

def log(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ─────────────────────────── HELPERS ────────────────────────────
def load_audio(path: Path):
    """Load WAV → mono numpy array"""
    y, sr = librosa.load(path, sr=SR, mono=True)
    return y, sr

def compute_waveform(y, sr):
    """Return time–amplitude frame table"""
    t = np.arange(len(y)) / sr
    return pd.DataFrame({"Year": t, "MedianHouseholdIncome": y})

def compute_energy(y, sr, hop=512):
    """Return time–energy RMS frame table"""
    rms = librosa.feature.rms(y=y, hop_length=hop)[0]
    t = librosa.frames_to_time(np.arange(len(rms)), sr=sr, hop_length=hop)
    return pd.DataFrame({"Year": t, "MedianHouseholdIncome": rms})

def write_csv(df: pd.DataFrame, csv_path: Path):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    log(f"💾 CSV written → {csv_path}")

def write_spec(csv_path: Path, mode: str, title: str):
    """Generate full YAML spec in econ-style format"""
    base_name = csv_path.stem
    out_dir = ROOT / "videos"
    out_dir.mkdir(exist_ok=True)

    spec = {
        "chart_type": "line" if mode == "waveform" else "bar_race",
        "data": str(csv_path.relative_to(ROOT)),
        "time": "Year",
        "category": None,
        "value": "MedianHouseholdIncome",
        "top_n": 1,
        "width": WIDTH,
        "height": HEIGHT,
        "dpi": DPI,
        "fps": FPS,
        "bitrate": BITRATE,
        "out": f"videos/{base_name}_1080x1920.mp4",
        "title": f"{title} — {{time:%Y}}",
        "x_label": "Amplitude" if mode == "waveform" else "Energy",
        "legend": False,
        "hold_frames": 1
    }

    yaml_path = SPEC_DIR / f"{base_name}.yaml"
    with open(yaml_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(spec, f, sort_keys=False)
    log(f"🧾 Spec written → {yaml_path}")

# ─────────────────────────── PIPELINE ─────────────────────────────
def process_audio(path: Path, modes: list[str]):
    try:
        with sf.SoundFile(path) as f:
            if f.format != "WAV":
                log(f"⚠️ Skipping non-WAV file: {path}")
                return
    except Exception as e:
        log(f"⚠️ Invalid WAV: {e}")
        return

    y, sr = load_audio(path)
    base = path.stem

    for mode in modes:
        try:
            log(f"🎨 Processing {base} in mode={mode}")
            if mode == "waveform":
                df = compute_waveform(y, sr)
            elif mode == "energy":
                df = compute_energy(y, sr)
            else:
                log(f"⚠️ Unknown mode: {mode}")
                continue

            csv_path = AUDIO_DIR / f"{base}_{mode}.csv"
            write_csv(df, csv_path)
            write_spec(csv_path, mode, base.replace("_", " ").title())
        except Exception as e:
            log(f"⚠️ Error during {mode} processing for {path}: {e}")

# ─────────────────────────── MAIN ─────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Generate CSV+YAML specs for viz2video from .wav audio")
    ap.add_argument("--dataset", type=str, help="Path or Hugging Face dataset", required=False)
    ap.add_argument("--modes", nargs="+", default=["waveform"], help="Modes: waveform, energy")
    ap.add_argument("--limit", type=int, default=5)
    args = ap.parse_args()

    if args.dataset and Path(args.dataset).is_file() and args.dataset.endswith(".wav"):
        paths = [Path(args.dataset)]
    elif args.dataset and Path(args.dataset).is_dir():
        paths = list(Path(args.dataset).glob("*.wav"))
    else:
        paths = list((ROOT / "plugins/midi/audio_out").glob("*.wav"))

    if not paths:
        log("❌ No WAV files found.")
        sys.exit(1)

    for path in paths[:args.limit]:
        process_audio(path, args.modes)

    log("✅ Done! Unified specs ready for viz2video.py")

if __name__ == "__main__":
    main()
