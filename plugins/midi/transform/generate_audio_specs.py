#!/usr/bin/env python3
# generate_audio_specs.py
# Convert .wav → CSV + YAML specs for viz2video.py (multi-chart)
# ✅ Fixed: Always writes to correct directories regardless of cwd.

import argparse, sys, time, librosa, numpy as np, pandas as pd, yaml, soundfile as sf
from pathlib import Path
from datasets import load_dataset

# ───────────────────────── CONFIG ─────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # go up to abstract-data-pipeline
AUDIO_DIR = PROJECT_ROOT / "plugins/midi/audio_data"
SPEC_DIR  = PROJECT_ROOT / "plugins/midi/specs"
AUDIO_DIR.mkdir(exist_ok=True, parents=True)
SPEC_DIR.mkdir(exist_ok=True, parents=True)
SR = 22050

def log(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ───────────────────────── HELPERS ─────────────────────────
def load_audio(path: Path):
    y, sr = librosa.load(path, sr=SR, mono=True)
    return y, sr

def write_csv(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False)
    log(f"💾 Wrote CSV → {path}")

def write_spec(csv_path: Path, chart_type: str, value_col: str, title: str):
    spec = {
        "chart_type": chart_type,
        "data": csv_path.name,
        "time": "time",
        "value": value_col,
        "width": 1080,
        "height": 1920,
        "dpi": 150,
        "fps": 24,
        "bitrate": "8M",
        "out": f"videos/{csv_path.stem}.mp4",
        "title": title,
        "legend": False,
        "hold_frames": 1,
    }
    yaml_path = SPEC_DIR / f"{csv_path.stem}.yaml"
    with open(yaml_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(spec, f, sort_keys=False)
    log(f"🧾 Spec written → {yaml_path}")

# ───────────────────────── FEATURES ─────────────────────────
def compute_waveform(y, sr):
    t = np.arange(len(y)) / sr
    return pd.DataFrame({"time": t, "amplitude": y})

def compute_energy(y, sr, hop=512):
    rms = librosa.feature.rms(y=y, hop_length=hop)[0]
    t = librosa.frames_to_time(np.arange(len(rms)), sr=sr, hop_length=hop)
    return pd.DataFrame({"time": t, "rms": rms})

def compute_spectrogram(y, sr, hop=512, n_fft=1024):
    S = librosa.amplitude_to_db(np.abs(librosa.stft(y, n_fft=n_fft, hop_length=hop)), ref=np.max)
    freqs = librosa.fft_frequencies(sr=sr, n_fft=n_fft)
    times = librosa.frames_to_time(np.arange(S.shape[1]), sr=sr, hop_length=hop)
    rows = [{"time": t, "frequency": f, "intensity": S[i, j]} for i, f in enumerate(freqs) for j, t in enumerate(times)]
    return pd.DataFrame(rows)

def compute_beats(y, sr):
    onset_env = librosa.onset.onset_strength(y=y, sr=sr)
    tempo, beats = librosa.beat.beat_track(onset_envelope=onset_env, sr=sr)
    times = librosa.frames_to_time(beats, sr=sr)
    return pd.DataFrame({"time": times, "onset_strength": onset_env[:len(times)]})

def compute_pitch(y, sr):
    f0 = librosa.yin(y, fmin=50, fmax=2000, sr=sr)
    t = librosa.frames_to_time(np.arange(len(f0)), sr=sr)
    return pd.DataFrame({"time": t, "frequency": f0})

def compute_tempo(y, sr):
    oenv = librosa.onset.onset_strength(y=y, sr=sr)
    tempos = librosa.beat.tempo(onset_envelope=oenv, sr=sr, aggregate=None)
    times = librosa.frames_to_time(np.arange(len(tempos)), sr=sr)
    return pd.DataFrame({"time": times, "tempo": tempos})

# ───────────────────────── PIPELINE ─────────────────────────
def process_audio(path: Path):
    y, sr = load_audio(path)
    base = path.stem

    modes = {
        "audio_waveform": (compute_waveform, "amplitude"),
        "audio_energy": (compute_energy, "rms"),
        "audio_spectrogram": (compute_spectrogram, "intensity"),
        "audio_beats": (compute_beats, "onset_strength"),
        "audio_pitch_curve": (compute_pitch, "frequency"),
        "audio_tempo": (compute_tempo, "tempo"),
    }

    for chart_type, (func, value_col) in modes.items():
        try:
            df = func(y, sr)
            csv_path = AUDIO_DIR / f"{base}_{chart_type}.csv"
            write_csv(df, csv_path)
            title = f"{base.replace('_', ' ').title()} — {chart_type.replace('audio_', '').title()}"
            write_spec(csv_path, chart_type, value_col, title)
        except Exception as e:
            log(f"⚠️ Failed {chart_type} for {base}: {e}")

# ───────────────────────── MAIN ─────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", help="Folder or HF dataset with .wav", required=True)
    ap.add_argument("--limit", type=int, default=5)
    args = ap.parse_args()

    if Path(args.dataset).is_dir():
        paths = list(Path(args.dataset).glob("*.wav"))
    else:
        paths = [Path(args.dataset)]

    if not paths:
        log("❌ No audio files found.")
        sys.exit(1)

    for p in paths[:args.limit]:
        process_audio(p)

    log("✅ Done! Specs + CSVs ready for viz2video.py")

if __name__ == "__main__":
    main()
