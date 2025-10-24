#!/usr/bin/env python3
# generate_audio_specs.py
# Convert .wav files (local or HF dataset) → CSV + YAML specs for viz2video.py
# Supports waveform, energy, spectrogram, beats, tempo, and pitch modes.

import argparse, os, sys, time, json, librosa, librosa.display, numpy as np, pandas as pd, yaml
from pathlib import Path
from datasets import load_dataset

# ─────────────────────────── CONFIG ─────────────────────────────
SR = 22050  # target sample rate for analysis
MAX_SAMPLES = 5  # limit for HF streaming mode
AUDIO_DIR = Path("audio_data")
SPEC_DIR = Path("specs")
AUDIO_DIR.mkdir(exist_ok=True, parents=True)
SPEC_DIR.mkdir(exist_ok=True, parents=True)

def log(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ─────────────────────────── HELPERS ────────────────────────────
def load_audio(path_or_bytes):
    """Load a WAV file from path or bytes → mono, resampled"""
    if isinstance(path_or_bytes, (str, Path)):
        y, sr = librosa.load(path_or_bytes, sr=SR, mono=True)
    else:
        import io, soundfile as sf
        y, sr = sf.read(io.BytesIO(path_or_bytes))
        if len(y.shape) > 1: y = np.mean(y, axis=1)
        if sr != SR: y = librosa.resample(y, orig_sr=sr, target_sr=SR)
    return y, SR

def write_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    log(f"💾 Wrote CSV → {path}")

def write_spec(csv_path: Path, mode: str, title: str):
    """Generate YAML spec for viz2video.py"""
    spec = {
        "chart_type": "line" if mode in ["waveform", "energy", "pitch", "tempo"] else "choropleth",
        "data": str(csv_path),
        "time": "time",
        "value": "amplitude" if mode == "waveform" else (
            "rms" if mode == "energy" else (
                "intensity" if mode == "spectrogram" else (
                    "onset_strength" if mode == "beats" else "frequency"
                ))),
        "title": title,
        "palette": "Blues" if mode == "waveform" else "Reds",
        "fps": 24,
        "out": f"out/{csv_path.stem}.mp4"
    }
    out_yaml = SPEC_DIR / f"{csv_path.stem}.yaml"
    out_yaml.parent.mkdir(parents=True, exist_ok=True)
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

def compute_spectrogram(y, sr, hop=512, n_fft=1024):
    S = librosa.amplitude_to_db(np.abs(librosa.stft(y, n_fft=n_fft, hop_length=hop)), ref=np.max)
    freqs = librosa.fft_frequencies(sr=sr, n_fft=n_fft)
    times = librosa.frames_to_time(np.arange(S.shape[1]), sr=sr, hop_length=hop)
    rows = []
    for i, f in enumerate(freqs):
        for j, t in enumerate(times):
            rows.append({"time": t, "frequency": f, "intensity": S[i, j]})
    return pd.DataFrame(rows)

def compute_beats(y, sr):
    onset_env = librosa.onset.onset_strength(y=y, sr=sr)
    tempo, beats = librosa.beat.beat_track(onset_envelope=onset_env, sr=sr)
    times = librosa.frames_to_time(beats, sr=sr)
    return pd.DataFrame({"time": times, "onset_strength": onset_env[:len(times)]})

def compute_tempo(y, sr):
    oenv = librosa.onset.onset_strength(y=y, sr=sr)
    tempos = librosa.beat.tempo(onset_envelope=oenv, sr=sr, aggregate=None)
    times = librosa.frames_to_time(np.arange(len(tempos)), sr=sr)
    return pd.DataFrame({"time": times, "tempo": tempos})

def compute_pitch(y, sr, frame_length=2048, hop_length=256):
    f0 = librosa.yin(y, fmin=50, fmax=2000, sr=sr, frame_length=frame_length, hop_length=hop_length)
    t = librosa.frames_to_time(np.arange(len(f0)), sr=sr, hop_length=hop_length)
    return pd.DataFrame({"time": t, "frequency": f0})

# ─────────────────────────── PIPELINE ─────────────────────────────
def process_audio(path: Path, modes: list[str]):
    y, sr = load_audio(path)
    base = path.stem
    for mode in modes:
        log(f"🎨 Processing {base} in mode={mode}")
        if mode == "waveform":
            df = compute_waveform(y, sr)
        elif mode == "energy":
            df = compute_energy(y, sr)
        elif mode == "spectrogram":
            df = compute_spectrogram(y, sr)
        elif mode == "beats":
            df = compute_beats(y, sr)
        elif mode == "tempo":
            df = compute_tempo(y, sr)
        elif mode == "pitch":
            df = compute_pitch(y, sr)
        else:
            log(f"⚠️ Unknown mode: {mode}")
            continue

        csv_path = AUDIO_DIR / f"{base}_{mode}.csv"
        write_csv(df, csv_path)
        write_spec(csv_path, mode, f"{base.replace('_', ' ').title()} ({mode.title()})")

# ─────────────────────────── MAIN ─────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Generate CSV+specs for viz2video from audio.")
    ap.add_argument("--dataset", help="Hugging Face dataset or local folder with .wav files", required=False)
    ap.add_argument("--modes", nargs="+", default=["waveform"], help="Modes to generate (waveform, energy, spectrogram, beats, tempo, pitch)")
    ap.add_argument("--limit", type=int, default=5)
    args = ap.parse_args()

    if args.dataset and args.dataset.endswith(".wav"):
        paths = [Path(args.dataset)]
    elif args.dataset and Path(args.dataset).exists():
        paths = list(Path(args.dataset).glob("*.wav"))
    elif args.dataset:
        log(f"🎧 Loading from HF dataset: {args.dataset}")
        ds = load_dataset(args.dataset, split="train", streaming=True)
        paths = []
        for i, row in enumerate(ds):
            if i >= args.limit: break
            tmp = Path(f"tmp_{i}.wav")
            tmp.write_bytes(row["audio"]["bytes"] if "audio" in row else row["file"].read())
            paths.append(tmp)
    else:
        paths = list(Path("audio_out").glob("*.wav"))

    if not paths:
        log("❌ No audio files found.")
        sys.exit(1)

    for path in paths:
        process_audio(path, args.modes)

    log("✅ Done! Specs ready for viz2video.py.")

if __name__ == "__main__":
    main()
