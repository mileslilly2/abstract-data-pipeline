#!/usr/bin/env python3
# hf_midi_enrich_and_push_full_idempotent.py
# Build MIDI → WAV → CSV → multiple specs (waveform, energy, spectrogram, beats, pitch, tempo)
# and push all to Hugging Face. Idempotent.

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
    files = list_repo_files(repo_id, repo_type="dataset")
    return [f for f in files if not ext or f.endswith(ext)]

def run_fluidsynth(midi, sf3, wav_out):
    cmd = ["fluidsynth", "-ni", str(sf3), str(midi), "-F", str(wav_out), "-r", "44100"]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return wav_out

# ─────────── FEATURE COMPUTATIONS ───────────
def compute_waveform(y, sr): 
    t = np.arange(len(y)) / sr
    return pd.DataFrame({"time": t, "amplitude": y})

def compute_energy(y, sr, hop=HOP): 
    rms = librosa.feature.rms(y=y, hop_length=hop)[0]
    t = librosa.frames_to_time(np.arange(len(rms)), sr=sr, hop_length=hop)
    return pd.DataFrame({"time": t, "rms": rms})

def compute_spectrogram(y, sr, hop=HOP, n_fft=1024): 
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

# ─────────── CSV + SPEC OUTPUT ───────────
def write_csv(df, path):
    df.to_csv(path, index=False)
    log(f"💾 CSV → {path}")

def write_spec(csv_path, chart_type, value_col, title):
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
        "hold_frames": 1
    }
    yaml_path = csv_path.with_suffix(".yaml")
    with open(yaml_path, "w") as f:
        yaml.safe_dump(spec, f, sort_keys=False)
    log(f"🧾 Spec → {yaml_path}")
    return yaml_path

# ─────────── MANIFEST BUILDER ───────────
def build_manifest(mid, wav, csvs, yamls, sf3):
    info = sf.info(str(wav))
    manifest = {
        "id": mid.stem,
        "midi": mid.name,
        "wav": wav.name,
        "csv": [p.name for p in csvs],
        "specs": [p.name for p in yamls],
        "soundfont": sf3.name,
        "samplerate": info.samplerate,
        "duration_sec": round(info.duration, 2)
    }
    out_path = wav.with_suffix(".manifest.yaml")
    with open(out_path, "w") as f:
        yaml.dump(manifest, f)
    log(f"📄 Manifest → {out_path}")
    return out_path

# ─────────── MAIN PIPELINE ───────────
def main():
    log("📥 Checking existing Hugging Face datasets …")
    midi_files = list_files(HF_REPO_MIDI, ".mid")
    sf3_files  = list_files(HF_REPO_SF, ".sf3") or list_files(HF_REPO_SF, ".sf2")
    enriched   = list_files(HF_REPO_OUT, ".manifest.yaml")

    if not midi_files or not sf3_files:
        raise RuntimeError("Missing MIDI or SoundFont files.")

    done_ids = {Path(f).stem.replace(".manifest", "") for f in enriched}
    log(f"🎶 {len(midi_files)} MIDI, {len(sf3_files)} SF, {len(done_ids)} done")

    for mid_remote in midi_files:
        mid_id = Path(mid_remote).stem
        if mid_id in done_ids:
            log(f"⏭️ Skip {mid_id}")
            continue

        sf3_remote = random.choice(sf3_files)
        with tempfile.TemporaryDirectory(dir=TMP) as tmpdir:
            tmpdir = Path(tmpdir)
            mid_local = Path(hf_hub_download(HF_REPO_MIDI, mid_remote, cache_dir=tmpdir, repo_type="dataset"))
            sf3_local = Path(hf_hub_download(HF_REPO_SF, sf3_remote, cache_dir=tmpdir, repo_type="dataset"))
            wav_local = tmpdir / f"{mid_local.stem}.wav"

            log(f"[🎧] Rendering {mid_local.name}")
            run_fluidsynth(mid_local, sf3_local, wav_local)

            y, sr = librosa.load(wav_local, sr=SR, mono=True)
            charts = {
                "audio_waveform": (compute_waveform, "amplitude"),
                "audio_energy": (compute_energy, "rms"),
                "audio_spectrogram": (compute_spectrogram, "intensity"),
                "audio_beats": (compute_beats, "onset_strength"),
                "audio_pitch_curve": (compute_pitch, "frequency"),
                "audio_tempo": (compute_tempo, "tempo")
            }

            csvs, yamls = [], []
            for ctype, (func, val) in charts.items():
                df = func(y, sr)
                csv_path = tmpdir / f"{mid_local.stem}_{ctype}.csv"
                write_csv(df, csv_path)
                yaml_path = write_spec(csv_path, ctype, val, f"{mid_local.stem.title()} — {ctype.replace('audio_', '').title()}")
                csvs.append(csv_path)
                yamls.append(yaml_path)

            manifest_local = build_manifest(mid_local, wav_local, csvs, yamls, sf3_local)

            for f in [mid_local, wav_local, *csvs, *yamls, manifest_local]:
                (TMP / f.name).write_bytes(f.read_bytes())

    log("⬆️ Uploading all enriched files …")
    upload_folder(repo_id=HF_REPO_OUT, folder_path=str(TMP), repo_type="dataset")
    log(f"✅ Done → https://huggingface.co/datasets/{HF_REPO_OUT}")

if __name__ == "__main__":
    main()
