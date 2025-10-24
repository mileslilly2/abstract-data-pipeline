#!/usr/bin/env python3
# midi_to_audio.py
# Render MIDI → Audio using MIDIs + SoundFonts stored as raw files on Hugging Face

import argparse, subprocess, tempfile, os
from pathlib import Path
from huggingface_hub import list_repo_files, hf_hub_download

def midi_to_audio(midi_path: Path, soundfont: Path, out_path: Path, rate: int = 44100):
    """Render one MIDI file with one SoundFont via FluidSynth CLI."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "fluidsynth", "-ni",
        str(soundfont), str(midi_path),
        "-F", str(out_path), "-r", str(rate)
    ]
    subprocess.run(cmd, check=True)
    print(f"✓ {midi_path.name} → {out_path.name} using {soundfont.name}")

def main():
    ap = argparse.ArgumentParser(description="Render Hugging Face MIDI + SoundFont datasets via FluidSynth.")
    ap.add_argument("--midi-dataset", required=True, help="HF dataset containing raw .mid files")
    ap.add_argument("--soundfont-dataset", required=True, help="HF dataset containing .sf2/.sf3 files")
    ap.add_argument("--sample-midi", type=int, default=5)
    ap.add_argument("--sample-sf", type=int, default=3)
    ap.add_argument("--rate", type=int, default=44100)
    ap.add_argument("--outdir", default="audio_out")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # ─── List and download MIDI files ──────────────────────────────
    print(f"🎶 Listing MIDI files from {args.midi_dataset}")
    midi_files = [
        f for f in list_repo_files(args.midi_dataset, repo_type="dataset")
        if f.lower().endswith((".mid", ".midi"))
    ]
    if not midi_files:
        raise SystemExit(f"No MIDI files found in {args.midi_dataset}")

    # ─── List and download SoundFonts ─────────────────────────────
    print(f"🎹 Listing SoundFonts from {args.soundfont_dataset}")
    sf_files = [
        f for f in list_repo_files(args.soundfont_dataset, repo_type="dataset")
        if f.lower().endswith((".sf2", ".sf3"))
    ]
    if not sf_files:
        raise SystemExit(f"No SoundFonts found in {args.soundfont_dataset}")

    # ─── Sample selections ────────────────────────────────────────
    import random
    random.shuffle(midi_files)
    random.shuffle(sf_files)
    midi_files = midi_files[: args.sample_midi]
    sf_files = sf_files[: args.sample_sf]

    # ─── Download and render ──────────────────────────────────────
    for sf_file in sf_files:
        sf_local = Path(hf_hub_download(args.soundfont_dataset, sf_file, repo_type="dataset"))
        for midi_file in midi_files:
            midi_local = Path(hf_hub_download(args.midi_dataset, midi_file, repo_type="dataset"))
            out = outdir / f"{Path(midi_local.name).stem}_{Path(sf_local.name).stem}.wav"
            midi_to_audio(midi_local, sf_local, out, args.rate)

    print(f"✅ Done — rendered audio written to {outdir.resolve()}")

if __name__ == "__main__":
    main()
