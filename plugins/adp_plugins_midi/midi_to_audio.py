#!/usr/bin/env python3
# midi_to_audio_hf2hf.py
# Render MIDI → Audio using MIDIs + SoundFonts streamed directly from two HF datasets
import argparse, subprocess, tempfile, os
from pathlib import Path
from datasets import load_dataset

def save_temp_file(data_field, filename_hint, suffix):
    """Save streamed binary data to a temporary file and return its Path."""
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(data_field.read())
        tmp_path = Path(tmp.name)
    print(f"🗂️  Cached {filename_hint} at {tmp_path}")
    return tmp_path

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
    ap.add_argument("--midi-dataset", required=True, help="HF dataset containing .mid files")
    ap.add_argument("--soundfont-dataset", required=True, help="HF dataset containing .sf2/.sf3 files")
    ap.add_argument("--midi-split", default="train")
    ap.add_argument("--sf-split", default="train")
    ap.add_argument("--sample-midi", type=int, default=5)
    ap.add_argument("--sample-sf", type=int, default=3)
    ap.add_argument("--rate", type=int, default=44100)
    ap.add_argument("--outdir", default="audio_out")
    args = ap.parse_args()

    outdir = Path(args.outdir)

    print(f"🎶 Loading MIDI dataset: {args.midi_dataset}")
    ds_midis = load_dataset(args.midi_dataset, split=args.midi_split)
    midis = ds_midis.shuffle(seed=42).select(range(min(args.sample_midi, len(ds_midis))))

    print(f"🎹 Loading SoundFont dataset: {args.soundfont_dataset}")
    ds_sfs = load_dataset(args.soundfont_dataset, split=args.sf_split)
    soundfonts = ds_sfs.shuffle(seed=42).select(range(min(args.sample_sf, len(ds_sfs))))

    for sf_row in soundfonts:
        # Create a temporary SoundFont file
        suffix = ".sf3" if sf_row["filename"].endswith(".sf3") else ".sf2"
        tmp_sf = save_temp_file(sf_row["file"], sf_row["filename"], suffix)

        for midi_row in midis:
            tmp_midi = save_temp_file(midi_row["file"], midi_row["filename"], ".mid")
            out = outdir / f"{Path(midi_row['filename']).stem}_{Path(sf_row['filename']).stem}.wav"
            midi_to_audio(tmp_midi, tmp_sf, out, args.rate)
            os.remove(tmp_midi)

        os.remove(tmp_sf)
        print(f"🧹 Deleted temporary {tmp_sf}")

if __name__ == "__main__":
    main()
