#!/usr/bin/env python3
# midi_to_audio.py
# Generic MIDI → Audio converter (WAV/MP3) using FluidSynth
import argparse, subprocess
from pathlib import Path

def midi_to_audio(midi_path: Path, soundfont: Path, out_path: Path, rate: int = 44100):
    """Render MIDI to audio via FluidSynth CLI."""
    midi_path, soundfont, out_path = map(Path, [midi_path, soundfont, out_path])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "fluidsynth", "-ni",
        str(soundfont), str(midi_path),
        "-F", str(out_path), "-r", str(rate)
    ]
    subprocess.run(cmd, check=True)
    print(f"✓ Rendered {midi_path.name} → {out_path}")

def main():
    ap = argparse.ArgumentParser(description="Convert MIDI to audio using FluidSynth")
    ap.add_argument("midi", help="Path to MIDI file or folder")
    ap.add_argument("--soundfont", default="FluidR3_GM.sf2", help="Path to .sf2 soundfont")
    ap.add_argument("--outdir", default="audio_out", help="Output directory")
    ap.add_argument("--rate", type=int, default=44100)
    args = ap.parse_args()

    midi = Path(args.midi)
    outdir = Path(args.outdir)
    if midi.is_dir():
        for f in midi.glob("*.mid"):
            out = outdir / (f.stem + ".wav")
            midi_to_audio(f, Path(args.soundfont), out, args.rate)
    else:
        out = outdir / (midi.stem + ".wav")
        midi_to_audio(midi, Path(args.soundfont), out, args.rate)

if __name__ == "__main__":
    main()
