#!/usr/bin/env python3
"""
midi_to_json.py — Transform: MIDI → JSON event specs (FLEX v2)

Changes in v2:
--------------
✔ Regex-based discovery of MIDI files (case-insensitive).
✔ Recursively scans ANY directory structure.
✔ Handles .mid and .midi regardless of filenames.
✔ Safe: ignores bad/corrupted files with warnings.
✔ More robust path defaults.

Usage:
    python plugins/midi/transform/midi_to_json.py \
        --input hf_repo_files \
        --output plugins/midi/specs/midi_events
"""

import argparse
import json
import logging
import re
from pathlib import Path
from typing import Dict, Any, List

from mido import MidiFile

# ---------------------------------------------------------------------
# Flexible regex for MIDI detection
# ---------------------------------------------------------------------
MIDI_REGEX = re.compile(r".*\.(mid|midi)$", re.IGNORECASE)


# ---------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------
# Detect the project root: transform/ → midi/ → plugins/ → project root
PROJECT_ROOT = Path(__file__).resolve().parents[3]

DEFAULT_INPUT = PROJECT_ROOT / "plugins" / "midi" / "source" / "hf_repo_files"
DEFAULT_OUTPUT = PROJECT_ROOT / "plugins" / "midi" / "data" / "json"


# ---------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------
def extract_events(midi_path: Path) -> Dict[str, Any]:
    """Extract note on/off events with timestamps."""
    try:
        midi = MidiFile(midi_path)
    except (OSError, IOError, MidiFileError) as e:
        logging.error("Failed to load MIDI: %s (%s)", midi_path, e)
        return None

    time_sec = 0.0
    events: List[Dict[str, Any]] = []

    for msg in midi:
        time_sec += msg.time
        if msg.type == "note_on" and msg.velocity > 0:
            events.append(
                {
                    "type": "note_on",
                    "pitch": msg.note,
                    "velocity": msg.velocity,
                    "time": float(time_sec),
                }
            )
        elif msg.type == "note_off" or (msg.type == "note_on" and msg.velocity == 0):
            events.append(
                {
                    "type": "note_off",
                    "pitch": msg.note,
                    "time": float(time_sec),
                }
            )

    return {
        "source": str(midi_path),
        "ticks_per_beat": midi.ticks_per_beat,
        "length_seconds": float(midi.length),
        "events": events,
    }


# ---------------------------------------------------------------------
# MIDI discovery (FLEX v2)
# ---------------------------------------------------------------------
def find_midi_files(root: Path) -> List[Path]:
    """Recursively find all MIDI files using regex."""
    return [
        p for p in root.rglob("*")
        if p.is_file() and MIDI_REGEX.match(p.name)
    ]


# ---------------------------------------------------------------------
# Convert
# ---------------------------------------------------------------------
def convert_midi_to_json(input_path: Path, output_dir: Path) -> None:
    logging.info("Converting MIDI → JSON | input=%s output=%s", input_path, output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Collect all MIDI files
    if input_path.is_file() and MIDI_REGEX.match(input_path.name):
        midi_files = [input_path]
    elif input_path.is_dir():
        midi_files = find_midi_files(input_path)
    else:
        raise FileNotFoundError(f"No MIDI files found at {input_path}")

    if not midi_files:
        logging.warning("No MIDI files found under %s", input_path)
        return

    logging.info("Found %d MIDI files", len(midi_files))

    for midi_path in midi_files:
        spec = extract_events(midi_path)
        if spec is None:
            continue  # skip bad file

        out_name = midi_path.stem + ".json"
        out_path = output_dir / out_name

        with out_path.open("w", encoding="utf-8") as f:
            json.dump(spec, f, indent=2)

        logging.info("Wrote JSON spec: %s", out_path)


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform MIDI → JSON event specs (flex v2).")
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="MIDI file or directory (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output directory for JSON specs (default: %(default)s)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    convert_midi_to_json(args.input, args.output)


if __name__ == "__main__":
    main()
