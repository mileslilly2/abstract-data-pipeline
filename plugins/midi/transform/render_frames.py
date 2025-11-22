#!/usr/bin/env python3
"""
render_frames.py — JSON + WAV → PNG via Processing

Processing sketch receives <job_name>
Loads:
    ../../specs/midi_events/<job>.json
    ../../audio/<job>.wav
Outputs:
    ../../frames/<job>/frame-00001.png
"""

import argparse
import logging
import subprocess
from pathlib import Path

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SPEC_DIR = PLUGIN_ROOT / "specs" / "midi_events"
DEFAULT_SKETCH_DIR = PLUGIN_ROOT / "assets" / "batch_renderer"

def render_job(name: str, sketch_dir: Path):
    cmd = [
        "processing-java",
        f"--sketch={sketch_dir}",
        "--run",
        name
    ]
    logging.info("Rendering frames for: %s", name)
    subprocess.run(cmd, check=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec-dir", type=Path, default=DEFAULT_SPEC_DIR)
    parser.add_argument("--sketch-dir", type=Path, default=DEFAULT_SKETCH_DIR)
    parser.add_argument("--job", type=str, default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level))

    names = [args.job] if args.job else [p.stem for p in args.spec_dir.glob("*.json")]

    for name in names:
        render_job(name, args.sketch-dir)

if __name__ == "__main__":
    main()
