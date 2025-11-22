#!/usr/bin/env python3
"""
midi_to_video.py  —  Orchestrate: MIDI → JSON → frames → MP4.

Pipeline:

    MIDI (.mid/.midi)
      → transform/midi_to_json.py      (JSON specs)
      → (your existing midi_to_audio)  (MP3 audio)   [run separately or wire here]
      → transform/render_frames.py     (PNG frames via Processing)
      → sink/merge_video.py            (MP4 via ffmpeg)

Usage (from repo root, adjust paths as needed):

    python -m plugins.midi.orchestrate.midi_to_video \
        --midi-dir data/midi
"""

import argparse
import logging
from pathlib import Path

from ..transform.midi_to_json import convert_midi_to_json
from ..transform.render_frames import render_all_specs
from ..sink.merge_video import merge_all_frames

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MIDI_DIR = PLUGIN_ROOT / "midi_files"
DEFAULT_SPEC_DIR = PLUGIN_ROOT / "specs" / "midi_events"
DEFAULT_AUDIO_DIR = PLUGIN_ROOT / "audio"
DEFAULT_FRAMES_ROOT = PLUGIN_ROOT / "frames"
DEFAULT_SKETCH_DIR = PLUGIN_ROOT / "assets" / "batch_renderer"
DEFAULT_VIDEO_DIR = PLUGIN_ROOT / "videos"


def run_pipeline(
    midi_dir: Path,
    spec_dir: Path,
    audio_dir: Path,
    frames_root: Path,
    sketch_dir: Path,
    video_dir: Path,
    skip_frames: bool = False,
    skip_merge: bool = False,
) -> None:
    logging.info("Running MIDI → video pipeline")

    # 1. MIDI → JSON
    convert_midi_to_json(midi_dir, spec_dir)

    # 2. JSON + MP3 → frames (Processing)
    if not skip_frames:
        render_all_specs(spec_dir, sketch_dir)

    # 3. frames + MP3 → MP4 (ffmpeg)
    if not skip_merge:
        merge_all_frames(frames_root, audio_dir, video_dir)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orchestrate MIDI → JSON → frames → MP4."
    )
    parser.add_argument(
        "--midi-dir",
        type=Path,
        default=DEFAULT_MIDI_DIR,
        help="Directory containing MIDI files (default: %(default)s)",
    )
    parser.add_argument(
        "--spec-dir",
        type=Path,
        default=DEFAULT_SPEC_DIR,
        help="Directory for JSON specs (default: %(default)s)",
    )
    parser.add_argument(
        "--audio-dir",
        type=Path,
        default=DEFAULT_AUDIO_DIR,
        help="Directory containing MP3 audio files (default: %(default)s)",
    )
    parser.add_argument(
        "--frames-root",
        type=Path,
        default=DEFAULT_FRAMES_ROOT,
        help="Root directory for rendered frames (default: %(default)s)",
    )
    parser.add_argument(
        "--sketch-dir",
        type=Path,
        default=DEFAULT_SKETCH_DIR,
        help="Processing sketch directory (default: %(default)s)",
    )
    parser.add_argument(
        "--video-dir",
        type=Path,
        default=DEFAULT_VIDEO_DIR,
        help="Output directory for MP4 videos (default: %(default)s)",
    )
    parser.add_argument(
        "--skip-frames",
        action="store_true",
        help="Skip the Processing frame rendering step.",
    )
    parser.add_argument(
        "--skip-merge",
        action="store_true",
        help="Skip the ffmpeg merge step.",
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

    run_pipeline(
        midi_dir=args.midi_dir,
        spec_dir=args.spec_dir,
        audio_dir=args.audio_dir,
        frames_root=args.frames_root,
        sketch_dir=args.sketch_dir,
        video_dir=args.video_dir,
        skip_frames=args.skip_frames,
        skip_merge=args.skip_merge,
    )


if __name__ == "__main__":
    main()
