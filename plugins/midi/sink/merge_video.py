#!/usr/bin/env python3
"""
merge_video.py  —  Sink: PNG frames + MP3 → MP4 via ffmpeg.

- Expects frames in: plugins/midi/frames/<job_name>/frame-00001.png ...
- Expects MP3 in:    plugins/midi/audio/<job_name>.mp3
- Writes MP4 to:     plugins/midi/videos/<job_name>.mp4
"""

import argparse
import logging
import subprocess
from pathlib import Path

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FRAMES_ROOT = PLUGIN_ROOT / "frames"
DEFAULT_AUDIO_DIR = PLUGIN_ROOT / "audio"
DEFAULT_VIDEO_DIR = PLUGIN_ROOT / "videos"


def merge_job(
    job_name: str,
    frames_root: Path,
    audio_dir: Path,
    video_dir: Path,
    fps: int = 30,
) -> None:
    frame_pattern = frames_root / job_name / "frame-%05d.png"
    audio_path = audio_dir / f"{job_name}.mp3"
    out_path = video_dir / f"{job_name}.mp4"

    if not frame_pattern.parent.exists():
        logging.warning("No frames found for job=%s at %s", job_name, frame_pattern)
        return
    if not audio_path.exists():
        logging.warning("No audio found for job=%s at %s", job_name, audio_path)
        return

    video_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        "ffmpeg",
        "-y",
        "-framerate",
        str(fps),
        "-i",
        str(frame_pattern),
        "-i",
        str(audio_path),
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-shortest",
        str(out_path),
    ]
    logging.info("Merging job=%s → %s", job_name, out_path)
    logging.debug("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


def merge_all_frames(
    frames_root: Path,
    audio_dir: Path,
    video_dir: Path,
    fps: int = 30,
    job: str | None = None,
) -> None:
    if job:
        names = [job]
    else:
        if not frames_root.exists():
            logging.warning("Frames root does not exist: %s", frames_root)
            return
        names = sorted([p.name for p in frames_root.iterdir() if p.is_dir()])

    for name in names:
        merge_job(name, frames_root, audio_dir, video_dir, fps=fps)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sink: Merge PNG frames + MP3 → MP4 via ffmpeg."
    )
    parser.add_argument(
        "--frames-root",
        type=Path,
        default=DEFAULT_FRAMES_ROOT,
        help="Root directory containing per-job frame folders (default: %(default)s)",
    )
    parser.add_argument(
        "--audio-dir",
        type=Path,
        default=DEFAULT_AUDIO_DIR,
        help="Directory containing MP3 audio files (default: %(default)s)",
    )
    parser.add_argument(
        "--video-dir",
        type=Path,
        default=DEFAULT_VIDEO_DIR,
        help="Output directory for MP4 videos (default: %(default)s)",
    )
    parser.add_argument(
        "--fps",
        type=int,
        default=30,
        help="Frame rate for the output videos (default: 30)",
    )
    parser.add_argument(
        "--job",
        type=str,
        default=None,
        help="Optional single job name to merge.",
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
    merge_all_frames(
        args.frames_root, args.audio_dir, args.video_dir, fps=args.fps, job=args.job
    )


if __name__ == "__main__":
    main()
