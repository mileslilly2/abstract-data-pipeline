#!/usr/bin/env python3
"""
render_pipeline.py
-------------------
1. Runs Processing sketch (mandala runner)
2. Watches "frames/" for progress
3. Merges PNG frames + WAV audio into final video with ffmpeg
4. Outputs: final_render.mp4

Requirements:
- Processing installed (processing-java)
- ffmpeg installed
- Your sketch folder must contain the .pde file

Edit the SKETCH_PATH and WAV_PATH below.
"""

import subprocess
import time
from pathlib import Path


# ------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------

# Set the directory containing your Processing sketch
SKETCH_PATH = Path("/home/miles/Documents/abstract-data-pipeline/plugins/midi/processing_sketch")

# Full path to WAV file to merge with frames
WAV_PATH = Path("/home/miles/Documents/abstract-data-pipeline/plugins/midi/data/wav/104_funk-rock_92_fill_4-4.wav")

# Output video name
OUTPUT_FILE = Path("final_render.mp4")

# Expected number of frames (matches your Processing settings)
FPS = 30
SECONDS = 15
TOTAL_FRAMES = FPS * SECONDS

# Frame folder produced by Processing
FRAMES_DIR = SKETCH_PATH / "frames"


# ------------------------------------------------------
# STEP 1 — Run Processing sketch
# ------------------------------------------------------

def run_processing():
    print("\n=== Running Processing Sketch ===")
    
    if not FRAMES_DIR.exists():
        FRAMES_DIR.mkdir(parents=True)

    # Clear existing frames
    for f in FRAMES_DIR.glob("*.png"):
        f.unlink()

    cmd = [
        "processing-java",
        "--sketch=" + str(SKETCH_PATH),
        "--run"
    ]

    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    print("Processing sketch finished.")


# ------------------------------------------------------
# STEP 2 — Wait for frames to finish rendering
# ------------------------------------------------------

def wait_for_frames():
    print("\n=== Waiting for Frames to Render ===")

    last_count = -1

    while True:
        pngs = list(FRAMES_DIR.glob("*.png"))
        count = len(pngs)

        print(f"Frames rendered: {count}/{TOTAL_FRAMES}", end="\r")

        # If the count hasn't changed in 5 checks, assume done
        if count == TOTAL_FRAMES:
            print("\nAll frames complete.")
            return

        time.sleep(1)


# ------------------------------------------------------
# STEP 3 — Merge with ffmpeg
# ------------------------------------------------------

def merge_video():
    print("\n=== Merging Frames + Audio (ffmpeg) ===\n")

    cmd = [
        "ffmpeg",
        "-framerate", str(FPS),
        "-i", str(FRAMES_DIR / "frame_%05d.png"),
        "-i", str(WAV_PATH),
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-crf", "18",
        "-c:a", "aac",
        "-b:a", "192k",
        "-shortest",
        str(OUTPUT_FILE)
    ]

    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)

    print("\nVideo created:", OUTPUT_FILE)


# ------------------------------------------------------
# MAIN
# ------------------------------------------------------

if __name__ == "__main__":
    run_processing()
    wait_for_frames()
    merge_video()
    print("\n=== DONE! ===")
