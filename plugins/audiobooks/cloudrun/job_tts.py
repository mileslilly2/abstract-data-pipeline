#!/usr/bin/env python3
# Cloud Run Job: Batch Kokoro audiobook generator (no HTTP timeout)

from kokoro import KPipeline
import soundfile as sf, numpy as np, io, re, os, time
from pathlib import Path

print(f"[{time.strftime('%H:%M:%S')}] 🔧 Loading Kokoro model (CPU mode)...")
pipe = KPipeline(lang_code='a')

INPUT_FILE  = os.getenv("TTS_INPUT", "input.txt")
VOICE       = os.getenv("TTS_VOICE", "af_heart")
OUTFILE     = os.getenv("TTS_OUTPUT", "audiobook.wav")

def synthesize_story(title, text):
    chunks = re.split(r'(?<=[.!?])\s+|\n{2,}', text)
    audio_segments = []
    for i, chunk in enumerate(chunks):
        chunk = chunk.strip()
        if not chunk:
            continue
        print(f"[{time.strftime('%H:%M:%S')}] 🔊 Synth {i+1}/{len(chunks)}...")
        audio, sr = pipe(chunk, voice=VOICE)
        audio_segments.append(audio)
    if not audio_segments:
        print("⚠️ No text synthesized.")
        return
    full = np.concatenate(audio_segments)
    sf.write(OUTFILE, full, sr, format="WAV")
    print(f"✅ Exported {OUTFILE}")

if __name__ == "__main__":
    if not Path(INPUT_FILE).exists():
        raise FileNotFoundError(f"{INPUT_FILE} not found")
    text = Path(INPUT_FILE).read_text()
    synthesize_story("Job", text)
