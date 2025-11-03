#!/usr/bin/env python3
# transform_tts_kokoro.py
# Loop through cleaned Gutenberg text files and generate audiobooks using Hugging Face-hosted Kokoro

import io, re, time, os
from pydub import AudioSegment
from huggingface_hub import InferenceClient
from pathlib import Path

# ────────────────────────────────
# Hugging Face API setup
# ────────────────────────────────
HF_MODEL = os.getenv("HF_MODEL", "hexgrad/Kokoro-82M")
HF_TOKEN = os.getenv("HF_TOKEN")  # optional; set via: export HF_TOKEN=hf_xxx
VOICE    = os.getenv("TTS_VOICE", "af_heart")

client = InferenceClient(model=HF_MODEL, token=HF_TOKEN)

# ────────────────────────────────
# TTS helpers
# ────────────────────────────────
def synthesize_chunk(text, retries=3):
    """Send one text chunk to the HF-hosted Kokoro model and get a WAV segment."""
    for attempt in range(1, retries + 1):
        try:
            # ✅ Pass voice via extra_body (current API)
            audio_bytes = client.text_to_speech(
                text,
                model=HF_MODEL,               # redundant if set on client, but explicit is fine
                extra_body={"voice": VOICE},  # <-- key fix
            )
            return AudioSegment.from_file(io.BytesIO(audio_bytes), format="wav")
        except Exception as e:
            if attempt == retries:
                raise
            print(f"[{time.strftime('%H:%M:%S')}] ⚠️ Retry {attempt}/{retries} after error: {e}")
            time.sleep(2 * attempt)

def synthesize_story(title: str, author: str, text: str) -> AudioSegment | None:
    """Generate concatenated AudioSegment for a full story."""
    text = f"{title} by {author}\n\n{text}"
    # Split on sentence boundaries or large newlines
    chunks = re.split(r'(?<=[.!?])\s+|\n{2,}', text)
    audio_segments = []

    for i, chunk in enumerate(chunks):
        chunk = chunk.strip()
        if not chunk:
            continue
        print(f"[{time.strftime('%H:%M:%S')}] 🔊 Synth {i+1}/{len(chunks)}: {title[:50]}...")
        seg = synthesize_chunk(chunk[:5000])  # keep ≤ ~5k chars per request
        audio_segments.append(seg)
    return sum(audio_segments) if audio_segments else None

# ────────────────────────────────
# Batch processor
# ────────────────────────────────
if __name__ == "__main__":
    ROOT = Path(__file__).resolve().parents[2]
    CLEAN_DIR = ROOT / "audiobooks" / "transform" / "data" / "gutenberg_clean"
    OUT_DIR   = ROOT / "audiobooks" / "tts_output"
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    txt_files = sorted(CLEAN_DIR.glob("*.txt"))
    if not txt_files:
        print(f"[{time.strftime('%H:%M:%S')}] ⚠️ No cleaned text files found in {CLEAN_DIR}")
        raise SystemExit

    print(f"[{time.strftime('%H:%M:%S')}] 🎙 Starting remote Kokoro synthesis for {len(txt_files)} stories...")

    for i, path in enumerate(txt_files, 1):
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
            title = path.stem
            author = "Unknown Author"
            story_audio = synthesize_story(title, author, text)

            if story_audio:
                out_path = OUT_DIR / f"{title}.mp3"
                story_audio.export(out_path, format="mp3")
                print(f"[{time.strftime('%H:%M:%S')}] ✅ Exported {out_path}")
            else:
                print(f"[{time.strftime('%H:%M:%S')}] ⚠️ Skipped {title} (empty output)")

        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] ❌ Failed {path.name}: {e}")

    print(f"[{time.strftime('%H:%M:%S')}] ✅ Finished all TTS exports.")
