#!/usr/bin/env python3
# transform_tts_kokoro.py
# Kokoro TTS using Hugging Face remote inference only

import io, re, time, os
from pydub import AudioSegment
from huggingface_hub import InferenceClient
from pathlib import Path

HF_MODEL = "hexgrad/Kokoro-82M"
HF_TOKEN = os.getenv("HF_TOKEN")  # optional, but recommended if private rate limit

client = InferenceClient(model=HF_MODEL, token=HF_TOKEN)
VOICE = os.getenv("TTS_VOICE", "af_heart")

def synthesize_chunk(text):
    """Use Hugging Face-hosted Kokoro to synthesize a short text chunk."""
    audio_bytes = client.text_to_speech(
        inputs=text,
        parameters={"voice": VOICE}
    )
    return AudioSegment.from_file(io.BytesIO(audio_bytes), format="wav")

def synthesize_story(story):
    """Takes {'title','author','text'} dict → concatenated AudioSegment."""
    text = f"{story['title']} by {story['author']}\n\n{story['text']}"
    chunks = re.split(r'(?<=[.!?])\s+', text)
    audio_segments = []
    for i, chunk in enumerate(chunks):
        if not chunk.strip():
            continue
        print(f"[{time.strftime('%H:%M:%S')}] 🔊 Synth {i+1}/{len(chunks)}...")
        seg = synthesize_chunk(chunk[:5000])  # ≤5k chars for API
        audio_segments.append(seg)
    return sum(audio_segments)

if __name__ == "__main__":
    story_path = Path("audiobooks/transform/data/gutenberg_clean/25525.txt")
    text = story_path.read_text()
    story = {"title": "The Works of Edgar Allan Poe, Vol. I", "author": "Edgar Allan Poe", "text": text}
    full_audio = synthesize_story(story)
    out_path = Path("audiobooks/tts_output") / f"{story['title']}.mp3"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    full_audio.export(out_path, format="mp3")
    print(f"✅ Exported {out_path}")
