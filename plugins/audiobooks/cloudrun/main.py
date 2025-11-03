#!/usr/bin/env python3
# main.py — FastAPI Kokoro TTS microservice for Cloud Run

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from kokoro import KPipeline
import numpy as np, io, soundfile as sf, time

# ──────────────────────────────────────────────
# Initialize FastAPI
# ──────────────────────────────────────────────
app = FastAPI(
    title="Kokoro TTS Service",
    description="Text-to-speech API powered by Kokoro ONNX for Cloud Run",
    version="1.0.0"
)

# ──────────────────────────────────────────────
# Load Kokoro ONNX pipeline (CPU)
# ──────────────────────────────────────────────
print(f"[{time.strftime('%H:%M:%S')}] 🔧 Loading Kokoro model (CPU)...")
pipe = KPipeline(lang_code="a")  # English model
print(f"[{time.strftime('%H:%M:%S')}] ✅ Kokoro model loaded.")

# ──────────────────────────────────────────────
# Health check route
# ──────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "ok", "service": "kokoro-tts", "time": time.strftime("%H:%M:%S")}

# ──────────────────────────────────────────────
# POST /tts endpoint
# ──────────────────────────────────────────────
@app.post("/tts")
async def tts(request: Request):
    """
    Accepts JSON: {"text": "...", "voice": "af_heart"}
    Returns WAV audio as a streaming response.
    """
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    text = data.get("text", "").strip()
    voice = data.get("voice", "af_heart")

    if not text:
        raise HTTPException(status_code=400, detail="No text provided")

    try:
        print(f"[{time.strftime('%H:%M:%S')}] 🎙 Synthesizing {len(text)} chars with voice={voice}")
        audio, sr = pipe(text, voice=voice)

        # Convert to WAV bytes in memory
        buf = io.BytesIO()
        sf.write(buf, audio, sr, format="WAV")
        buf.seek(0)

        print(f"[{time.strftime('%H:%M:%S')}] ✅ Synth complete, streaming WAV...")
        return StreamingResponse(buf, media_type="audio/wav")

    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] ❌ Error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
