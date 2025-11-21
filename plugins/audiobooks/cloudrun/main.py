# main.py
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from kokoro import KPipeline
import soundfile as sf
import numpy as np
import time
import io

app = FastAPI()

@app.get("/")
def root():
    return {"ok": True, "ready": True, "time": time.strftime("%H:%M:%S")}

# Load TTS model
pipe = KPipeline(lang_code="a")

@app.post("/tts")
async def tts(request: Request):
    data = await request.json()
    text = data.get("text", "").strip()
    voice = data.get("voice", "af_heart")

    if not text:
        raise HTTPException(status_code=400, detail="No text provided")

    audio, sr = pipe(text, voice=voice)

    buf = io.BytesIO()
    sf.write(buf, audio, sr, format="WAV")
    buf.seek(0)

    return StreamingResponse(buf, media_type="audio/wav")
