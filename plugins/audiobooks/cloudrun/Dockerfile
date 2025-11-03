# ───────────────────────────────────────────────
# Kokoro TTS microservice — Cloud Run compatible
# Supports both FastAPI (main.py) and batch jobs (job_tts.py)
# ───────────────────────────────────────────────

FROM python:3.11-slim

# Working directory
WORKDIR /app

# Copy dependency list
COPY requirements.txt .

# Install lightweight system deps (ffmpeg for pydub/audio export)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
 && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Python dependencies
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy application code
COPY . .

# Environment variable for Cloud Run
ENV PORT=8080

# Expose port for FastAPI (ignored by Cloud Run but good practice)
EXPOSE 8080

# Start FastAPI when Cloud Run uses this image as a web service
# When used as a Job, Cloud Run will override this CMD
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
