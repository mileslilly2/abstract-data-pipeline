# Dockerfile
FROM python:3.10-slim

# Prevent Python buffering
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir fastapi uvicorn soundfile kokoro-onnx
RUN apt-get update && apt-get install -y libsndfile1 && apt-get clean


COPY . .

# Cloud Run requires the app to listen on PORT
ENV PORT=8080

# Expose port for local testing (optional)
EXPOSE 8080

# Start FastAPI server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "1000"]
