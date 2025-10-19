#!/usr/bin/env bash
set -e

echo "=== STEP 1: Download ACS data ==="
python3 scripts/download_by_state.py

echo "=== STEP 2: Transform data ==="
python3 scripts/transform_data.py

echo "=== STEP 3: Generate specs ==="
python3 scripts/generate_specs.py

echo "=== STEP 4: Render videos ==="
for f in specs/*.yaml; do
  echo "Rendering $f..."
  python3 viz2video.py "$f"
done

echo "=== DONE! Videos are in ./videos/ ==="
