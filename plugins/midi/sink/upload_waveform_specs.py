#!/usr/bin/env python3
# hf_upload_waveform_specs_batched_manifest.py
# Upload waveform spec triplets (CSV + YAML + WAV) to Hugging Face with batching, manifest, and resume.

import json, time, shutil, sys
from pathlib import Path
from huggingface_hub import upload_folder

# ───────────── CONFIG ─────────────
HF_REPO_OUT = "mileslilly/waveform_specs"
BATCH_SIZE = 10  # number of triads (id sets) per batch

PROJECT_ROOT   = Path(__file__).resolve().parents[2]
AUDIO_DATA_DIR = PROJECT_ROOT / "midi/audio_data"
SPEC_DIR       = PROJECT_ROOT / "midi/specs"
AUDIO_OUT_DIR  = PROJECT_ROOT / "midi/audio_out"

TMP_DIR = Path.home() / "waveform_specs_upload"
PROGRESS_PATH = TMP_DIR / "upload_progress.json"

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ───────────── HELPERS ─────────────
def yield_batches(items, size):
    for i in range(0, len(items), size):
        yield items[i:i+size]

def load_progress():
    if PROGRESS_PATH.exists():
        try:
            return json.loads(PROGRESS_PATH.read_text())
        except Exception:
            return {}
    return {}

def save_progress(done):
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    PROGRESS_PATH.write_text(json.dumps({"done_batches": done}, indent=2))

def get_ids():
    """Return base IDs derived from CSV/YAML/WAV stems."""
    csv_ids  = [p.stem for p in AUDIO_DATA_DIR.glob("*.csv")]
    yaml_ids = [p.stem for p in SPEC_DIR.glob("*.yaml")]
    wav_ids  = [p.stem for p in AUDIO_OUT_DIR.glob("*.wav")]
    all_ids = sorted(set(csv_ids + yaml_ids + wav_ids))
    return all_ids

def collect_triplet(base):
    """Find matching files for one base id."""
    return {
        "id": base,
        "csv": next((f"{base}.csv" for f in AUDIO_DATA_DIR.glob(f"{base}.csv")), None),
        "yaml": next((f"{base}.yaml" for f in SPEC_DIR.glob(f"{base}.yaml")), None),
        "wav": next((f"{base}.wav" for f in AUDIO_OUT_DIR.glob(f"{base}.wav")), None),
    }

# ───────────── MAIN ─────────────
def main():
    ids = get_ids()
    if not ids:
        log("❌ No files found — run generation first.")
        sys.exit(1)

    batches = list(yield_batches(ids, BATCH_SIZE))
    total_batches = len(batches)
    progress = load_progress()
    done_batches = set(progress.get("done_batches", []))

    log(f"🚀 Starting batched upload ({total_batches} batches, ≤{BATCH_SIZE} triads each). Resuming from batch {len(done_batches)+1}.")

    for batch_i, batch_ids in enumerate(batches, 1):
        if batch_i in done_batches:
            log(f"⏭️ Skipping batch {batch_i} (already uploaded).")
            continue

        batch_dir = TMP_DIR / f"batch_{batch_i}"
        if batch_dir.exists():
            shutil.rmtree(batch_dir)
        batch_dir.mkdir(parents=True, exist_ok=True)

        manifest_entries = []

        for base in batch_ids:
            triplet = collect_triplet(base)
            # Copy each file if it exists
            for key, fname in [("csv", triplet["csv"]), ("yaml", triplet["yaml"]), ("wav", triplet["wav"])]:
                if fname:
                    src_dir = AUDIO_DATA_DIR if key == "csv" else SPEC_DIR if key == "yaml" else AUDIO_OUT_DIR
                    src = src_dir / fname
                    if src.exists():
                        shutil.copy(src, batch_dir / fname)
            manifest_entries.append(triplet)

        # Write manifest.jsonl for this batch
        manifest_path = batch_dir / "manifest.jsonl"
        with open(manifest_path, "w", encoding="utf-8") as f:
            for entry in manifest_entries:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

        log(f"⬆️ Uploading batch {batch_i}/{total_batches} ({len(manifest_entries)} triads)…")
        start_time = time.time()

        try:
            upload_folder(
                repo_id=HF_REPO_OUT,
                folder_path=str(batch_dir),
                repo_type="dataset",
                commit_message=f"Batch {batch_i} upload ({len(manifest_entries)} triads)",
            )
            elapsed = time.time() - start_time
            log(f"✅ Uploaded batch {batch_i}/{total_batches} in {elapsed:.1f}s")

            done_batches.add(batch_i)
            save_progress(list(sorted(done_batches)))

        except Exception as e:
            log(f"❌ Failed batch {batch_i}: {e}")
            log("⚠️ Stopping to avoid partial uploads. Re-run to resume safely.")
            sys.exit(1)
        finally:
            shutil.rmtree(batch_dir, ignore_errors=True)

    # Merge all manifests into one master manifest
    master_manifest = TMP_DIR / "manifest.jsonl"
    all_entries = []
    for batch_json in sorted(TMP_DIR.glob("batch_*/manifest.jsonl")):
        all_entries.extend(Path(batch_json).read_text().splitlines())

    with open(master_manifest, "w", encoding="utf-8") as f:
        f.write("\n".join(all_entries))

    upload_folder(
        repo_id=HF_REPO_OUT,
        folder_path=str(TMP_DIR),
        repo_type="dataset",
        commit_message="Unified manifest upload",
    )

    log(f"🏁 All batches uploaded successfully → https://huggingface.co/datasets/{HF_REPO_OUT}")
    log("🧾 Master manifest.jsonl created and uploaded.")

if __name__ == "__main__":
    main()
