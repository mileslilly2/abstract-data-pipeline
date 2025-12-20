import requests
from pathlib import Path
import json
import re
import time
import hashlib

def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def harvest_batches(
    base_url: str,
    metadata_prefix: str,
    out_dir: str,
    max_batches: int | None = None,
    preview_records: int = 3,
    max_retries: int = 5,
):
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    state_path = out_path / "state.json"

    # ---- Load state
    if state_path.exists():
        state = json.loads(state_path.read_text())
        batch = state.get("batch", 0)
        token = state.get("resumptionToken")
        checksums = state.get("checksums", {})
        print(f"🔁 Resuming from batch {batch}")
        url = f"{base_url}?verb=ListRecords&resumptionToken={token}"
    else:
        batch = 0
        checksums = {}
        url = f"{base_url}?verb=ListRecords&metadataPrefix={metadata_prefix}"

    while url:
        file_path = out_path / f"{metadata_prefix}_page_{batch:04d}.xml"

        # ---- Skip if checksum already verified
        if file_path.exists() and str(batch) in checksums:
            print(f"⏭️  Batch {batch} already verified — skipping fetch")
            xml_text = file_path.read_text(encoding="utf-8")
        else:
            print(f"\n⬇️ Fetching batch {batch}")

            # ---- Exponential backoff
            for attempt in range(max_retries):
                try:
                    resp = requests.get(url, timeout=60)
                    resp.raise_for_status()
                    xml_text = resp.text
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    sleep = 2 ** attempt
                    print(f"⚠️  Error: {e} — retrying in {sleep}s")
                    time.sleep(sleep)

            file_path.write_text(xml_text, encoding="utf-8")
            checksum = sha256(xml_text)
            checksums[str(batch)] = checksum
            print(f"✅ Saved {file_path} (sha256={checksum[:12]}…)")

        # ---- Debug: sample titles
        titles = re.findall(r"<dc:title>(.*?)</dc:title>", xml_text, re.DOTALL)
        for t in titles[:preview_records]:
            print("  📄", t.strip())

        # ---- Extract resumptionToken
        start = xml_text.find("<resumptionToken")
        if start == -1:
            print("⛔ No resumptionToken — harvest complete.")
            state_path.unlink(missing_ok=True)
            break

        token = xml_text[start:].split(">", 1)[1].split("<", 1)[0].strip()
        if not token:
            print("⛔ Empty resumptionToken — harvest complete.")
            state_path.unlink(missing_ok=True)
            break

        # ---- Persist state AFTER successful batch
        state = {
            "batch": batch + 1,
            "resumptionToken": token,
            "checksums": checksums,
        }
        state_path.write_text(json.dumps(state, indent=2))

        url = f"{base_url}?verb=ListRecords&resumptionToken={token}"
        batch += 1

        if max_batches is not None and batch >= max_batches:
            print("⏹️ Reached max_batches limit.")
            break
