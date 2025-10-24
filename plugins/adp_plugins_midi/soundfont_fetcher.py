#!/usr/bin/env python3
# soundfont_osuosl_fetcher.py
# Fetch SoundFonts from the MuseScore OSUOSL mirror and produce JSONL + Parquet manifest.

import io, re, sys, time, json, random, tarfile, zipfile, urllib.parse
from pathlib import Path
from urllib.request import urlopen, urlretrieve

BASE = "https://ftp.osuosl.org/pub/musescore/soundfont/"
SUBDIRS = ["", "MuseScore_General/"]
DEST = Path("soundfonts")
DEST.mkdir(exist_ok=True)
MANIFEST = DEST / "soundfonts_manifest.jsonl"

MAX_ARCHIVE_MB_DEFAULT = 500  # MB limit
USER_AGENT = "ADP-SoundFontFetcher/1.2 (+OSUOSL only)"

def log(m): 
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def fetch_html(url: str) -> str:
    req = urlopen(url)
    return req.read().decode("utf-8", errors="ignore")

def list_links(url: str):
    """Return list of (name, absolute_url) from a simple Apache/Index page."""
    html = fetch_html(url)
    names = re.findall(r'href="([^"]+)"', html)
    out = []
    for n in names:
        if n in ("../", "./"):
            continue
        out.append((n, urllib.parse.urljoin(url, n)))
    return out

def extract_sf2_from_zip(zip_path: Path, out_dir: Path) -> int:
    count = 0
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if name.lower().endswith(".sf2"):
                out = out_dir / Path(name).name
                if not out.exists():
                    with z.open(name) as src, open(out, "wb") as dst:
                        dst.write(src.read())
                    count += 1
    return count

def extract_sf2_from_tar_gz(tar_path: Path, out_dir: Path) -> int:
    count = 0
    try:
        with tarfile.open(tar_path, "r:*") as t:  # handles .tar, .tar.gz, .tgz
            for m in t.getmembers():
                if m.isfile() and m.name.lower().endswith(".sf2"):
                    out = out_dir / Path(m.name).name
                    if not out.exists():
                        src = t.extractfile(m)
                        if src:
                            with open(out, "wb") as dst:
                                dst.write(src.read())
                            count += 1
    except tarfile.ReadError:
        log(f"⚠️  Could not read tar archive {tar_path}")
    return count

def save_manifest(entry: dict):
    with open(MANIFEST, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")

def download(url: str, out: Path):
    if out.exists():
        return out
    log(f"⬇️  {out.name}")
    urlretrieve(url, out)
    return out

def run(sample_n: int = 3, max_archive_mb: int = MAX_ARCHIVE_MB_DEFAULT):
    DEST.mkdir(exist_ok=True)
    if not MANIFEST.exists():
        MANIFEST.touch()
    log(f"✅ Soundfonts directory: {DEST.resolve()}")
    log("Listing OSUOSL soundfont directories …")

    candidates = []  # (name, url, kind)
    for sub in SUBDIRS:
        base = urllib.parse.urljoin(BASE, sub)
        for name, url in list_links(base):
            lower = name.lower()
            if lower.endswith(".sf2"):
                candidates.append((name, url, "sf2"))
            elif lower.endswith(".zip"):
                candidates.append((name, url, "zip"))
            elif lower.endswith((".tar.gz", ".tgz", ".tar")):
                candidates.append((name, url, "tar"))

    if not candidates:
        log("No candidates found on OSUOSL.")
        sys.exit(1)

    log(f"Found {len(candidates)} items. Sampling {sample_n} …")
    random.shuffle(candidates)
    chosen = candidates[:min(sample_n, len(candidates))]

    downloaded = 0
    extracted = 0
    for name, url, kind in chosen:
        if kind == "sf2":
            out = DEST / name
            if out.exists():
                log(f"✓ {name} already exists")
                save_manifest({"file": str(out), "src": url, "type": "sf2", "status": "exists"})
                continue
            download(url, out)
            size_mb = out.stat().st_size / (1024*1024)
            log(f"  ✓ saved {name} ({size_mb:.1f} MB)")
            save_manifest({"file": str(out), "src": url, "type": "sf2", "status": "downloaded", "size_mb": round(size_mb,1)})
            downloaded += 1
            continue

        # Handle archives (.zip / .tar.gz / .tgz)
        arc = DEST / name
        if not arc.exists():
            download(url, arc)
        size_mb = arc.stat().st_size / (1024*1024)
        if size_mb > max_archive_mb:
            log(f"⚠️  {name} is {size_mb:.0f} MB (> {max_archive_mb} MB cap). Skipping archive.")
            save_manifest({"file": str(arc), "src": url, "type": kind, "status": "skipped_big", "size_mb": round(size_mb,1)})
            arc.unlink(missing_ok=True)
            continue

        # Extract .sf2 and then delete the archive
        if kind == "zip":
            n = extract_sf2_from_zip(arc, DEST)
        else:
            n = extract_sf2_from_tar_gz(arc, DEST)
        extracted += n
        log(f"  ✓ extracted {n} .sf2 from {name}")
        save_manifest({"archive": str(arc), "src": url, "type": kind, "status": "extracted", "sf2_files": n, "size_mb": round(size_mb,1)})

        arc.unlink(missing_ok=True)
        log(f"  🗑️  deleted archive {arc.name}")

    log("—" * 60)
    log(f"Done. Downloaded {downloaded} direct .sf2 and extracted {extracted} from archives → {DEST.resolve()}")
    log(f"Manifest: {MANIFEST.resolve()}")

    # 🔽 NEW: auto-generate Parquet version of the manifest
    try:
        import pandas as pd
        rows = [json.loads(line) for line in MANIFEST.open()]
        df = pd.DataFrame(rows)
        pq_path = MANIFEST.with_suffix(".parquet")
        df.to_parquet(pq_path, index=False)
        log(f"📦 Also wrote Parquet manifest → {pq_path}")
    except Exception as e:
        log(f"⚠️  Could not write Parquet manifest: {e}")

if __name__ == "__main__":
    run(sample_n=3, max_archive_mb=500)
