#!/usr/bin/env python3
# soundfont_osuosl_fetcher.py
# Fetch SoundFonts from the MuseScore OSUOSL mirror:
# - Parses directory listing(s)
# - Downloads .sf2 directly OR .zip/.tar.gz and extracts ONLY .sf2
# - Samples N items if requested
# - Skips already-downloaded files; logs a manifest

import io, re, sys, time, json, random, tarfile, zipfile, urllib.parse
from pathlib import Path
from urllib.request import urlopen, urlretrieve

BASE = "https://ftp.osuosl.org/pub/musescore/soundfont/"
SUBDIRS = ["", "MuseScore_General/"]  # top-level + subfolder
DEST = Path("soundfonts")
DEST.mkdir(exist_ok=True)
MANIFEST = DEST / "soundfonts_manifest.jsonl"

MAX_ARCHIVE_MB_DEFAULT = 500  # refuse archives larger than this unless you override in code/CLI later
USER_AGENT = "ADP-SoundFontFetcher/1.0 (+OSUOSL only)"

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def fetch_html(url: str) -> str:
    req = urlopen(url)
    return req.read().decode("utf-8", errors="ignore")

def list_links(url: str):
    """Return list of (name, absolute_url) from a simple Apache/Index page."""
    html = fetch_html(url)
    # capture href="file.ext"
    names = re.findall(r'href="([^"]+)"', html)
    out = []
    for n in names:
        if n in ("../", "./"): 
            continue
        # only keep file-like (avoid folders here; we pass subfolders via SUBDIRS)
        out.append((n, urllib.parse.urljoin(url, n)))
    return out

def extract_sf2_from_zip(zip_path: Path, out_dir: Path) -> int:
    count = 0
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if name.lower().endswith(".sf2"):
                out = out_dir / Path(name).name
                if out.exists():
                    continue
                with z.open(name) as src, open(out, "wb") as dst:
                    dst.write(src.read())
                count += 1
    return count

def extract_sf2_from_tar_gz(tar_path: Path, out_dir: Path) -> int:
    count = 0
    with tarfile.open(tar_path, "r:gz") as t:
        for m in t.getmembers():
            if m.isfile() and m.name.lower().endswith(".sf2"):
                out = out_dir / Path(m.name).name
                if out.exists():
                    continue
                src = t.extractfile(m)
                if src:
                    with open(out, "wb") as dst:
                        dst.write(src.read())
                    count += 1
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
    log("Listing OSUOSL soundfont directories …")
    candidates = []  # (name, url, kind) kind ∈ {"sf2","zip","tar"}
    for sub in SUBDIRS:
        base = urllib.parse.urljoin(BASE, sub)
        for name, url in list_links(base):
            lower = name.lower()
            if lower.endswith(".sf2"):
                candidates.append((name, url, "sf2"))
            elif lower.endswith(".zip"):
                candidates.append((name, url, "zip"))
            elif lower.endswith(".tar.gz"):
                candidates.append((name, url, "tar"))
            # ignore folders & others

    if not candidates:
        log("No candidates found on OSUOSL.")
        sys.exit(1)

    log(f"Found {len(candidates)} items (sf2/zip/tar.gz). Sampling {sample_n} …")
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

        elif kind in ("zip", "tar"):
            # guard on size (HEAD not available easily on index; we’ll download but bail early if huge)
            arc = DEST / name
            if not arc.exists():
                download(url, arc)
            size_mb = arc.stat().st_size / (1024*1024)
            if size_mb > max_archive_mb:
                log(f"⚠️  {name} is {size_mb:.0f} MB (> {max_archive_mb} MB cap). Skipping archive.")
                save_manifest({"file": str(arc), "src": url, "type": kind, "status": "skipped_big", "size_mb": round(size_mb,1)})
                arc.unlink(missing_ok=True)
                continue

            # extract only .sf2
            if kind == "zip":
                n = extract_sf2_from_zip(arc, DEST)
            else:
                n = extract_sf2_from_tar_gz(arc, DEST)
            extracted += n
            log(f"  ✓ extracted {n} .sf2 from {name}")
            save_manifest({"archive": str(arc), "src": url, "type": kind, "status": "extracted", "sf2_files": n, "size_mb": round(size_mb,1)})
            # keep the archive so you can re-extract later; delete if you prefer:
            # arc.unlink()

    log("—" * 60)
    log(f"Done. Downloaded {downloaded} direct .sf2 and extracted {extracted} from archives → {DEST.resolve()}")
    log(f"Manifest: {MANIFEST.resolve()}")

if __name__ == "__main__":
    # tweak numbers here if you want different defaults
    run(sample_n=3, max_archive_mb=500)
