#!/usr/bin/env python3
# transform_clean_text.py
# Clean every Gutenberg HTML file in audiobooks/source/data/gutenberg_raw → gutenberg_clean (story text only)

from bs4 import BeautifulSoup
import re, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "audiobooks" / "source" / "data" / "gutenberg_raw"
CLEAN_DIR = ROOT / "audiobooks" / "transform" / "data" / "gutenberg_clean"
CLEAN_DIR.mkdir(exist_ok=True, parents=True)

# Patterns to isolate the true story section
START_PATTERN = re.compile(
    r"\*\*\*\s*start of (?:the|this)\s+project gutenberg e(?:book|text).*?\*\*\*",
    re.IGNORECASE | re.DOTALL,
)
END_PATTERN = re.compile(
    r"\*\*\*\s*end of (?:the|this)\s+project gutenberg e(?:book|text).*?\*\*\*",
    re.IGNORECASE | re.DOTALL,
)

# Remove standard and postscript boilerplate
LINE_FILTERS = [
    r"project gutenberg", r"gutenberg\-tm license", r"www\.gutenberg\.org",
    r"produced by", r"transcriber", r"scanned by", r"foundation", r"e?text",
    r"public domain", r"start of this project gutenberg", r"end of this project gutenberg",
]

# New explicit block for post-story disclaimers
END_DISCLAIMER_PATTERN = re.compile(
    r"Extensive research did not uncover any evidence that the U\.S\..+?$",
    re.IGNORECASE | re.DOTALL,
)

def soup_with_fallback(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")

def strip_pg_containers(soup: BeautifulSoup) -> None:
    selectors = [
        "#pg-header", ".pgheader", ".pgfooter", "#pg-footer", ".pgtop",
        ".pg-bottom", "header", "footer", "nav",
    ]
    for sel in selectors:
        for tag in soup.select(sel):
            tag.decompose()

def slice_between_banners(plain: str) -> str | None:
    low = plain.lower()
    s = START_PATTERN.search(low)
    e = END_PATTERN.search(low)
    if s and e and e.start() > s.end():
        return plain[s.end():e.start()].strip()
    return None

def drop_boilerplate_lines(plain: str) -> str:
    filters = [re.compile(pat, re.IGNORECASE) for pat in LINE_FILTERS]
    kept = []
    for line in plain.splitlines():
        l = line.strip()
        if not l:
            continue
        if any(rx.search(l) for rx in filters):
            continue
        kept.append(l)
    return "\n".join(kept)

def remove_end_disclaimer(plain: str) -> str:
    """Remove 'Extensive research...' or similar end-of-book disclaimers."""
    match = END_DISCLAIMER_PATTERN.search(plain)
    if match:
        plain = plain[:match.start()].strip()
    return plain

def extract_story_text(html_path: Path) -> dict:
    html = html_path.read_text(encoding="utf-8", errors="ignore")

    soup = soup_with_fallback(html)
    strip_pg_containers(soup)
    body = soup.body or soup
    plain = body.get_text("\n", strip=True)

    between = slice_between_banners(plain)
    if between:
        clean = between
    else:
        clean = drop_boilerplate_lines(plain)

    clean = remove_end_disclaimer(clean)
    clean = re.sub(r"\n{3,}", "\n\n", clean).strip()
    return {"text": clean}

def clean_all_raw():
    html_files = sorted(RAW_DIR.rglob("*.html"))
    if not html_files:
        print(f"[{time.strftime('%H:%M:%S')}] ⚠️ No HTML files found in {RAW_DIR}")
        return

    print(f"[{time.strftime('%H:%M:%S')}] 🧹 Cleaning {len(html_files)} raw files...")
    ok = fail = 0
    for html_path in html_files:
        try:
            story = extract_story_text(html_path)
            out_path = CLEAN_DIR / (html_path.stem + ".txt")
            out_path.write_text(story["text"], encoding="utf-8")
            print(f"[{time.strftime('%H:%M:%S')}] ✅ {html_path.name} → {out_path.name}")
            ok += 1
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] ❌ {html_path.name}: {e}")
            fail += 1
    print(f"[{time.strftime('%H:%M:%S')}] ✅ Finished. Cleaned: {ok}  Failed: {fail}")

if __name__ == "__main__":
    clean_all_raw()
