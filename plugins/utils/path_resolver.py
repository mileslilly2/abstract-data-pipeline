#!/usr/bin/env python3
"""
path_resolver.py — Unified ADP data path resolver
-------------------------------------------------
Lets any plugin (midi, geo, audio, viz, etc.) locate its input data
without hard-coding folder layouts.

Rules (in order of precedence):
1. Absolute path → returned directly.
2. Relative to spec file.
3. Sibling folder under plugin root (../audio_data, ../geo_data, etc.).
4. Domain-based search (infers folder from file extension or keywords).
5. Top-level project data_* folders.
6. Raises FileNotFoundError with helpful trace.

This abstraction allows composable pipelines where plugins reference each
other’s outputs without caring about directory structure.
"""

from pathlib import Path
from typing import Optional, Iterable



# Domain folders and extension hints
DOMAIN_MAP = {
    "audio": ["audio_data", "data_audio"],
    "midi":  ["data_midi", "midi_data"],
    "geo":   ["geo_data", "data_geo", "shapefiles"],
    "video": ["viz_videos", "data_video"],
    "csv":   ["data_clean", "data_raw", "audio_data"],  # ← add audio_data here
}


EXTENSION_MAP = {
    ".mid": "midi",
    ".midi": "midi",
    ".wav": "audio",
    ".mp3": "audio",
    ".flac": "audio",
    ".csv": "csv",
    ".tsv": "csv",
    ".parquet": "csv",
    ".geojson": "geo",
    ".shp": "geo",
    ".mp4": "video",
}

def infer_domain_from_name(path: Path) -> Optional[str]:
    ext = path.suffix.lower()
    if ext in EXTENSION_MAP:
        return EXTENSION_MAP[ext]

    # filename keywords
    name = path.stem.lower()
    for domain in DOMAIN_MAP:
        if domain in name:
            return domain

    # folder hints
    parts = {p.lower() for p in path.parts}
    if "audio_data" in parts:
        return "audio"
    if "geo_data" in parts or "shapefiles" in parts:
        return "geo"
    if "data_midi" in parts or "midi_data" in parts:
        return "midi"
    return None


def _try_paths(candidates: Iterable[Path]) -> Optional[Path]:
    for c in candidates:
        if c.exists():
            return c.resolve()
    return None

def resolve_data_path(spec_path: Path, data_path: Path) -> Path:
    """
    Robust resolver:
      1) absolute
      2) relative to spec dir
      3) relative to plugin root (PRESERVE any subdirs from data_path)
      4) domain-based sibling folders under plugin root
      5) project-root data_* and *_data folders
    """
    if data_path.is_absolute():
        if data_path.exists():
            return data_path.resolve()
        raise FileNotFoundError(f"Data file not found: {data_path}")

    spec_dir    = spec_path.parent                   # e.g., plugins/midi/specs
    plugin_root = spec_path.parents[1]               # e.g., plugins/midi
    project_root = Path(__file__).resolve().parents[3]  # repo root

    # 1) as-written, relative to the spec file
    found = _try_paths([(spec_dir / data_path).resolve()])
    if found:
        return found

    # 2) relative to plugin root — PRESERVE SUBPATHS
    found = _try_paths([
        (plugin_root / data_path).resolve(),        # keep any subdirs like audio_data/x.csv
        (plugin_root / data_path.name).resolve(),   # basename fallback
    ])
    if found:
        return found

    # 3) domain-based sibling search under plugin root
    domain = infer_domain_from_name(data_path) or "csv"
    sibling_folders = DOMAIN_MAP.get(domain, [])
    # also try all known folders as safety net
    all_folders = sorted({f for lst in DOMAIN_MAP.values() for f in lst})
    search_folders = sibling_folders + [f for f in all_folders if f not in sibling_folders]

    candidates = []
    for folder in search_folders:
        candidates.append((plugin_root / folder / data_path).resolve())      # keep subpath
        candidates.append((plugin_root / folder / data_path.name).resolve()) # basename
    found = _try_paths(candidates)
    if found:
        return found

    # 4) project-root data folders: data_*, *_data, viz_videos
    pr_candidates = []
    for pattern in ["data_*", "*_data", "viz_videos"]:
        for folder in project_root.glob(pattern):
            pr_candidates.append((folder / data_path).resolve())
            pr_candidates.append((folder / data_path.name).resolve())
    found = _try_paths(pr_candidates)
    if found:
        return found

    # 5) not found
    raise FileNotFoundError(
        f"❌ Could not resolve data path for {data_path}\n"
        f"Tried (examples):\n"
        f"  - {(spec_dir / data_path)}\n"
        f"  - {(plugin_root / data_path)}\n"
        f"  - {(plugin_root / 'audio_data' / data_path.name)}\n"
        f"  - project root data_* / *_data / viz_videos\n"
    )
