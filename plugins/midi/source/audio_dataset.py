"""MIDI audio source utilities.

This module provides a ``Source`` implementation that can download or copy
raw ``.wav`` files into the repository for downstream processing.  It is
driven by simple parameters so it can be reused from YAML pipelines.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Optional

from adp.core.base import Context, Record, Source

try:  # pragma: no cover - import guard is exercised in runtime environments
    from huggingface_hub import snapshot_download  # type: ignore
except Exception:  # pragma: no cover - handled gracefully at runtime
    snapshot_download = None  # type: ignore


@dataclass
class _Entry:
    """Represent one fetched audio file for manifest + downstream use."""

    dataset: str
    source: Path
    destination: Path
    status: str
    bytes: int

    def to_record(self) -> Record:
        return {
            "dataset": self.dataset,
            "source": str(self.source),
            "destination": str(self.destination),
            "status": self.status,
            "bytes": self.bytes,
            "id": self.destination.stem,
        }


class FetchAudioDataset(Source):
    """Fetch or copy raw WAV files for downstream MIDI audio pipelines.

    Parameters (all optional and supplied via ``params`` in pipeline YAML):

    ``dataset``
        Hugging Face dataset ID (``repo_id``) *or* path to a local folder
        containing ``.wav`` files.  Required.
    ``limit``
        Maximum number of files to bring in (default: ``None`` → all files).
    ``local_cache_dir``
        Directory where ``snapshot_download`` should cache files.  Relative
        paths are resolved from ``ctx.workdir``.
    ``download_method``
        Either ``"snapshot_download"`` (default) or ``"local_copy"``.
    ``overwrite_existing``
        When ``False`` (default) existing files in ``audio_in`` are preserved
        and marked as ``"skipped"`` in the manifest.
    ``output_dir``
        Destination directory for the WAV files.  Defaults to
        ``plugins/midi/audio_in``.
    ``manifest_filename``
        Name of the JSONL manifest written alongside the files.  Defaults to
        ``fetched_files_manifest.jsonl``.
    """

    DEFAULT_OUTPUT_DIR = Path("plugins/midi/audio_in")
    DEFAULT_MANIFEST = "fetched_files_manifest.jsonl"

    def run(self, ctx: Context) -> Iterator[Record]:
        dataset = (self.kw.get("dataset") or "").strip()
        if not dataset:
            raise ValueError("dataset parameter is required for FetchAudioDataset")

        limit = self._parse_limit(self.kw.get("limit"))
        overwrite = bool(self.kw.get("overwrite_existing", False))
        method = (self.kw.get("download_method") or "snapshot_download").strip().lower()

        output_dir = self._resolve_dir(ctx.workdir, self.kw.get("output_dir") or self.DEFAULT_OUTPUT_DIR)
        manifest_path = output_dir / (self.kw.get("manifest_filename") or self.DEFAULT_MANIFEST)
        output_dir.mkdir(parents=True, exist_ok=True)

        source_paths = self._collect_sources(ctx, dataset, method, self.kw.get("local_cache_dir"))
        wav_paths = [p for p in sorted(source_paths) if p.suffix.lower() == ".wav"]
        if limit is not None:
            wav_paths = wav_paths[:limit]

        entries: List[_Entry] = []
        for src in wav_paths:
            dest = output_dir / src.name
            if dest.exists() and not overwrite:
                status = "skipped"
            elif dest.resolve() == src.resolve():
                status = "exists"
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dest)
                status = "copied"

            size = dest.stat().st_size if dest.exists() else src.stat().st_size
            entry = _Entry(dataset=dataset, source=src, destination=dest, status=status, bytes=size)
            entries.append(entry)
            ctx.log.info(f"{status.upper()}: {src.name} → {dest}")
            yield entry.to_record()

        self._write_manifest(manifest_path, entries)
        ctx.log.info(f"Wrote manifest with {len(entries)} entries to {manifest_path}")

    # ----- helpers -----
    @staticmethod
    def _parse_limit(value: Optional[int | str]) -> Optional[int]:
        if value in (None, "", 0, "0"):
            return None
        try:
            return max(int(value), 0)
        except (TypeError, ValueError):
            raise ValueError(f"limit must be an integer, got {value!r}")

    @staticmethod
    def _resolve_dir(base: Path, path: Path | str) -> Path:
        p = Path(path)
        if not p.is_absolute():
            p = base / p
        return p

    def _collect_sources(self, ctx: Context, dataset: str, method: str, cache_dir: Optional[str]) -> Iterable[Path]:
        if method == "snapshot_download":
            if snapshot_download is None:  # pragma: no cover - depends on optional dep
                raise RuntimeError("huggingface_hub is required for snapshot_download but is not installed")

            kwargs = {
                "repo_id": dataset,
                "repo_type": "dataset",
                "allow_patterns": ["*.wav"],
                "local_dir_use_symlinks": False,
            }
            if cache_dir:
                kwargs["local_dir"] = str(self._resolve_dir(ctx.workdir, cache_dir))

            ctx.log.info(f"Downloading {dataset} via snapshot_download …")
            local_path = Path(snapshot_download(**kwargs))
            return list(local_path.rglob("*.wav"))

        if method == "local_copy":
            src_dir = Path(dataset)
            if not src_dir.is_absolute():
                src_dir = ctx.workdir / src_dir
            if not src_dir.exists():
                raise FileNotFoundError(f"Local dataset path not found: {src_dir}")
            return list(src_dir.rglob("*.wav"))

        raise ValueError(f"Unsupported download_method: {method}")

    @staticmethod
    def _write_manifest(path: Path, entries: List[_Entry]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            for entry in entries:
                fh.write(json.dumps(entry.to_record(), ensure_ascii=False) + "\n")
