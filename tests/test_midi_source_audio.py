from __future__ import annotations

import json
from pathlib import Path

import pytest

from adp.core.base import Context
from plugins.midi.source.audio_dataset import FetchAudioDataset


class _DummyState:
    def get(self, key, default=None):
        return default

    def set(self, key, value):
        pass

    def save(self):
        pass


class _DummyLog:
    def info(self, *args, **kwargs):
        pass

    def warn(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


def _build_ctx(tmp_path: Path) -> Context:
    return Context(
        workdir=tmp_path,
        outdir=tmp_path / "out",
        state=_DummyState(),
        log=_DummyLog(),
        config={},
        env={},
    )


def _make_wav(dir_path: Path, name: str, payload: bytes = b"data") -> Path:
    path = dir_path / name
    dir_path.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def test_fetch_audio_dataset_local_copy(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _make_wav(dataset_dir, "a.wav")
    _make_wav(dataset_dir, "b.wav")
    _make_wav(dataset_dir, "c.wav")

    ctx = _build_ctx(tmp_path)
    source = FetchAudioDataset(
        dataset=str(dataset_dir),
        download_method="local_copy",
        limit=2,
        output_dir="plugins/midi/audio_in",
    )

    records = list(source.run(ctx))

    assert len(records) == 2
    dest_dir = tmp_path / "plugins/midi/audio_in"
    copied = sorted(p.name for p in dest_dir.glob("*.wav"))
    assert copied == ["a.wav", "b.wav"]

    manifest = dest_dir / "fetched_files_manifest.jsonl"
    assert manifest.exists()
    lines = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert [entry["destination"] for entry in lines] == [str(dest_dir / "a.wav"), str(dest_dir / "b.wav")]
    assert all(entry["status"] in {"copied", "exists", "skipped"} for entry in lines)


def test_fetch_audio_dataset_skip_existing(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _make_wav(dataset_dir, "keep.wav", b"original")

    ctx = _build_ctx(tmp_path)
    source = FetchAudioDataset(
        dataset=str(dataset_dir),
        download_method="local_copy",
        output_dir="plugins/midi/audio_in",
    )

    list(source.run(ctx))  # first run copies the file

    dest_file = tmp_path / "plugins/midi/audio_in" / "keep.wav"
    dest_file.write_bytes(b"modified")

    records = list(source.run(ctx))
    assert len(records) == 1
    assert records[0]["status"] == "skipped"
    assert dest_file.read_bytes() == b"modified"  # ensure not overwritten

    manifest = dest_file.parent / "fetched_files_manifest.jsonl"
    last_line = json.loads(manifest.read_text().splitlines()[-1])
    assert last_line["status"] == "skipped"


def test_fetch_audio_dataset_invalid_dataset(tmp_path: Path) -> None:
    ctx = _build_ctx(tmp_path)
    source = FetchAudioDataset(dataset="", download_method="local_copy")
    with pytest.raises(ValueError):
        list(source.run(ctx))


def test_fetch_audio_dataset_missing_local_dir(tmp_path: Path) -> None:
    ctx = _build_ctx(tmp_path)
    source = FetchAudioDataset(dataset="missing", download_method="local_copy")
    with pytest.raises(FileNotFoundError):
        list(source.run(ctx))
