"""Stage: sink_audio
=====================

Utility for packaging generated audio artifacts (WAV, CSV feature tables,
and YAML visualization specs) into a single directory that can be pushed to
Hugging Face Datasets.  The stage performs two primary actions:

1. ``copy_and_structure_files`` – Collect all inputs, mirror them into a
   structured output directory, and build lightweight metadata (README +
   manifest JSON) describing the bundle.
2. ``upload_folder_to_hf`` – Upload the prepared folder to a Hugging Face
   dataset repository using :func:`huggingface_hub.upload_folder`.

The module can be used as a CLI, but the functions are also importable so it
can be orchestrated by higher-level workflow definitions.
"""
from __future__ import annotations

import argparse
import json
import shutil
import textwrap
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Mapping, MutableMapping, Sequence

from huggingface_hub import upload_folder


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_AUDIO_IN = PROJECT_ROOT / "plugins/midi/audio_in"
DEFAULT_AUDIO_DATA = PROJECT_ROOT / "plugins/midi/audio_data"
DEFAULT_SPECS = PROJECT_ROOT / "plugins/midi/specs"
DEFAULT_OUTPUT = PROJECT_ROOT / "plugins/midi/dataset_package"

DATASET_CARD_NAME = "README.md"
MANIFEST_NAME = "manifest.json"
SUMMARY_NAME = "summary.json"


@dataclass
class StageParams:
    """Configuration passed to the stage."""

    repo_id: str = "mileslilly/sound_videos"
    license: str = "CC-BY-4.0"
    description: str = (
        "Dataset containing .wav, extracted feature CSVs, and YAML visualization "
        "specs for each audio file. Ready for processing by viz2video.py."
    )
    upload_method: str = "huggingface_hub.upload_folder"
    delete_local_after_upload: bool = False


@dataclass
class StageInputs:
    audio_in: Path = DEFAULT_AUDIO_IN
    audio_data: Path = DEFAULT_AUDIO_DATA
    specs: Path = DEFAULT_SPECS


@dataclass
class StageOutputs:
    dataset_package: Path = DEFAULT_OUTPUT


def _copy_tree(src: Path, dest: Path, *, allow_ext: Sequence[str]) -> List[Path]:
    """Copy ``src`` into ``dest`` (preserving structure) filtering by extension."""
    copied: List[Path] = []
    if not src.exists():
        return copied

    src = src.resolve()
    dest.mkdir(parents=True, exist_ok=True)

    for path in sorted(src.rglob("*")):
        if not path.is_file():
            continue
        if allow_ext and path.suffix.lower() not in allow_ext:
            continue
        rel = path.relative_to(src)
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        copied.append(target)
    return copied


def _group_by_audio_stem(paths: Iterable[Path]) -> MutableMapping[str, List[Path]]:
    groups: MutableMapping[str, List[Path]] = defaultdict(list)
    for p in paths:
        stem = p.stem
        key = stem.split("_audio_", 1)[0] if "_audio_" in stem else stem
        groups[key].append(p)
    return groups


def _write_manifest(output_dir: Path, audio: List[Path], csvs: List[Path], yamls: List[Path]) -> Path:
    audio_by_stem = _group_by_audio_stem(audio)
    csv_by_stem = _group_by_audio_stem(csvs)
    yaml_by_stem = _group_by_audio_stem(yamls)

    manifest = []
    for stem, audio_paths in sorted(audio_by_stem.items()):
        manifest.append(
            {
                "id": stem,
                "audio": [str(p.relative_to(output_dir)) for p in audio_paths],
                "tables": [str(p.relative_to(output_dir)) for p in sorted(csv_by_stem.get(stem, []))],
                "specs": [str(p.relative_to(output_dir)) for p in sorted(yaml_by_stem.get(stem, []))],
            }
        )

    unmatched_tables = {
        stem: [str(p.relative_to(output_dir)) for p in sorted(paths)]
        for stem, paths in sorted(csv_by_stem.items())
        if stem not in audio_by_stem
    }
    unmatched_specs = {
        stem: [str(p.relative_to(output_dir)) for p in sorted(paths)]
        for stem, paths in sorted(yaml_by_stem.items())
        if stem not in audio_by_stem
    }

    manifest_data: Mapping[str, object] = {
        "items": manifest,
        "unmatched_tables": unmatched_tables,
        "unmatched_specs": unmatched_specs,
        "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "version": 1,
    }

    manifest_path = output_dir / MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest_data, indent=2), encoding="utf-8")
    return manifest_path


def _write_summary(output_dir: Path, audio: List[Path], csvs: List[Path], yamls: List[Path]) -> Path:
    summary = {
        "audio_count": len(audio),
        "table_count": len(csvs),
        "spec_count": len(yamls),
        "relative_audio_dir": "audio",
        "relative_table_dir": "tables",
        "relative_spec_dir": "specs",
    }
    path = output_dir / SUMMARY_NAME
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return path


def _write_dataset_card(output_dir: Path, params: StageParams) -> Path:
    card = output_dir / DATASET_CARD_NAME
    header = f"# {params.repo_id.split('/')[-1].replace('_', ' ').title()}\n\n"
    body = textwrap.dedent(
        f"""
        ## Dataset Description
        {params.description}

        ## Contents
        - Audio WAV files rendered from MIDI inputs.
        - CSV feature tables (waveform, energy, tempo, beats, pitch, spectrogram).
        - YAML visualization specs compatible with `viz2video.py`.

        ## License
        {params.license}

        Generated by the Abstract Data Pipeline MIDI workflow.
        """
    ).strip()
    card.write_text(header + body + "\n", encoding="utf-8")
    return card


def copy_and_structure_files(
    inputs: StageInputs,
    outputs: StageOutputs,
    params: StageParams,
) -> Mapping[str, Path]:
    """Collect WAV/CSV/YAML files and place them into the dataset package."""
    out_dir = outputs.dataset_package
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    audio_dest = out_dir / "audio"
    csv_dest = out_dir / "tables"
    spec_dest = out_dir / "specs"

    copied_audio = _copy_tree(inputs.audio_in, audio_dest, allow_ext=(".wav",))
    copied_csv = _copy_tree(inputs.audio_data, csv_dest, allow_ext=(".csv",))
    copied_specs = _copy_tree(inputs.specs, spec_dest, allow_ext=(".yaml", ".yml"))

    manifest_path = _write_manifest(out_dir, copied_audio, copied_csv, copied_specs)
    summary_path = _write_summary(out_dir, copied_audio, copied_csv, copied_specs)
    card_path = _write_dataset_card(out_dir, params)

    return {
        "output_dir": out_dir,
        "manifest": manifest_path,
        "summary": summary_path,
        "dataset_card": card_path,
    }


def upload_folder_to_hf(
    folder: Path,
    params: StageParams,
    *,
    commit_message: str | None = None,
    dry_run: bool = False,
) -> None:
    """Upload ``folder`` to the configured Hugging Face dataset repo."""
    if params.upload_method != "huggingface_hub.upload_folder":
        raise ValueError(
            "Unsupported upload_method: "
            f"{params.upload_method!r} (expected 'huggingface_hub.upload_folder')"
        )

    if not folder.exists():
        raise FileNotFoundError(folder)

    commit_message = commit_message or f"Update dataset package ({time.strftime('%Y-%m-%d')})"

    print(f"[sink_audio] Uploading {folder} → {params.repo_id}")
    if dry_run:
        print("[sink_audio] Dry run enabled; skipping upload.")
        return

    upload_folder(
        repo_id=params.repo_id,
        folder_path=str(folder),
        repo_type="dataset",
        commit_message=commit_message,
    )
    print(f"[sink_audio] ✅ Upload complete → https://huggingface.co/datasets/{params.repo_id}")

    if params.delete_local_after_upload:
        shutil.rmtree(folder)
        print(f"[sink_audio] 🧹 Removed local folder {folder}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Package audio artifacts and upload to Hugging Face")
    parser.add_argument("--audio-in", type=Path, default=DEFAULT_AUDIO_IN)
    parser.add_argument("--audio-data", type=Path, default=DEFAULT_AUDIO_DATA)
    parser.add_argument("--specs", type=Path, default=DEFAULT_SPECS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--repo-id", default=StageParams.repo_id)
    parser.add_argument("--license", default=StageParams.license)
    parser.add_argument("--description", default=StageParams.description)
    parser.add_argument("--delete-local-after-upload", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    inputs = StageInputs(audio_in=args.audio_in, audio_data=args.audio_data, specs=args.specs)
    outputs = StageOutputs(dataset_package=args.output)
    params = StageParams(
        repo_id=args.repo_id,
        license=args.license,
        description=args.description,
        delete_local_after_upload=args.delete_local_after_upload,
    )

    results = copy_and_structure_files(inputs, outputs, params)
    print(
        "[sink_audio] Packaged dataset at",
        results["output_dir"],
        "(manifest:",
        results["manifest"],
        ")",
    )

    if not args.skip_upload:
        upload_folder_to_hf(outputs.dataset_package, params, dry_run=args.dry_run)
    else:
        print("[sink_audio] Upload skipped by flag.")


if __name__ == "__main__":
    main()
