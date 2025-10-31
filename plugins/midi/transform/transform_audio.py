"""Utility helpers for the ``transform_audio`` stage.

The stage expects a folder of ``.wav`` files in ``plugins/midi/audio_in`` and
produces feature CSVs as well as YAML spec files that can be consumed by
``viz2video``.  Each input waveform is expanded into six analytic views:

* waveform
* energy
* spectrogram
* beats
* pitch curve
* tempo

The implementation mirrors the behaviour of the historical
``generate_audio_specs.py`` script but is organised into two task-like
functions so that the new declarative stage configuration can call them
individually.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, Union

import numpy as np
import pandas as pd
import yaml

import librosa


# ---------------------------------------------------------------------------
# Dataclasses and shared constants


FEATURE_TO_SUFFIX = {
    "waveform": "audio_waveform",
    "energy": "audio_energy",
    "spectrogram": "audio_spectrogram",
    "beats": "audio_beats",
    "pitch_curve": "audio_pitch_curve",
    "tempo": "audio_tempo",
}


@dataclass
class FeatureArtifact:
    """Description of one generated CSV artifact."""

    source_wav: Path
    feature: str
    csv_path: Path
    columns: Sequence[str]

    @property
    def csv_name(self) -> str:
        return self.csv_path.name

    @property
    def chart_type(self) -> str:
        return FEATURE_TO_SUFFIX[self.feature]

    @property
    def base_stem(self) -> str:
        """Return the stem of the original wav without the feature suffix."""

        stem = self.source_wav.stem
        for suffix in FEATURE_TO_SUFFIX.values():
            if stem.endswith("_" + suffix) or stem.endswith(suffix):
                stem = stem[: -len(suffix)].rstrip("_")
                break
        return stem


# ---------------------------------------------------------------------------
# Librosa feature computation helpers


def _ensure_audio_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _load_audio(path: Path, sr: int) -> tuple[np.ndarray, int]:
    y, actual_sr = librosa.load(path, sr=sr, mono=True)
    return y, actual_sr


def _compute_waveform(y: np.ndarray, sr: int) -> pd.DataFrame:
    t = np.arange(len(y)) / sr
    return pd.DataFrame({"time": t, "amplitude": y})


def _compute_energy(y: np.ndarray, sr: int, hop_length: int = 512) -> pd.DataFrame:
    rms = librosa.feature.rms(y=y, hop_length=hop_length)[0]
    times = librosa.frames_to_time(np.arange(len(rms)), sr=sr, hop_length=hop_length)
    return pd.DataFrame({"time": times, "rms": rms})


def _compute_spectrogram(
    y: np.ndarray, sr: int, hop_length: int = 512, n_fft: int = 1024
) -> pd.DataFrame:
    stft = librosa.stft(y, n_fft=n_fft, hop_length=hop_length)
    magnitudes = librosa.amplitude_to_db(np.abs(stft), ref=np.max)
    freqs = librosa.fft_frequencies(sr=sr, n_fft=n_fft)
    times = librosa.frames_to_time(np.arange(magnitudes.shape[1]), sr=sr, hop_length=hop_length)

    records = (
        {"time": times[j], "frequency": freqs[i], "intensity": magnitudes[i, j]}
        for i in range(len(freqs))
        for j in range(len(times))
    )
    return pd.DataFrame.from_records(records)


def _compute_beats(y: np.ndarray, sr: int) -> pd.DataFrame:
    onset_env = librosa.onset.onset_strength(y=y, sr=sr)
    tempo, beats = librosa.beat.beat_track(onset_envelope=onset_env, sr=sr)
    times = librosa.frames_to_time(beats, sr=sr)
    onset_at_beats = onset_env[beats] if len(beats) else np.array([], dtype=float)
    return pd.DataFrame({"time": times, "onset_strength": onset_at_beats})


def _compute_pitch_curve(y: np.ndarray, sr: int) -> pd.DataFrame:
    f0 = librosa.yin(y, fmin=50, fmax=2000, sr=sr)
    times = librosa.frames_to_time(np.arange(len(f0)), sr=sr)
    return pd.DataFrame({"time": times, "frequency": f0})


def _compute_tempo(y: np.ndarray, sr: int) -> pd.DataFrame:
    onset_env = librosa.onset.onset_strength(y=y, sr=sr)
    tempos = librosa.beat.tempo(onset_envelope=onset_env, sr=sr, aggregate=None)
    times = librosa.frames_to_time(np.arange(len(tempos)), sr=sr)
    return pd.DataFrame({"time": times, "tempo": tempos})


FEATURE_FUNCTIONS = {
    "waveform": _compute_waveform,
    "energy": _compute_energy,
    "spectrogram": _compute_spectrogram,
    "beats": _compute_beats,
    "pitch_curve": _compute_pitch_curve,
    "tempo": _compute_tempo,
}


# ---------------------------------------------------------------------------
# Public task helpers


def extract_features_with_librosa(
    input_dir: Path,
    output_dir: Path,
    *,
    sample_rate: int,
    features: Sequence[str],
    csv_columns: Dict[str, Sequence[str]],
) -> List[FeatureArtifact]:
    """Compute per-feature CSVs for all ``.wav`` files inside ``input_dir``.

    Parameters
    ----------
    input_dir:
        Directory containing ``.wav`` files to process.
    output_dir:
        Destination directory for generated CSV files.  The directory is
        created on demand.
    sample_rate:
        Sample rate passed to ``librosa.load``.
    features:
        Iterable of feature names.  Must be keys from ``FEATURE_FUNCTIONS``.
    csv_columns:
        Mapping from feature name to an ordered collection of column names.

    Returns
    -------
    list[FeatureArtifact]
        Metadata describing each generated CSV – used by the subsequent
        ``write_yaml_specs_for_viz2video`` task.
    """

    input_dir = Path(input_dir)
    output_dir = _ensure_audio_dir(Path(output_dir))

    artifacts: List[FeatureArtifact] = []

    wav_files = sorted(p for p in input_dir.glob("*.wav") if p.is_file())
    if not wav_files:
        return artifacts

    for wav_path in wav_files:
        y, sr = _load_audio(wav_path, sample_rate)

        for feature in features:
            if feature not in FEATURE_FUNCTIONS:
                raise ValueError(f"Unsupported feature '{feature}'. Known: {sorted(FEATURE_FUNCTIONS)}")

            compute = FEATURE_FUNCTIONS[feature]
            df = compute(y, sr)

            expected_cols = list(csv_columns.get(feature, df.columns))
            # ``df`` might include additional columns (e.g. spectrogram).  Reorder and
            # keep intersection so that the CSV matches the declarative schema.
            cols_present = [c for c in expected_cols if c in df.columns]
            if cols_present:
                df = df[cols_present]

            suffix = FEATURE_TO_SUFFIX[feature]
            csv_path = output_dir / f"{wav_path.stem}_{suffix}.csv"
            df.to_csv(csv_path, index=False)

            artifacts.append(
                FeatureArtifact(
                    source_wav=wav_path,
                    feature=feature,
                    csv_path=csv_path,
                    columns=cols_present,
                )
            )

    return artifacts


def _normalise_artifacts_input(
    artifacts_or_dir: Union[Iterable[FeatureArtifact], Path, str, None]
) -> List[FeatureArtifact]:
    if artifacts_or_dir is None:
        return []

    if isinstance(artifacts_or_dir, Iterable) and not isinstance(
        artifacts_or_dir, (str, bytes, os.PathLike)
    ):
        return list(artifacts_or_dir)

    # Interpret as a directory of CSV files.
    csv_dir = Path(artifacts_or_dir)
    if not csv_dir.exists():
        return []

    artifacts: List[FeatureArtifact] = []
    for csv_path in sorted(csv_dir.glob("*_audio_*.csv")):
        suffix = csv_path.stem.split("_audio_")[-1]
        feature = next((k for k, v in FEATURE_TO_SUFFIX.items() if v.endswith(suffix)), None)
        if feature is None:
            continue
        artifacts.append(
            FeatureArtifact(
                source_wav=csv_path,
                feature=feature,
                csv_path=csv_path,
                columns=tuple(),
            )
        )
    return artifacts


def write_yaml_specs_for_viz2video(
    artifacts_or_dir: Union[Iterable[FeatureArtifact], Path, str, None],
    spec_dir: Path,
    *,
    csv_columns: Dict[str, Sequence[str]],
) -> List[Path]:
    """Generate viz spec YAML files.

    ``artifacts_or_dir`` may be either an iterable of :class:`FeatureArtifact`
    instances (the return value of :func:`extract_features_with_librosa`) or a
    path to a directory containing the generated CSV files.
    """

    spec_dir = Path(spec_dir)
    spec_dir.mkdir(parents=True, exist_ok=True)

    written: List[Path] = []

    for artifact in _normalise_artifacts_input(artifacts_or_dir):
        feature = artifact.feature
        csv_name = artifact.csv_name
        cols = csv_columns.get(feature, artifact.columns)

        time_col = cols[0] if cols else "time"
        value_col = cols[1] if len(cols) > 1 else cols[0] if cols else "value"

        title_feature = feature.replace("_", " ").title()
        title_track = artifact.base_stem.replace("_", " ").title()

        spec = {
            "chart_type": artifact.chart_type,
            "data": csv_name,
            "time": time_col,
            "value": value_col,
            "width": 1080,
            "height": 1920,
            "dpi": 150,
            "fps": 24,
            "bitrate": "8M",
            "out": f"videos/{artifact.csv_path.stem}.mp4",
            "title": f"{title_track} — {title_feature}",
            "legend": False,
            "hold_frames": 1,
        }

        # Include auxiliary column hints when present (e.g. spectrogram frequency).
        if len(cols) > 2:
            spec["extra_columns"] = list(cols[1:-1])

        yaml_path = spec_dir / f"{artifact.csv_path.stem}.yaml"
        yaml_path.write_text(yaml.safe_dump(spec, sort_keys=False), encoding="utf-8")
        written.append(yaml_path)

    return written


def run_stage(
    *,
    input_dir: Path,
    csv_out_dir: Path,
    spec_out_dir: Path,
    sample_rate: int,
    features: Sequence[str],
    csv_columns: Dict[str, Sequence[str]],
) -> tuple[List[FeatureArtifact], List[Path]]:
    """Convenience wrapper that performs both stage tasks sequentially."""

    artifacts = extract_features_with_librosa(
        Path(input_dir),
        Path(csv_out_dir),
        sample_rate=sample_rate,
        features=features,
        csv_columns=csv_columns,
    )

    specs = write_yaml_specs_for_viz2video(
        artifacts,
        Path(spec_out_dir),
        csv_columns=csv_columns,
    )

    return artifacts, specs


__all__ = [
    "FeatureArtifact",
    "extract_features_with_librosa",
    "write_yaml_specs_for_viz2video",
    "run_stage",
]

