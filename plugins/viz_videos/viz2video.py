#!/usr/bin/env python3
# viz2video.py
# Generalized "time-series → video" engine for IG/TikTok
# Supports: choropleth | line | bar_race

from __future__ import annotations
from plugins.utils.path_resolver import resolve_data_path
import sys
from pathlib import Path
import os

sys.path.append(str(Path(__file__).resolve().parents[2]))

import math, warnings, re, subprocess, textwrap
from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import imageio.v2 as imageio
import yaml
import matplotlib.pyplot as plt
import matplotlib as mpl
import imageio.v2 as imageio
import yaml
from tqdm import tqdm  # <-- NEW

# Optional (only needed for choropleth)
try:
    import geopandas as gpd
    _HAS_GEO = True
except Exception:
    _HAS_GEO = False

warnings.filterwarnings("ignore", category=UserWarning, module="matplotlib")

# ---------- HELPERS ----------
def split_camel_case(s: str) -> str:
    return re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', s)

def derive_title_from_spec(path: str) -> str:
    base = Path(path).stem
    base = re.sub(r'[_\-]+', ' ', base)
    return base.replace("map", "").strip().title()

def wrap_title(title: str, width_chars: int = 28) -> str:
    return textwrap.fill(title, width=width_chars)

# ---------- FIGURE CONVERSION ----------
def fig_to_ndarray(fig: plt.Figure) -> np.ndarray:
    fig.canvas.draw()
    w, h = fig.canvas.get_width_height()
    try:
        buf = np.asarray(fig.canvas.renderer.buffer_rgba())
        if buf.shape[2] == 4:
            buf = buf[..., :3]
    except Exception:
        try:
            buf = np.frombuffer(fig.canvas.tostring_rgb(), dtype=np.uint8)
            buf = buf.reshape((h, w, 3))
        except Exception:
            buf = np.frombuffer(fig.canvas.tostring_argb(), dtype=np.uint8)
            buf = buf.reshape((h, w, 4))[..., 1:4]
    return np.asarray(buf, dtype=np.uint8)

# ---------- DATA HELPERS ----------
def read_table(path: str) -> pd.DataFrame:
    p = path.lower()
    if p.endswith(".parquet"):
        return pd.read_parquet(path)
    if p.endswith(".csv"):
        return pd.read_csv(path)
    if p.endswith(".tsv"):
        return pd.read_csv(path, sep="\t")
    return pd.read_csv(path)

def ensure_datetime(df: pd.DataFrame, col: str) -> pd.Series:
    s = pd.to_datetime(df[col], errors="coerce")
    if s.dt.tz is not None:
        s = s.dt.tz_localize(None)
    return s

def safe_title(fmt: Optional[str], **kw) -> str:
    if not fmt:
        return ""
    try:
        return fmt.format(**kw)
    except Exception:
        return fmt

def make_figure(width_px: int, height_px: int, dpi: int) -> plt.Figure:
    fig = plt.figure(figsize=(width_px / dpi, height_px / dpi), dpi=dpi)
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    return fig

# ---------- SPEC CONFIG ----------
@dataclass
class Spec:
    chart_type: str
    data: str
    time: str
    value: Optional[str] = None
    group: Optional[str] = None
    category: Optional[str] = None
    top_n: int = 8
    geo: Optional[str] = None
    join_left_on: Optional[str] = None
    join_right_on: Optional[str] = None
    palette: str = "Reds"
    width: int = 1080
    height: int = 1920
    dpi: int = 150
    fps: int = 24
    bitrate: str = "8M"
    out: str = "out.mp4"
    title: Optional[str] = None
    legend: bool = True
    vmin: Optional[float] = None
    vmax: Optional[float] = None
    hold_frames: int = 1

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Spec":
        return Spec(**d)

# ---------- BASE RENDERER ----------
class BaseRenderer:
    def __init__(self, spec: Spec, df: pd.DataFrame):
        self.spec = spec
        self.df = df.copy()
        try:
            t = ensure_datetime(self.df, spec.time)
            if t.isna().all():
                self.df["_time"] = pd.to_datetime(self.df[spec.time].astype(int), format="%Y", errors="coerce")
            else:
                self.df["_time"] = t
        except Exception:
            self.df["_time"] = pd.to_datetime(self.df[spec.time].astype(int), format="%Y", errors="coerce")

        self.df = self.df.sort_values("_time")
        self.times = [t for t in self.df["_time"].unique() if pd.notna(t)]

        if len(self.times) > 0 and self.spec.fps > 0:
            target_seconds = 10
            total_frames = len(self.times)
            frames_per_time = max(1, int(target_seconds * self.spec.fps / total_frames))
            self.spec.hold_frames = frames_per_time
            print(f"[INFO] Auto duration: {len(self.times)} steps × {frames_per_time} holds @ {self.spec.fps}fps")



    def writer(self):
        """Always use CPU-based libx264 encoder for stability."""
        codec = "libx264"
        print(f"[INFO] Using CPU encoder: {codec}")
        return imageio.get_writer(
            self.spec.out,
            fps=self.spec.fps,
            codec=codec,
            bitrate=self.spec.bitrate,
            ffmpeg_log_level="error",
            output_params=[
                "-pix_fmt", "yuv420p",
            ],
        )

      
# ---------- CHOROPLETH RENDERER ----------
class ChoroplethRenderer(BaseRenderer):
    def render(self):
        if not _HAS_GEO:
            raise RuntimeError("geopandas required for choropleth.")
        sp = self.spec
        assert sp.geo and sp.value and sp.join_left_on and sp.join_right_on, \
            "Spec.geo, Spec.value, Spec.join_left_on, Spec.join_right_on required."

        gdf = gpd.read_file(sp.geo)
        all_vals = pd.to_numeric(self.df[sp.value], errors="coerce").dropna()
        vmin = sp.vmin if sp.vmin is not None else (float(all_vals.min()) if not all_vals.empty else 0.0)
        vmax = sp.vmax if sp.vmax is not None else (float(all_vals.max()) if not all_vals.empty else 1.0)
        norm = plt.Normalize(vmin=vmin, vmax=vmax)
        cmap = plt.colormaps.get(sp.palette, plt.colormaps["Reds"])

        writer = self.writer()
        try:
            for t in tqdm(self.times, desc="[Choropleth] Rendering", unit="frame"):
                cur = self.df[self.df["_time"] == t]
                if sp.join_right_on in gdf.columns:
                    gdf[sp.join_right_on] = gdf[sp.join_right_on].astype(str).str.zfill(5)
                if sp.join_left_on in cur.columns:
                    cur[sp.join_left_on] = cur[sp.join_left_on].astype(str).str.zfill(5)
                frame_gdf = gdf.merge(cur, left_on=sp.join_right_on, right_on=sp.join_left_on, how="left")

                fig = make_figure(sp.width, sp.height, sp.dpi)
                ax = fig.add_subplot(111)
                legend_label = split_camel_case(sp.value.replace("_", " "))
                frame_gdf.plot(column=sp.value, ax=ax, cmap=cmap, linewidth=0.2, edgecolor="0.3", norm=norm)
                ax.set_axis_off()

                if sp.legend:
                    cax = fig.add_axes([0.1, 0.06, 0.8, 0.015])
                    cb = mpl.colorbar.ColorbarBase(cax, cmap=cmap, norm=norm, orientation="horizontal")
                    cb.set_label(re.sub(r'([a-z])([A-Z])', r'\1 \2', sp.value.replace("_", " ")).title(),
                                 fontsize=14, weight="bold")

                if sp.time in cur.columns and not cur[sp.time].dropna().empty:
                    year_label = str(int(cur[sp.time].iloc[0]))
                else:
                    year_label = str(pd.to_datetime(t, errors="coerce").year)

                base_title = re.sub(r'([a-z])([A-Z])', r'\1 \2', sp.title or derive_title_from_spec(sp.data)).strip()
                wrapped_title = wrap_title(base_title)
                fig.suptitle(wrapped_title, fontsize=30, fontweight="bold", ha="center", y=0.985, color="#222")

                ax.text(0.5, 1.02, year_label, transform=ax.transAxes,
                        ha="center", va="bottom", fontsize=22, weight="bold", color="#333")
                ax.text(0.96, -0.25, "↑ N", transform=ax.transAxes,
                        ha="right", va="bottom", fontsize=36, fontweight="bold", color="#111")

                fig.tight_layout()
                nd = fig_to_ndarray(fig)
                plt.close(fig)
                for _ in range(max(1, sp.hold_frames)):
                    writer.append_data(nd)
        finally:
            writer.close()

class LineRenderer(BaseRenderer):
    def render(self):
        sp = self.spec
        writer = self.writer()
        ycol = sp.value or self.df.columns[-1]
        all_vals = pd.to_numeric(self.df[ycol], errors="coerce").dropna()
        vmin = sp.vmin if sp.vmin is not None else float(all_vals.min())
        vmax = sp.vmax if sp.vmax is not None else float(all_vals.max())

        try:
            for t in tqdm(self.times, desc="[Line] Rendering", unit="frame"):
                cur = self.df[self.df["_time"] <= t]
                fig = make_figure(sp.width, sp.height, sp.dpi)
                ax = fig.add_subplot(111)
                ax.plot(cur["_time"], cur[ycol], color="tab:blue", linewidth=3)
                ax.set_xlim(self.df["_time"].min(), self.df["_time"].max())
                ax.set_ylim(vmin, vmax)
                ax.grid(True, alpha=0.3)
                title = wrap_title(sp.title or derive_title_from_spec(sp.data))
                fig.suptitle(title, fontsize=26, weight="bold", y=0.96)
                ax.text(0.95, 0.9, str(pd.to_datetime(t).year),
                        transform=ax.transAxes, ha="right", va="top",
                        fontsize=22, weight="bold", color="#444")
                fig.tight_layout()
                nd = fig_to_ndarray(fig)
                plt.close(fig)
                for _ in range(sp.hold_frames):
                    writer.append_data(nd)
        finally:
            writer.close()

class AudioWaveRenderer(BaseRenderer):
    def render(self):
        sp = self.spec
        writer = self.writer()
        ycol = sp.value or "energy"
        tcol = sp.time or "frame"
        vals = pd.to_numeric(self.df[ycol], errors="coerce").fillna(0)
        vmin, vmax = float(vals.min()), float(vals.max())

        try:
            for i, t in enumerate(tqdm(self.df[tcol], desc="[AudioWave] Rendering", unit="frame")):
                fig = make_figure(sp.width, sp.height, sp.dpi)
                ax = fig.add_subplot(111)
                ax.plot(self.df[tcol][:i], self.df[ycol][:i], color="dodgerblue", linewidth=2)
                ax.set_xlim(self.df[tcol].min(), self.df[tcol].max())
                ax.set_ylim(vmin, vmax)
                ax.axis("off")
                fig.suptitle(wrap_title(sp.title or derive_title_from_spec(sp.data)),
                             fontsize=26, weight="bold", y=0.96)
                nd = fig_to_ndarray(fig)
                plt.close(fig)
                for _ in range(sp.hold_frames):
                    writer.append_data(nd)
        finally:
            writer.close()

class BarRaceRenderer(BaseRenderer):
    def render(self):
        sp = self.spec
        writer = self.writer()
        try:
            for t in tqdm(self.times, desc="[BarRace] Rendering", unit="frame"):
                cur = self.df[self.df["_time"] == t]
                if sp.group and sp.value:
                    cur = cur.groupby(sp.group)[sp.value].sum().nlargest(sp.top_n).reset_index()
                fig = make_figure(sp.width, sp.height, sp.dpi)
                ax = fig.add_subplot(111)
                ax.barh(cur[sp.group], cur[sp.value], color="tab:blue")
                ax.invert_yaxis()
                title = wrap_title(sp.title or derive_title_from_spec(sp.data))
                fig.suptitle(title, fontsize=26, weight="bold", y=0.96)
                ax.text(0.98, 0.1, str(pd.to_datetime(t).year),
                        transform=ax.transAxes, ha="right", va="bottom",
                        fontsize=28, weight="bold", color="#333")
                fig.tight_layout()
                nd = fig_to_ndarray(fig)
                plt.close(fig)
                for _ in range(sp.hold_frames):
                    writer.append_data(nd)
        finally:
            writer.close()

# ---------- FACTORY ----------
def build_renderer(spec: Spec, df: pd.DataFrame):
    chart = spec.chart_type.lower()
    if chart == "choropleth":
        return ChoroplethRenderer(spec, df)
    elif chart == "line":
        return AudioWaveRenderer(spec, df)
    elif chart == "audio_waveform":
        return AudioWaveRenderer(spec, df)
    else:
        raise ValueError(f"Unsupported chart_type: {spec.chart_type}")

# ---------- CLI ----------
def run(spec_path: str) -> str:
    with open(spec_path, "r", encoding="utf-8") as f:
        d = yaml.safe_load(f)
        spec = Spec.from_dict(d)
        data_path = Path(spec.data)
        if not data_path.is_absolute():
            spec.data = str(resolve_data_path(Path(spec_path), data_path))
        if not spec.title:
            spec.title = derive_title_from_spec(spec_path)

    df = read_table(spec.data)
    rend = build_renderer(spec, df)
    rend.render()

    src = Path(spec.out)
    out_path = src.with_name(f"{src.stem}_vertical.mp4")

    cmd = [
        "ffmpeg", "-y", "-i", str(src),
        "-vf", (
            "scale=1080:-2:force_original_aspect_ratio=decrease,"
            "pad=1080:1920:(ow-iw)/2:(oh-ih)/2:white,setsar=1:1"
        ),
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-r", str(spec.fps),
        str(out_path)
    ]
    subprocess.run(cmd, check=True)
    print(f"📱 Vertical formatted version written to: {out_path}")

    audio_path = Path(spec.data).with_suffix(".wav").name
    audio_file = Path("plugins/midi/audio_out") / audio_path

    if audio_file.exists():
        with_audio = out_path.with_name(f"{out_path.stem}_with_audio.mp4")
        subprocess.run([
            "ffmpeg", "-y", "-i", str(out_path),
            "-i", str(audio_file),
            "-c:v", "copy", "-c:a", "aac", "-shortest", str(with_audio)
        ], check=True)
        print(f"🎧 Audio merged: {with_audio}")
    else:
        print(f"⚠️ No matching audio file found: {audio_file}")

    return str(out_path)

def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(
        description="Generalized time-series → video engine (supports per-plugin specs directories)"
    )
    parser.add_argument("--spec", help="YAML spec filename or full path", type=str)
    parser.add_argument("--index", help="Index of spec in the given --dir folder", type=int)
    parser.add_argument("--dir", help="Path to specs directory (defaults to ./specs)", type=str, default="specs")
    args = parser.parse_args(argv)

    specs_dir = Path(args.dir).resolve()
    if not specs_dir.exists():
        sys.exit(f"❌ Specs directory not found: {specs_dir}")

    spec_path = None

    if args.index is not None:
        specs = sorted(specs_dir.glob("*.yaml"))
        if not specs:
            sys.exit(f"❌ No YAML specs found in {specs_dir}")
        try:
            spec_path = specs[args.index]
            print(f"✅ Using spec #{args.index}: {spec_path.name}")
        except IndexError:
            sys.exit(f"❌ Invalid index {args.index}. Only {len(specs)} specs available in {specs_dir}.")
    elif args.spec:
        candidate = Path(args.spec)
        spec_path = candidate if candidate.exists() else specs_dir / args.spec
        if not spec_path.exists():
            sys.exit(f"❌ Spec not found: {spec_path}")
    else:
        specs = sorted(specs_dir.glob("*.yaml"))
        if not specs:
            sys.exit(f"❌ No specs found in {specs_dir}")
        print(f"Available specs in {specs_dir}:")
        for i, s in enumerate(specs):
            print(f" [{i}] {s.name}")
        try:
            choice = int(input("Select index: "))
            spec_path = specs[choice]
            print(f"✅ Using spec: {spec_path.name}")
        except (ValueError, IndexError):
            sys.exit("❌ Invalid selection.")

    run(str(spec_path))

if __name__ == "__main__":
    main()
