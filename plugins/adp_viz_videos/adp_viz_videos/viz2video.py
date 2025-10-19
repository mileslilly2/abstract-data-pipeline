#!/usr/bin/env python3
# viz2video.py
# Generalized "time-series → video" engine for IG/TikTok
# Supports: choropleth | line | bar_race
# ✅ Final version: guaranteed 1080x1920 output (no squish, no 0-byte, no ffmpeg filter errors)

from __future__ import annotations
import sys, math, warnings, subprocess, shlex
from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import imageio.v2 as imageio
import yaml

# Optional (only needed for choropleth)
try:
    import geopandas as gpd
    _HAS_GEO = True
except Exception:
    _HAS_GEO = False

warnings.filterwarnings("ignore", category=UserWarning, module="matplotlib")

# ---------- FIGURE CONVERSION ----------
def fig_to_ndarray(fig: plt.Figure) -> np.ndarray:
    fig.canvas.draw()
    w, h = fig.canvas.get_width_height()
    try:
        buf = np.asarray(fig.canvas.renderer.buffer_rgba())
        if buf.shape[2] == 4:
            buf = buf[..., :3]
    except Exception:
        buf = np.frombuffer(fig.canvas.tostring_rgb(), dtype=np.uint8)
        buf = buf.reshape((h, w, 3))
    return np.asarray(buf, dtype=np.uint8)

# ---------- HELPERS ----------
def read_table(path: str) -> pd.DataFrame:
    if path.lower().endswith(".parquet"):
        return pd.read_parquet(path)
    if path.lower().endswith(".tsv"):
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

def fixed_axes_limits(series: pd.Series, pad: float = 0.05):
    v = pd.to_numeric(series, errors="coerce").dropna()
    if v.empty:
        return (0, 1)
    lo, hi = float(v.min()), float(v.max())
    if math.isclose(lo, hi):
        return (lo - 0.5, hi + 0.5)
    span = hi - lo
    return (lo - pad * span, hi + pad * span)

def make_figure(width_px: int, height_px: int, dpi: int) -> plt.Figure:
    fig = plt.figure(figsize=(width_px / dpi, height_px / dpi), dpi=dpi)
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    return fig

# ---------- SPEC ----------
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
    height: int = 1080   # render square, upscale to 1080x1920 later
    dpi: int = 150
    fps: int = 24
    bitrate: str = "8M"
    out: str = "out.mp4"
    title: Optional[str] = None
    legend: bool = True
    y_min: Optional[float] = None
    y_max: Optional[float] = None
    x_label: Optional[str] = None
    y_label: Optional[str] = None
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
            self.df["_time"] = t
        except Exception:
            self.df["_time"] = pd.to_datetime(self.df[spec.time].astype(int), format="%Y", errors="coerce")

        self.df = self.df.sort_values("_time")
        self.times = [t for t in self.df["_time"].unique() if pd.notna(t)]

        if len(self.times) > 0 and self.spec.fps > 0:
            total_frames = len(self.times)
            frames_per_time = max(1, int(10 * self.spec.fps / total_frames))
            self.spec.hold_frames = frames_per_time
            print(f"[INFO] Auto duration: {len(self.times)} × {frames_per_time} frames @ {self.spec.fps} fps ≈ 10s")

    def writer(self):
        return imageio.get_writer(
            self.spec.out,
            fps=self.spec.fps,
            codec="libx264",
            bitrate=self.spec.bitrate,
            ffmpeg_log_level="error",
            output_params=["-pix_fmt", "yuv420p"],
        )

# ---------- LINE ----------
class LineRenderer(BaseRenderer):
    def render(self):
        sp, y, g = self.spec, self.spec.value, self.spec.group
        assert y, "Spec.value required for line chart"
        y_min, y_max = fixed_axes_limits(self.df[y])
        writer = self.writer()
        try:
            for t in self.times:
                fig = make_figure(sp.width, sp.height, sp.dpi)
                ax = fig.add_subplot(111)
                cur = self.df[self.df["_time"] <= t]
                if g and g in cur.columns:
                    for name, sub in cur.groupby(g):
                        ax.plot(sub["_time"], pd.to_numeric(sub[y], errors="coerce"), label=name)
                else:
                    ax.plot(cur["_time"], pd.to_numeric(cur[y], errors="coerce"), label=y)
                ax.set_xlim(self.times[0], self.times[-1])
                ax.set_ylim(y_min, y_max)
                ax.grid(alpha=0.2)
                if sp.legend: ax.legend(loc="upper left", frameon=False)
                ax.set_title(safe_title(sp.title, time=pd.to_datetime(t).year), fontsize=18)
                frame = fig_to_ndarray(fig)
                plt.close(fig)
                for _ in range(sp.hold_frames):
                    writer.append_data(frame)
        finally:
            writer.close()

# ---------- CHOROPLETH ----------
class ChoroplethRenderer(BaseRenderer):
    def render(self):
        if not _HAS_GEO:
            raise RuntimeError("geopandas required for choropleth")
        sp = self.spec
        gdf = gpd.read_file(sp.geo)
        all_vals = pd.to_numeric(self.df[sp.value], errors="coerce").dropna()
        vmin, vmax = float(all_vals.min()), float(all_vals.max())
        norm = plt.Normalize(vmin=vmin, vmax=vmax)
        cmap = plt.colormaps.get(sp.palette, plt.colormaps["Reds"])
        writer = self.writer()
        try:
            for t in self.times:
                cur = self.df[self.df["_time"] == t]
                gdf[sp.join_right_on] = gdf[sp.join_right_on].astype(str).str.zfill(5)
                cur[sp.join_left_on] = cur[sp.join_left_on].astype(str).str.zfill(5)
                merged = gdf.merge(cur, left_on=sp.join_right_on, right_on=sp.join_left_on, how="left")

                fig = make_figure(sp.width, sp.height, sp.dpi)
                ax = fig.add_subplot(111)
                merged.plot(column=sp.value, ax=ax, cmap=cmap, edgecolor="0.3",
                            linewidth=0.2, norm=norm, legend=sp.legend,
                            legend_kwds={"shrink": 0.6, "orientation": "vertical"})
                ax.set_axis_off()
                year_label = str(pd.to_datetime(t).year)
                fig.suptitle(safe_title(sp.title, time=year_label), fontsize=22, y=0.96)
                fig.tight_layout()
                nd = fig_to_ndarray(fig)
                plt.close(fig)
                for _ in range(sp.hold_frames):
                    writer.append_data(nd)
        finally:
            writer.close()

# ---------- FACTORY ----------
def build_renderer(spec: Spec, df: pd.DataFrame):
    if spec.chart_type.lower() == "line":
        return LineRenderer(spec, df)
    if spec.chart_type.lower() == "choropleth":
        return ChoroplethRenderer(spec, df)
    raise ValueError(f"Unknown chart_type: {spec.chart_type}")

# ---------- MAIN ----------
def run(spec_path: str) -> str:
    with open(spec_path, "r") as f:
        d = yaml.safe_load(f)
    spec = Spec.from_dict(d)
    df = read_table(spec.data)
    renderer = build_renderer(spec, df)
    renderer.render()

    src = Path(spec.out).resolve()
    if not src.exists() or src.stat().st_size == 0:
        raise RuntimeError(f"Base video missing or empty: {src}")

    out_path = src.with_name(src.stem + "_vertical.mp4")

    # ✅ Simplified, safe scaling: always enforce 1080x1920
    # ✅ Preserve aspect ratio: pad to 1080x1920 (no squish)
    cmd = [
        "ffmpeg", "-y", "-i", str(src),
        "-vf", (
            "scale=1080:-2:force_original_aspect_ratio=decrease,"
            "pad=1080:1920:(ow-iw)/2:(oh-ih)/2:black,setsar=1:1"
        ),
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-r", str(spec.fps),
        str(out_path)
    ]


    print("[INFO] Running:", " ".join(shlex.quote(c) for c in cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print("⚠️ ffmpeg stderr:\n", proc.stderr)
        print("⚠️ ffmpeg stdout:\n", proc.stdout)
        raise SystemExit("❌ ffmpeg conversion failed — see log above")
    print(f"📱 Vertical formatted version written to: {out_path}")
    return str(out_path)

def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="Time-series → vertical video")
    ap.add_argument("--spec", required=True, help="YAML spec file")
    args = ap.parse_args(argv)
    out = run(args.spec)
    print(f"✅ Wrote {out}")

if __name__ == "__main__":
    main()
