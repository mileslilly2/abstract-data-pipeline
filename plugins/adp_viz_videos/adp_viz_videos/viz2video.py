#!/usr/bin/env python3
# viz2video.py
# Generalized "time-series → video" engine for IG/TikTok
# Supports: choropleth | line | bar_race

from __future__ import annotations
import sys, math, warnings
from dataclasses import dataclass
from typing import Dict, Any, Optional, List
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
    """Convert a Matplotlib figure to an RGB numpy array safely across backends."""
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

def fixed_axes_limits(series: pd.Series, pad: float = 0.05):
    v = pd.to_numeric(series, errors="coerce").dropna()
    if v.empty:
        return (0, 1)
    lo, hi = float(v.min()), float(v.max())
    if math.isclose(lo, hi):
        return (lo - 0.5, hi + 0.5)
    span = hi - lo
    return (lo - pad * span, hi + pad * span)

# ---------- FIXED: make_figure ----------
def make_figure(width_px: int, height_px: int, dpi: int) -> plt.Figure:
    """Create a Matplotlib figure that truly fills a 1080x1920 9:16 frame."""
    fig = plt.figure(figsize=(width_px / dpi, height_px / dpi), dpi=dpi)
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)  # fill entire canvas
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
    width: int = 1080      # 1080 pixels wide
    height: int = 1920     # 1920 pixels tall (vertical 9:16)
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
            if t.isna().all():
                self.df["_time"] = pd.to_datetime(
                    self.df[spec.time].astype(int), format="%Y", errors="coerce"
                )
            else:
                self.df["_time"] = t
        except Exception:
            self.df["_time"] = pd.to_datetime(
                self.df[spec.time].astype(int), format="%Y", errors="coerce"
            )

        self.df = self.df.sort_values("_time")
        self.times: List[pd.Timestamp] = [
            t for t in self.df["_time"].unique() if pd.notna(t)
        ]

        # Auto-scale duration to ~10 seconds
        if len(self.times) > 0 and self.spec.fps > 0:
            target_seconds = 10
            total_frames = len(self.times)
            frames_per_time = max(1, int(target_seconds * self.spec.fps / total_frames))
            self.spec.hold_frames = frames_per_time
            print(
                f"[INFO] Auto duration: {len(self.times)} time steps × "
                f"{frames_per_time} holds @ {self.spec.fps} fps ≈ {target_seconds:.1f}s"
            )

    def writer(self):
        return imageio.get_writer(
            self.spec.out,
            fps=self.spec.fps,
            codec="libx264",
            bitrate=self.spec.bitrate,
            ffmpeg_log_level="error",
            output_params=["-pix_fmt", "yuv420p", "-vf", "scale=1080:1920,setsar=1:1"],
        )

    def render(self):
        raise NotImplementedError

# ---------- LINE RENDERER ----------
class LineRenderer(BaseRenderer):
    def render(self):
        sp = self.spec
        y = sp.value
        g = sp.group
        assert y, "Spec.value required for line chart"

        if sp.y_min is not None and sp.y_max is not None:
            y_min, y_max = sp.y_min, sp.y_max
        else:
            y_min, y_max = fixed_axes_limits(self.df[y])

        writer = self.writer()
        try:
            for t in self.times:
                fig = make_figure(sp.width, sp.height, sp.dpi)
                ax = fig.add_subplot(111)
                cur = self.df[self.df["_time"] <= t]

                if g and g in cur.columns:
                    for name, sub in cur.groupby(g):
                        ax.plot(
                            sub["_time"],
                            pd.to_numeric(sub[y], errors="coerce"),
                            label=str(name),
                        )
                else:
                    ax.plot(
                        cur["_time"],
                        pd.to_numeric(cur[y], errors="coerce"),
                        label=y,
                    )

                ax.set_xlim(self.times[0], self.times[-1])
                ax.set_ylim(y_min, y_max)
                if sp.legend:
                    ax.legend(loc="upper left", frameon=False)
                if sp.x_label:
                    ax.set_xlabel(sp.x_label)
                if sp.y_label:
                    ax.set_ylabel(sp.y_label)
                ax.grid(alpha=0.2, linewidth=0.6)

                # Dynamic year label
                if sp.time in cur.columns and not cur[sp.time].dropna().empty:
                    year_label = str(int(cur[sp.time].max()))
                else:
                    year_label = str(pd.to_datetime(t, errors="coerce").year)
                title_text = safe_title(sp.title, time=year_label)
                ax.set_title(title_text, fontsize=18, weight="bold")

                frame = fig_to_ndarray(fig)
                # --- Force vertical orientation ---
                if frame.shape[1] > frame.shape[0]:
                    frame = np.rot90(frame, 3)
                # ---------------------------------
                plt.close(fig)
                for _ in range(max(1, sp.hold_frames)):
                    writer.append_data(frame)
        finally:
            writer.close()

# ---------- BAR RACE RENDERER ----------
class BarRaceRenderer(BaseRenderer):
    def render(self):
        sp = self.spec
        cat = sp.category
        val = sp.value
        assert cat and val, "Spec.category and Spec.value required for bar_race"
        y_min, y_max = 0, max(1, pd.to_numeric(self.df[val], errors="coerce").max())

        writer = self.writer()
        try:
            for t in self.times:
                cur = self.df[self.df["_time"] == t].copy()
                cur[val] = pd.to_numeric(cur[val], errors="coerce")
                cur = (
                    cur.dropna(subset=[val])
                    .sort_values(val, ascending=False)
                    .head(sp.top_n)[::-1]
                )
                labels = cur[cat].astype(str).values
                vals = cur[val].values

                fig = make_figure(sp.width, sp.height, sp.dpi)
                ax = fig.add_subplot(111)
                ax.barh(labels, vals)
                ax.set_xlim(y_min, y_max)

                if sp.time in cur.columns and not cur[sp.time].dropna().empty:
                    year_label = str(int(cur[sp.time].iloc[0]))
                else:
                    year_label = str(pd.to_datetime(t, errors="coerce").year)
                title_text = safe_title(sp.title, time=year_label)
                ax.set_title(title_text, fontsize=18, weight="bold")

                if sp.x_label:
                    ax.set_xlabel(sp.x_label)
                for i, v in enumerate(vals):
                    ax.text(v, i, f" {v:,.0f}", va="center", ha="left")
                ax.grid(axis="x", alpha=0.2, linewidth=0.6)

                frame = fig_to_ndarray(fig)
                # --- Force vertical orientation ---
                if frame.shape[1] > frame.shape[0]:
                    frame = np.rot90(frame, 3)
                # ---------------------------------
                plt.close(fig)
                for _ in range(max(1, sp.hold_frames)):
                    writer.append_data(frame)
        finally:
            writer.close()

# ---------- CHOROPLETH RENDERER ----------
class ChoroplethRenderer(BaseRenderer):
    def render(self):
        if not _HAS_GEO:
            raise RuntimeError(
                "geopandas is required for choropleth charts. pip install geopandas shapely"
            )
        sp = self.spec
        assert (
            sp.geo and sp.value and sp.join_left_on and sp.join_right_on
        ), "Spec.geo, Spec.value, Spec.join_left_on, Spec.join_right_on are required for choropleth"

        gdf = gpd.read_file(sp.geo)
        all_vals = pd.to_numeric(self.df[sp.value], errors="coerce").dropna()
        vmin = (
            sp.vmin
            if sp.vmin is not None
            else (float(all_vals.min()) if not all_vals.empty else 0.0)
        )
        vmax = (
            sp.vmax
            if sp.vmax is not None
            else (float(all_vals.max()) if not all_vals.empty else 1.0)
        )
        norm = plt.Normalize(vmin=vmin, vmax=vmax)
        cmap = plt.colormaps.get(sp.palette, plt.colormaps["Reds"])

        writer = self.writer()
        try:
            for t in self.times:
                cur = self.df[self.df["_time"] == t]
                if sp.join_right_on in gdf.columns:
                    gdf[sp.join_right_on] = gdf[sp.join_right_on].astype(str).str.zfill(5)
                if sp.join_left_on in cur.columns:
                    cur[sp.join_left_on] = cur[sp.join_left_on].astype(str).str.zfill(5)

                frame_gdf = gdf.merge(
                    cur, left_on=sp.join_right_on, right_on=sp.join_left_on, how="left"
                )

                fig = make_figure(sp.width, sp.height, sp.dpi)
                ax = fig.add_subplot(111)
                frame_gdf.plot(
                    column=sp.value,
                    ax=ax,
                    cmap=cmap,
                    edgecolor="0.3",
                    linewidth=0.2,
                    norm=norm,
                    legend=sp.legend,
                    legend_kwds={
                        "label": sp.value.replace("_", " "),
                        "shrink": 0.6,
                        "orientation": "vertical",
                    },
                )
                ax.set_axis_off()

                if sp.time in cur.columns and not cur[sp.time].dropna().empty:
                    year_label = str(int(cur[sp.time].iloc[0]))
                else:
                    year_label = str(pd.to_datetime(t, errors="coerce").year)
                title_text = safe_title(sp.title, time=year_label)
                fig.suptitle(
                    title_text, fontsize=22, fontweight="bold", ha="center", y=0.97
                )
                ax.set_title(year_label, fontsize=18, fontweight="bold", y=-0.08)

                fig.tight_layout()
                nd = fig_to_ndarray(fig)
                # --- Force vertical orientation ---
                if nd.shape[1] > nd.shape[0]:
                    nd = np.rot90(nd, 3)
                # ---------------------------------
                plt.close(fig)
                for _ in range(max(1, sp.hold_frames)):
                    writer.append_data(nd)
        finally:
            writer.close()

# ---------- FACTORY ----------
def build_renderer(spec: Spec, df: pd.DataFrame):
    ct = spec.chart_type.lower()
    if ct == "line":
        return LineRenderer(spec, df)
    if ct in ("bar_race", "barrace", "bar-race"):
        return BarRaceRenderer(spec, df)
    if ct == "choropleth":
        return ChoroplethRenderer(spec, df)
    raise ValueError(f"Unknown chart_type: {spec.chart_type}")

# ---------- CLI ----------
def run(spec_path: str) -> str:
    with open(spec_path, "r", encoding="utf-8") as f:
        d = yaml.safe_load(f)
    spec = Spec.from_dict(d)
    df = read_table(spec.data)
    rend = build_renderer(spec, df)
    rend.render()
    return spec.out

def main(argv=None):
    import argparse

    ap = argparse.ArgumentParser(description="Time-series → video")
    ap.add_argument("--spec", required=True, help="YAML spec file")
    args = ap.parse_args(argv)
    out = run(args.spec)
    print(f"✅ Wrote {out}")

if __name__ == "__main__":
    main()
