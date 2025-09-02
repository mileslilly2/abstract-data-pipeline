from __future__ import annotations
from typing import Any, Dict
from pathlib import Path
import yaml
from importlib.metadata import entry_points
from types import SimpleNamespace
import os

from adp.core.base import (
    Record,
    Batch,
    Context,
    Source,
    Transform,
    Sink,
)


# ---------- helpers ----------
def _resolve_class(ref: str):
    """Handle 'module:Class', 'module.Class', and 'ep:<name>'."""
    if ref.startswith("ep:"):
        name = ref[3:]
        ep = next((e for e in entry_points(group="adp.plugins") if e.name == name), None)
        if not ep:
            raise ImportError(f"No entry point adp.plugins named {name!r}")
        return ep.load()

    if ":" in ref:
        mod, cls = ref.split(":", 1)
    else:
        mod, cls = ref.rsplit(".", 1)
    mod_obj = __import__(mod, fromlist=[cls])
    return getattr(mod_obj, cls)


class _InMemoryState(dict):
    def get(self, k, d=None): return super().get(k, d)
    def set(self, k, v): self[k] = v
    def save(self): ...


class _Logger(SimpleNamespace):
    def info(self, *a): print("[INFO]", *a)
    def warn(self, *a): print("[WARN]", *a)
    def error(self, *a): print("[ERR ]", *a)


def _sample_rows(it, n=3):
    """Yield rows but capture the first n for logging."""
    buf = []
    for i, row in enumerate(it):
        if i < n:
            buf.append(row)
        yield row
    if buf:
        print("[INFO]   sample:", buf)


# ---------- public API ----------
def run_pipeline(spec_path: str | Path, *, workdir: str | Path = ".") -> None:
    """
    Execute a YAML pipeline spec.
    Supports both:
      - legacy style: source / transform / sink
      - new style: steps: [ {id, uses/class/ref, params}, ... ]
    """
    spec_path = Path(spec_path)
    if not spec_path.exists():
        raise FileNotFoundError(spec_path)

    conf = yaml.safe_load(spec_path.read_text())

    ctx = Context(
        workdir=Path(workdir),
        outdir=Path(workdir) / conf.get("outdir", "out"),
        state=_InMemoryState(),
        log=_Logger(),
        config={},
        env=dict(os.environ),
    )

    # ---------- helper ----------
    def _load_component(step_conf: Dict[str, Any]):
        if "class" in step_conf:
            return _resolve_class(step_conf["class"]), step_conf.get("params", {})
        elif "ref" in step_conf:
            return _resolve_class(step_conf["ref"]), step_conf.get("params", {})
        elif "uses" in step_conf:
            return _resolve_class(step_conf["uses"]), step_conf.get("params", {})
        else:
            raise KeyError("Pipeline step must define 'class', 'ref', or 'uses'")

    # ---------- steps mode ----------
    if "steps" in conf:
        rows = None
        for step in conf["steps"]:
            step_id = step.get("id", "<no-id>")
            ctx.log.info(f"▶ Running step: {step_id} ({step.get('uses')})")

            Cls, params = _load_component(step)
            comp = Cls(**params)

            if isinstance(comp, Source):
                rows = list(_sample_rows(comp.run(ctx), 3))
                ctx.log.info(f"   ↳ {step_id} produced {len(rows)} records")

            elif isinstance(comp, Transform):
                if rows is None:
                    raise RuntimeError(f"Transform step {step_id} has no input rows")
                rows = list(_sample_rows(comp.run(ctx, rows), 3))
                ctx.log.info(f"   ↳ {step_id} produced {len(rows)} records")

            elif isinstance(comp, Sink):
                if rows is None:
                    raise RuntimeError(f"Sink step {step_id} has no input rows")
                result = comp.run(ctx, rows)
                ctx.log.info(f"   ↳ {step_id} wrote {result}")

            else:
                raise TypeError(f"Unsupported component type: {Cls}")

        ctx.log.info("✅ steps pipeline done.")

    # ---------- legacy mode ----------
    else:
        # Source
        src_conf = conf["source"]
        SourceCls, src_params = _load_component(src_conf)
        source = SourceCls(**src_params)

        # Transforms
        t_confs = conf.get("transform") or conf.get("transforms") or []
        if isinstance(t_confs, dict):
            t_confs = [t_confs]
        tfms = []
        for tconf in t_confs:
            TCls, t_params = _load_component(tconf)
            tfms.append(TCls(**t_params))

        # Sink
        sink_conf = conf["sink"]
        SinkCls, sink_params = _load_component(sink_conf)
        sink = SinkCls(**sink_params)

        # Execute
        rows = source.run(ctx)
        for t in tfms:
            rows = t.run(ctx, rows)
        sink.run(ctx, rows)

        ctx.log.info("✅ legacy pipeline done.")
