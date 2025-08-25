from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Protocol, Any, Dict, Optional, List
from abc import ABC, abstractmethod
from pathlib import Path
import yaml
from importlib.metadata import entry_points
from pathlib import Path
from types import SimpleNamespace
import os

# adp/core/base.py

Record = Dict[str, Any]
Batch = Iterable[Record]

@dataclass
class Context:
    workdir: Path
    outdir: Path
    state: "State"           # incremental markers, cursors, etags
    log: "Logger"            # simple logger wrapper
    config: Dict[str, Any]   # step-level config (from YAML)
    env: Dict[str, str]      # env vars (API keys, etc.)

class State(Protocol):
    def get(self, key: str, default: Any=None) -> Any: ...
    def set(self, key: str, value: Any) -> None: ...
    def save(self) -> None: ...

class Source(ABC):
    """Fetch zero or more records."""
    def __init__(self, **kwargs): self.kw = kwargs
    @abstractmethod
    def run(self, ctx: Context) -> Batch: ...

class Transform(ABC):
    """Map/filter/enrich records."""
    def __init__(self, **kwargs): self.kw = kwargs
    @abstractmethod
    def run(self, ctx: Context, rows: Batch) -> Batch: ...

class Sink(ABC):
    """Write output (one or many files)."""
    def __init__(self, **kwargs): self.kw = kwargs
    @abstractmethod
    def run(self, ctx: Context, rows: Batch) -> Optional[Path]: ...



# adp/core/runner.py  (append to the bottom of the file)


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

# ---------- public API ----------
def run_pipeline(spec_path: str | Path, *, workdir: str | Path = ".") -> None:
    """
    Execute a YAML pipeline spec.
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

    # ---------- build components ----------
    src_conf = conf["source"]
    SourceCls = _resolve_class(src_conf["class"])
    source = SourceCls(**src_conf.get("params", {}))

    tfms = []
    for tconf in conf.get("transforms", []):
        T = _resolve_class(tconf["class"])
        tfms.append(T(**tconf.get("params", {})))

    sink_conf = conf["sink"]
    SinkCls = _resolve_class(sink_conf["class"])
    sink = SinkCls(**sink_conf.get("params", {}))

    ctx.log.info("Pipeline constructed; running …")

    # ---------- execute ----------
    rows = source.run(ctx)
    for t in tfms:
        rows = t.run(ctx, rows)
    sink.run(ctx, rows)

    ctx.log.info("✅  done.")
