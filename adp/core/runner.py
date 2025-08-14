from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Protocol, Any, Dict, Optional, List
from abc import ABC, abstractmethod
from pathlib import Path

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
