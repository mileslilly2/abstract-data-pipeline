# adp/core/base.py
"""Core abstract base classes and context for ADP (Abstract Data Pipeline)."""

from __future__ import annotations
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Protocol, Optional
from pathlib import Path


# Basic record / batch types
Record = Dict[str, Any]
Batch = Iterable[Record]


@dataclass
class Context:
    """Lightweight context object passed to Sources / Transforms / Sinks.

    Attributes:
        workdir: root path where pipeline was executed
        outdir: output directory for generated files
        state: state backend (FileState or InMemoryState)
        log: a logger instance (implements .info/.debug/.error)
        config: the pipeline spec (dict)
        env: environment mapping (os.environ copy)
    """
    workdir: Path
    outdir: Path
    state: Any
    log: Any
    config: Dict[str, Any]
    env: Dict[str, str]


class State(Protocol):
    """Protocol for state backends used by pipelines (FileState, InMemoryState).

    Methods:
      - get(key, default) -> Any
      - set(key, value) -> None
      - save() -> None
    """
    def get(self, key: str, default: Any = None) -> Any: ...
    def set(self, key: str, value: Any) -> None: ...
    def save(self) -> None: ...


class Source(ABC):
    """Abstract Source.

    A Source is responsible for fetching raw data and yielding one or more
    records (dict-like objects). Sources should be memory-friendly and may
    yield a generator.

    Implementations should accept keyword args in __init__ for parameterization.
    """
    def __init__(self, **kwargs):
        self.kw = kwargs

    @abstractmethod
    def run(self, ctx: Context) -> Batch:
        """Return an iterable (or generator) of records."""
        raise NotImplementedError


class Transform(ABC):
    """Abstract Transform.

    A Transform receives an iterable of records and should return another
    iterable of records (mapping / generator). Useful for mapping, filtering,
    enrichment, normalization, joining, etc.
    """
    def __init__(self, **kwargs):
        self.kw = kwargs

    @abstractmethod
    def run(self, ctx: Context, rows: Batch) -> Batch:
        """Consume rows and yield transformed rows."""
        raise NotImplementedError


class Sink(ABC):
    """Abstract Sink.

    A Sink accepts an iterable of records and writes them somewhere (file,
    DB, remote store). Should return the path(s) or identifier of the produced
    artifact(s) where appropriate.
    """
    def __init__(self, **kwargs):
        self.kw = kwargs

    @abstractmethod
    def run(self, ctx: Context, rows: Batch) -> Optional[Path]:
        """Write rows and return a Path (or None)."""
        raise NotImplementedError
