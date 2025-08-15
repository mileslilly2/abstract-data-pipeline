# adp/core/state.py
"""Simple state backends used by pipelines.

Provides:
  - FileState: JSON-backed key/value store (default)
  - InMemoryState: ephemeral state (useful for tests)
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, MutableMapping, Optional


class FileState:
    """Simple JSON file-backed state.

    Usage:
        s = FileState("pipeline_state.json")
        last = s.get("last_signing_date")
        s.set("last_signing_date", "2025-06-01")
        s.save()
    """
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._data: MutableMapping[str, Any] = {}
        if self.path.exists():
            try:
                self._data = json.loads(self.path.read_text(encoding="utf8"))
            except Exception:
                # if file corrupt or unreadable, start empty
                self._data = {}

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._data, indent=2, ensure_ascii=False), encoding="utf8")


class InMemoryState:
    """Lightweight in-memory state (not persisted). Useful for tests."""
    def __init__(self):
        self._data: dict[str, Any] = {}

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value

    def save(self) -> None:
        # memory-only; nothing to do
        pass
