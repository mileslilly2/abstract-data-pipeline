# plugins/cj/source/cj_catalog_source.py

import json
import time
from pathlib import Path
from typing import Dict, Any, Iterator, Optional
from cj_client import make_client_from_env, CJClient


def _load_state(path: Path) -> Optional[int]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text()).get("last_time_start_ms")
    except Exception:
        return None


def _save_state(path: Path, ms: int) -> None:
    path.write_text(json.dumps({"last_time_start_ms": ms}, indent=2))


def run_source(config: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """ADP Source: stream enriched CJ products (listV2 + detail + inventory)."""

    keyword = config.get("keyword", "")
    page_start = int(config.get("page_start", 1))
    page_end = int(config.get("page_end", 1000))
    size = int(config.get("size", 100))
    state_path = Path(config.get("state_path", ".cj_state.json"))
    incremental = bool(config.get("incremental", True))

    client: CJClient = make_client_from_env()

    time_start_ms = _load_state(state_path) if incremental else None
    now_ms = int(time.time() * 1000)

    for product in client.iter_hybrid_catalog(
        keyword=keyword,
        page_start=page_start,
        page_end=page_end,
        size=size,
        time_start_ms=time_start_ms,
        time_end_ms=None,
        sleep_between_pages=0.5,
    ):
        yield product

    if incremental:
        _save_state(state_path, now_ms)
