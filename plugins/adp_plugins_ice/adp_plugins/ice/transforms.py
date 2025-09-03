# plugins/adp_plugins_ice/transforms.py
from __future__ import annotations
from adp.core.base import Transform, Context, Record, Batch
from pathlib import Path
from collections import Counter
import datetime as dt
import json
from typing import Dict, Any, Iterable

class SummaryJsonTransform(Transform):
    """
    Stream rows, compute lightweight summaries, write a small JSON file,
    and yield rows onward unchanged.
    Params:
      outfile: str (path for summary.json)
      group_by: list[str]  (fields to rank by frequency)
      numeric_fields: list[str] (fields to summarize with mean/std/min/max)
      top_n: int (default 20)
    """
    def run(self, ctx: Context, rows: Iterable[Record]) -> Batch:
        group_by = list(self.kw.get("group_by", []))
        numeric_fields = list(self.kw.get("numeric_fields", []))
        top_n = int(self.kw.get("top_n", 20))

        outpath = Path(self.kw.get("outfile", ctx.outdir / "summary.json"))
        outpath.parent.mkdir(parents=True, exist_ok=True)

        # --- counters / stats ---
        total = 0
        nulls: Counter[str] = Counter()
        tops: Dict[str, Counter[Any]] = {f: Counter() for f in group_by}

        class _Stats:
            __slots__ = ("n","mean","M2","min","max")
            def __init__(self):
                self.n = 0; self.mean = 0.0; self.M2 = 0.0
                self.min = None; self.max = None
            def add(self, x: float):
                self.n += 1
                d = x - self.mean
                self.mean += d / self.n
                self.M2 += d * (x - self.mean)
                self.min = x if self.min is None or x < self.min else self.min
                self.max = x if self.max is None or x > self.max else self.max
            def to_dict(self):
                var = (self.M2 / (self.n - 1)) if self.n > 1 else 0.0
                return {
                    "count": self.n,
                    "mean": self.mean,
                    "stdev": var ** 0.5,
                    "min": self.min,
                    "max": self.max,
                }

        num_stats: Dict[str, _Stats] = {f: _Stats() for f in numeric_fields}

        # --- stream rows; compute summaries; yield onward ---
        for row in rows:
            total += 1

            # null counts (per field)
            for k, v in row.items():
                if v is None or v == "":
                    nulls[k] += 1

            # top values per requested group field
            for f in group_by:
                v = row.get(f)
                if v not in (None, ""):
                    tops[f][v] += 1

            # numeric streaming stats
            for f in numeric_fields:
                v = row.get(f)
                if v not in (None, ""):
                    try:
                        x = float(v)
                        num_stats[f].add(x)
                    except Exception:
                        # ignore non-parsable values
                        pass

            # pass through
            yield row

        # --- write compact summary JSON ---
        summary = {
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
            "total_rows": total,
            "null_counts": dict(nulls),
            "top_values": {
                f: tops[f].most_common(top_n) for f in group_by
            },
            "numeric_stats": {f: s.to_dict() for f, s in num_stats.items()},
        }
        outpath.write_text(json.dumps(summary, indent=2))
        ctx.log.info(f"SummaryJsonTransform wrote {outpath} (rows={total})")

        return {"summary_json": str(outpath)}
