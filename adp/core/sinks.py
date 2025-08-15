# adp/io/sinks.py
"""Common sink implementations: CSV, GeoJSON, Parquet, SQLite.

These sinks accept an *iterable* of mapping records (dict-like). They
convert to a pandas.DataFrame where appropriate and write files into
the pipeline `ctx.outdir`.
"""
from __future__ import annotations
from pathlib import Path
import json
from typing import Iterable, Mapping, Any, Optional

import pandas as pd


Record = Mapping[str, Any]


class CsvSink:
    """Write rows to a single CSV file using pandas."""
    def __init__(self, filename: str = "out.csv", index: bool = False, **kw):
        self.filename = filename
        self.index = index

    def run(self, ctx, rows: Iterable[Record]) -> Path:
        out = Path(ctx.outdir) / self.filename
        # allow generator, so materialize once
        df = pd.DataFrame(list(rows))
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=self.index)
        return out


class ParquetSink:
    """Write rows to a Parquet file. Requires pyarrow or fastparquet."""
    def __init__(self, filename: str = "out.parquet", compression: str = "snappy", **kw):
        self.filename = filename
        self.compression = compression

    def run(self, ctx, rows: Iterable[Record]) -> Path:
        out = Path(ctx.outdir) / self.filename
        df = pd.DataFrame(list(rows))
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, compression=self.compression, index=False)
        return out


class GeoJsonSink:
    """Write GeoJSON FeatureCollection. Expects each record to include
    a geometry value under the key '_geometry' which is already a GeoJSON
    geometry mapping (or None).
    """
    def __init__(self, filename: str = "out.geojson", **kw):
        self.filename = filename

    def run(self, ctx, rows: Iterable[Record]) -> Path:
        out = Path(ctx.outdir) / self.filename
        features = []
        for r in rows:
            # copy props and remove the geometry key if present
            d = dict(r)
            geom = d.pop("_geometry", None)
            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": d
            })
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False, indent=2), encoding="utf8")
        return out


class JsonLinesSink:
    """Write newline-delimited JSON (ndjson)."""
    def __init__(self, filename: str = "out.ndjson", **kw):
        self.filename = filename

    def run(self, ctx, rows: Iterable[Record]) -> Path:
        out = Path(ctx.outdir) / self.filename
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", encoding="utf8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False))
                fh.write("\n")
        return out


class SQLiteSink:
    """Write rows to an SQLite table using pandas.to_sql.

    usage:
        SQLiteSink(filename="data.db", table_name="detentions")
    """
    def __init__(self, filename: str = "data.db", table_name: str = "data", if_exists: str = "replace", **kw):
        self.filename = filename
        self.table_name = table_name
        self.if_exists = if_exists

    def run(self, ctx, rows: Iterable[Record]) -> Path:
        out = Path(ctx.outdir) / self.filename
        df = pd.DataFrame(list(rows))
        out.parent.mkdir(parents=True, exist_ok=True)
        # SQLAlchemy optional; pandas will use sqlite3 builtin
        df.to_sql(self.table_name, f"sqlite:///{out}", index=False, if_exists=self.if_exists)
        return out
