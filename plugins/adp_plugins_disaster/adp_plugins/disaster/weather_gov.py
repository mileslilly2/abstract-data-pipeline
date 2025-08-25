from __future__ import annotations
from adp.core.base import Source, Transform, Sink, Context, Record
from pathlib import Path
from typing import Iterable, Dict, Any, Iterator, List, Optional
import datetime as dt
import requests
import time

UA = "adp-disaster-pipeline (contact: you@example.com)"

# ---------- SOURCE ----------
class WeatherGovAlertsSource(Source):
    """
    Fetch NWS /alerts with optional point or area, with proper pagination.

    kw:
      location: "lat,lon"  OR  2-letter state code (e.g. "WV"). If omitted, nationwide.
      days_back: integer window (default 2)
      status: "actual" (default)
      message_type: "alert" (default)
      event: optional string to filter event name (e.g., "Flash Flood Warning")
      limit: page size (default 500)
      pause: seconds between pages (default 0.6, be polite)
    """

    URL = "https://api.weather.gov/alerts"

    def run(self, ctx: Context) -> Iterator[Record]:
        loc = (self.kw.get("location") or "").strip()
        days_back = int(self.kw.get("days_back", 2))
        status = self.kw.get("status", "actual")
        message_type = self.kw.get("message_type", "alert")
        event = self.kw.get("event")  # optional
        limit = int(self.kw.get("limit", 500))
        pause = float(self.kw.get("pause", 0.6))

        end = dt.datetime.utcnow()
        start = end - dt.timedelta(days=days_back)

        spatial: Dict[str, Any] = {}
        if "," in loc and loc:
            spatial["point"] = loc  # "lat,lon"
        elif len(loc) == 2:
            spatial["area"] = loc.upper()  # 2-letter state
        # else: nationwide

        params = {
            **spatial,
            "status": status,
            "message_type": message_type,
            "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "limit": limit,
        }
        if event:
            params["event"] = event

        headers = {"User-Agent": UA}

        url = self.URL
        total = 0
        while url:
            r = requests.get(
                url,
                params=params if url == self.URL else None,
                headers=headers,
                timeout=30,
            )
            r.raise_for_status()
            js = r.json()
            feats = js.get("features", [])
            for f in feats:
                yield f  # raw GeoJSON Feature
            total += len(feats)
            ctx.logger.info(f"Fetched {len(feats)} (total {total}) from {url}")
            # pagination
            url = js.get("pagination", {}).get("next")
            params = None
            if url and pause > 0:
                time.sleep(pause)


# ---------- TRANSFORMS ----------
class NormalizeAlerts(Transform):
    """
    Flatten each feature -> record with a few useful fields retained.

    Produces dicts:
      id, sent, event, severity, headline, area, geometry (GeoJSON or None)
    """
    def run(self, ctx: Context, rows: Iterable[Record]) -> Iterator[Record]:
        for f in rows:
            prop = f.get("properties", {}) or {}
            yield {
                "id": f.get("id"),
                "sent": prop.get("sent"),
                "event": prop.get("event"),
                "severity": prop.get("severity"),
                "headline": prop.get("headline"),
                "area": prop.get("areaDesc"),
                "geometry": f.get("geometry"),
            }


class FilterAlertsByKeywords(Transform):
    """
    Keep only alerts whose event/headline/area match any keyword (case-insensitive).
    kw:
      keywords: list[str] or pipe-string "flood|tornado|wildfire"
    """
    def run(self, ctx: Context, rows: Iterable[Record]) -> Iterator[Record]:
        import re
        kws = self.kw.get("keywords", [])
        if isinstance(kws, str):
            pat = re.compile(kws, re.I)
        else:
            pat = re.compile("|".join(kws), re.I) if kws else None

        for r in rows:
            if not pat:
                yield r
                continue
            hay = " ".join(
                str(x or "")
                for x in (r.get("event"), r.get("headline"), r.get("area"))
            )
            if pat.search(hay):
                yield r


# ---------- SINKS ----------
class GeoJsonSink(Sink):
    """
    Write a FeatureCollection to GeoJSON.
    Expects upstream rows to be either raw Features OR records with 'geometry'.
    kw:
      filename: relative path under ctx.outdir (default 'alerts.geojson')
      collection_props: dict to add at top level (optional)
    """
    def run(self, ctx: Context, rows: Iterable[Record]) -> Path:
        from pathlib import Path

        out = ctx.outdir / self.kw.get("filename", "alerts.geojson")
        out.parent.mkdir(parents=True, exist_ok=True)

        features: List[Dict[str, Any]] = []
        for r in rows:
            if "type" in r and r.get("type") == "Feature":
                features.append(r)
            else:
                features.append(
                    {
                        "type": "Feature",
                        "geometry": r.get("geometry"),
                        "properties": {k: v for k, v in r.items() if k != "geometry"},
                    }
                )

        fc = {"type": "FeatureCollection", "features": features}
        # optional top-level properties
        if isinstance(self.kw.get("collection_props"), dict):
            fc.update(self.kw["collection_props"])

        import json
        out.write_text(json.dumps(fc))
        ctx.logger.info(f"Wrote {len(features)} features -> {out}")
        return out


class CsvSink(Sink):
    """CSV writer that creates parent dirs first."""
    def run(self, ctx: Context, rows: Iterable[Record]) -> Path:
        import pandas as pd
        out = ctx.outdir / self.kw.get("filename", "alerts.csv")
        out.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(list(rows))
        df.to_csv(out, index=False)
        ctx.logger.info(f"Wrote {len(df)} rows -> {out}")
        return out
