from __future__ import annotations
from typing import Any, Dict, Iterable, Iterator, List, Optional
from pathlib import Path
import datetime as dt
import requests
import time
import re

from adp.core.base import Source, Transform, Sink, Context, Record, Batch

UA = "adp-disaster-pipeline (contact: you@example.com)"


# ---------- SOURCE ----------
class WeatherGovAlertsSource(Source):
    """
    Fetch NWS /alerts with optional point or area, with proper pagination.
    """
    URL = "https://api.weather.gov/alerts"

    def run(self, ctx: Context) -> Iterator[Record]:
        loc = (self.kw.get("location") or "").strip()
        days_back = int(self.kw.get("days_back", 2))
        status = self.kw.get("status", "actual")
        message_type = self.kw.get("message_type", "alert")
        event = self.kw.get("event")
        limit = int(self.kw.get("limit", 500))
        pause = float(self.kw.get("pause", 0.6))

        end = dt.datetime.utcnow()
        start = end - dt.timedelta(days=days_back)

        spatial: Dict[str, Any] = {}
        if "," in loc and loc:
            spatial["point"] = loc
        elif len(loc) == 2:
            spatial["area"] = loc.upper()

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
                yield f
            total += len(feats)
            ctx.log.info(f"Fetched {len(feats)} (total {total}) from {url}")
            url = js.get("pagination", {}).get("next")
            params = None
            if url and pause > 0:
                time.sleep(pause)


# ---------- TRANSFORMS ----------
class AlertsToFlatRecords(Transform):
    """Flatten weather.gov GeoJSON features into row dicts."""
    def run(self, ctx: Context, rows: Iterable[Record]) -> Iterator[Record]:
        for rec in rows:
            if rec.get("type") == "FeatureCollection":
                for feat in rec["features"]:
                    yield self._flat(feat)
            elif rec.get("type") == "Feature":
                yield self._flat(rec)
            else:
                yield rec

    def _flat(self, feat: Dict[str, Any]) -> Dict[str, Any]:
        props = feat.get("properties", {}) or {}
        return {
            "id": props.get("id") or feat.get("id"),
            "event": props.get("event"),
            "severity": props.get("severity"),
            "urgency": props.get("urgency"),
            "certainty": props.get("certainty"),
            "status": props.get("status"),
            "sent": props.get("sent"),
            "effective": props.get("effective"),
            "expires": props.get("expires"),
            "areaDesc": props.get("areaDesc"),
            "headline": props.get("headline"),
            "description": props.get("description"),
            "geometry": feat.get("geometry"),
        }


class FilterAlertsByKeywords(Transform):
    """
    Keep only alerts whose event/headline/area match any keyword (case-insensitive).
    kw:
      keywords: list[str] or pipe-string "flood|tornado|wildfire"
    """
    def run(self, ctx: Context, rows: Iterable[Record]) -> Iterator[Record]:
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
                for x in (r.get("event"), r.get("headline"), r.get("areaDesc"))
            )
            if pat.search(hay):
                yield r


# ---------- SINKS ----------
class GeoJsonAlertsSink(Sink):
    """
    Write a FeatureCollection to GeoJSON.
    Expects upstream rows to be either raw Features OR records with 'geometry'.
    """
    def run(self, ctx: Context, rows: Iterable[Record]) -> Path:
        out = ctx.outdir / self.kw.get("filename", "alerts.geojson")
        out.parent.mkdir(parents=True, exist_ok=True)

        features: List[Dict[str, Any]] = []
        for r in rows:
            if "type" in r and r.get("type") == "Feature":
                features.append(r)
            else:
                features.append({
                    "type": "Feature",
                    "geometry": r.get("geometry"),
                    "properties": {k: v for k, v in r.items() if k != "geometry"},
                })

        fc = {"type": "FeatureCollection", "features": features}
        if isinstance(self.kw.get("collection_props"), dict):
            fc.update(self.kw["collection_props"])

        import json
        out.write_text(json.dumps(fc))
        ctx.log.info(f"Wrote {len(features)} features -> {out}")
        return out


class CsvSink(Sink):
    """CSV writer that creates parent dirs first."""
    def run(self, ctx: Context, rows: Iterable[Record]) -> Path:
        import pandas as pd
        out = ctx.outdir / self.kw.get("filename", "alerts.csv")
        out.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(list(rows))
        df.to_csv(out, index=False)
        ctx.log.info(f"Wrote {len(df)} rows -> {out}")
        return out
