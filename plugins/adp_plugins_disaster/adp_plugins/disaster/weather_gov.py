# plugins/adp_plugins.disaster/adp_plugins/disaster/weather_gov.py
"""
Small example plugin: Weather.gov alerts source / transform / sink.

Usage:
    from adp.core.base import Source, Transform, Sink
    # Then reference via module path: 
    # plugins.adp_plugins.disaster.adp_plugins.disaster.weather_gov:WeatherGovAlertsSource
"""
from __future__ import annotations
import time
from typing import Iterable, Dict, Any, Generator
from pathlib import Path
import json
import requests

# Import base classes from your core package
from adp.core.base import Source, Transform, Sink

Record = Dict[str, Any]


class WeatherGovAlertsSource(Source):
    """Fetch alerts from https://api.weather.gov/alerts.

    Params accepted via kwargs (self.kw):
      - location: "lat,lon" point or 2-letter state code (str)
      - limit: page size (int, default 200)
      - status: 'actual'|'exercise' etc. (default 'actual')

    Yields raw GeoJSON features (dicts).
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.limit = int(self.kw.get("limit", 200))
        self.location = self.kw.get("location")
        self.status = self.kw.get("status", "actual")
        self.base = "https://api.weather.gov/alerts"

    def _headers(self, ctx):
        ua = ctx.env.get("ADP_USER_AGENT") or "adp-plugin-weathergov/0.1 (you@example.com)"
        return {"User-Agent": ua, "Accept": "application/geo+json, application/json"}

    def run(self, ctx) -> Iterable[Record]:
        params = {"status": self.status, "message_type": "alert", "limit": self.limit}
        if self.location:
            if "," in str(self.location):
                params["point"] = str(self.location)
            else:
                params["area"] = str(self.location)
        url = self.base
        headers = self._headers(ctx)

        # paginate until no "next" in "pagination"
        while url:
            resp = requests.get(url, params=params if url == self.base else None, headers=headers, timeout=30)
            resp.raise_for_status()
            js = resp.json()
            features = js.get("features", []) or []
            for feat in features:
                yield feat
            # get next page URL (API returns pagination.next)
            url = js.get("pagination", {}).get("next")
            # once we follow next, we don't resend params
            params = None
            # polite pause to avoid hammering API if large
            time.sleep(0.15)


class AlertsToFlatRecords(Transform):
    """Flatten weather.gov feature -> simple record.

    Produces records like:
      { "id": "...", "event": "...", "severity": "...", "sent": "...", "_geometry": {...} }
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def run(self, ctx, rows: Iterable[Record]) -> Iterable[Record]:
        for feat in rows:
            props = feat.get("properties", {}) or {}
            rec = {
                "id": feat.get("id"),
                "event": props.get("event"),
                "severity": props.get("severity"),
                "sent": props.get("sent"),
                # keep original geometry as GeoJSON mapping under _geometry for GeoJsonSink
                "_geometry": feat.get("geometry"),
                # include other properties as needed; don't make record too heavy here
                "headline": props.get("headline"),
                "areaDesc": props.get("areaDesc"),
            }
            yield rec


class GeoJsonAlertsSink(Sink):
    """Write a GeoJSON FeatureCollection to ctx.outdir/<filename>.

    Expects records to include `_geometry` key containing a GeoJSON geometry (or None).
    """
    def __init__(self, filename: str = "alerts.geojson", **kwargs):
        super().__init__(**kwargs)
        self.filename = filename

    def run(self, ctx, rows: Iterable[Record]):
        features = []
        for r in rows:
            r = dict(r)  # make a copy
            geom = r.pop("_geometry", None)
            # properties should not contain non-serializable objects
            properties = {k: v for k, v in r.items()}
            features.append({"type": "Feature", "geometry": geom, "properties": properties})

        outdir = Path(ctx.outdir)
        outdir.mkdir(parents=True, exist_ok=True)
        out = outdir / self.filename
        out.write_text(json.dumps({"type": "FeatureCollection", "features": features}, indent=2, ensure_ascii=False))
        return out
