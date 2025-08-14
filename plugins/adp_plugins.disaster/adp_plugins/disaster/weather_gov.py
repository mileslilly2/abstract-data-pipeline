from dataclasses import dataclass
import requests, time
from typing import Iterable, Dict, Any
from adp.core.base import Source, Transform, Sink, Context, Record
from adp.io.sinks import GeoJsonSink
from shapely.geometry import shape, mapping

class WeatherGovAlertsSource(Source):
    """
    Pulls NWS alerts with pagination and polite User-Agent.
    params:
      location: "lat,lon" or "WV" etc.
      days_back: int
    """
    def run(self, ctx: Context) -> Iterable[Record]:
        base = "https://api.weather.gov/alerts"
        headers = {"User-Agent": ctx.env.get("ADP_USER_AGENT","adp/0.1")}
        # build params
        from datetime import datetime, timedelta, timezone
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=int(self.kw.get("days_back", 7)))
        params = {"status":"actual","message_type":"alert",
                  "start":start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "end":end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "limit":500}
        loc = self.kw.get("location")
        if loc and "," in loc: params["point"]=loc
        elif loc: params["area"]=loc

        url, total = base, 0
        while url:
            r = requests.get(url, params=params if url==base else None, headers=headers, timeout=30)
            r.raise_for_status()
            js = r.json()
            feats = js.get("features", [])
            total += len(feats)
            for f in feats:
                yield f
            url = js.get("pagination", {}).get("next")
            params = None
            time.sleep(0.4)
        ctx.log.info("Fetched %d alerts", total)

class AlertsToFlatRecords(Transform):
    """Normalize feature → flat record; keep geometry for GeoJSON sink."""
    def run(self, ctx: Context, rows: Iterable[Record]) -> Iterable[Record]:
        for f in rows:
            props = f.get("properties", {})
            geom  = f.get("geometry")
            yield {
                "id": f.get("id"),
                "event": props.get("event"),
                "severity": props.get("severity"),
                "sent": props.get("sent"),
                "_geometry": geom,   # carry through for GeoJSON sink
            }

class GeoJsonAlertsSink(Sink):
    """Write a GeoJSON FeatureCollection to file."""
    def run(self, ctx: Context, rows: Iterable[Record]):
        features = []
        for r in rows:
            geom = r.pop("_geometry", None)
            if geom:
                features.append({
                    "type":"Feature",
                    "geometry": geom,
                    "properties": r
                })
        out = ctx.outdir / (self.kw.get("filename","alerts.geojson"))
        out.write_text(__import__("json").dumps({"type":"FeatureCollection","features":features}))
        return out
