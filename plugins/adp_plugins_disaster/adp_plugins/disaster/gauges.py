from adp.core.base import Source, Sink, Context, Record
from typing import Iterator, Iterable, Dict, Any
from pathlib import Path
import requests
import json

class GaugesSource(Source):
    """
    Fetch USGS gauge data via NWIS JSON API.
    kw:
      bbox: bounding box "minLon,minLat,maxLon,maxLat"
    """
    def run(self, ctx: Context) -> Iterator[Record]:
        bbox = self.kw.get("bbox")  # e.g., "-81.0,38.5,-79.0,40.5"
        url = "https://waterservices.usgs.gov/nwis/site/?format=rdb&parameterCd=00065&hasDataTypeCd=iv"
        if bbox:
            url += f"&bBox={bbox}"

        ctx.log.info(f"Fetching gauges from: {url}")
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        lines = r.text.splitlines()
        for line in lines:
            if line.startswith("#") or not line.strip():
                continue
            if "site_no" in line:
                headers = line.strip().split("\t")
                continue
            parts = line.strip().split("\t")
            if len(parts) != len(headers):
                continue
            yield dict(zip(headers, parts))

class GeoJsonGaugesSink(Sink):
    """Write gauges as Point FeatureCollection to GeoJSON."""
    def run(self, ctx: Context, rows: Iterable[Record]) -> Path:
        out = ctx.outdir / self.kw.get("filename", "gauges.geojson")
        out.parent.mkdir(parents=True, exist_ok=True)

        features = []
        for r in rows:
            try:
                lon = float(r["dec_long_va"])
                lat = float(r["dec_lat_va"])
            except Exception:
                continue
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": r,
            })

        fc = {"type": "FeatureCollection", "features": features}
        out.write_text(json.dumps(fc))
        ctx.log.info(f"Wrote {len(features)} gauge features -> {out}")
        return out
