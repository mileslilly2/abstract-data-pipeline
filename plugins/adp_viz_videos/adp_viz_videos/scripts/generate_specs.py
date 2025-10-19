#!/usr/bin/env python3
"""
scripts/3_generate_specs.py

Auto-generate render specs from cleaned data for all available metrics.

Outputs → specs/{STATE}_{METRIC}_map.yaml + line.yaml
"""

from pathlib import Path
import yaml
import pandas as pd

CLEAN_DIR = Path("data_clean")
SPECS_DIR = Path("specs")
SPECS_DIR.mkdir(parents=True, exist_ok=True)

# US county-level GeoJSON (for easy joins by 5-digit FIPS)
US_COUNTIES_GEOJSON = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"

def write_yaml(path: Path, spec: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(spec, f, sort_keys=False, allow_unicode=True)

def make_choropleth_spec(state_abbr: str, data_path: str, metric: str) -> dict:
    return {
        "chart_type": "choropleth",
        "data": data_path,
        "time": "Year",
        "value": metric,
        "geo": US_COUNTIES_GEOJSON,
        "join_left_on": "CountyFIPS",
        "join_right_on": "id",
        "palette": "Reds",
        "width": 1080,
        "height": 1080,
        "dpi": 150,
        "fps": 4,
        "bitrate": "6M",
        "out": f"videos/{state_abbr}_{metric}_map.mp4",
        "title": f"{state_abbr} County {metric.replace('_',' ')} — "+"{time:%Y}",
        "legend": True,
        "hold_frames": 10
    }

def make_line_spec(state_abbr: str, data_path: str, metric: str) -> dict:
    return {
        "chart_type": "line",
        "data": data_path,
        "time": "Year",
        "value": metric,
        "width": 1080,
        "height": 1080,
        "dpi": 150,
        "fps": 10,
        "bitrate": "6M",
        "out": f"videos/{state_abbr}_{metric}_trend.mp4",
        "title": f"{state_abbr} {metric.replace('_',' ')} Trend (Statewide)",
        "x_label": "Year",
        "y_label": metric,
        "legend": False,
        "hold_frames": 5
    }

def main():
    clean_files = sorted(CLEAN_DIR.glob("*_county_metrics.csv"))
    for p in clean_files:
        st = p.name[:2]
        df = pd.read_csv(p, nrows=10)
        numeric_cols = [c for c in df.columns if c not in {"CountyFIPS","CountyName","Year","state_abbr","state_fips"}]
        for metric in numeric_cols:
            spec = make_choropleth_spec(st, str(p), metric)
            write_yaml(SPECS_DIR / f"{st}_{metric}_map.yaml", spec)
            print(f"🗺️  Spec: {st}_{metric}_map.yaml")

        # state aggregates (line charts)
        agg_path = CLEAN_DIR / f"{st}_state_aggregates.csv"
        if agg_path.exists():
            df2 = pd.read_csv(agg_path, nrows=10)
            numeric_cols2 = [c for c in df2.columns if c not in {"state_abbr","Year"}]
            for metric in numeric_cols2:
                spec = make_line_spec(st, str(agg_path), metric)
                write_yaml(SPECS_DIR / f"{st}_{metric}_line.yaml", spec)
                print(f"📈  Spec: {st}_{metric}_line.yaml")

    print("🏁 Spec generation complete.")

if __name__ == "__main__":
    main()
