#!/usr/bin/env python3
"""
scripts/3_generate_specs.py

Create render specs from cleaned data:
- For each {STATE}_county_metrics.csv:
    1) Choropleth map (MedianHouseholdIncome by county, by year)
    2) Bar-race (top N counties by MedianHouseholdIncome per year)
- For each {STATE}_state_aggregates.csv:
    3) Line chart (PopulationSum by Year)

Outputs → specs/*.yaml
"""

from pathlib import Path
import argparse
import yaml
import pandas as pd

CLEAN_DIR = Path("data_clean")
SPECS_DIR = Path("specs")
SPECS_DIR.mkdir(parents=True, exist_ok=True)

# Lightweight US counties GeoJSON (GEOID = 5-digit county FIPS)
# Join: left CountyFIPS (data) -> right GEOID (geojson)
US_COUNTIES_GEOJSON = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"

def write_yaml(path: Path, d: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(d, f, sort_keys=False, allow_unicode=True)

def make_choropleth_spec(state_abbr: str, data_path: str) -> dict:
    return {
        "chart_type": "choropleth",
        "data": data_path,
        "time": "Year",
        "value": "MedianHouseholdIncome",
        "geo": US_COUNTIES_GEOJSON,
        "join_left_on": "CountyFIPS",
        "join_right_on": "GEOID",
        "palette": "Reds",
        "width": 1080,
        "height": 1920,
        "dpi": 150,
        "fps": 24,
        "bitrate": "8M",
        "out": f"videos/{state_abbr}_income_map_1080x1920.mp4",
        "title": f"{state_abbr} Median Household Income — "+"{time:%Y}",
        "legend": True,
        "vmin": None,
        "vmax": None,
        "hold_frames": 1
    }

def make_barrace_spec(state_abbr: str, data_path: str, top_n: int = 12) -> dict:
    return {
        "chart_type": "bar_race",
        "data": data_path,
        "time": "Year",
        "category": "CountyName",
        "value": "MedianHouseholdIncome",
        "top_n": int(top_n),
        "width": 1080,
        "height": 1920,
        "dpi": 150,
        "fps": 24,
        "bitrate": "8M",
        "out": f"videos/{state_abbr}_income_barrace_1080x1920.mp4",
        "title": f"Top {top_n} {state_abbr} Counties by Median Income — "+"{time:%Y}",
        "x_label": "Median Household Income ($)",
        "legend": False,
        "hold_frames": 1
    }

def make_line_spec(state_abbr: str, data_path: str) -> dict:
    return {
        "chart_type": "line",
        "data": data_path,
        "time": "Year",
        "value": "PopulationSum",
        "width": 1080,
        "height": 1920,
        "dpi": 150,
        "fps": 24,
        "bitrate": "8M",
        "out": f"videos/{state_abbr}_population_line_1080x1920.mp4",
        "title": f"{state_abbr} Total Population — "+"{time:%Y}",
        "x_label": "Year",
        "y_label": "Population",
        "legend": False,
        "hold_frames": 1
    }

def main():
    ap = argparse.ArgumentParser(description="Generate render specs from cleaned data")
    ap.add_argument("--top-n", type=int, default=12, help="Top N for bar-race (default 12)")
    args = ap.parse_args()

    county_files = sorted(CLEAN_DIR.glob("*_county_metrics.csv"))
    state_files = sorted(CLEAN_DIR.glob("*_state_aggregates.csv"))

    # Specs for county metrics (choropleth + bar-race)
    for p in county_files:
        st = p.name[:2]  # e.g., "WV"
        # Quick sanity: ensure required columns exist
        df = pd.read_csv(p, nrows=5, dtype={"CountyFIPS": str})
        required = {"CountyFIPS","CountyName","Year","MedianHouseholdIncome"}
        if not required.issubset(df.columns):
            print(f"⚠️  Skipping {p.name} (missing columns).")
            continue

        choropleth = make_choropleth_spec(st, str(p))
        barrace = make_barrace_spec(st, str(p), top_n=args.top_n)

        write_yaml(SPECS_DIR / f"{st}_income_map_1080x1920.yaml", choropleth)
        write_yaml(SPECS_DIR / f"{st}_income_barrace_1080x1920.yaml", barrace)
        print(f"✅ Specs for {st} (county): choropleth + bar-race")

    # Specs for state aggregates (line)
    for p in state_files:
        st = p.name[:2]
        df = pd.read_csv(p, nrows=5)
        if "Year" not in df.columns or "PopulationSum" not in df.columns:
            print(f"⚠️  Skipping {p.name} (missing Year/PopulationSum).")
            continue

        line = make_line_spec(st, str(p))
        write_yaml(SPECS_DIR / f"{st}_population_line_1080x1920.yaml", line)
        print(f"✅ Spec for {st} (state): line")

    print("🏁 Spec generation complete. Specs are in specs/.")

if __name__ == "__main__":
    main()
