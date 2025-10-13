#!/usr/bin/env python3
"""
scripts/2_transform_data.py

Normalize raw downloads → clean, tidy tables:

Inputs:  data_raw/{STATE}_acs5_{YEAR}_county.csv  (from script 1)
Outputs: data_clean/{STATE}_county_metrics.csv
         data_clean/{STATE}_state_aggregates.csv
"""

from pathlib import Path
import re
import pandas as pd

RAW_DIR = Path("data_raw")
CLEAN_DIR = Path("data_clean")
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

def parse_state_from_filename(p: Path) -> str:
    # Expect file like: WV_acs5_2023_county.csv
    m = re.match(r"([A-Z]{2})_acs5_(\d{4})_county\.csv$", p.name)
    if not m:
        return ""
    return m.group(1)

def parse_year_from_filename(p: Path) -> int:
    m = re.match(r"[A-Z]{2}_acs5_(\d{4})_county\.csv$", p.name)
    return int(m.group(1)) if m else None

def main():
    files = sorted(RAW_DIR.glob("*_acs5_*_county.csv"))
    if not files:
        print("No raw files found in data_raw/. Run scripts/1_download_by_state.py first.")
        return

    # group files by state
    by_state = {}
    for f in files:
        st = parse_state_from_filename(f)
        if not st:
            print(f"Skipping unexpected file: {f.name}")
            continue
        by_state.setdefault(st, []).append(f)

    for st, paths in by_state.items():
        all_rows = []
        for p in sorted(paths, key=parse_year_from_filename):
            df = pd.read_csv(p, dtype={"county_fips": str})
            # Clean NAME → "County, State" -> extract County name (first token before comma)
            county_name = df["NAME"].astype(str).str.split(",").str[0].str.strip().str.upper()
            df["CountyName"] = county_name
            df["CountyFIPS"] = df["county_fips"].str.zfill(5)
            df["Year"] = df["year"].astype(int)
            # Numeric coercion
            df["MedianHouseholdIncome"] = pd.to_numeric(df["MedianHouseholdIncome"], errors="coerce")
            df["Population"] = pd.to_numeric(df["Population"], errors="coerce")

            all_rows.append(df[[
                "state_abbr","state_fips","CountyFIPS","CountyName","Year",
                "MedianHouseholdIncome","Population"
            ]])

        if not all_rows:
            print(f"⚠️  No rows for state {st}")
            continue

        county_ts = pd.concat(all_rows, ignore_index=True)
        county_ts = county_ts.sort_values(["CountyFIPS","Year"]).reset_index(drop=True)
        out_county = CLEAN_DIR / f"{st}_county_metrics.csv"
        county_ts.to_csv(out_county, index=False)
        print(f"✅ Wrote {out_county} ({len(county_ts)} rows)")

        # Build state aggregate per year
        agg = (county_ts
               .groupby(["state_abbr","Year"], as_index=False)
               .agg(PopulationSum=("Population","sum"),
                    MedianIncomeAvg=("MedianHouseholdIncome","mean")))
        out_state = CLEAN_DIR / f"{st}_state_aggregates.csv"
        agg.to_csv(out_state, index=False)
        print(f"✅ Wrote {out_state} ({len(agg)} rows)")

    print("🏁 Transform complete.")

if __name__ == "__main__":
    main()
