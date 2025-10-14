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
    """Extract state abbreviation (e.g., WV) from filename."""
    m = re.match(r"([A-Z]{2})_acs5_(\d{4})_county\.csv$", p.name)
    return m.group(1) if m else ""


def parse_year_from_filename(p: Path) -> int:
    """Extract 4-digit year from filename."""
    m = re.match(r"[A-Z]{2}_acs5_(\d{4})_county\.csv$", p.name)
    return int(m.group(1)) if m else None


def main():
    files = sorted(RAW_DIR.glob("*_acs5_*_county.csv"))
    if not files:
        print("⚠️ No raw files found. Run scripts/1_download_by_state.py first.")
        return

    # Group files by state abbreviation
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
            df = pd.read_csv(p, dtype={"county_fips": str}, low_memory=False)

            # Normalize NAME → "COUNTY, STATE" → COUNTY (uppercase)
            df["CountyName"] = (
                df["NAME"]
                .astype(str)
                .str.split(",")
                .str[0]
                .str.replace(r"\s+COUNTY$", "", regex=True)
                .str.strip()
                .str.upper()
            )

            df["CountyFIPS"] = df["county_fips"].astype(str).str.zfill(5)

            # Fix column capitalization and type
            if "year" in df.columns and "Year" not in df.columns:
                df.rename(columns={"year": "Year"}, inplace=True)
            df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")

            # Coerce all numeric columns to numeric
            exclude_cols = {
                "state_abbr", "state_fips", "county_fips",
                "NAME", "CountyName", "CountyFIPS", "state", "county", "Year"
            }
            numeric_cols = [c for c in df.columns if c not in exclude_cols]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            all_rows.append(
                df[["state_abbr", "state_fips", "CountyFIPS", "CountyName", "Year"] + numeric_cols]
            )

        if not all_rows:
            print(f"⚠️ No rows for {st}")
            continue

        # Combine years and remove duplicate columns
        county_ts = pd.concat(all_rows, ignore_index=True)
        county_ts = county_ts.loc[:, ~county_ts.columns.duplicated(keep="first")]

        # Ensure consistent column types
        county_ts["CountyFIPS"] = county_ts["CountyFIPS"].astype(str).str.zfill(5)
        county_ts["Year"] = pd.to_numeric(county_ts["Year"], errors="coerce").astype("Int64")

        county_ts = county_ts.sort_values(["CountyFIPS", "Year"]).reset_index(drop=True)

        # Write cleaned per-county time series
        out_county = CLEAN_DIR / f"{st}_county_metrics.csv"
        county_ts.to_csv(out_county, index=False)
        print(f"✅ Wrote {out_county} ({len(county_ts)} rows)")

        # Compute state aggregates
        numeric_cols = [c for c in county_ts.columns if c not in {"state_abbr", "state_fips", "CountyFIPS", "CountyName", "Year"}]
        agg_dict = {col: "mean" for col in numeric_cols}
        if "Population" in numeric_cols:
            agg_dict["Population"] = "sum"

        state_agg = (
            county_ts.groupby(["state_abbr", "Year"], as_index=False)
            .agg(agg_dict)
        )

        out_state = CLEAN_DIR / f"{st}_state_aggregates.csv"
        state_agg.to_csv(out_state, index=False)
        print(f"✅ Wrote {out_state} ({len(state_agg)} rows)")

    print("🏁 Transform complete.")


if __name__ == "__main__":
    main()
