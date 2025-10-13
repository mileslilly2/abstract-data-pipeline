#!/usr/bin/env python3
"""
scripts/1_download_by_state.py

Batch download ACS 5-year county-level metrics by state and year:
- Median Household Income (B19013_001E)
- Total Population (B01003_001E)

Outputs → data_raw/{STATE}_acs5_{YEAR}_county.csv
"""

import os
import time
import argparse
from pathlib import Path
from typing import Dict, List
import requests
import pandas as pd

CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "")

# Full state FIPS → abbreviation map (50 + DC)
STATE_FIPS: Dict[str, str] = {
    "01": "AL","02": "AK","04": "AZ","05": "AR","06": "CA","08": "CO","09": "CT","10": "DE","11": "DC",
    "12": "FL","13": "GA","15": "HI","16": "ID","17": "IL","18": "IN","19": "IA","20": "KS","21": "KY",
    "22": "LA","23": "ME","24": "MD","25": "MA","26": "MI","27": "MN","28": "MS","29": "MO","30": "MT",
    "31": "NE","32": "NV","33": "NH","34": "NJ","35": "NM","36": "NY","37": "NC","38": "ND","39": "OH",
    "40": "OK","41": "OR","42": "PA","44": "RI","45": "SC","46": "SD","47": "TN","48": "TX","49": "UT",
    "50": "VT","51": "VA","53": "WA","54": "WV","55": "WI","56": "WY"
}

def fetch_acs5_county(year: int, state_fips: str, state_abbr: str) -> pd.DataFrame:
    """
    Fetch NAME, median household income, population for all counties in a state for given ACS 5-year 'year'.
    """
    variables = "NAME,B19013_001E,B01003_001E"
    url = (
        f"https://api.census.gov/data/{year}/acs/acs5"
        f"?get={variables}&for=county:*&in=state:{state_fips}"
    )
    if CENSUS_API_KEY:
        url += f"&key={CENSUS_API_KEY}"

    r = requests.get(url, timeout=60)
    r.raise_for_status()
    arr = r.json()
    df = pd.DataFrame(arr[1:], columns=arr[0])

    # Normalize
    df.rename(columns={
        "B19013_001E": "MedianHouseholdIncome",
        "B01003_001E": "Population"
    }, inplace=True)
    df["state_abbr"] = state_abbr
    df["state_fips"] = state_fips
    df["year"] = int(year)
    # Build 5-digit CountyFIPS: state(2) + county(3)
    df["county_fips"] = df["state"] + df["county"]

    return df[["state_abbr","state_fips","county_fips","NAME","year","MedianHouseholdIncome","Population"]]

def main():
    ap = argparse.ArgumentParser(description="Download ACS county metrics by state/year")
    ap.add_argument("--states", default="WV,PA,OH",
                    help="Comma-separated state abbreviations or 'ALL' (default: WV,PA,OH)")
    ap.add_argument("--years", default="2019,2020,2021,2022,2023",
                    help="Comma-separated ACS 5-year datasets years (end years)")
    ap.add_argument("--sleep", type=float, default=0.5,
                    help="Sleep seconds between calls to be polite (default 0.5)")
    args = ap.parse_args()

    states_arg = args.states.strip().upper()
    if states_arg == "ALL":
        targets = list(STATE_FIPS.values())
    else:
        targets = [s.strip() for s in states_arg.split(",") if s.strip()]

    # Build reverse map: abbr -> fips
    abbr_to_fips = {abbr: fips for fips, abbr in STATE_FIPS.items()}

    years: List[int] = [int(y.strip()) for y in args.years.split(",") if y.strip().isdigit()]

    out_dir = Path("data_raw")
    out_dir.mkdir(parents=True, exist_ok=True)

    for y in years:
        for abbr in targets:
            fips = abbr_to_fips.get(abbr)
            if not fips:
                print(f"⚠️  Unknown state abbr: {abbr}, skipping.")
                continue
            print(f"⬇️  {abbr} {y}: ACS5 county metrics …")
            try:
                df = fetch_acs5_county(y, fips, abbr)
                out_path = out_dir / f"{abbr}_acs5_{y}_county.csv"
                df.to_csv(out_path, index=False)
                print(f"   ✅ Saved {out_path} ({len(df)} rows)")
            except Exception as e:
                print(f"   ❌ Error for {abbr} {y}: {e}")
            time.sleep(args.sleep)

    print("🏁 Done.")

if __name__ == "__main__":
    main()
