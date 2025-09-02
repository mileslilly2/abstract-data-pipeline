from adp.core.base import Source, Transform, Sink, Context, Record, Batch
from pathlib import Path
import pandas as pd, geopandas as gpd, re, requests, zipfile, io, json
from typing import Iterator


class DownloadStatesShapefile(Source):
    """
    Download and unzip TIGER/Line US state shapefile.

    Params:
      - year (int, default=2024): TIGER release year
      - outdir (str): directory to extract shapefiles into (default: data/shapes)
    """
    def run(self, ctx: Context) -> Iterator[Record]:
        year = int(self.kw.get("year", 2024))
        outdir = Path(self.kw.get("outdir", ctx.workdir / "data/shapes"))
        outdir.mkdir(parents=True, exist_ok=True)

        url = f"https://www2.census.gov/geo/tiger/TIGER{year}/STATE/tl_{year}_us_state.zip"
        ctx.log.info(f"[ice.states_shapefile] Downloading {url}")

        r = requests.get(url, timeout=60)
        r.raise_for_status()

        ctx.log.info(f"[ice.states_shapefile] Extracting → {outdir}")
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            zf.extractall(outdir)

        shp_path = outdir / f"tl_{year}_us_state.shp"
        yield {"year": year, "outdir": str(outdir), "shapefile": str(shp_path)}


class DownloadIceZip(Source):
    """
    Download and unzip an ICE release ZIP archive.

    Params:
      - url (str): direct URL to the ZIP file
      - outdir (str): directory to extract into (default: data/ice/raw)
    """
    def run(self, ctx: Context) -> Iterator[Record]:
        url = self.kw.get("url")
        if not url:
            raise ValueError("DownloadIceZip requires a 'url' parameter")
        outdir = Path(self.kw.get("outdir", ctx.workdir / "data/ice/raw"))
        outdir.mkdir(parents=True, exist_ok=True)

        ctx.log.info(f"[ice.download_zip] Downloading {url}")
        r = requests.get(url, timeout=60)
        r.raise_for_status()

        ctx.log.info(f"[ice.download_zip] Extracting to {outdir}")
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            zf.extractall(outdir)

        # Return list of extracted files
        files = [str(p) for p in outdir.glob("**/*") if p.is_file()]
        yield {"outdir": str(outdir), "files": files}


class LocalExcelFiles(Source):
    """Yield paths to *.xls[x] in a folder."""
    def run(self, ctx: Context):
        folder = Path(self.kw.get("folder", ctx.workdir / "data"))
        for fp in folder.glob("*.xls*"):
            yield {"path": str(fp)}


class DetectHeaderAndRead(Transform):
    """
    Find header row by keyword scan, load DataFrame, normalize columns,
    drop NaN/unnamed, yield row dicts.
    """
    def run(self, ctx: Context, rows):
        keys = tuple(self.kw.get("keywords", (
            "Event Date", "Apprehension Date", "Stay Book In Date Time",
            "Detainer Issued Date", "Book In Date", "Encounter Date", "Departed Date"
        )))
        look = int(self.kw.get("lookahead", 40))

        for r in rows:
            fp = Path(r["path"])
            # Scan first few rows to detect header
            sample = pd.read_excel(fp, header=None, nrows=look)
            hdr = 0
            for idx, row in sample.iterrows():
                row_str = ",".join(str(x) for x in row.values)
                if any(k.lower() in row_str.lower() for k in keys):
                    hdr = idx
                    break

            # Re-read with detected header
            df = pd.read_excel(fp, header=hdr, engine="openpyxl")

            # Drop empty cols/rows
            df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")

            # Normalize column names
            def normalize(c: str) -> str:
                return re.sub(r'[^0-9a-zA-Z_]+', '', c.strip().replace(" ", "_")).lower()
            df.columns = [normalize(str(c)) for c in df.columns]

            # Remove unnamed cols
            df = df.loc[:, ~df.columns.str.startswith("unnamed")]

            for rec in df.to_dict(orient="records"):
                yield rec




# ─────────────────────────────────────────────
#  Excel ingest / clean
# ─────────────────────────────────────────────

class LocalExcelFiles(Source):
    """Yield paths to *.xls[x] in a folder."""
    def run(self, ctx: Context):
        folder = Path(self.kw.get("folder", ctx.workdir / "data"))
        for fp in folder.glob("*.xls*"):
            yield {"path": str(fp)}

class DetectHeaderAndRead(Transform):
    """Detect header row, normalize cols, drop unnamed/NaN, yield records."""
    def run(self, ctx: Context, rows):
        keys = tuple(self.kw.get("keywords", ("Date","Area of Responsibility","State")))
        look = int(self.kw.get("lookahead", 40))

        for r in rows:
            fp = Path(r["path"])
            sample = pd.read_excel(fp, header=None, nrows=look)
            hdr = 0
            for idx, row in sample.iterrows():
                row_str = ",".join(str(x) for x in row.values)
                if any(k.lower() in row_str.lower() for k in keys):
                    hdr = idx; break

            df = pd.read_excel(fp, skiprows=hdr, engine="openpyxl")
            df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")

            def normalize(c): 
                return re.sub(r'[^0-9a-zA-Z_]+', '', c.strip().replace(" ", "_")).lower()
            df.columns = [normalize(str(c)) for c in df.columns]
            df = df.loc[:, ~df.columns.str.startswith("unnamed")]

            for rec in df.to_dict(orient="records"):
                yield rec

class CsvSink(Sink):
    """Dump records to a CSV+JSON pair."""
    def run(self, ctx: Context, rows):
        outbase = Path(self.kw.get("outfile", ctx.outdir / "clean"))
        outbase.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(list(rows))
        df.to_csv(str(outbase) + ".csv", index=False)
        df.to_json(str(outbase) + ".json", orient="records")
        return {"csv": str(outbase)+".csv", "json": str(outbase)+".json"}

# ─────────────────────────────────────────────
#  Download shapefiles
# ─────────────────────────────────────────────

class DownloadStatesShapefile(Source):
    """Download and unzip TIGER/Line US state shapefile."""
    def run(self, ctx: Context):
        year = int(self.kw.get("year", 2024))
        outdir = Path(self.kw.get("outdir", ctx.workdir/"shapes"))
        outdir.mkdir(parents=True, exist_ok=True)
        url = f"https://www2.census.gov/geo/tiger/TIGER{year}/STATE/tl_{year}_us_state.zip"

        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            zf.extractall(outdir)

        shp = outdir / f"tl_{year}_us_state.shp"
        yield {"shp": str(shp)}

# ─────────────────────────────────────────────
#  Analysis transforms
# ─────────────────────────────────────────────

class TimeSeriesArrests(Transform):
    """Aggregate arrests by month into JSON."""
    def run(self, ctx: Context, rows):
        df = pd.DataFrame(list(rows))
        if "apprehension_date" not in df.columns:
            raise KeyError("apprehension_date not in columns")
        df["apprehension_date"] = pd.to_datetime(df["apprehension_date"], errors="coerce")
        ts = (df.set_index("apprehension_date")
                .groupby(pd.Grouper(freq="M"))
                .size()
                .rename("n_arrests")
                .to_frame())
        out = ctx.outdir / "timeseries_arrests.json"
        ts.to_json(out, orient="table")
        yield {"timeseries": str(out)}

class DetentionsChoropleth(Transform):
    """Join detentions per state with TIGER shapefile to GeoJSON."""
    def run(self, ctx: Context, rows):
        df = pd.DataFrame(list(rows))
        if "state" not in df.columns:
            raise KeyError("state not in columns")
        counts = df.groupby("state").size().rename("n_detentions").reset_index()

        shp = Path(self.kw.get("shapefile"))
        gdf = gpd.read_file(shp)
        merged = gdf.merge(counts, left_on="STUSPS", right_on="state", how="left")
        merged["n_detentions"] = merged["n_detentions"].fillna(0).astype(int)

        out = ctx.outdir / "detentions_choropleth.geojson"
        merged[["STUSPS","n_detentions","geometry"]].to_file(out, driver="GeoJSON")
        yield {"choropleth": str(out)}



# ─────────────────────────────────────────────
#  ML feature importance
# ─────────────────────────────────────────────

class FeatureImportanceRemoval(Transform):
    """
    Train XGBoost model to predict <=180 day detention and
    save top-20 feature importances to JSON.
    """
    def run(self, ctx: Context, rows):
        from xgboost import XGBClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import roc_auc_score

        df = pd.DataFrame(list(rows))
        for col in ("book_in_date","book_out_date"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        if "book_in_date" not in df.columns or "book_out_date" not in df.columns:
            raise KeyError("Expected book_in_date and book_out_date columns")

        df["removed_180"] = (
            (df["book_out_date"] - df["book_in_date"]).dt.days.le(180)
        )

        use_cols = self.kw.get("use_cols", ["risk_category","gender","state"])
        X = pd.get_dummies(df[use_cols].fillna("UNK"))
        y = df["removed_180"].astype(int)

        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=0.25, random_state=42
        )

        mdl = XGBClassifier(n_estimators=200, max_depth=3, verbosity=0)
        mdl.fit(X_train, y_train)
        auc = float(roc_auc_score(y_val, mdl.predict_proba(X_val)[:,1]))

        imp = (pd.Series(mdl.feature_importances_, index=X.columns)
                 .sort_values(ascending=False)[:20])

        out = ctx.outdir / "feature_importance_removal.json"
        imp.to_json(out, orient="index")

        yield {"outfile": str(out), "auc": auc, "n_features": int(imp.shape[0])}


# ─────────────────────────────────────────────
#  Pipeline events (Sankey-style JSON)
# ─────────────────────────────────────────────

class PipelineEventsSample(Transform):
    """
    Build cross-stage events dataset for Sankey viz.
    Requires rows from multiple stages (arrests, detainers, detentions, removals).
    """
    DEFAULT_DATE_COLS = {
        "arrests": "apprehension_date",
        "detainers": "detainer_issued_date",
        "detentions": "book_in_date",
        "removals": "departed_date",
    }

    def run(self, ctx: Context, rows):
        df = pd.DataFrame(list(rows))
        stage = self.kw.get("stage")   # must set in YAML
        id_col = self.kw.get("id_col","individual_id")
        date_col = self.kw.get("date_col") or self.DEFAULT_DATE_COLS.get(stage)

        if stage is None:
            raise ValueError("stage param required")
        if id_col not in df.columns or date_col not in df.columns:
            raise KeyError(f"{stage}: missing {id_col} or {date_col}")

        df = df[[id_col,date_col]].copy()
        df.columns = ["individual_id","date"]
        df["stage"] = stage
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

        out = ctx.outdir / f"{stage}_events.json"
        df.to_json(out, orient="records", date_format="iso")

        yield {"stage": stage, "outfile": str(out), "rows": len(df)}


class PipelineEventsMerged(Transform):
    """
    Collect rows from all four ICE stages and emit a single JSON
    for Sankey / flow visualization.

    Params:
      - cleaned_dir (str): folder with *_clean.csv files
      - patterns (dict[str,str]): stage -> glob pattern
      - date_cols (dict[str,str]): stage -> date column
      - id_col (str): identifier column (default: individual_id)
      - sample_n (int): subsample size (default: 100000)
      - outfile (str): path to final JSON
    """

    DEFAULT_PATTERNS = {
        "arrest": "*Arrests*_clean.csv",
        "detainer": "*Detainers*_clean.csv",
        "detention": "*Detentions*_clean.csv",
        "removal": "*Removals*_clean.csv",
    }

    DEFAULT_DATE_COLS = {
        "arrest": "apprehension_date",
        "detainer": "detainer_issued_date",
        "detention": "book_in_date",
        "removal": "departed_date",
    }

    def run(self, ctx: Context, rows):
        import pandas as pd
        cleaned_dir = Path(self.kw.get("cleaned_dir", ctx.workdir / "data/ice/clean"))
        patterns = {**self.DEFAULT_PATTERNS, **(self.kw.get("patterns") or {})}
        date_cols = {**self.DEFAULT_DATE_COLS, **(self.kw.get("date_cols") or {})}
        id_col = self.kw.get("id_col", "individual_id")
        sample_n = int(self.kw.get("sample_n", 100_000))
        outfile = Path(self.kw.get("outfile", ctx.outdir / "pipeline_events_sample.json"))
        outfile.parent.mkdir(parents=True, exist_ok=True)

        parts = []
        for stage, pat in patterns.items():
            matches = list(cleaned_dir.glob(pat))
            if not matches:
                ctx.log.info(f"[ice.events_merged] no file for stage={stage} pattern={pat}")
                continue
            df = pd.read_csv(matches[0])
            df.columns = df.columns.str.lower().str.strip()
            date_col = date_cols.get(stage)
            if id_col not in df.columns or date_col not in df.columns:
                ctx.log.info(f"[ice.events_merged] stage={stage} missing {id_col} or {date_col}")
                continue
            slim = df[[id_col, date_col]].copy()
            slim.columns = ["individual_id", "date"]
            slim["stage"] = stage
            slim["date"] = pd.to_datetime(slim["date"], errors="coerce")
            parts.append(slim)

        if not parts:
            raise RuntimeError("No stages produced events")

        events = pd.concat(parts, ignore_index=True).dropna(subset=["date"])
        if sample_n and len(events) > sample_n:
            events = events.sample(sample_n, random_state=42)

        events.to_json(outfile, orient="records", date_format="iso")
        ctx.log.info(f"[ice.events_merged] wrote {outfile} with {len(events)} rows")
        yield {"outfile": str(outfile), "rows": len(events)}



class XlsxToCsv(Source):
    """
    Convert all .xlsx files in a folder into .csv.
    
    Params:
      - folder (str): input folder containing Excel files
      - outdir (str): where to write CSV files (default: same folder)
    """
    def run(self, ctx: Context) -> Batch:
        folder = Path(self.kw.get("folder", ctx.workdir / "data/ice/raw"))
        outdir = Path(self.kw.get("outdir", folder))
        outdir.mkdir(parents=True, exist_ok=True)

        ctx.log.info(f"[ice.xlsx_to_csv] Converting .xlsx → .csv in {folder}")

        for fp in folder.glob("*.xlsx"):
            try:
                df = pd.read_excel(fp, engine="openpyxl")
                csv_path = outdir / (fp.stem + ".csv")
                df.to_csv(csv_path, index=False)
                ctx.log.info(f"[ice.xlsx_to_csv] {fp.name} → {csv_path.name}")
                yield {"xlsx": str(fp), "csv": str(csv_path)}
            except Exception as e:
                ctx.log.error(f"[ice.xlsx_to_csv] Failed on {fp}: {e}")
