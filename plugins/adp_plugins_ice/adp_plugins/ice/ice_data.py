from adp.core.base import Source, Transform, Sink, Context, Record, Batch
from pathlib import Path
import pandas as pd, geopandas as gpd, re, requests, zipfile, io, json
from typing import Iterator
import certifi
import ftplib, io, zipfile
import os
from collections import Counter
import datetime as dt



class DownloadIceZip(Source):
    """aaa
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
        


class DownloadStatesShapefile(Source):
    """Download and unzip TIGER/Line US state shapefile (via FTP)."""

    def run(self, ctx: Context) -> Iterator[Record]:
        year = int(self.kw.get("year", 2024))
        outdir = Path(self.kw.get("outdir", ctx.workdir / "shapes"))
        outdir.mkdir(parents=True, exist_ok=True)

        # Connect to FTP
        ftp = ftplib.FTP("ftp2.census.gov")
        ftp.login()  # anonymous by default

        path = f"/geo/tiger/TIGER{year}/STATE/tl_{year}_us_state.zip"
        ctx.log.info(f"[ice.states_shapefile] FTP GET {path}")

        # Download to memory
        bio = io.BytesIO()
        ftp.retrbinary(f"RETR {path}", bio.write)
        ftp.quit()

        # Extract ZIP
        bio.seek(0)
        with zipfile.ZipFile(bio) as zf:
            zf.extractall(outdir)

        shp = outdir / f"tl_{year}_us_state.shp"
        ctx.log.info(f"[ice.states_shapefile] Extraction complete: {shp}")
        yield {"shp": str(shp)}


class LocalExcelFiles(Source):
    """Yield paths to *.xls[x] in a folder."""
    def run(self, ctx: Context):
        folder = Path(self.kw.get("folder", ctx.workdir / "data"))
        for fp in folder.glob("*.xls*"):
            yield {"path": str(fp)}
class DetectHeaderAndRead(Transform):
    """
    Detect header row in CSV, normalize columns, drop unnamed/NaN, yield records.
    """
    def run(self, ctx: Context, rows):
        keys = tuple(self.kw.get("keywords", (
            "Apprehension Date", "Event Date", "Book In Date",
            "Detainer Issued Date", "Encounter Date", "Departed Date"
        )))
        look = int(self.kw.get("lookahead", 40))

        for r in rows:
            fp = Path(r["path"])
            if fp.suffix.lower() != ".csv":
                ctx.log.warn(f"[detect_headers] skipping non-CSV file {fp}")
                continue

            # --- Step 1: sample first rows to find header ---
            sample = pd.read_csv(fp, header=None, nrows=look, encoding="utf-8", on_bad_lines="skip")

            hdr = 0
            for idx, row in sample.iterrows():
                row_str = ",".join(str(x) for x in row.values)
                if any(k.lower() in row_str.lower() for k in keys):
                    hdr = idx
                    break

            # --- Step 2: load full CSV starting at detected header ---
            df = pd.read_csv(fp, skiprows=hdr, encoding="utf-8", on_bad_lines="skip")

            # --- Step 3: clean DataFrame ---
            df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")

            def normalize(c: str) -> str:
                # Replace any whitespace with underscore
                c = re.sub(r"\s+", "_", str(c).strip())
                # Keep only letters/numbers/underscores
                c = re.sub(r"[^0-9a-zA-Z_]+", "", c)
                c = c.lower()
                # Map ICE raw headers to canonical schema
                rename_map = {
                    "unique_identifier": "individual_id",
                    "detainer_prepare_date": "detainer_issued_date",
                    "stay_book_in_date_time": "book_in_date",
                }
                return rename_map.get(c, c)

            df.columns = [normalize(str(c)) for c in df.columns]
            df = df.loc[:, ~df.columns.str.startswith("unnamed")]

            # --- Debug ---
            ctx.log.info(f"[detect_headers] {fp.name} → {df.columns.tolist()}")

            # --- Step 4: yield row dicts ---
            for rec in df.to_dict(orient="records"):
                yield rec

    """
    Detect header row in Excel or CSV, normalize columns, drop unnamed/NaN, yield records.
    """
    def run(self, ctx: Context, rows):
        keys = tuple(self.kw.get("keywords", (
            "Apprehension Date", "Event Date", "Stay Book In Date Time",
            "Detainer Issued Date", "Book In Date", "Encounter Date", "Departed Date"
        )))
        look = int(self.kw.get("lookahead", 40))

        for r in rows:
            fp = Path(r["path"])

            # --- Step 1: sample first rows (Excel vs CSV) ---
            if fp.suffix.lower() == ".csv":
                sample = pd.read_csv(fp, header=None, nrows=look, encoding="utf-8", on_bad_lines="skip")
            else:
                sample = pd.read_excel(fp, header=None, nrows=look, engine="openpyxl")

            # --- Step 2: find header row ---
            hdr = 0
            for idx, row in sample.iterrows():
                row_str = ",".join(str(x) for x in row.values)
                if any(k.lower() in row_str.lower() for k in keys):
                    hdr = idx
                    break

            # --- Step 3: load full file starting at detected header ---
            if fp.suffix.lower() == ".csv":
                df = pd.read_csv(fp, skiprows=hdr, encoding="utf-8", on_bad_lines="skip")
            else:
                df = pd.read_excel(fp, skiprows=hdr, engine="openpyxl")

            # --- Step 4: clean DataFrame ---
            df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")

            def normalize(c: str) -> str:
                # Replace any whitespace with underscore
                c = re.sub(r"\s+", "_", str(c).strip())
                # Keep only letters/numbers/underscores
                c = re.sub(r"[^0-9a-zA-Z_]+", "", c)
                return c.lower()

            df.columns = [normalize(str(c)) for c in df.columns]
            df = df.loc[:, ~df.columns.str.startswith("unnamed")]

            # --- Debug ---
            print(f"[DEBUG] {fp.name} → {df.columns.tolist()}")

            # --- Step 5: yield row dicts ---
            for rec in df.to_dict(orient="records"):
                yield rec

# ─────────────────────────────────────────────
#  Excel ingest / clean
# ─────────────────────────────────────────────

class LocalFiles(Source):
    """Yield paths to files matching a glob pattern (default: *.csv)."""
    def run(self, ctx: Context):
        folder = Path(self.kw.get("folder", ctx.workdir / "data/ice/csv"))
        pattern = self.kw.get("pattern", "*.csv")
        folder = Path(folder)
        if not folder.exists():
            ctx.log.warn(f"[local_files] folder not found: {folder}")
            return
        files = sorted(folder.glob(pattern))
        for fp in files:
            if fp.is_file():
                yield {"path": str(fp)}


class LocalExcelFiles(Source):
    """Yield paths to *.xls[x] in a folder."""
    def run(self, ctx: Context):
        folder = Path(self.kw.get("folder", ctx.workdir / "data"))
        for fp in folder.glob("*.xls*"):
            yield {"path": str(fp)}




class CsvSink(Sink):
    """Dump records to a CSV file (streaming)."""
    def run(self, ctx: Context, rows):
        outbase = Path(self.kw.get("outfile", ctx.outdir / "clean"))
        outbase.parent.mkdir(parents=True, exist_ok=True)

        csv_path = str(outbase) + ".csv"
        chunksize = int(self.kw.get("chunksize", 50000))

        buffer = []
        total = 0
        first_chunk = True

        for row in rows:
            buffer.append(row)
            if len(buffer) >= chunksize:
                df = pd.DataFrame(buffer)
                df.to_csv(csv_path, index=False,
                          mode="w" if first_chunk else "a",
                          header=first_chunk)
                first_chunk = False
                total += len(buffer)
                ctx.log.info(f"  ↳ wrote {len(buffer)} rows (total {total})")
                buffer = []

        if buffer:
            df = pd.DataFrame(buffer)
            df.to_csv(csv_path, index=False,
                      mode="w" if first_chunk else "a",
                      header=first_chunk)
            total += len(buffer)

        return {"csv": csv_path}

# ─────────────────────────────────────────────
#  Download shapefiles
# ─────────────────────────────────────────────



# ─────────────────────────────────────────────
#  Analysis transforms
# ─────────────────────────────────────────────


class TimeSeriesArrests(Transform):
    """Aggregate arrests by month into JSON (streaming)."""
    def run(self, ctx: Context, rows):
        counts = Counter()

        for row in rows:
            date_str = row.get("apprehension_date")
            if not date_str:
                continue
            try:
                d = pd.to_datetime(date_str, errors="coerce")
            except Exception:
                continue
            if pd.isna(d):
                continue
            # round to month
            month_key = dt.date(d.year, d.month, 1).isoformat()
            counts[month_key] += 1
            # pass through unchanged if you want
            yield row  

        # convert to DataFrame for output
        ts = (pd.DataFrame.from_dict(counts, orient="index", columns=["n_arrests"])
                .sort_index())
        out = ctx.outdir / "timeseries_arrests.json"
        ts.to_json(out, orient="table")

        ctx.log.info(f"TimeSeriesArrests wrote {out} with {len(ts)} months")
        yield {"timeseries": str(out)}

class DetentionsChoropleth(Transform):
    """Join detentions per state with TIGER shapefile to GeoJSON (streaming counts)."""
    def run(self, ctx: Context, rows):
        from collections import Counter

        # count states without materializing all rows
        counts = Counter()
        for row in rows:
            st = row.get("state")
            if st:
                counts[st] += 1

        df = pd.DataFrame.from_dict(counts, orient="index", columns=["n_detentions"]).reset_index()
        df.columns = ["state", "n_detentions"]

        shp = Path(self.kw.get("shapefile"))
        gdf = gpd.read_file(shp)

        merged = gdf.merge(df, left_on="STUSPS", right_on="state", how="left")
        merged["n_detentions"] = merged["n_detentions"].fillna(0).astype(int)

        out = ctx.outdir / "detentions_choropleth.geojson"
        merged[["STUSPS","n_detentions","geometry"]].to_file(out, driver="GeoJSON")
        ctx.log.info(f"[ice.choropleth] wrote {out}")
        yield {"choropleth": str(out)}


# ─────────────────────────────────────────────
#  ML feature importance
# ─────────────────────────────────────────────
class FeatureImportanceRemoval(Transform):
    """
    Train XGBoost model to predict <=180 day detention.
    Uses sampling to prevent memory blowups.
    """
    def run(self, ctx: Context, rows):
        if not self.kw.get("enable_ml", True):
            ctx.log.info("[ice.feature_importance] Skipped (enable_ml=False)")
            return  # no output
        from xgboost import XGBClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import roc_auc_score

        df = pd.DataFrame(list(rows))

        # date parsing
        for col in ("book_in_date","book_out_date"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        if "book_in_date" not in df.columns or "book_out_date" not in df.columns:
            raise KeyError("Expected book_in_date and book_out_date columns")

        df["removed_180"] = (df["book_out_date"] - df["book_in_date"]).dt.days.le(180)

        use_cols = self.kw.get("use_cols", ["risk_category","gender","state"])
        X = pd.get_dummies(df[use_cols].fillna("UNK"))
        y = df["removed_180"].astype(int)

        # ✅ sample rows for training
        max_rows = int(self.kw.get("sample_n", 1000))
        if len(X) > max_rows:
            X = X.sample(max_rows, random_state=42)
            y = y.loc[X.index]

        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=0.25, random_state=42
        )

        mdl = XGBClassifier(n_estimators=3, max_depth=2, tree_method="hist", verbosity=0)
        mdl.fit(X_train, y_train)
        auc = float(roc_auc_score(y_val, mdl.predict_proba(X_val)[:,1]))

        imp = (pd.Series(mdl.feature_importances_, index=X.columns)
                 .sort_values(ascending=False)[:20])

        out = ctx.outdir / "feature_importance_removal.json"
        imp.to_json(out, orient="index")

        ctx.log.info(f"[ice.feature_importance] wrote {out}, auc={auc:.3f}")
        yield {"outfile": str(out), "auc": auc, "n_features": int(imp.shape[0])}

# ─────────────────────────────────────────────
#  Pipeline events (Sankey-style JSON)
# ─────────────────────────────────────────────
class PipelineEventsSample(Transform):
    """
    Build cross-stage events dataset for Sankey viz.
    Streams rows to avoid giant DataFrames.
    """
    DEFAULT_DATE_COLS = {
        "arrests": "apprehension_date",
        "detainers": "detainer_issued_date",
        "detentions": "book_in_date",
        "removals": "departed_date",
    }

    def run(self, ctx: Context, rows):
        stage = self.kw.get("stage")
        id_col = self.kw.get("id_col", "individual_id")
        date_col = self.kw.get("date_col") or self.DEFAULT_DATE_COLS.get(stage)
        if stage is None:
            raise ValueError("stage param required")

        events = []
        for row in rows:
            if id_col in row and date_col in row:
                events.append({
                    "individual_id": row[id_col],
                    "date": pd.to_datetime(row[date_col], errors="coerce"),
                    "stage": stage,
                })

        df = pd.DataFrame(events).dropna(subset=["date"])
        out = ctx.outdir / f"{stage}_events.json"
        df.to_json(out, orient="records", date_format="iso")
        ctx.log.info(f"[ice.events_sample] wrote {out} with {len(df)} rows")
        yield {"stage": stage, "outfile": str(out), "rows": len(df)}

class PipelineEventsMerged(Transform):
    """
    Collect rows from all ICE stages and emit a single JSON for Sankey viz.
    Uses chunked reads for memory safety.
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
                continue
            date_col = date_cols.get(stage)

            # read only needed columns, in chunks
            for chunk in pd.read_csv(matches[0], usecols=[id_col, date_col],
                                     chunksize=50000):
                slim = chunk.rename(columns={id_col: "individual_id", date_col: "date"})
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
