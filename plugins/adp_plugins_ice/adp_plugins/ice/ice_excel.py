from adp.core.base import Source, Transform, Sink, Context, Record
from pathlib import Path
import pandas as pd, re, zipfile

class LocalExcelFiles(Source):
    """Reads *.xls[x] from a folder."""
    def run(self, ctx: Context):
        folder = Path(self.kw.get("folder", ctx.workdir/"data"))
        for fp in folder.glob("*.xls*"):
            yield {"path": str(fp)}

class DetectHeaderAndRead(Transform):
    """Find header row by keyword scan and load DataFrame → records."""
    def run(self, ctx: Context, rows):
        keys = tuple(self.kw.get("keywords", ("Date","Area of Responsibility","State")))
        look = int(self.kw.get("lookahead", 30))
        for r in rows:
            fp = Path(r["path"])
            sample = pd.read_excel(fp, sheet_name=0, header=None, nrows=look)
            hdr = 2
            for idx, row in sample.iterrows():
                if row.count() >= 5 and any(str(c).find(k)>=0 for c in row for k in keys):
                    hdr = idx; break
            df = pd.read_excel(fp, sheet_name=0, skiprows=hdr, engine="openpyxl")
            df.columns = (df.columns.astype(str).str.lower().str.strip().str.replace(r"\s+","_",regex=True))
            for rec in df.to_dict(orient="records"):
                yield rec

class CsvSink(Sink):
    """Dump records to a single CSV."""
    def run(self, ctx: Context, rows):
        import pandas as pd
        out = ctx.outdir / (self.kw.get("filename","clean.csv"))

        # Ensure the parent directory exists
        out.parent.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame(list(rows))
        df.to_csv(out, index=False)
        return out

