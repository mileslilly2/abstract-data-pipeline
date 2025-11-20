"""
Write DataFrame to Parquet
"""
import pandas as pd

def write_parquet(rows, path):
    df = pd.DataFrame(rows)
    df.to_parquet(path, index=False)
