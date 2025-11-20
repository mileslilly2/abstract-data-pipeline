"""
Pipeline: CivilNarrative → Parquet files
"""

from ..sync.flatten_for_parquet import flatten_events
from ..sync.sync_parquet import write_parquet

def run(narrative, out_path):
    rows = flatten_events(narrative)
    write_parquet(rows, out_path)
