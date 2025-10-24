# plugins/adp_plugins_dropship/adp_plugins_dropship/transforms.py

from adp.core.base import Transform, Context, Record, Batch
from typing import Iterator
from .score_and_canonicalize import compute_score, canonicalize


class DropshipTransform(Transform):
    """
    Transform that scores and canonicalizes supplier product records.

    Steps:
      1. Compute a numeric score for each product (profit margin, shipping ETA, etc.).
      2. Filter out products below a threshold (min_score).
      3. Canonicalize into a Shopify-ready schema.
    """

    def run(self, ctx: Context, rows: Batch) -> Iterator[Record]:
        min_score = float(self.kw.get("min_score", 5.0))

        for r in rows:
            r["_score"] = compute_score(r)
            if r["_score"] < min_score:
                ctx.log.debug(f"Filtered out {r.get('sku')} (score {r['_score']})")
                continue
            canonical = canonicalize(r)
            ctx.log.debug(f"Accepted {canonical['productHandle']} (score {r['_score']})")
            yield canonical
