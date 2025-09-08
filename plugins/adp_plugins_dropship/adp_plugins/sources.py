# plugins/adp_plugins_dropship/adp_plugins_dropship/sources.py

# ADP Source that fetches products from DSers or CJ
from adp.core.base import Source, Context, Record, Batch
import time

from .clients.dsers import DSersClient
from .clients.cj import CJClient


class DropshipSource(Source):
    """
    Fetch product lists from DSers or CJ API and yield records.

    Config params:
      - provider: 'dsers' or 'cj'
      - query: search term or supplier id
      - limit: int
      - dsers_api_key: str (if provider == 'dsers')
      - cj_app_key / cj_app_secret: str (if provider == 'cj')
    """
    def run(self, ctx: Context) -> Batch:
        provider = self.kw.get("provider", "dsers")
        q = self.kw.get("query", "")
        limit = int(self.kw.get("limit", 200))

        if provider == "dsers":
            client = DSersClient(api_key=self.kw.get("dsers_api_key"))
            products = client.search_products(q, limit=limit)
        else:
            client = CJClient(self.kw.get("cj_app_key"), self.kw.get("cj_app_secret"))
            products = client.search_products(q, page=1, size=limit)

        for p in products:
            # normalize minimal fields; include raw payload for provenance
            yield {
                "sku": p.get("sku") or p.get("product_id") or p.get("id"),
                "title": p.get("title") or p.get("name"),
                "price": p.get("price") or p.get("sale_price") or 0,
                "compare_at_price": p.get("compare_at_price") or p.get("market_price"),
                "images": p.get("images") or p.get("image_urls") or [],
                "vendor": p.get("vendor") or p.get("supplier") or p.get("store_name"),
                "inventory": p.get("inventory") or p.get("stock") or 0,
                "raw": p,
                "retrieved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
