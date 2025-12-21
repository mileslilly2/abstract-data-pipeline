from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from .source.config import get_shopify_settings

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

settings = get_shopify_settings()

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] dropship: %(message)s",
)

logger = logging.getLogger("dropship.order_handler")

# ---------------------------------------------------------------------
# Shopify client
# ---------------------------------------------------------------------


class ShopifyClient:
    def __init__(self) -> None:
        self.base_url = (
            f"https://{settings.DROPSHIP_SHOPIFY_STORE_DOMAIN}"
            f"/admin/api/{settings.DROPSHIP_SHOPIFY_API_VERSION}"
        )
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Shopify-Access-Token": settings.DROPSHIP_SHOPIFY_ADMIN_TOKEN,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Dict[str, Any] | None = None,
        json_body: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        resp = self.session.request(
            method,
            url,
            params=params,
            json=json_body,
            timeout=30,
        )

        if not resp.ok:
            raise RuntimeError(
                f"Shopify API error {resp.status_code}: {resp.text}"
            )

        return resp.json()

    # -------------------------------------------------------------

    def find_product_by_sku(self, sku: str) -> Optional[Dict[str, Any]]:
        """
        Shopify does not support direct SKU lookup.
        This fetches recent products and checks variants.
        Fine for small/medium catalogs.
        """
        data = self._request("GET", "/products.json", params={"limit": 250})
        for product in data.get("products", []):
            for variant in product.get("variants", []):
                if variant.get("sku") == sku:
                    return product
        return None

    # -------------------------------------------------------------

    def create_product(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("Creating product: %s", payload["product"]["title"])
        return self._request("POST", "/products.json", json_body=payload)


# ---------------------------------------------------------------------
# Dataset → Shopify mapping
# ---------------------------------------------------------------------


def dataset_item_to_shopify_product(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert one dataset item into a Shopify product payload.
    Assumes dataset schema you described earlier.
    """

    title = item["title"]
    sku = item["sku"]

    description_lines = [
        f"<p><strong>Platforms:</strong> {', '.join(item.get('platforms', []))}</p>",
        f"<p><strong>Category:</strong> {item.get('category')}</p>",
        f"<p><strong>Notes:</strong> {item.get('notes', '')}</p>",
    ]

    images = [{"src": url} for url in item.get("image_urls", [])]

    price_range = item.get("price_usd_estimate", [])
    price = str(price_range[1] if len(price_range) == 2 else price_range[0])

    tags = list(
        set(
            item.get("tags", [])
            + item.get("platforms", [])
            + item.get("retro_systems", [])
        )
    )

    product_payload = {
        "product": {
            "title": title,
            "body_html": "\n".join(description_lines),
            "vendor": "ADP Dropship",
            "product_type": item.get("category"),
            "tags": ", ".join(tags),
            "variants": [
                {
                    "sku": sku,
                    "price": price,
                    "inventory_management": None,
                    "requires_shipping": True,
                }
            ],
            "images": images,
            "status": "active",
        }
    }

    return product_payload


# ---------------------------------------------------------------------
# Main entrypoints
# ---------------------------------------------------------------------


def load_dataset(path: Path) -> List[Dict[str, Any]]:
    logger.info("Loading dataset: %s", path)
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Dataset JSON must be a list of items")

    return data


def sync_dataset_to_shopify(dataset_path: Path) -> None:
    client = ShopifyClient()
    items = load_dataset(dataset_path)

    created = 0
    skipped = 0

    for item in items:
        sku = item.get("sku")
        if not sku:
            logger.warning("Skipping item with no SKU: %s", item)
            continue

        existing = client.find_product_by_sku(sku)
        if existing:
            logger.info("SKU already exists, skipping: %s", sku)
            skipped += 1
            continue

        payload = dataset_item_to_shopify_product(item)
        client.create_product(payload)
        created += 1

    logger.info(
        "Sync complete — created=%d skipped=%d total=%d",
        created,
        skipped,
        len(items),
    )


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sync dataset to Shopify")
    parser.add_argument(
        "dataset",
        type=Path,
        help="Path to dataset JSON file",
    )

    args = parser.parse_args()
    sync_dataset_to_shopify(args.dataset)
