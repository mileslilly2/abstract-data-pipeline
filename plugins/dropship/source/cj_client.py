#!/usr/bin/env python3
# cj_client.py — CJ Dropshipping API client with full product attribute extraction

import requests
import time
import json
import random
from pathlib import Path
from typing import Dict, Any, List, Optional
import os
from dotenv import load_dotenv


class CJRateLimitError(RuntimeError):
    """Raised when the CJ API keeps returning 429 after multiple retries."""
    pass


class CJClient:
    def __init__(
        self,
        email: str,
        api_key: str,
        base_url: str = "https://developers.cjdropshipping.com/api2.0/v1",
        token_path: Path = Path.home() / ".cj_tokens.json",
    ):
        self.email = email
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.token_path = token_path
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_expiry: float = 0
        self._load_tokens()

    # ─────────────────────────────────────────────
    # TOKEN SAVE & LOAD
    # ─────────────────────────────────────────────
    def _save_tokens(self):
        data = {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_expiry": self.token_expiry,
        }
        self.token_path.write_text(json.dumps(data, indent=2))

    def _load_tokens(self):
        if self.token_path.exists():
            try:
                data = json.loads(self.token_path.read_text())
                self.access_token = data.get("access_token")
                self.refresh_token = data.get("refresh_token")
                self.token_expiry = data.get("token_expiry", 0)
            except Exception:
                pass

    # ─────────────────────────────────────────────
    # AUTHENTICATION
    # ─────────────────────────────────────────────
    def authenticate(self) -> str:
        url = f"{self.base_url}/authentication/getAccessToken"
        payload = {"email": self.email, "apiKey": self.api_key}
        resp = self.session.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            raise RuntimeError(f"Authentication failed: {data}")

        auth_data = data["data"]
        self.access_token = auth_data["accessToken"]
        self.refresh_token = auth_data["refreshToken"]
        self.token_expiry = time.time() + auth_data.get("expiresIn", 3600 * 24 * 15)
        self._save_tokens()
        return self.access_token

    def refresh_access_token(self) -> str:
        if not self.refresh_token:
            return self.authenticate()

        url = f"{self.base_url}/authentication/refreshAccessToken"
        payload = {"refreshToken": self.refresh_token}
        resp = self.session.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            return self.authenticate()

        auth_data = data["data"]
        self.access_token = auth_data["accessToken"]
        self.token_expiry = time.time() + auth_data.get("expiresIn", 3600 * 24 * 15)
        self._save_tokens()
        return self.access_token

    # ─────────────────────────────────────────────
    # AUTH HEADERS
    # ─────────────────────────────────────────────
    def _auth_headers(self) -> Dict[str, str]:
        if not self.access_token or time.time() >= self.token_expiry:
            self.refresh_access_token()
        return {"CJ-Access-Token": self.access_token, "Accept": "application/json"}

    # ─────────────────────────────────────────────
    # FULL PRODUCT NORMALIZATION (listV2 fields)
    # ─────────────────────────────────────────────
    @staticmethod
    def normalize_product(raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize ALL relevant listV2 attributes for Shopify + ADP ingest."""
        return {
            "id": raw.get("id"),
            "sku": raw.get("sku"),
            "spu": raw.get("spu"),
            "name_en": raw.get("nameEn"),
            "image": raw.get("bigImage"),
            "video_list": raw.get("videoList") or [],
            "has_video": bool(raw.get("isVideo")),
            "sell_price": raw.get("sellPrice"),
            "now_price": raw.get("nowPrice"),
            "discount_price": raw.get("discountPrice"),
            "discount_rate": raw.get("discountPriceRate"),
            "currency": raw.get("currency", "USD"),
            "category_id": raw.get("categoryId"),
            "category_lvl1_name": raw.get("oneCategoryName"),
            "category_lvl2_name": raw.get("twoCategoryName"),
            "category_lvl3_name": raw.get("threeCategoryName"),
            "category_lvl1_id": raw.get("oneCategoryId"),
            "category_lvl2_id": raw.get("twoCategoryId"),
            "category_lvl3_id": raw.get("categoryId"),
            "product_type": raw.get("productType"),
            "supplier_name": raw.get("supplierName"),
            "customization": raw.get("customization"),
            "is_personalized": raw.get("isPersonalized"),
            "has_ce_cert": raw.get("hasCECertification"),
            "is_free_shipping": bool(raw.get("addMarkStatus") == 1),
            "description": raw.get("description"),
            "warehouse_inventory_num": raw.get("warehouseInventoryNum"),
            "verified_inventory_total": raw.get("totalVerifiedInventory"),
            "unverified_inventory_total": raw.get("totalUnVerifiedInventory"),
            "verified_warehouse_flag": raw.get("verifiedWarehouse"),
            "listed_num": raw.get("listedNum"),
            "sale_status": raw.get("saleStatus"),
            "authority_status": raw.get("authorityStatus"),
            "create_at": raw.get("createAt"),
            "delivery_cycle": raw.get("deliveryCycle"),
        }

    # ─────────────────────────────────────────────
    # PRODUCT SEARCH listV2  (with 429-safe backoff)
    # ─────────────────────────────────────────────
    def search_products(
        self,
        keyword: str,
        page: int = 1,
        size: int = 100,
        max_retries: int = 8,
    ) -> List[Dict[str, Any]]:
        """
        Calls CJ product/listV2 with automatic rate-limit handling (429),
        exponential backoff + Retry-After + jitter.
        Raises CJRateLimitError if still 429 after retries.
        """
        url = f"{self.base_url}/product/listV2"
        params = {
            "page": page,
            "size": size,
            "keyword": keyword,
            # always include rich features
            "features": "enable_category,enable_description,enable_video",
        }

        last_status = None

        for attempt in range(max_retries):
            try:
                resp = self.session.get(
                    url,
                    params=params,
                    headers=self._auth_headers(),
                    timeout=30,
                )
            except requests.RequestException as e:
                # network/connection error → backoff + retry
                backoff = min(2 ** attempt, 60)
                wait_time = backoff + random.random()
                print(f"⚠️ Network error on page {page}: {e}. Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue

            status = resp.status_code
            last_status = status

            # Success
            if status < 400:
                data = resp.json()
                if isinstance(data, dict):
                    return data.get("data", []) or []
                return []

            # Rate limit
            if status == 429:
                retry_after_header = resp.headers.get("Retry-After")
                retry_after = 0
                if retry_after_header:
                    try:
                        retry_after = int(retry_after_header)
                    except ValueError:
                        retry_after = 0

                backoff = min(2 ** attempt, 60)
                wait_time = max(retry_after, backoff) + random.random()

                print(
                    f"⚠️ 429 rate limited on page {page}, keyword='{keyword}'. "
                    f"Waiting {wait_time:.1f}s (retry {attempt+1}/{max_retries})..."
                )
                time.sleep(wait_time)
                continue

            # Any other HTTP error
            try:
                resp.raise_for_status()
            except Exception as e:
                print(f"❌ HTTP error on page {page}, keyword='{keyword}': {e}")
                raise

        # If we exhausted retries and still mostly saw 429s, signal rate-limit
        if last_status == 429:
            raise CJRateLimitError(
                f"CJ API still rate-limiting after {max_retries} retries "
                f"(keyword={keyword}, page={page})"
            )

        raise RuntimeError(
            f"CJ API request failed after {max_retries} retries "
            f"(keyword={keyword}, page={page}, last_status={last_status})"
        )

    # ─────────────────────────────────────────────
    # FULL PRODUCT DETAIL LOOKUP
    # ─────────────────────────────────────────────
    def get_product(self, pid: str) -> Dict[str, Any]:
        url = f"{self.base_url}/product/query"
        params = {"pid": pid}
        resp = self.session.get(
            url, params=params, headers=self._auth_headers(), timeout=30
        )
        resp.raise_for_status()
        return resp.json().get("data", {})

    def iter_hybrid_catalog(
        self,
        keyword: str,
        page_start: int = 1,
        page_end: int = 100,
        size: int = 100,
        time_start_ms: Optional[int] = None,
        time_end_ms: Optional[int] = None,
        sleep_between_pages: float = 0.5,
    ):
        """
        Iterate through search pages and yield raw products.
        NOTE: CJRateLimitError can bubble up to the caller if search_products
        keeps getting 429s; callers should handle that.
        """
        for page in range(page_start, page_end + 1):
            results = self.search_products(
                keyword=keyword,
                page=page,
                size=size,
            )

            if not results:
                # no more products for this keyword
                break

            for product in results:
                yield product

            # polite pause between pages so we don't hammer the API
            time.sleep(sleep_between_pages)


# ─────────────────────────────────────────────
# FACTORY: make_client_from_env
# ─────────────────────────────────────────────
def make_client_from_env() -> CJClient:
    """
    Automatically loads CJ_EMAIL and CJ_API_KEY from environment or .env file.
    """
    load_dotenv()
    email = os.getenv("CJ_EMAIL")
    api_key = os.getenv("CJ_API_KEY")

    if not email or not api_key:
        raise RuntimeError("CJ_EMAIL and CJ_API_KEY must be set in environment or .env")

    return CJClient(email=email, api_key=api_key)


# ─────────────────────────────────────────────
# Manual test
# ─────────────────────────────────────────────
if __name__ == "__main__":
    client = make_client_from_env()
    print(client)
    products = client.search_products("hoodie", size=5)
    print(json.dumps(products, indent=2))
