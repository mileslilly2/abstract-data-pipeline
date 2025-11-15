#!/usr/bin/env python3
# cj_client.py — CJ Dropshipping API client with product search, normalization, and auto-refresh tokens

import requests
import time
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
import os
from dotenv import load_dotenv


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

    # ────────────────────────────────
    #  TOKEN PERSISTENCE
    # ────────────────────────────────
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

    # ────────────────────────────────
    #  AUTHENTICATION
    # ────────────────────────────────
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

    # ────────────────────────────────
    #  AUTH HEADERS
    # ────────────────────────────────
    def _auth_headers(self) -> Dict[str, str]:
        if not self.access_token or time.time() >= self.token_expiry:
            self.refresh_access_token()
        return {"CJ-Access-Token": self.access_token, "Accept": "application/json"}

    # ────────────────────────────────
    #  PRODUCT NORMALIZER (listV2)
    # ────────────────────────────────
    @staticmethod
    def normalize_product(raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": raw.get("id"),
            "name": raw.get("nameEn"),
            "sku": raw.get("sku"),
            "image": raw.get("bigImage"),
            "price": raw.get("sellPrice"),
            "discount_price": raw.get("discountPrice") or raw.get("nowPrice"),
            "category": raw.get("threeCategoryName") or raw.get("twoCategoryName"),
            "listed_count": raw.get("listedNum"),
            "inventory": raw.get("warehouseInventoryNum"),
            "currency": raw.get("currency", "USD"),
        }

    # ────────────────────────────────
    #  PRODUCT SEARCH (listV2 — correct endpoint)
    # ────────────────────────────────
    def search_products(self, keyword: str, page: int = 1, size: int = 20) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/product/listV2"
        params = {"page": page, "size": size, "keyWord": keyword}

        resp = self.session.get(url, params=params, headers=self._auth_headers(), timeout=30)
        resp.raise_for_status()
        data = resp.json()

        content_blocks = (data.get("data") or {}).get("content") or []
        results: List[Dict[str, Any]] = []

        for block in content_blocks:
            for p in block.get("productList", []) or []:
                results.append(self.normalize_product(p))

        return results

    # ────────────────────────────────
    #  PRODUCT DETAIL LOOKUP (pid or sku)
    # ────────────────────────────────
    def get_product(self, pid: str) -> Dict[str, Any]:
        url = f"{self.base_url}/product/query"
        params = {"pid": pid}
        resp = self.session.get(url, params=params, headers=self._auth_headers(), timeout=30)
        resp.raise_for_status()
        return resp.json().get("data", {})


# ────────────────────────────────
#  USAGE EXAMPLE
# ────────────────────────────────
if __name__ == "__main__":
    load_dotenv()
    EMAIL = os.getenv("CJ_EMAIL")
    API_KEY = os.getenv("CJ_API_KEY")

    cj = CJClient(email=EMAIL, api_key=API_KEY)
    products = cj.search_products("hoodie", size=5)

    print(json.dumps(products, indent=2))
