#!/usr/bin/env python3
# cj_client.py — Persistent CJ Dropshipping API client with auto-refresh tokens

import requests
import time
import json
from pathlib import Path
from typing import Dict, Any, List, Optional


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
        print(f"[saved tokens → {self.token_path}]")

    def _load_tokens(self):
        if self.token_path.exists():
            try:
                data = json.loads(self.token_path.read_text())
                self.access_token = data.get("access_token")
                self.refresh_token = data.get("refresh_token")
                self.token_expiry = data.get("token_expiry", 0)
                print(f"[loaded tokens from {self.token_path}]")
            except Exception:
                pass

    # ────────────────────────────────
    #  AUTHENTICATION
    # ────────────────────────────────
    def authenticate(self) -> str:
        """Login with email + API key."""
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
        """Refresh access token using refresh token."""
        if not self.refresh_token:
            return self.authenticate()
        url = f"{self.base_url}/authentication/refreshAccessToken"
        payload = {"refreshToken": self.refresh_token}
        resp = self.session.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            print("[refresh failed → reauthenticating]")
            return self.authenticate()
        auth_data = data["data"]
        self.access_token = auth_data["accessToken"]
        self.token_expiry = time.time() + auth_data.get("expiresIn", 3600 * 24 * 15)
        self._save_tokens()
        return self.access_token

    # ────────────────────────────────
    #  HEADER BUILDER
    # ────────────────────────────────
    def _auth_headers(self) -> Dict[str, str]:
        if not self.access_token or time.time() >= self.token_expiry:
            print("[token expired → refreshing]")
            self.refresh_access_token()
        return {"CJ-Access-Token": self.access_token, "Accept": "application/json"}

    # ────────────────────────────────
    #  PRODUCT SEARCH
    # ────────────────────────────────
    def search_products(self, q: str, page: int = 1, size: int = 50) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/product/list"
        payload = {"keyword": q, "pageNum": page, "pageSize": size}
        resp = self.session.post(url, json=payload, headers=self._auth_headers(), timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", {}).get("list", [])

    # ────────────────────────────────
    #  PRODUCT DETAIL
    # ────────────────────────────────
    def get_product(self, product_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/product/detail"
        payload = {"productId": product_id}
        resp = self.session.post(url, json=payload, headers=self._auth_headers(), timeout=30)
        resp.raise_for_status()
        return resp.json()

    # ────────────────────────────────
    #  ORDER CREATION
    # ────────────────────────────────
    def create_order(self, order_payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/order/create"
        resp = self.session.post(url, json=order_payload, headers=self._auth_headers(), timeout=30)
        resp.raise_for_status()
        return resp.json()


# ────────────────────────────────
#  SAMPLE USAGE
# ────────────────────────────────
if __name__ == "__main__":
    # Replace with your CJ credentials
    EMAIL = "mieslilly@egmail.com"
    API_KEY = "c80c88e4863b42a4a9eff4bc3f06c8e6"

    cj = CJClient(email=EMAIL, api_key=API_KEY)

    # Search for products
    products = cj.search_products("t-shirt", page=1, size=5)

    for p in products:
        print(f"{p.get('productName')} — {p.get('productSku')}")
