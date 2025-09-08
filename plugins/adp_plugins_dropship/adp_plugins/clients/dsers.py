# Minimal DSers client stub
import requests
from typing import List, Dict, Any

class DSersClient:
    def __init__(self, api_key: str, base_url: str = "https://api.dsers.com/v1"):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {"Authorization": f"Bearer {self.api_key}", "Accept": "application/json"}

    def search_products(self, q: str, limit: int = 50) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/products"
        resp = requests.get(url, params={"q": q, "limit": limit}, headers=self.headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        # DSers responses vary; return consistent list in 'data'
        return payload.get("data") or payload.get("products") or []

    def get_product(self, product_id: str) -> Dict[str, Any]:
        resp = requests.get(f"{self.base_url}/products/{product_id}", headers=self.headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def place_order(self, order_payload: Dict[str, Any]) -> Dict[str, Any]:
        resp = requests.post(f"{self.base_url}/orders", json=order_payload, headers=self.headers, timeout=30)
        resp.raise_for_status()
        return resp.json()