# plugins/adp_plugins_dropship/sinks.py
"""
Sink implementation that writes canonicalized product records to a JSONL file
and (optionally) uploads it to Shopify via Bulk Operations.
"""

from __future__ import annotations
from pathlib import Path
from typing import Iterable, Mapping, Any, Optional
import json
import requests

Record = Mapping[str, Any]

API_TEMPLATE = "https://{shop}/admin/api/2024-07/graphql.json"


class ShopifyBulkSink:
    """
    Write products to JSONL and trigger a Shopify Bulk Operation.
    
    Parameters:
        filename: local JSONL filename to write (default: "out_products.jsonl")
        shop: Shopify shop domain, e.g. "your-shop.myshopify.com"
        token: Admin API access token
        inner_mutation: GraphQL mutation string to run for each JSONL line
        upload: if False, just writes JSONL without contacting Shopify
    """
    def __init__(
        self,
        filename: str = "out_products.jsonl",
        shop: Optional[str] = None,
        token: Optional[str] = None,
        inner_mutation: Optional[str] = None,
        upload: bool = True,
        **kw
    ):
        self.filename = filename
        self.shop = shop
        self.token = token
        self.inner_mutation = inner_mutation
        self.upload = upload

    def run(self, ctx, rows: Iterable[Record]) -> Path:
        out = Path(ctx.outdir) / self.filename
        out.parent.mkdir(parents=True, exist_ok=True)

        # Write newline-delimited JSON
        with out.open("w", encoding="utf8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False))
                fh.write("\n")

        if self.upload and self.shop and self.token and self.inner_mutation:
            self._upload_to_shopify(out, ctx)

        return out

    def _upload_to_shopify(self, jsonl_path: Path, ctx) -> None:
        api_url = API_TEMPLATE.format(shop=self.shop)
        headers = {
            "X-Shopify-Access-Token": self.token,
            "Content-Type": "application/json"
        }

        # Step 1: stagedUploadsCreate
        q_staged = """
        mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
          stagedUploadsCreate(input: $input) {
            stagedTargets { url resourceUrl parameters { name value } }
            userErrors { field message }
          }
        }
        """
        variables = {
            "input": [
                {
                    "resource": "BULK_MUTATION_VARIABLES",
                    "filename": jsonl_path.name,
                    "mimeType": "text/jsonl",
                    "httpMethod": "POST"
                }
            ]
        }
        resp = requests.post(api_url, json={"query": q_staged, "variables": variables}, headers=headers, timeout=30)
        resp.raise_for_status()
        tgt = resp.json()["data"]["stagedUploadsCreate"]["stagedTargets"][0]

        # Step 2: upload the JSONL to staged target
        with open(jsonl_path, "rb") as f:
            files = {"file": (jsonl_path.name, f, "text/jsonl")}
            data = {p["name"]: p["value"] for p in tgt["parameters"]}
            s3resp = requests.post(tgt["url"], data=data, files=files, timeout=60)
            s3resp.raise_for_status()
        resource_url = tgt["resourceUrl"]

        # Step 3: bulkOperationRunMutation
        q_bulk = """
        mutation bulkOperationRunMutation($mutation: String!, $stagedUploadPath: String!) {
          bulkOperationRunMutation(mutation: $mutation, stagedUploadPath: $stagedUploadPath) {
            bulkOperation { id status }
            userErrors { field message }
          }
        }
        """
        variables = {"mutation": self.inner_mutation, "stagedUploadPath": resource_url}
        bulk_resp = requests.post(api_url, json={"query": q_bulk, "variables": variables}, headers=headers, timeout=30)
        bulk_resp.raise_for_status()
        ctx.log.info("Shopify bulk operation response: %s", bulk_resp.json())
