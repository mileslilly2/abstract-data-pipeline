from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


# ------------------------------------------------------------
# Env loader (PLUGIN-LOCAL, SINGLE SOURCE OF TRUTH)
# ------------------------------------------------------------

def _load_dotenv() -> None:
    """
    Load env from plugins/dropship/.env ONLY.
    Designed for running scripts from inside the plugin.
    """
    try:
        from dotenv import load_dotenv
    except Exception:
        return

    source_dir = Path(__file__).resolve().parent      # plugins/dropship/source
    plugin_root = source_dir.parent                  # plugins/dropship

    env_path = plugin_root / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
    else:
        raise RuntimeError(f"Missing required env file: {env_path}")


def _env(name: str, default: str | None = None, *, required: bool = False) -> str | None:
    val = os.getenv(name, default)
    if required and not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


# ------------------------------------------------------------
# Harvest (AliExpress only)
# ------------------------------------------------------------

@dataclass(frozen=True)
class HarvestSettings:
    ALIEXPRESS_APP_KEY: str
    ALIEXPRESS_TRACKING_ID: str


@lru_cache(maxsize=1)
def get_harvest_settings() -> HarvestSettings:
    _load_dotenv()
    return HarvestSettings(
        ALIEXPRESS_APP_KEY=_env("ALIEXPRESS_APP_KEY", required=True) or "",
        ALIEXPRESS_TRACKING_ID=_env("ALIEXPRESS_TRACKING_ID", required=True) or "",
    )


# ------------------------------------------------------------
# Shopify (sync only)
# ------------------------------------------------------------

@dataclass(frozen=True)
class ShopifySettings:
    DROPSHIP_SHOPIFY_STORE_DOMAIN: str
    DROPSHIP_SHOPIFY_ADMIN_TOKEN: str
    DROPSHIP_SHOPIFY_API_VERSION: str = "2024-07"
    DROPSHIP_OUT_DIR: str = "export/dropship"


@lru_cache(maxsize=1)
def get_shopify_settings() -> ShopifySettings:
    _load_dotenv()
    return ShopifySettings(
        DROPSHIP_SHOPIFY_STORE_DOMAIN=_env(
            "DROPSHIP_SHOPIFY_STORE_DOMAIN", required=True
        ) or "",
        DROPSHIP_SHOPIFY_ADMIN_TOKEN=_env(
            "DROPSHIP_SHOPIFY_ADMIN_TOKEN", required=True
        ) or "",
        DROPSHIP_SHOPIFY_API_VERSION=_env(
            "DROPSHIP_SHOPIFY_API_VERSION", "2024-07"
        ) or "2024-07",
        DROPSHIP_OUT_DIR=_env(
            "DROPSHIP_OUT_DIR", "export/dropship"
        ) or "export/dropship",
    )
