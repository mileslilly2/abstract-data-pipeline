import requests
import json
import os

APP_KEY = os.getenv("ALIEXPRESS_APP_KEY")
TRACKING_ID = os.getenv("ALIEXPRESS_TRACKING_ID")

URL = "https://gw.api.alibaba.com/openapi/param2/2/portals.open/api.listPromotionProductV2"

params = {
    "app_key": APP_KEY,
    "tracking_id": TRACKING_ID,
    "keywords": "retro usb controller",
    "page_no": 1,
    "page_size": 20,
    "target_currency": "USD",
    "target_language": "EN",
}

r = requests.get(URL, params=params, timeout=30)

print("STATUS:", r.status_code)
print(json.dumps(r.json(), indent=2))
