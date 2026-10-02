import json
import logging
import os
import tempfile
import threading
from datetime import datetime, timedelta, timezone

import requests

SQUARE_API_URL = "https://connect.squareup.com/v2"
SQUARE_TOKEN_URL = "https://connect.squareup.com/oauth2/token"
SQUARE_VERSION = "2025-10-16"
REFRESH_WINDOW = timedelta(days=7)

_token_lock = threading.Lock()


def _token_path():
    return os.getenv("SQUARE_TOKEN_FILE", "square_tokens.json").strip()


def _parse_expiry(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _load_tokens(path):
    with open(path, "r", encoding="utf-8") as token_file:
        return json.load(token_file)


def _save_tokens(path, tokens):
    directory = os.path.dirname(os.path.abspath(path))
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".square_tokens.")
    with os.fdopen(fd, "w", encoding="utf-8") as tmp_file:
        json.dump(tokens, tmp_file, indent=2)
    os.chmod(tmp_path, 0o600)
    os.replace(tmp_path, path)


def get_access_token(merchant_id, now=None):
    """Return a valid access token for the merchant, refreshing it when it
    expires within REFRESH_WINDOW. The refreshed token is written back."""
    now = now or datetime.now(timezone.utc)
    path = _token_path()
    with _token_lock:
        tokens = _load_tokens(path)
        token = tokens.get(merchant_id)
        if not token:
            raise RuntimeError(f"No Square token stored for merchant {merchant_id}")
        expires_at = token.get("expires_at")
        if expires_at and _parse_expiry(expires_at) - now > REFRESH_WINDOW:
            return token["access_token"]

        logging.info(f"Refreshing Square access token for merchant {merchant_id}")
        response = requests.post(
            SQUARE_TOKEN_URL,
            json={
                "client_id": os.getenv("SQUARE_APPLICATION_ID", "").strip(),
                "client_secret": os.getenv("SQUARE_APPLICATION_SECRET", "").strip(),
                "grant_type": "refresh_token",
                "refresh_token": token["refresh_token"],
            },
            timeout=30,
        )
        if not response.ok:
            logging.error(
                f"Square token refresh failed: {response.status_code} {response.text}"
            )
            response.raise_for_status()
        refreshed = response.json()
        token["access_token"] = refreshed["access_token"]
        token["expires_at"] = refreshed.get("expires_at", token.get("expires_at"))
        if refreshed.get("refresh_token"):
            token["refresh_token"] = refreshed["refresh_token"]
        tokens[merchant_id] = token
        _save_tokens(path, tokens)
        return token["access_token"]


def _available_at(obj, location_id):
    """Whether a catalog item or variation is sold at the location."""
    if obj.get("is_deleted"):
        return False
    if location_id in (obj.get("absent_at_location_ids") or []):
        return False
    if obj.get("present_at_all_locations"):
        return True
    return location_id in (obj.get("present_at_location_ids") or [])


def _dollars(money):
    if not money or money.get("amount") is None:
        return None
    return money["amount"] / 100


def catalog_to_products(objects, location_id):
    """Flatten catalog ITEM objects into one product dict per variation sold
    at the location. Location price overrides win over the base price."""
    categories = {
        o["id"]: o.get("category_data", {}).get("name")
        for o in objects
        if o.get("type") == "CATEGORY"
    }
    products = []
    for item in objects:
        if item.get("type") != "ITEM" or not _available_at(item, location_id):
            continue
        item_data = item.get("item_data", {})
        if item_data.get("is_archived"):
            continue
        category_id = (item_data.get("reporting_category") or {}).get("id") or item_data.get(
            "category_id"
        )
        variations = [
            v for v in item_data.get("variations", []) if _available_at(v, location_id)
        ]
        for variation in variations:
            var_data = variation.get("item_variation_data", {})
            price_money = var_data.get("price_money")
            for override in var_data.get("location_overrides") or []:
                if override.get("location_id") == location_id and override.get(
                    "price_override_money"
                ):
                    price_money = override["price_override_money"]
            name = item_data.get("name", "")
            variation_name = var_data.get("name")
            if len(item_data.get("variations", [])) > 1 and variation_name:
                name = f"{name} - {variation_name}"
            products.append(
                {
                    "variationId": variation["id"],
                    "itemId": item["id"],
                    "itemName": item_data.get("name"),
                    "productName": name,
                    "variationName": variation_name,
                    "description": item_data.get("description"),
                    "sku": var_data.get("sku"),
                    "upc": var_data.get("upc"),
                    "price": _dollars(price_money),
                    "categoryName": categories.get(category_id),
                }
            )
    return products


def fetch_square(customer_name, merchant_id, location_id):
    logging.info(f"Fetching catalog from Square for {customer_name}")
    headers = {
        "Authorization": f"Bearer {get_access_token(merchant_id)}",
        "Square-Version": SQUARE_VERSION,
    }
    objects, cursor = [], None
    while True:
        params = {"types": "ITEM,CATEGORY"}
        if cursor:
            params["cursor"] = cursor
        resp = requests.get(
            f"{SQUARE_API_URL}/catalog/list", headers=headers, params=params, timeout=60
        )
        resp.raise_for_status()
        body = resp.json()
        objects += body.get("objects", [])
        cursor = body.get("cursor")
        if not cursor:
            break
    products = catalog_to_products(objects, location_id)
    logging.info(
        f"Fetched {len(objects)} catalog objects from Square; "
        f"{len(products)} variations sold at {location_id}"
    )
    return products, None
