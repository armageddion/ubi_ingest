import logging
import requests
from plugins.base import register


def fetch_dutchie_inventory(location_key):
    url = "https://api.pos.dutchie.com/reporting/inventory"
    logging.info("Fetching inventory from Dutchie POS reporting API")
    resp = requests.get(
        url,
        auth=(location_key, ""),
        headers={"Accept": "application/json"},
        timeout=120,
    )
    resp.raise_for_status()
    items = resp.json()
    logging.info(f"Fetched {len(items)} inventory items from Dutchie POS")
    return items


def build_inventory_map(inventory_items):
    inventory = {}
    for item in inventory_items:
        pid = str(item.get("productId", ""))
        qty = item.get("quantityAvailable")
        if pid and qty is not None:
            inventory[pid] = str(qty)
    return inventory


class CksPlugin:
    @staticmethod
    def applies_to(customer):
        return "cks" in customer["name"].lower()

    def transform_articles(self, customer, articles, products=None):
        product_map = {p["productId"]: p for p in (products or [])}

        inventory_map = {}
        try:
            location_key = customer["creds"]["location_key"]
            inventory_items = fetch_dutchie_inventory(location_key)
            inventory_map = build_inventory_map(inventory_items)
        except Exception as e:
            logging.error(f"CksPlugin: failed to fetch inventory: {e}")

        for article in articles:
            raw = product_map.get(int(article["articleId"]))
            if not raw:
                continue

            data = article["data"]

            for field in ("LIST_PRICE", "SALE_PRICE", "CLEARANCE_PRICE"):
                val = data.get(field)
                if val is not None:
                    try:
                        data[field] = f"{float(val):.2f}"
                    except (ValueError, TypeError):
                        pass

            weight = data.get("WEIGHT")
            unit = data.get("WEIGHT_UNIT")
            if weight and unit:
                data["WEIGHT"] = f"{weight}{unit}"
            data.pop("WEIGHT_UNIT", None)

            category = raw.get("category")
            if category:
                data["CATEGORY"] = category

            strain_type = raw.get("strainType")
            if strain_type:
                data["SUBCATEGORY"] = strain_type

            thc = raw.get("thcContent")
            thc_unit = raw.get("thcContentUnit")
            if thc is not None and thc_unit:
                data["THC"] = f"{thc}{thc_unit}"
            elif thc is not None:
                data["THC"] = thc

            cbd = raw.get("cbdContent")
            cbd_unit = raw.get("cbdContentUnit")
            if cbd is not None and cbd_unit:
                data["CBD"] = f"{cbd}{cbd_unit}"
            elif cbd is not None:
                data["CBD"] = cbd

            strain = raw.get("strain")
            if strain:
                data["PRODUCT_NAME"] = strain

            pid = str(raw.get("productId", ""))
            if pid in inventory_map:
                data["INVENTORY"] = inventory_map[pid]

        logging.info(f"CksPlugin: transformed {len(articles)} articles")
        return articles


register(CksPlugin)
