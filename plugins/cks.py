import datetime
import logging

import requests
from plugins.base import register

DAY_FLAGS = {
    0: "monday",
    1: "tuesday",
    2: "wednesday",
    3: "thursday",
    4: "friday",
    5: "saturday",
    6: "sunday",
}

PRODUCT_DIMENSION_FIELDS = {
    "Brand": "brandId",
    "Category": "categoryId",
    "Product": "productId",
    "Strain": "strainId",
    "Vendor": "vendorId",
}


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


def build_package_id_map(inventory_items):
    package_ids = {}
    for item in inventory_items or []:
        pid = item.get("productId")
        package_id = item.get("packageId") or item.get("externalPackageId")
        if pid is not None and package_id:
            package_ids.setdefault(pid, str(package_id))
    return package_ids


def fetch_dutchie_deals(location_key):
    url = "https://api.pos.dutchie.com/discounts/v2/list"
    logging.info("Fetching deals from Dutchie POS discounts API")
    resp = requests.get(
        url,
        auth=(location_key, ""),
        headers={"Accept": "application/json"},
        params={
            "includeInactive": True,
            "includeInclusionExclusionData": True,
        },
        timeout=120,
    )
    resp.raise_for_status()
    deals = resp.json()
    logging.info(f"Fetched {len(deals)} deals from Dutchie POS")
    return deals


def fetch_location_id(location_key):
    resp = requests.get(
        "https://api.pos.dutchie.com/whoami",
        auth=(location_key, ""),
        headers={"Accept": "application/json"},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json().get("locationId")


def build_tag_map(products, inventory_items):
    tag_map = {}
    for p in products or []:
        pid = p.get("productId")
        if pid is None:
            continue
        tag_ids = {t.get("tagId") for t in (p.get("tags") or []) if t.get("tagId") is not None}
        if tag_ids:
            tag_map.setdefault(pid, set()).update(tag_ids)
    for item in inventory_items or []:
        pid = item.get("productId")
        if pid is None:
            continue
        tag_ids = {t.get("tagId") for t in (item.get("tags") or []) if t.get("tagId") is not None}
        if tag_ids:
            tag_map.setdefault(pid, set()).update(tag_ids)
    return tag_map


def restriction_ids(restriction):
    if not restriction:
        return set()
    return {
        i
        for i in (restriction.get("restrictionIds") or restriction.get("ids") or [])
        if i is not None
    }


def product_matches_restriction(product, rtype, restriction, tag_ids=None):
    ids = restriction_ids(restriction)
    if not ids:
        return True
    is_exclusion = bool(restriction.get("isExclusion"))

    if rtype == "Weight":
        net_weight = product.get("netWeight")
        try:
            net_weight = float(net_weight)
        except (TypeError, ValueError):
            net_weight = None
        hit = False
        if net_weight is not None:
            hit = any(
                isinstance(i, (int, float)) and abs(net_weight - float(i)) < 1e-6 for i in ids
            )
        return not hit if is_exclusion else hit

    if rtype == "InventoryTag":
        hit = bool((tag_ids or set()) & ids)
        return not hit if is_exclusion else hit

    field = PRODUCT_DIMENSION_FIELDS.get(rtype)
    if field is None:
        return True
    value = product.get(field)
    hit = value in ids
    return not hit if is_exclusion else hit


def deal_applies_today(deal, today=None):
    today = today or datetime.date.today()
    flags = [deal.get(f) for f in DAY_FLAGS.values()]
    if all(f is None for f in flags):
        return True
    return deal.get(DAY_FLAGS[today.weekday()]) is True


def deal_applies_to_location(deal, location_id):
    locations = deal.get("locationRestrictions")
    if not locations:
        return True
    return location_id in locations


def deal_sale_price(deal, base_price):
    if base_price is None:
        return None
    try:
        base_price = float(base_price)
    except (TypeError, ValueError):
        return None
    if base_price <= 0:
        return None
    reward = deal.get("reward") or {}
    calc = (reward.get("calculationMethod") or "").upper()
    value = reward.get("discountValue")
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if calc == "PERCENT_OFF":
        return base_price * (1.0 - value)
    logging.warning(
        f"Deal {deal.get('id')} uses unsupported calculationMethod {calc}; "
        "flagged sale without price"
    )
    return None


def compute_sale_prices(deals, products, location_id, inventory_items=None, today=None):
    if not deals or not products:
        return {}
    today = today or datetime.date.today()
    tag_map = build_tag_map(products, inventory_items)
    product_by_id = {p["productId"]: p for p in products if p.get("productId") is not None}
    sale_prices = {}

    for deal in deals:
        if not deal.get("isActive"):
            continue
        if deal.get("isBundledDiscount"):
            continue
        if not deal_applies_today(deal, today):
            continue
        if not deal_applies_to_location(deal, location_id):
            continue

        restrictions = (deal.get("reward") or {}).get("restrictions") or {}
        candidates = None
        for rtype, restriction in restrictions.items():
            if rtype == "NoCannabis":
                continue
            matching = {
                p["productId"]
                for p in products
                if p.get("productId") is not None
                and product_matches_restriction(p, rtype, restriction, tag_map.get(p["productId"]))
            }
            candidates = matching if candidates is None else (candidates & matching)
        if candidates is None:
            candidates = set(product_by_id)

        for pid in candidates:
            raw = product_by_id.get(pid)
            if raw is None:
                continue
            price = deal_sale_price(deal, raw.get("recPrice"))
            if price is None:
                if pid not in sale_prices:
                    sale_prices[pid] = None
                continue
            existing = sale_prices.get(pid)
            if existing is None or price < existing:
                sale_prices[pid] = price

    return sale_prices


class CksPlugin:
    @staticmethod
    def applies_to(customer):
        return "cks" in customer["name"].lower()

    def transform_articles(self, customer, articles, products=None):
        product_map = {p["productId"]: p for p in (products or [])}
        template_field = customer.get("template_field", "MISC_03")
        uses_derived_prices = (
            customer.get("list_price") == "BEFORE_PRICE"
            and customer.get("sale_price") == "AFTER_PRICE"
        )

        inventory_map = {}
        package_id_map = {}
        sale_prices = {}
        try:
            location_key = customer["creds"]["location_key"]
            inventory_items = fetch_dutchie_inventory(location_key)
            inventory_map = build_inventory_map(inventory_items)
            package_id_map = build_package_id_map(inventory_items)
            deals = fetch_dutchie_deals(location_key)
            location_id = fetch_location_id(location_key)
            sale_prices = compute_sale_prices(deals, products or [], location_id, inventory_items)
            logging.info(f"CksPlugin: sale prices for {len(sale_prices)} products")
        except Exception as e:
            logging.error(f"CksPlugin: failed to fetch inventory/deals: {e}")

        for article in articles:
            raw = product_map.get(int(article["articleId"]))
            if not raw:
                continue

            package_id = package_id_map.get(raw.get("productId"))
            if package_id is not None:
                article["articleId"] = str(package_id)

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

            sale = sale_prices.get(raw.get("productId"))
            if sale is not None:
                data[template_field] = "sale"
                data["SALE_PRICE"] = f"{sale:.2f}"

                try:
                    source_list_price = data.get("LIST_PRICE")
                    if uses_derived_prices and source_list_price is None:
                        source_list_price = raw.get("recPrice")
                    list_price = float(source_list_price or 0)
                    if list_price > 0:
                        # 6% Local Tax → $45.98 × 1.06 = $48.7388
                        # 15% Excise Tax → $48.7388 × 1.15 = $56.0496
                        # 7.75% Sales Tax → $56.0496 × 1.0775 = $60.3935                        
                        percent_off = round((1 - (sale / list_price)) * 100)
                        data["MISC_02"] = f"{percent_off}% OFF"
                        adjusted_list_price = list_price * 1.06 * 1.15 * 1.0775
                        data["BEFORE_PRICE"] = f"{adjusted_list_price:.2f}"
                        final_clearance_price = (list_price * (1 - percent_off / 100)) * 1.06 * 1.15 * 1.0775
                        data["AFTER_PRICE"] = f"{final_clearance_price:.2f}"
                        if uses_derived_prices:
                            data["LIST_PRICE"] = data["BEFORE_PRICE"]
                            data["SALE_PRICE"] = data["AFTER_PRICE"]
                except (ValueError, TypeError):
                    pass
            else:
                data[template_field] = "default"

        logging.info(f"CksPlugin: transformed {len(articles)} articles")
        return articles


register(CksPlugin)
