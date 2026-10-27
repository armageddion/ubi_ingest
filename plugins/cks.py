import copy
import datetime
import logging
import math
from zoneinfo import ZoneInfo

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

# Fallback for customers with no {NAME}_TAX_RATES configured.
# Orcutt's compounding order: 6% local, 15% cannabis excise, 7.75% sales.
DUTCHIE_TAX_MULTIPLIER = 1.06 * 1.15 * 1.0775


def parse_tax_rates(raw):
    """Parse a comma-separated list of tax percentages into a list of floats.

    The order matters: the rates compound in the order given.
    """
    rates = []
    for part in str(raw or "").split(","):
        part = part.strip().rstrip("%").strip()
        if not part:
            continue
        try:
            rates.append(float(part))
        except ValueError:
            logging.warning(f"CksPlugin: ignoring unparsable tax rate '{part}'")
    return rates


def tax_multiplier(customer):
    """Compounding tax multiplier for a customer, e.g. 4,15,8.75 -> 1.30065."""
    rates = parse_tax_rates(customer.get("tax_rates"))
    if not rates:
        logging.warning(
            f"CksPlugin: no TAX_RATES for {customer.get('name')}; "
            f"falling back to {DUTCHIE_TAX_MULTIPLIER:.6f}"
        )
        return DUTCHIE_TAX_MULTIPLIER
    multiplier = 1.0
    for rate in rates:
        multiplier *= 1 + rate / 100
    return multiplier


def get_date_in_timezone(timezone_str="UTC"):
    """Get the current date in the specified timezone.
    
    Args:
        timezone_str: IANA timezone string (e.g., 'America/Los_Angeles', 'US/Eastern', 'UTC')
                     Defaults to UTC.
    
    Returns:
        datetime.date: The current date in the specified timezone.
    """
    try:
        tz = ZoneInfo(timezone_str)
    except Exception as e:
        logging.warning(f"Invalid timezone '{timezone_str}': {e}. Falling back to UTC.")
        tz = ZoneInfo("UTC")
    
    now = datetime.datetime.now(tz)
    return now.date()


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


def build_inventory_totals(inventory_items):
    totals = {}
    for item in inventory_items or []:
        pid = item.get("productId")
        qty = item.get("quantityAvailable")
        if pid is None or qty is None:
            continue
        try:
            totals[str(pid)] = totals.get(str(pid), 0) + float(qty)
        except (TypeError, ValueError):
            continue
    return {pid: str(qty) for pid, qty in totals.items()}


def build_package_eans_map(inventory_items):
    """Map productId -> every in-stock package ID (packageId and externalPackageId).

    A package ID claimed by several products is kept only on the lowest
    productId so a scan resolves to one article.
    """
    by_product = {}
    for item in inventory_items or []:
        pid = item.get("productId")
        if pid is None:
            continue
        ids = by_product.setdefault(pid, [])
        for key in ("packageId", "externalPackageId"):
            value = item.get(key)
            if value and str(value) not in ids:
                ids.append(str(value))

    owner = {}
    for pid in sorted(by_product):
        for package_id in by_product[pid]:
            owner.setdefault(package_id, pid)
    for pid, ids in by_product.items():
        kept = [i for i in ids if owner[i] == pid]
        if len(kept) != len(ids):
            logging.warning(
                f"CksPlugin: product {pid} shares package IDs "
                f"{sorted(set(ids) - set(kept))} with another product; not attached"
            )
        by_product[pid] = kept
    return by_product


def build_package_product_map(inventory_items):
    """Map every in-stock package ID to the product that owns it.

    Used to find which product a legacy package-keyed article belongs to; a
    package shared by several products goes to the lowest productId, matching
    build_package_eans_map.
    """
    owner = {}
    for item in sorted(
        (i for i in inventory_items or [] if i.get("productId") is not None),
        key=lambda i: i["productId"],
    ):
        for key in ("packageId", "externalPackageId"):
            value = item.get(key)
            if value:
                owner.setdefault(str(value), item["productId"])
    return owner


def fetch_labeled_article_ids(customer, page_size=500):
    """Return the IDs of SOLUM articles that currently have labels attached."""
    endpoint = customer["output_endpoint"]
    token_resp = requests.post(
        endpoint + "/common/api/v2/token",
        timeout=30,
        json={"username": customer["output_user"], "password": customer["output_pass"]},
    )
    token_resp.raise_for_status()
    token = token_resp.json()["responseMessage"]["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    labeled = set()
    page = 0
    while True:
        resp = requests.get(
            endpoint + "/common/api/v2/common/articles",
            headers=headers,
            params={
                "store": customer["store_name"],
                "company": customer["company_name"],
                "page": page,
                "size": page_size,
            },
            timeout=120,
        )
        resp.raise_for_status()
        articles = resp.json().get("articleList") or []
        labeled.update(a["articleId"] for a in articles if a.get("labelCount"))
        if len(articles) < page_size:
            return labeled
        page += 1


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


def deal_applies_today(deal, today=None, timezone_str=None):
    """Check if a deal applies to today's day of week.
    
    Args:
        deal: The deal object with day flags (monday, tuesday, etc.)
        today: Optional datetime.date to use instead of current date
        timezone_str: Optional IANA timezone string to determine current date (e.g., 'America/Los_Angeles')
                     Ignored if 'today' is provided. Defaults to UTC.
    
    Returns:
        bool: True if the deal applies to the current day or has no day restrictions.
    """
    if today is None:
        if timezone_str is None:
            timezone_str = "UTC"
        today = get_date_in_timezone(timezone_str)
    
    flags = [deal.get(f) for f in DAY_FLAGS.values()]
    if all(f is None for f in flags):
        return True
    return deal.get(DAY_FLAGS[today.weekday()]) is True


def _parse_window_datetime(wall_clock_value, utc_value):
    """Parse a Dutchie deal date window value into a datetime.

    Dutchie provides two mirrors of the same moment:

    - ``wallClockValidDateFrom/To``: naive, already expressed in the
      location's wall-clock (local) time
    - ``validDateFrom/To``: timezone-aware, UTC (ends with ``Z``)

    The wall-clock value is preferred; the UTC value is the fallback.
    Returns a naive datetime for wall-clock values and an aware (UTC)
    datetime for UTC values, or ``None`` if neither is present/parseable.
    """
    value = wall_clock_value or utc_value
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(str(value).strip())
    except ValueError:
        logging.warning(f"Could not parse Dutchie deal datetime: {value!r}")
        return None


def deal_applies_on_date(deal, today=None, timezone_str=None):
    """Check if a deal's valid date window covers the given date.

    Uses ``wallClockValidDateFrom/To`` when available (already local
    wall-clock), falling back to ``validDateFrom/To`` (UTC). Deals without
    any date window are always applicable.

    Args:
        deal: The deal object.
        today: Optional ``datetime.date`` to test. Defaults to the current
               date in ``timezone_str``.
        timezone_str: IANA timezone used to interpret wall-clock values and
                      the ``today`` boundary. Defaults to UTC.

    Returns:
        bool: True if ``today`` falls within ``[start, end]`` (inclusive).
    """
    if timezone_str is None:
        timezone_str = "UTC"
    if today is None:
        today = get_date_in_timezone(timezone_str)

    start_dt = _parse_window_datetime(
        deal.get("wallClockValidDateFrom"), deal.get("validDateFrom")
    )
    end_dt = _parse_window_datetime(
        deal.get("wallClockValidDateTo"), deal.get("validDateTo")
    )
    if start_dt is None and end_dt is None:
        return True

    tz = ZoneInfo(timezone_str)
    today_start = datetime.datetime.combine(today, datetime.time.min, tzinfo=tz)
    if start_dt is not None and start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=tz)
    if end_dt is not None and end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=tz)

    if start_dt is not None and today_start < start_dt:
        return False
    if end_dt is not None and today_start > end_dt:
        return False
    return True


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


def compute_sale_prices(deals, products, location_id, inventory_items=None, today=None, timezone_str=None):
    """Compute the best sale price for each product based on applicable deals.
    
    Args:
        deals: List of deal objects from Dutchie API
        products: List of product objects from Dutchie API
        location_id: The location ID to filter deals by
        inventory_items: Optional inventory items for tag mapping
        today: Optional datetime.date to use instead of current date
        timezone_str: Optional IANA timezone string to determine current date (e.g., 'America/Los_Angeles')
                     Ignored if 'today' is provided. Defaults to UTC.
    
    Returns:
        dict: Mapping of product ID to best sale price
    """
    if not deals or not products:
        return {}
    if timezone_str is None:
        timezone_str = "UTC"
    if today is None:
        today = get_date_in_timezone(timezone_str)
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
        if not deal_applies_on_date(deal, today, timezone_str):
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
        multiplier = tax_multiplier(customer)
        uses_derived_prices = (
            customer.get("list_price") == "BEFORE_PRICE"
            and customer.get("sale_price") == "AFTER_PRICE"
        )

        package_eans = bool(customer.get("package_eans"))
        inventory_map = {}
        package_id_map = {}
        package_eans_map = {}
        package_product_map = {}
        sale_prices = {}
        try:
            location_key = customer["creds"]["location_key"]
            timezone_str = customer.get("timezone", "UTC")
            inventory_items = fetch_dutchie_inventory(location_key)
            if package_eans:
                inventory_map = build_inventory_totals(inventory_items)
                package_eans_map = build_package_eans_map(inventory_items)
                package_product_map = build_package_product_map(inventory_items)
            else:
                inventory_map = build_inventory_map(inventory_items)
                package_id_map = build_package_id_map(inventory_items)
            deals = fetch_dutchie_deals(location_key)
            location_id = fetch_location_id(location_key)
            sale_prices = compute_sale_prices(deals, products or [], location_id, inventory_items, timezone_str=timezone_str)
            logging.info(f"CksPlugin: sale prices for {len(sale_prices)} products")
        except Exception as e:
            logging.error(f"CksPlugin: failed to fetch inventory/deals: {e}")

        for article in articles:
            raw = product_map.get(int(article["articleId"]))
            if not raw:
                continue

            if package_eans:
                article["articleId"] = str(raw["productId"])
                eans = package_eans_map.get(raw.get("productId"))
                if eans:
                    article["eans"] = eans
            else:
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

            try:
                source_list_price = data.get("LIST_PRICE")
                if source_list_price is None:
                    source_list_price = raw.get("recPrice")
                list_price = float(source_list_price or 0)
            except (ValueError, TypeError):
                list_price = 0

            if list_price > 0:
                adjusted_list_price = list_price * multiplier
                data["BEFORE_PRICE"] = f"{adjusted_list_price:.2f}"

                if sale is not None:
                    data[template_field] = "sale"
                    data["SALE_PRICE"] = f"{sale:.2f}"
                    percent_off = round((1 - (sale / list_price)) * 100)
                    data["MISC_02"] = f"{percent_off}% OFF"
                    final_price = sale * multiplier
                else:
                    data[template_field] = "default"
                    final_price = adjusted_list_price

                data["AFTER_PRICE"] = str(math.ceil(final_price))
                if uses_derived_prices:
                    data["LIST_PRICE"] = data["BEFORE_PRICE"]
                    data["SALE_PRICE"] = data["AFTER_PRICE"]
            else:
                data[template_field] = "default"
                logging.warning(
                    f"CksPlugin: product {raw.get('productId')} has no usable regular "
                    "price; BEFORE_PRICE/AFTER_PRICE left unset"
                )

        if package_eans:
            articles = articles + self._legacy_label_articles(
                customer, articles, package_product_map
            )

        logging.info(f"CksPlugin: transformed {len(articles)} articles")
        return articles

    @staticmethod
    def _legacy_label_articles(customer, articles, package_product_map):
        """Keep package-keyed articles updating while labels are still on them.

        Labels bound to the old package-ID articles would otherwise freeze once
        articleId becomes the productId. A copy of the product's article, without
        EANs, is emitted under each such ID until its labels are relinked.
        """
        try:
            labeled = fetch_labeled_article_ids(customer)
        except Exception as e:
            logging.error(f"CksPlugin: could not list labeled articles, skipping legacy copies: {e}")
            return []

        by_id = {a["articleId"]: a for a in articles}
        copies = []
        unmapped = 0
        for legacy_id in sorted(labeled):
            if legacy_id in by_id:
                continue
            product_id = package_product_map.get(legacy_id)
            source = by_id.get(str(product_id)) if product_id is not None else None
            if source is None:
                unmapped += 1
                continue
            legacy = copy.deepcopy(source)
            legacy["articleId"] = legacy_id
            legacy.pop("eans", None)
            copies.append(legacy)
        logging.info(
            f"CksPlugin: {len(copies)} legacy labeled articles kept in sync, "
            f"{unmapped} labeled articles match no in-stock package"
        )
        return copies


register(CksPlugin)
