#!/usr/bin/env python3
"""Check Weed Demon OG 3.5g across both Dutchie locations"""

import requests
import os
import datetime
from plugins.cks import (
    fetch_dutchie_inventory,
    fetch_dutchie_deals,
    fetch_location_id,
    compute_sale_prices,
    deal_sale_price,
    deal_applies_today,
    deal_applies_to_location,
    product_matches_restriction,
    build_tag_map,
    build_package_id_map,
    get_date_in_timezone,
    DAY_FLAGS,
    DUTCHIE_TAX_MULTIPLIER,
)
import logging
import math

logging.basicConfig(level=logging.INFO)

def fetch_dutchie_products(location_key):
    url = "https://api.pos.dutchie.com/products"
    resp = requests.get(
        url,
        auth=(location_key, ""),
        headers={"Accept": "application/json"},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()


def check_deal_eligibility(product, deal, tag_ids=None):
    restrictions = (deal.get("reward") or {}).get("restrictions") or {}
    for rtype, restriction in restrictions.items():
        if rtype == "NoCannabis":
            continue
        if not product_matches_restriction(product, rtype, restriction, tag_ids):
            return False
    return True


def check_location(customer_name, location_key, timezone_str="America/Los_Angeles"):
    print(f"\n{'='*70}")
    print(f"  {customer_name}")
    print(f"  Location Key: {location_key}")
    print(f"{'='*70}")

    products = fetch_dutchie_products(location_key)
    inventory_items = fetch_dutchie_inventory(location_key)
    deals = fetch_dutchie_deals(location_key)
    location_id = fetch_location_id(location_key)
    tag_map = build_tag_map(products, inventory_items)
    package_id_map = build_package_id_map(inventory_items)

    print(f"Total products: {len(products)}")
    print(f"Total deals: {len(deals)}")
    print(f"Location ID: {location_id}")

    # Build reverse map: packageId -> productId for searching
    pkg_to_pid = {v: k for k, v in package_id_map.items()}
    target_pkg = "1A4060300043B99000023514"

    # Check if target packageId exists in this location
    if target_pkg in pkg_to_pid:
        print(f"  *** Package ID {target_pkg} FOUND in inventory (product ID: {pkg_to_pid[target_pkg]}) ***")

    matching = []
    for p in products:
        name = (p.get("productName") or "").lower()
        brand = (p.get("brandName") or "")
        internal = (p.get("internalName") or "").lower()
        upc = (p.get("upc") or "")
        sku = (p.get("sku") or "")
        pid = p.get("productId")

        # Check if this product's packageId matches our target
        product_pkg = package_id_map.get(pid)

        if ("weed demon" in name or "weed demon" in internal or
            "weed demon" in brand.lower() or "wavvy" in brand.lower()):
            matching.append(p)

        if "1A4060300043B99000023514" in (upc + sku + (product_pkg or "")):
            if p not in matching:
                matching.append(p)

    if not matching:
        print("  No matching products found.")
        return

    print(f"\n  Found {len(matching)} matching product(s):\n")

    today = get_date_in_timezone(timezone_str)

    for p in matching:
        pid = p.get("productId")
        name = p.get("productName")
        brand = p.get("brandName")
        internal = p.get("internalName")
        grams = p.get("productGrams")
        rec_price = p.get("recPrice", 0)
        upc = p.get("upc", "")
        sku = p.get("sku", "")
        category = p.get("category")
        strain = p.get("strain")
        is_active = p.get("isActive")
        package_id = package_id_map.get(pid, "")

        print(f"  Product ID: {pid}")
        print(f"  Name: {name}")
        print(f"  Internal Name: {internal}")
        print(f"  Brand: {brand}")
        print(f"  Grams: {grams}")
        print(f"  Category: {category}")
        print(f"  Strain: {strain}")
        print(f"  UPC: {upc or '(none)'}")
        print(f"  SKU: {sku or '(none)'}")
        print(f"  Package ID: {package_id or '(none)'}")
        print(f"  Regular Price: ${rec_price:.2f}" if rec_price else "  Regular Price: N/A")
        print(f"  Active: {is_active}")

        # Calculate tax-inclusive prices
        if rec_price:
            tax_inclusive = rec_price * DUTCHIE_TAX_MULTIPLIER
            print(f"  Tax-Inclusive Price: ${tax_inclusive:.2f} (before any discount)")

        # Find deals
        applicable_deals = []
        for deal in deals:
            if not deal.get("isActive"):
                continue
            if deal.get("isBundledDiscount"):
                continue
            if not deal_applies_today(deal, today):
                continue
            if not deal_applies_to_location(deal, location_id):
                continue

            if check_deal_eligibility(p, deal, tag_map.get(pid)):
                deal_price = deal_sale_price(deal, rec_price)
                if deal_price is not None:
                    applicable_deals.append((deal, deal_price))

        applicable_deals.sort(key=lambda x: x[1])

        if applicable_deals:
            best_deal, best_price = applicable_deals[0]
            percent_off = round((1 - (best_price / rec_price)) * 100) if rec_price else 0
            discount_value = best_deal.get("reward", {}).get("discountValue", 0)
            deal_name = best_deal.get("name", "N/A")

            print(f"\n  BEST Sale Price: ${best_price:.2f} ({percent_off}% OFF)")
            print(f"  Best Deal: {deal_name} ({discount_value*100:.0f}% configured)")

            # Show tax-adjusted final price
            final_price = math.ceil(best_price * DUTCHIE_TAX_MULTIPLIER)
            print(f"  Final Price (tax-incl): ${final_price}")

            print(f"\n  ALL applicable deals ({len(applicable_deals)} total):")
            for deal, price in applicable_deals:
                deal_name = deal.get("name", "N/A")
                discount_value = deal.get("reward", {}).get("discountValue", 0)
                pct = round((1 - (price / rec_price)) * 100) if rec_price else 0
                final = math.ceil(price * DUTCHIE_TAX_MULTIPLIER)
                print(f"    - {deal_name}: {discount_value*100:.0f}% OFF -> ${price:.2f} (tax-incl: ${final})")
        else:
            print("\n  No active deals apply to this product today.")

        print()


if __name__ == "__main__":
    locations = {
        "cks_orcutt": ("7590df726a4c4dff9ce6dcf66cc2c86c", "America/Los_Angeles"),
        "cks_cookies": ("76a0db5d2e8c49ccb153ec58678072fe", "America/Los_Angeles"),
    }

    for name, (key, tz) in locations.items():
        check_location(name, key, tz)
