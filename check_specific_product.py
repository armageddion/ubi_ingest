#!/usr/bin/env python3
"""Check specific deal details for product 4419158"""

import requests
import json
import datetime
import os
from plugins.cks import (
    fetch_dutchie_inventory,
    fetch_dutchie_deals,
    fetch_location_id,
    deal_applies_today,
    deal_applies_to_location,
    product_matches_restriction,
    build_tag_map,
    deal_sale_price,
    get_date_in_timezone,
    DAY_FLAGS,
)

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

location_key = "7590df726a4c4dff9ce6dcf66cc2c86c"  # cks_orcutt
timezone_str = os.getenv("TIMEZONE", "UTC")

print("Fetching data...")
products = fetch_dutchie_products(location_key)
inventory = fetch_dutchie_inventory(location_key)
deals = fetch_dutchie_deals(location_key)
location_id = fetch_location_id(location_key)
tag_map = build_tag_map(products, inventory)

# Find product 4419158
target_pid = 4419158
product = next((p for p in products if p.get("productId") == target_pid), None)

if not product:
    print(f"Product {target_pid} not found")
    exit(1)

print(f"\n{'='*70}")
print(f"Product: {product.get('internalName')}")
print(f"Product ID: {target_pid}")
print(f"Brand: {product.get('brandName')}")
print(f"Regular Price: ${product.get('recPrice'):.2f}")
print(f"{'='*70}\n")

today = get_date_in_timezone(timezone_str)
current_day = DAY_FLAGS[today.weekday()]

print(f"Today is {today.strftime('%A, %B %d, %Y')} ({timezone_str})\n")

# Check each deal
print(f"Checking {len(deals)} deals:\n")

applicable_deals = []

for i, deal in enumerate(deals):
    deal_id = deal.get("id", "N/A")
    deal_name = deal.get("name", "N/A")
    is_active = deal.get("isActive", False)
    is_bundled = deal.get("isBundledDiscount", False)
    
    # Check basic conditions
    if not is_active:
        print(f"Deal {i+1}: {deal_name} - INACTIVE")
        continue
    
    if is_bundled:
        print(f"Deal {i+1}: {deal_name} - BUNDLED DISCOUNT (skipped)")
        continue
    
    # Check if applies today
    if not deal_applies_today(deal, today):
        day_flags = [deal.get(DAY_FLAGS[d]) for d in range(7)]
        print(f"Deal {i+1}: {deal_name} - Does NOT apply on {current_day} (days: {day_flags})")
        continue
    
    # Check location
    if not deal_applies_to_location(deal, location_id):
        locations = deal.get("locationRestrictions", [])
        print(f"Deal {i+1}: {deal_name} - Does NOT apply at location {location_id}")
        continue
    
    # Check restrictions
    reward = deal.get("reward", {})
    restrictions = reward.get("restrictions", {})
    tag_ids = tag_map.get(target_pid)
    
    matches_all = True
    restriction_check = []
    
    for rtype, restriction in restrictions.items():
        if rtype == "NoCannabis":
            continue
        
        matches = product_matches_restriction(product, rtype, restriction, tag_ids)
        restriction_check.append(f"{rtype}={matches}")
        
        if not matches:
            matches_all = False
    
    if not matches_all:
        print(f"Deal {i+1}: {deal_name} - Restrictions NOT met ({', '.join(restriction_check)})")
        continue
    
    # Calculate sale price
    discount_value = reward.get("discountValue", 0)
    calc_method = reward.get("calculationMethod", "")
    sale_price = deal_sale_price(deal, product.get("recPrice"))
    
    if sale_price is None:
        print(f"Deal {i+1}: {deal_name} - Could not calculate sale price")
        continue
    
    percent_off = round((1 - (sale_price / product.get("recPrice"))) * 100) if product.get("recPrice") else 0
    
    print(f"Deal {i+1}: ✓ {deal_name}")
    print(f"  Discount: {discount_value*100:.0f}% OFF ({calc_method})")
    print(f"  Sale Price: ${sale_price:.2f}")
    print(f"  Effective Discount: {percent_off}% OFF")
    
    applicable_deals.append({
        "name": deal_name,
        "discount": discount_value*100,
        "price": sale_price,
        "percent_off": percent_off,
        "id": deal_id
    })
    print()

print(f"\n{'='*70}")
print(f"SUMMARY:")
print(f"{'='*70}")
print(f"Total applicable deals: {len(applicable_deals)}\n")

if applicable_deals:
    # Sort by lowest price
    applicable_deals.sort(key=lambda x: x["price"])
    best = applicable_deals[0]
    
    print(f"✓ BEST DEAL: {best['name']}")
    print(f"  Discount: {best['discount']:.0f}% OFF")
    print(f"  Sale Price: ${best['price']:.2f}")
    print(f"  Effective Discount: {best['percent_off']}% OFF")
    print(f"\nCURRENT STATUS: {'Should be' if best['discount'] in [30, 50] else 'Unexpected discount'} on {best['discount']:.0f}% sale")
else:
    print("❌ No applicable deals found for this product")
