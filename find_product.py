#!/usr/bin/env python3
"""Debug script to find exact product names"""

import requests
import json

def fetch_dutchie_products(location_key):
    """Fetch all products from Dutchie POS API"""
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

products = fetch_dutchie_products(location_key)
print(f"Searching in {len(products)} products for 'Magic Berries'...\n")

# Find Magic Berries products
results = []
for p in products:
    name = p.get("productName", "").lower()
    if "magic" in name and "berrie" in name:
        results.append(p)

print(f"Found {len(results)} matching products:\n")

for p in results:
    pid = p.get("productId")
    name = p.get("productName")
    internal_name = p.get("internalName")
    brand = p.get("brandName")
    grams = p.get("productGrams")
    price = p.get("recPrice")
    
    print(f"Product ID: {pid}")
    print(f"  Name: {name}")
    print(f"  Internal Name: {internal_name}")
    print(f"  Brand: {brand}")
    print(f"  Grams: {grams}")
    print(f"  Price: ${price}")
    print()
