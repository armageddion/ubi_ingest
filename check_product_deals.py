#!/usr/bin/env python3
"""Script to check product deals from Dutchie"""

import requests
import json
import sys
import os
from plugins.cks import (
    fetch_dutchie_inventory,
    fetch_dutchie_deals,
    fetch_location_id,
    compute_sale_prices,
    deal_sale_price,
)
import logging

logging.basicConfig(level=logging.INFO)

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
    products = resp.json()
    return products


def check_product_in_customer(customer_name, location_key, product_search_term, timezone_str="UTC"):
    """Check a product in a specific customer"""
    print(f"\n{'='*60}")
    print(f"Checking {customer_name} (location_key: {location_key[:20]}...)")
    print(f"Timezone: {timezone_str}")
    print(f"{'='*60}")
    
    try:
        # Fetch data
        products = fetch_dutchie_products(location_key)
        inventory_items = fetch_dutchie_inventory(location_key)
        deals = fetch_dutchie_deals(location_key)
        location_id = fetch_location_id(location_key)
        
        print(f"Total products: {len(products)}")
        print(f"Total deals: {len(deals)}")
        
        # Find matching products
        matching_products = []
        for p in products:
            name = p.get("productName", "")
            internal_name = p.get("internalName", "")
            if product_search_term.lower() in name.lower() or product_search_term.lower() in internal_name.lower():
                matching_products.append(p)
        
        if not matching_products:
            print(f"❌ Product '{product_search_term}' not found in {customer_name}")
            return
        
        print(f"\n✅ Found {len(matching_products)} matching product(s):")
        
        # Compute sale prices with timezone support
        sale_prices = compute_sale_prices(deals, products, location_id, inventory_items, timezone_str=timezone_str)
        
        for product in matching_products:
            pid = product.get("productId")
            name = product.get("productName", "N/A")
            rec_price = product.get("recPrice", 0)
            
            sale_price = sale_prices.get(pid)
            
            print(f"\n  Product: {name}")
            print(f"  Product ID: {pid}")
            print(f"  Regular Price: ${rec_price:.2f}" if rec_price else "  Regular Price: N/A")
            
            if sale_price is not None:
                percent_off = round((1 - (sale_price / rec_price)) * 100) if rec_price else 0
                print(f"  Sale Price: ${sale_price:.2f}")
                print(f"  Discount: {percent_off}% OFF")
            else:
                print(f"  Sale Price: No sale currently")
            
            # Check which deals apply to this product
            print(f"  Applicable deals:")
            applicable_deals = []
            for deal in deals:
                if deal.get("id") in [d.get("id") for d in deals]:
                    deal_price = deal_sale_price(deal, rec_price)
                    if deal_price is not None:
                        # Check if deal applies
                        restrictions = (deal.get("reward") or {}).get("restrictions") or {}
                        # Simplified check - in real code we'd check all restrictions
                        applicable_deals.append(deal)
            
            if applicable_deals:
                for deal in applicable_deals:
                    deal_id = deal.get("id", "N/A")
                    deal_name = deal.get("name", "N/A")
                    is_active = deal.get("isActive", False)
                    reward = deal.get("reward", {})
                    discount_value = reward.get("discountValue", 0)
                    calc_method = reward.get("calculationMethod", "N/A")
                    
                    if calc_method == "PERCENT_OFF":
                        print(f"    - {deal_name}: {discount_value*100:.0f}% OFF (Active: {is_active})")
            else:
                print(f"    - No active deals")
    except Exception as e:
        print(f"❌ Error fetching data for {customer_name}: {e}")
        import traceback
        traceback.print_exc()


# Main execution
if __name__ == "__main__":
    product_search_term = "Seed Junky - Magic Berries - 7g"
    timezone_str = os.getenv("TIMEZONE", "UTC")
    
    # Check both CKS locations
    locations = {
        "cks_orcutt": "7590df726a4c4dff9ce6dcf66cc2c86c",
        "cks_cookies": "76a0db5d2e8c49ccb153ec58678072fe",
    }
    
    for customer_name, location_key in locations.items():
        check_product_in_customer(customer_name, location_key, product_search_term, timezone_str)
