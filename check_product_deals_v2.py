#!/usr/bin/env python3
"""Enhanced script to check product deals from Dutchie"""

import requests
import json
import sys
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
    get_date_in_timezone,
    DAY_FLAGS,
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


def check_deal_eligibility(product, deal, tag_ids=None):
    """Check if a deal applies to a product based on restrictions"""
    restrictions = (deal.get("reward") or {}).get("restrictions") or {}
    
    for rtype, restriction in restrictions.items():
        if rtype == "NoCannabis":
            continue
        if not product_matches_restriction(product, rtype, restriction, tag_ids):
            return False
    return True


def check_product_in_customer(customer_name, location_key, product_search_term, timezone_str="UTC"):
    """Check a product in a specific customer"""
    print(f"\n{'='*70}")
    print(f"Checking {customer_name} (location_key: {location_key[:20]}...)")
    print(f"Timezone: {timezone_str}")
    print(f"{'='*70}")
    
    try:
        # Fetch data
        products = fetch_dutchie_products(location_key)
        inventory_items = fetch_dutchie_inventory(location_key)
        deals = fetch_dutchie_deals(location_key)
        location_id = fetch_location_id(location_key)
        tag_map = build_tag_map(products, inventory_items)
        
        print(f"Total products: {len(products)}")
        print(f"Total deals: {len(deals)}")
        
        # Find matching products - look for "Seed Junky" or product name containing search term
        matching_products = []
        for p in products:
            name = p.get("productName", "")
            internal_name = p.get("internalName", "")
            brand_name = p.get("brandName", "")
            product_grams = p.get("productGrams", "")
            
            # Build full product description
            full_desc = f"{brand_name} - {name} - {product_grams}g" if product_grams else f"{brand_name} - {name}"
            
            # Check if it matches search term or contains "Magic Berries" and brand is "Seed Junky"
            if (product_search_term.lower() in full_desc.lower() or 
                (product_search_term.lower() in name.lower() and "Seed Junky" in brand_name)):
                matching_products.append((full_desc, p))
        
        if not matching_products:
            print(f"❌ Product '{product_search_term}' not found in {customer_name}")
            return
        
        print(f"\n✅ Found {len(matching_products)} matching product(s):\n")
        
        today = get_date_in_timezone(timezone_str)
        current_day = DAY_FLAGS[today.weekday()]
        
        for full_desc, product in matching_products:
            pid = product.get("productId")
            rec_price = product.get("recPrice", 0)
            
            print(f"  Product: {full_desc}")
            print(f"  Product ID: {pid}")
            print(f"  Regular Price: ${rec_price:.2f}" if rec_price else "  Regular Price: N/A")
            
            # Find which deals apply to this product
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
                
                # Check restrictions
                if check_deal_eligibility(product, deal, tag_map.get(pid)):
                    deal_price = deal_sale_price(deal, rec_price)
                    if deal_price is not None:
                        applicable_deals.append((deal, deal_price))
            
            # Sort by lowest price
            applicable_deals.sort(key=lambda x: x[1])
            
            if applicable_deals:
                best_deal, best_price = applicable_deals[0]
                percent_off = round((1 - (best_price / rec_price)) * 100) if rec_price else 0
                discount_value = best_deal.get("reward", {}).get("discountValue", 0)
                deal_name = best_deal.get("name", "N/A")
                
                print(f"  ✓ BEST Sale Price: ${best_price:.2f}")
                print(f"  ✓ Best Deal: {deal_name} ({discount_value*100:.0f}% OFF)")
                print(f"  ✓ Calculated Discount: {percent_off}% OFF")
                print(f"\n  All applicable deals ({len(applicable_deals)} total):")
                for deal, price in applicable_deals[:5]:  # Show top 5
                    deal_name = deal.get("name", "N/A")
                    discount_value = deal.get("reward", {}).get("discountValue", 0)
                    percent_off_for_deal = round((1 - (price / rec_price)) * 100) if rec_price else 0
                    print(f"    - {deal_name}: {discount_value*100:.0f}% OFF (Final: ${price:.2f})")
                if len(applicable_deals) > 5:
                    print(f"    ... and {len(applicable_deals) - 5} more deals")
            else:
                print(f"  ✗ No active deals apply to this product")
            
            print()
            
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
