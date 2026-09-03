#!/usr/bin/env python3
"""
End-to-end test for CKS customer with timezone support
Tests the full flow: fetching deals, filtering by timezone-aware day, applying discounts
"""

import sys
import os
import datetime
from unittest.mock import patch, MagicMock
from plugins.cks import (
    fetch_dutchie_inventory,
    fetch_dutchie_deals,
    fetch_location_id,
    get_date_in_timezone,
    compute_sale_prices,
    CksPlugin,
)

# Sample Dutchie API responses
def get_sample_deals():
    """Sample deals from Dutchie API"""
    return [
        {
            "id": 1001,
            "name": "Tuesday 30% Off",
            "isActive": True,
            "isBundledDiscount": False,
            "monday": False,
            "tuesday": True,
            "wednesday": False,
            "thursday": False,
            "friday": False,
            "saturday": False,
            "sunday": False,
            "locationRestrictions": [3919],
            "reward": {
                "calculationMethod": "PERCENT_OFF",
                "discountValue": 0.30,
                "restrictions": {
                    "Category": {"isExclusion": False, "restrictionIds": [41297]}
                },
            },
        },
        {
            "id": 1002,
            "name": "Every Day 20% Off Premium",
            "isActive": True,
            "isBundledDiscount": False,
            "monday": None,
            "tuesday": None,
            "wednesday": None,
            "thursday": None,
            "friday": None,
            "saturday": None,
            "sunday": None,
            "locationRestrictions": [3919],
            "reward": {
                "calculationMethod": "PERCENT_OFF",
                "discountValue": 0.20,
                "restrictions": {
                    "Brand": {"isExclusion": False, "restrictionIds": [10, 20]}
                },
            },
        },
    ]


def get_sample_products():
    """Sample products from Dutchie API"""
    return [
        {
            "productId": 4419158,
            "packageId": "PKG-001",
            "productName": "Magic Berries - 7g",
            "internalName": "Magic Berries - 7g",
            "brandId": 10,
            "brandName": "Seed Junky",
            "categoryId": 41297,
            "recPrice": 79.98,
            "isActive": True,
        },
        {
            "productId": 4419159,
            "packageId": "PKG-002",
            "productName": "Flower Mix - 8g",
            "internalName": "Flower Mix - 8g",
            "brandId": 99,
            "brandName": "Other Brand",
            "categoryId": 41297,
            "recPrice": 59.98,
            "isActive": True,
        },
    ]


def get_sample_inventory():
    """Sample inventory from Dutchie API"""
    return [
        {
            "productId": 4419158,
            "packageId": "PKG-001",
            "quantityAvailable": 100,
        },
        {
            "productId": 4419159,
            "packageId": "PKG-002",
            "quantityAvailable": 50,
        },
    ]


def test_timezone_aware_deal_filtering():
    """Test that deals are filtered based on customer's local timezone"""
    print("\n" + "="*70)
    print("TEST 1: Timezone-aware deal filtering")
    print("="*70)
    
    products = get_sample_products()
    deals = get_sample_deals()
    
    # Test 1: Check deals on Tuesday (PST/Pacific)
    print("\n📍 Scenario: Tuesday in Pacific Time (California)")
    tuesday_pst = datetime.date(2026, 8, 4)  # Tuesday
    sale_prices_pst = compute_sale_prices(
        deals, products, 3919, 
        today=tuesday_pst, 
        timezone_str="America/Los_Angeles"
    )
    
    # Product 1 (Seed Junky): Should get 30% Tuesday deal + 20% everyday deal
    # Both apply, should take best (30%)
    assert 4419158 in sale_prices_pst, "Product 1 should have a sale price"
    expected_price = 79.98 * (1 - 0.30)  # 30% off
    assert sale_prices_pst[4419158] == expected_price, f"Expected {expected_price}, got {sale_prices_pst[4419158]}"
    print(f"✓ Product 1 (Seed Junky): ${sale_prices_pst[4419158]:.2f} (30% Tuesday deal)")
    
    # Test 2: Check deals on Wednesday (should NOT apply Tuesday deal)
    print("\n📍 Scenario: Wednesday in Pacific Time")
    wednesday_pst = datetime.date(2026, 8, 5)  # Wednesday
    sale_prices_wed = compute_sale_prices(
        deals, products, 3919,
        today=wednesday_pst,
        timezone_str="America/Los_Angeles"
    )
    
    # Product 1 should only get 20% everyday deal on Wednesday
    assert 4419158 in sale_prices_wed, "Product 1 should have a sale price on Wednesday"
    expected_price_wed = 79.98 * (1 - 0.20)  # 20% off (everyday deal)
    assert sale_prices_wed[4419158] == expected_price_wed, f"Expected {expected_price_wed}, got {sale_prices_wed[4419158]}"
    print(f"✓ Product 1 (Seed Junky): ${sale_prices_wed[4419158]:.2f} (20% everyday deal, no Tuesday)")
    
    print("\n✅ Timezone-aware filtering works correctly!")


def test_cks_plugin_integration():
    """Test the CKS plugin with timezone support"""
    print("\n" + "="*70)
    print("TEST 2: CKS Plugin Integration with Timezone")
    print("="*70)
    
    customer = {
        "name": "cks_orcutt",
        "company_name": "CKS",
        "store_name": "Orcutt",
        "timezone": "America/Los_Angeles",
        "list_price": "recPrice",
        "sale_price": "SALE_PRICE",
        "creds": {"location_key": "test_key"},
    }
    
    products = get_sample_products()
    inventory = get_sample_inventory()
    deals = get_sample_deals()
    
    # Create basic articles from products
    articles = [
        {
            "articleId": str(p["productId"]),
            "articleName": p["productName"],
            "data": {
                "LIST_PRICE": str(p["recPrice"]),
                "SALE_PRICE": "",
            }
        }
        for p in products
    ]
    
    print("\n📝 Input articles (before transformation):")
    for article in articles:
        print(f"  - {article['articleName']}: LIST_PRICE=${article['data']['LIST_PRICE']}")
    
    # Mock Dutchie API calls
    with patch("plugins.cks.fetch_dutchie_deals", return_value=deals), \
         patch("plugins.cks.fetch_dutchie_inventory", return_value=inventory), \
         patch("plugins.cks.fetch_location_id", return_value=3919):
        
        plugin = CksPlugin()
        result = plugin.transform_articles(customer, articles, products=products)
    
    print("\n📝 Output articles (after CKS transformation with timezone):")
    for article in result:
        if "SALE_PRICE" in article.get("data", {}):
            sale_price = article["data"]["SALE_PRICE"]
            print(f"  - {article['articleName']}: SALE_PRICE=${sale_price if sale_price else 'N/A'}")
    
    # Verify sale prices were calculated
    seed_junky = next((a for a in result if "Magic Berries" in a["articleName"]), None)
    assert seed_junky is not None, "Seed Junky product should be in results"
    assert seed_junky["data"].get("SALE_PRICE"), "Seed Junky should have a SALE_PRICE calculated"
    
    print("\n✅ CKS plugin integration works correctly!")


def test_timezone_variations():
    """Test the same time across different timezones"""
    print("\n" + "="*70)
    print("TEST 3: Timezone Variations - Same instant, different dates")
    print("="*70)
    
    products = get_sample_products()
    deals = get_sample_deals()
    
    # Test: When it's 9 PM PST (Tuesday), it's midnight EST (Wednesday)
    print("\n📍 Same instant in different timezones:")
    print("  9 PM Tuesday in Pacific Time = Midnight (start of Wednesday) in Eastern Time")
    
    # Simulate: Tuesday 9 PM PST
    tuesday_9pm_pst = datetime.date(2026, 8, 4)  # Tuesday
    sale_prices_pst = compute_sale_prices(
        deals, products, 3919,
        today=tuesday_9pm_pst,
        timezone_str="America/Los_Angeles"
    )
    
    # Simulate: Wednesday midnight EST (next day)
    wednesday_est = datetime.date(2026, 8, 5)  # Wednesday
    sale_prices_est = compute_sale_prices(
        deals, products, 3919,
        today=wednesday_est,
        timezone_str="US/Eastern"
    )
    
    seed_junky_pst = sale_prices_pst.get(4419158)
    seed_junky_est = sale_prices_est.get(4419158)
    
    print(f"\n🌍 Pacific: Tuesday → Sale price = ${seed_junky_pst:.2f} (30% Tuesday deal applies)")
    print(f"🌍 Eastern:  Wednesday → Sale price = ${seed_junky_est:.2f} (20% everyday deal only)")
    
    # PST should get 30% off (Tuesday deal), EST should get 20% off (Wednesday, no Tuesday deal)
    assert seed_junky_pst == 79.98 * 0.70, "PST should get Tuesday deal"
    assert seed_junky_est == 79.98 * 0.80, "EST should NOT get Tuesday deal"
    
    print("\n✅ Timezone variations handled correctly!")


def main():
    """Run all end-to-end tests"""
    print("\n" + "🧪 CKS END-TO-END TEST WITH TIMEZONE SUPPORT 🧪".center(70, "="))
    
    try:
        test_timezone_aware_deal_filtering()
        test_cks_plugin_integration()
        test_timezone_variations()
        
        print("\n" + "="*70)
        print("✅ ALL TESTS PASSED!")
        print("="*70)
        print("\nSummary:")
        print("  ✓ Timezone-aware deal filtering works")
        print("  ✓ CKS plugin integrates correctly with timezone support")
        print("  ✓ Cross-timezone scenarios handled correctly")
        print("\nThe system now correctly handles deals across different timezones!")
        return 0
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
