import datetime
from unittest.mock import patch, MagicMock

from plugins.cks import (
    CksPlugin,
    compute_sale_prices,
    deal_applies_today,
    deal_applies_to_location,
    deal_sale_price,
    product_matches_restriction,
)

MONDAY = datetime.date(2026, 8, 3)
TUESDAY = datetime.date(2026, 8, 4)
SUNDAY = datetime.date(2026, 8, 2)


def monday_deal():
    return {
        "id": 318230,
        "isActive": True,
        "isBundledDiscount": False,
        "monday": True,
        "tuesday": False,
        "wednesday": False,
        "thursday": False,
        "friday": False,
        "saturday": False,
        "sunday": False,
        "locationRestrictions": [3919],
        "reward": {
            "calculationMethod": "PERCENT_OFF",
            "discountValue": 0.5,
            "restrictions": {"Brand": {"isExclusion": False, "restrictionIds": [10, 20]}},
        },
    }


def product(pid, brand_id=None, category_id=None, rec_price=None, net_weight=None):
    p = {"productId": pid, "brandId": brand_id, "recPrice": rec_price}
    if category_id is not None:
        p["categoryId"] = category_id
    if net_weight is not None:
        p["netWeight"] = net_weight
    return p


def test_deal_applies_today_monday_only():
    deal = monday_deal()
    assert deal_applies_today(deal, MONDAY) is True
    assert deal_applies_today(deal, TUESDAY) is False


def test_deal_applies_today_all_days_null():
    deal = monday_deal()
    for flag in (
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ):
        deal[flag] = None
    assert deal_applies_today(deal, MONDAY) is True
    assert deal_applies_today(deal, SUNDAY) is True


def test_deal_applies_to_location():
    deal = monday_deal()
    assert deal_applies_to_location(deal, 3919) is True
    assert deal_applies_to_location(deal, 1) is False
    deal["locationRestrictions"] = []
    assert deal_applies_to_location(deal, 1) is True


def test_product_matches_brand_inclusion():
    restriction = {"isExclusion": False, "restrictionIds": [10, 20]}
    assert product_matches_restriction(product(1, brand_id=10), "Brand", restriction) is True
    assert product_matches_restriction(product(2, brand_id=99), "Brand", restriction) is False
    assert product_matches_restriction(product(3), "Brand", restriction) is False


def test_product_matches_brand_exclusion():
    restriction = {"isExclusion": True, "restrictionIds": [10, 20]}
    assert product_matches_restriction(product(1, brand_id=10), "Brand", restriction) is False
    assert product_matches_restriction(product(2, brand_id=99), "Brand", restriction) is True
    assert product_matches_restriction(product(3), "Brand", restriction) is True


def test_product_matches_weight():
    restriction = {"isExclusion": False, "restrictionIds": [3.5]}
    assert product_matches_restriction(product(1, net_weight=3.5), "Weight", restriction) is True
    assert product_matches_restriction(product(2, net_weight=14.0), "Weight", restriction) is False


def test_product_matches_inventory_tag():
    restriction = {"isExclusion": False, "restrictionIds": [13073]}
    assert (
        product_matches_restriction(product(1), "InventoryTag", restriction, tag_ids={13073})
        is True
    )
    assert (
        product_matches_restriction(product(2), "InventoryTag", restriction, tag_ids={99999})
        is False
    )


def test_deal_sale_price_percent_off():
    deal = monday_deal()
    assert deal_sale_price(deal, 20.0) == 10.0
    assert deal_sale_price(deal, None) is None
    assert deal_sale_price(deal, 0.0) is None


def test_compute_sale_prices_monday_only():
    products = [
        product(1, brand_id=10, rec_price=20.0),
        product(2, brand_id=99, rec_price=20.0),
    ]
    on_monday = compute_sale_prices([monday_deal()], products, 3919, today=MONDAY)
    assert on_monday == {1: 10.0}

    on_tuesday = compute_sale_prices([monday_deal()], products, 3919, today=TUESDAY)
    assert on_tuesday == {}


def test_compute_sale_prices_location_filter():
    products = [product(1, brand_id=10, rec_price=20.0)]
    wrong_location = compute_sale_prices([monday_deal()], products, 1, today=MONDAY)
    assert wrong_location == {}


def test_compute_sale_prices_takes_lowest():
    every_day = {
        "id": 318213,
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
            "discountValue": 0.3,
            "restrictions": {"Category": {"isExclusion": True, "restrictionIds": [500]}},
        },
    }
    products = [product(1, brand_id=10, category_id=100, rec_price=20.0)]
    result = compute_sale_prices([monday_deal(), every_day], products, 3919, today=MONDAY)
    assert result[1] == 10.0


def test_transform_articles_marks_sale_and_price():
    products = [
        {
            "productId": 1,
            "brandId": 10,
            "brandName": "STIIIZY",
            "recPrice": 27.99,
            "categoryId": 41297,
        },
        {
            "productId": 2,
            "brandId": 99,
            "brandName": "Pacific Stone",
            "recPrice": 18.00,
            "categoryId": 41297,
        },
    ]
    articles = [
        {"articleId": "1", "data": {}},
        {"articleId": "2", "data": {}},
    ]
    customer = {
        "name": "cks_orcutt",
        "creds": {"location_key": "test_key"},
        "template_field": "MISC_03",
    }

    mock_deals = MagicMock(return_value=[monday_deal()])
    mock_inventory = MagicMock(return_value=[])
    mock_location = MagicMock(return_value=3919)

    with patch("plugins.cks.datetime.date") as mock_date, patch(
        "plugins.cks.fetch_dutchie_deals", mock_deals
    ), patch("plugins.cks.fetch_dutchie_inventory", mock_inventory), patch(
        "plugins.cks.fetch_location_id", mock_location
    ):
        mock_date.today.return_value = MONDAY
        result = CksPlugin().transform_articles(customer, articles, products=products)

    assert result[0]["data"]["MISC_03"] == "sale"
    assert result[0]["data"]["SALE_PRICE"] == "13.99"
    assert result[1]["data"]["MISC_03"] == "default"
    assert "SALE_PRICE" not in result[1]["data"]


def test_transform_articles_without_deals_sets_default():
    products = [product(1, brand_id=10, rec_price=20.0)]
    articles = [{"articleId": "1", "data": {}}]
    customer = {
        "name": "cks_orcutt",
        "creds": {"location_key": "test_key"},
        "template_field": "MISC_03",
    }

    with patch("plugins.cks.fetch_dutchie_deals", MagicMock(return_value=[])), patch(
        "plugins.cks.fetch_dutchie_inventory", MagicMock(return_value=[])
    ), patch("plugins.cks.fetch_location_id", MagicMock(return_value=3919)):
        result = CksPlugin().transform_articles(customer, articles, products=products)

    assert result[0]["data"]["MISC_03"] == "default"
