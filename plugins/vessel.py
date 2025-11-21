import logging
from .base import BasePlugin, register
from decimal import Decimal


class VesselPlugin(BasePlugin):
    """Example plugin for customer named 'vessel'.

    This plugin demonstrates per-customer/custom-article logic by adjusting
    the article template and adding a marker field.
    """
    logging.debug("Initializing VesselPlugin")
    def applies_to(self, customer):
        return customer.get("name") == "vessel"

    def transform_articles(self, customer, articles):
        logging.debug(f"VesselPlugin transforming articles for customer {customer.get('name')}")
        for a in articles:
            # Example: set a custom field for tracking
            a.setdefault("data", {})
            #a["data"]["_plugin_applied"] = "vessel_plugin"
            # Example: modify template decision based on SALE_PRICE
            if a.get("data", {}).get("SALE_PRICE"):
                a["data"][customer.get("template_field", "MISC_03")] = "sale"
                a["data"]["MISC_02"] = "SAVE $"+str(Decimal(a["data"]["LIST_PRICE"]) - Decimal(a["data"]["SALE_PRICE"]))
        return articles


register(VesselPlugin())
