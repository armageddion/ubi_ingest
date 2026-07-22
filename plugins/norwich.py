import logging
import datetime
from .base import BasePlugin, register
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation


class NorwichPlugin(BasePlugin):
    """Example plugin for customer named 'Norwich'.

    This plugin demonstrates per-customer/custom-article logic by adjusting
    the article template and adding a marker field.
    """
    logging.debug("Initializing NorwichPlugin")
    def applies_to(self, customer):
        return customer.get("name") == "norwich"

    def transform_articles(self, customer, articles):
        logging.debug(f"NorwichPlugin transforming articles for customer {customer.get('name')}")
        for a in articles:
            # Example: set a custom field for tracking
            a.setdefault("data", {})
            #a["data"]["_plugin_applied"] = "Norwich_plugin"
            # Ensure SALE_PRICE is stored with two decimal places when present
            data = a.get("data", {})
            sale_val = data.get("SALE_PRICE")
            if sale_val not in (None, ""):
                try:
                    dec = Decimal(str(sale_val).strip())
                    dec = dec.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
                    data["SALE_PRICE"] = str(dec)
                except (InvalidOperation, ValueError, TypeError) as e:
                    logging.warning(f"Failed to format SALE_PRICE for article {a.get('articleId')}: {e}")

            # Example: modify template decision based on SALE_PRICE and date range
            if data.get("SALE_PRICE"):
                start_date_str = data.get("START_DATE")
                end_date_str = data.get("END_DATE")

                if start_date_str and end_date_str:
                    try:
                        # Parse dates in MM/DD/YYYY format
                        start_date = datetime.datetime.strptime(start_date_str, "%m/%d/%Y").date()
                        end_date = datetime.datetime.strptime(end_date_str, "%m/%d/%Y").date()
                        today = datetime.datetime.now().date()

                        if start_date <= today <= end_date:
                            a["data"][customer.get("template_field", "MISC_03")] = "sale"
                    except (ValueError, TypeError) as e:
                        logging.warning(f"Failed to parse dates for article {a.get('articleId')}: {e}")
        return articles


register(NorwichPlugin())
