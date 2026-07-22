import logging
from decimal import Decimal, DecimalException
from plugins.base import register


class VesselPlugin:
    @staticmethod
    def applies_to(customer):
        return customer.get("name", "").lower() == "vessel"

    def transform_articles(self, customer, articles):
        template_field = customer.get("template_field", "MISC_03")
        for article in articles:
            data = article.get("data", {})
            sale_price = data.get("SALE_PRICE")
            list_price = data.get("LIST_PRICE")
            if sale_price:
                data[template_field] = "sale"
                if list_price:
                    try:
                        savings = Decimal(str(list_price)) - Decimal(str(sale_price))
                        data["MISC_02"] = f"SAVE ${savings}"
                    except (DecimalException, ValueError):
                        logging.warning(f"Could not compute savings for LIST_PRICE={list_price}, SALE_PRICE={sale_price}")
            else:
                data[template_field] = "default"
        return articles


register(VesselPlugin)
