import logging
from plugins.base import register


class CksPlugin:
    @staticmethod
    def applies_to(customer):
        return "cks" in customer["name"].lower()

    def transform_articles(self, customer, articles, products=None):
        product_map = {p["productId"]: p for p in (products or [])}

        for article in articles:
            raw = product_map.get(int(article["articleId"]))
            if not raw:
                continue

            data = article["data"]

            for field in ("LIST_PRICE", "SALE_PRICE", "CLEARANCE_PRICE"):
                val = data.get(field)
                if val is not None:
                    try:
                        data[field] = f"{float(val):.2f}"
                    except (ValueError, TypeError):
                        pass

            weight = data.get("WEIGHT")
            unit = data.get("WEIGHT_UNIT")
            if weight and unit:
                data["WEIGHT"] = f"{weight}{unit}"
            data.pop("WEIGHT_UNIT", None)

            strain_type = raw.get("strainType")
            if strain_type:
                data["SUBCATEGORY"] = strain_type

            thc = raw.get("thcContent")
            thc_unit = raw.get("thcContentUnit")
            if thc is not None and thc_unit:
                data["THC"] = f"{thc}{thc_unit}"
            elif thc is not None:
                data["THC"] = thc

            cbd = raw.get("cbdContent")
            cbd_unit = raw.get("cbdContentUnit")
            if cbd is not None and cbd_unit:
                data["CBD"] = f"{cbd}{cbd_unit}"
            elif cbd is not None:
                data["CBD"] = cbd

        logging.info(f"CksPlugin: transformed {len(articles)} articles")
        return articles


register(CksPlugin)
