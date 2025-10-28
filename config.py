import os


class Config:
    def __init__(self, customer_name=None):
        self.customers = []
        customers_str = os.getenv("CUSTOMERS", "")
        if customers_str:
            customer_names = [c.strip() for c in customers_str.split(",")]
            if customer_name:
                if customer_name in customer_names:
                    customer_names = [customer_name]
                else:
                    print(
                        f"Customer '{customer_name}' not found in CUSTOMERS list"
                    )
                    customer_names = []
            for name in customer_names:
                print(f"Configuring customer: {name}")
                cust_config = {
                    "name": name,
                    "company_name": os.getenv(f"{name.upper()}_COMPANY_NAME"),
                    "store_name": os.getenv(f"{name.upper()}_STORE_NAME"),
                    "input_type": os.getenv(f"{name.upper()}_INPUT_TYPE"),
                    "output_endpoint": os.getenv(
                        f"{name.upper()}_OUTPUT_ENDPOINT"
                    ),
                    "output_user": os.getenv(f"{name.upper()}_OUTPUT_USER"),
                    "output_pass": os.getenv(f"{name.upper()}_OUTPUT_PASS"),
                    "creds": {},
                    "header_row": os.getenv(f"{name.upper()}_HEADER_ROW", ""),
                    "article_id": os.getenv(f"{name.upper()}_ARTICLE_ID", ""),
                    "article_name": os.getenv(
                        f"{name.upper()}_ARTICLE_NAME", ""
                    ),
                    "nfc_url": os.getenv(f"{name.upper()}_NFC_URL", ""),
                    "ean1": os.getenv(f"{name.upper()}_EAN_1", ""),
                    "ean2": os.getenv(f"{name.upper()}_EAN_2", ""),
                    "ean3": os.getenv(f"{name.upper()}_EAN_3", ""),
                    "store_code": os.getenv(f"{name.upper()}_STORE_CODE", ""),
                    "item_id": os.getenv(f"{name.upper()}_ITEM_ID", ""),
                    "item_name": os.getenv(f"{name.upper()}_ITEM_NAME", ""),
                    "item_description": os.getenv(
                        f"{name.upper()}_ITEM_DESCRIPTION", ""
                    ),
                    "barcode": os.getenv(f"{name.upper()}_BARCODE", ""),
                    "sku": os.getenv(f"{name.upper()}_SKU", ""),
                    "list_price": os.getenv(f"{name.upper()}_LIST_PRICE", ""),
                    "sale_price": os.getenv(f"{name.upper()}_SALE_PRICE", ""),
                    "clearance_price": os.getenv(
                        f"{name.upper()}_CLEARANCE_PRICE", ""
                    ),
                    "unit_price": os.getenv(f"{name.upper()}_UNIT_PRICE", ""),
                    "pack_quantity": os.getenv(
                        f"{name.upper()}_PACK_QUANTITY", ""
                    ),
                    "weight": os.getenv(f"{name.upper()}_WEIGHT", ""),
                    "weight_unit": os.getenv(
                        f"{name.upper()}_WEIGHT_UNIT", ""
                    ),
                    "department": os.getenv(f"{name.upper()}_DEPARTMENT", ""),
                    "aisle_location": os.getenv(
                        f"{name.upper()}_AISLE_LOCATION", ""
                    ),
                    "country_of_origin": os.getenv(
                        f"{name.upper()}_COUNTRY_OF_ORIGIN", ""
                    ),
                    "brand": os.getenv(f"{name.upper()}_BRAND", ""),
                    "model": os.getenv(f"{name.upper()}_MODEL", ""),
                    "color": os.getenv(f"{name.upper()}_COLOR", ""),
                    "inventory": os.getenv(f"{name.upper()}_INVENTORY", ""),
                    "start_date": os.getenv(f"{name.upper()}_START_DATE", ""),
                    "end_date": os.getenv(f"{name.upper()}_END_DATE", ""),
                    "language": os.getenv(f"{name.upper()}_LANGUAGE", ""),
                    "category_01": os.getenv(
                        f"{name.upper()}_CATEGORY_01", ""
                    ),
                    "category_02": os.getenv(
                        f"{name.upper()}_CATEGORY_02", ""
                    ),
                    "category_03": os.getenv(
                        f"{name.upper()}_CATEGORY_03", ""
                    ),
                    "misc_01": os.getenv(f"{name.upper()}_MISC_01", ""),
                    "misc_02": os.getenv(f"{name.upper()}_MISC_02", ""),
                    "misc_03": os.getenv(f"{name.upper()}_MISC_03", ""),
                    "display_page_1": os.getenv(
                        f"{name.upper()}_DISPLAY_PAGE_1", ""
                    ),
                    "display_page_2": os.getenv(
                        f"{name.upper()}_DISPLAY_PAGE_2", ""
                    ),
                    "display_page_3": os.getenv(
                        f"{name.upper()}_DISPLAY_PAGE_3", ""
                    ),
                    "display_page_4": os.getenv(
                        f"{name.upper()}_DISPLAY_PAGE_4", ""
                    ),
                    "display_page_5": os.getenv(
                        f"{name.upper()}_DISPLAY_PAGE_5", ""
                    ),
                    "display_page_6": os.getenv(
                        f"{name.upper()}_DISPLAY_PAGE_6", ""
                    ),
                    "display_page_7": os.getenv(
                        f"{name.upper()}_DISPLAY_PAGE_7", ""
                    ),
                    "nfc_data": os.getenv(f"{name.upper()}_NFC_DATA", ""),
                    "template_field": os.getenv(
                        f"{name.upper()}_TEMPLATE_FIELD", "MISC_03"
                    ),
                    "input_parser": os.getenv(
                        f"{name.upper()}_INPUT_PARSER", "csv"
                    ),
                }
                input_type = cust_config["input_type"]
                if input_type in ["ftp", "ftps"]:
                    cust_config["creds"] = {
                        "host": os.getenv(f"{name.upper()}_FTP_HOST"),
                        "user": os.getenv(f"{name.upper()}_FTP_USER"),
                        "pass": os.getenv(f"{name.upper()}_FTP_PASS"),
                        "path": os.getenv(f"{name.upper()}_FTP_PATH", "/"),
                    }
                elif input_type == "sftp":
                    cust_config["creds"] = {
                        "host": os.getenv(f"{name.upper()}_SFTP_HOST"),
                        "user": os.getenv(f"{name.upper()}_SFTP_USER"),
                        "pass": os.getenv(f"{name.upper()}_SFTP_PASS"),
                        "key_path": os.getenv(f"{name.upper()}_SFTP_KEY_PATH"),
                        "path": os.getenv(f"{name.upper()}_SFTP_PATH", "/"),
                    }
                elif input_type == "sql":
                    cust_config["creds"] = {
                        "host": os.getenv(f"{name.upper()}_SQL_HOST"),
                        "user": os.getenv(f"{name.upper()}_SQL_USER"),
                        "pass": os.getenv(f"{name.upper()}_SQL_PASS"),
                        "db": os.getenv(f"{name.upper()}_SQL_DB"),
                        "query": os.getenv(f"{name.upper()}_SQL_QUERY"),
                    }
                elif input_type == "local":
                    cust_config["creds"] = {
                        "path": os.getenv(f"{name.upper()}_LOCAL_PATH"),
                    }
                self.customers.append(cust_config)

        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.log_file = os.getenv("LOG_FILE", "/var/log/ubi_ingest.log")
