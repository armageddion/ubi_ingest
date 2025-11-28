import csv
import io
import time
import logging
import schedule
import requests
import ftplib
import paramiko
import pymysql
import subprocess
import os
import shutil
import importlib
import sys
import pkgutil

# Add parent directory to sys.path so ubi_ingest can be imported as a module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def fetch_ftp(customer_name, host, user, passw, path="/"):
    ftp = ftplib.FTP(host)
    ftp.login(user, passw)
    # List files in path
    files = ftp.nlst(path)
    if not files:
        ftp.quit()
        return "", None
    # Get modification times
    latest_file = None
    latest_time = 0
    for file in files:
        try:
            resp = ftp.voidcmd(f"MDTM {file}")
            # resp is like '213 20231027120000'
            date_str = resp.split()[1]
            # Parse to timestamp
            import time

            file_time = time.mktime(time.strptime(date_str, "%Y%m%d%H%M%S"))
            if file_time > latest_time:
                latest_time = file_time
                latest_file = file
        except:
            continue
    if not latest_file:
        ftp.quit()
        return "", None
    data = io.BytesIO()
    ftp.retrbinary(f"RETR {latest_file}", data.write)
    ftp.quit()
    data.seek(0)
    data_str = data.read().decode("utf-8")
    # Save to tmp
    os.makedirs(os.path.join("tmp", customer_name), exist_ok=True)
    file_path = os.path.join("tmp", customer_name, latest_file)
    with open(file_path, "w") as f:
        f.write(data_str)
    return data_str, file_path


def fetch_sftp(customer_name, host, user, passw, key_path=None, path="/"):
    import typing

    ssh = paramiko.SSHClient()
    ssh.load_host_keys(
        os.path.expanduser("~/.ssh/known_hosts")
    )  # Load known hosts
    ssh.set_missing_host_key_policy(
        paramiko.RejectPolicy()
    )  # Reject unknown keys
    if key_path:
        ssh.connect(host, username=user, key_filename=key_path)
    else:
        ssh.connect(host, username=user, password=passw)
    sftp = ssh.open_sftp()
    # List files in path
    files = sftp.listdir_attr(path)
    if not files:
        ssh.close()
        return "", None
    # Find latest by st_mtime
    valid_files = [f for f in files if f.st_mtime is not None]
    if not valid_files:
        ssh.close()
        return "", None
    latest_file = sorted(
        valid_files, key=lambda f: typing.cast(int, f.st_mtime)
    )[-1]
    with sftp.open(os.path.join(path, latest_file.filename), "r") as f:
        data = f.read().decode("utf-8")
    ssh.close()
    # Save to tmp
    os.makedirs(os.path.join("tmp", customer_name), exist_ok=True)
    file_path = os.path.join("tmp", customer_name, latest_file.filename)
    with open(file_path, "w") as f:
        f.write(data)
    return data, file_path


def fetch_sql(host, user, passw, db, query):
    conn = pymysql.connect(host=host, user=user, password=passw, database=db)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    # Convert to CSV string
    if rows:
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        return output.getvalue(), None
    return "", None


def fetch_local(path):
    if os.path.isdir(path):
        files = [
            f
            for f in os.listdir(path)
            if os.path.isfile(os.path.join(path, f))
        ]
        if not files:
            return "", None
        latest = max(
            files, key=lambda f: os.path.getmtime(os.path.join(path, f))
        )
        file_path = os.path.join(path, latest)
    else:
        file_path = path
    with open(file_path, "r") as f:
        return f.read(), file_path


def determine_template(customer_name, article_data):
    if customer_name == "vessel":
        if article_data.get("SALE_PRICE"):
            return "sale"
        else:
            return "default"
    # Add other customers here
    return None


def parse_csv_data(csv_data, customer):
    print("parsing csv data")
    logging.debug("Parsing CSV data")
    reader = csv.reader(io.StringIO(csv_data))
    print(f"header row: {customer['header_row']}")
    if customer["header_row"] == "YES":
        print("Skipping header row")
        logging.debug("Skipping header row")
        next(reader, None)
    rows = list(reader)
    rows = [[cell.rstrip() for cell in row] for row in rows]
    articles = []
    for row in rows:
        print(f"Processing row: {row}")
        logging.debug(f"Processing row: {row}")

        def get_value(mapping):
            # print(f"getting value of mapping {mapping}")
            # print(f"type of mapping: {type(mapping)}")
            if not mapping:
                return None
            if not mapping.isnumeric():
                return mapping
            # col_str = mapping.split()[1]
            col_idx = int(mapping) - 1
            # print(f"col_str = {mapping}")
            # print(f"col_idx = {col_idx}")
            if col_idx < len(row):
                # print(f"returning value {row[col_idx]}")
                return row[col_idx]
            return None

        article = {}
        if (aid := get_value(customer["article_id"])) is not None:
            article["articleId"] = aid
        if (aname := get_value(customer["article_name"])) is not None:
            article["articleName"] = aname
        if (nfc := get_value(customer["nfc_url"])) is not None:
            article["nfcUrl"] = nfc
        eans_list = [e for e in [get_value(customer["ean1"]), get_value(customer["ean2"]), get_value(customer["ean3"])] if e is not None]
        if eans_list:
            article["eans"] = [eans_list]
        data = {}
        for key, value in {
            "STORE_CODE": customer["store_code"],
            "ITEM_ID": get_value(customer["item_id"]),
            "ITEM_NAME": get_value(customer["item_name"]),
            "ITEM_DESCRIPTION": get_value(customer["item_description"]),
            "BARCODE": (lambda v: v.lstrip("0") if v else None)(get_value(customer["barcode"])),
            "SKU": get_value(customer["sku"]),
            "LIST_PRICE": get_value(customer["list_price"]),
            "SALE_PRICE": get_value(customer["sale_price"]),
            "CLEARANCE_PRICE": get_value(customer["clearance_price"]),
            "UNIT_PRICE": get_value(customer["unit_price"]),
            "PACK_QUANTITY": get_value(customer["pack_quantity"]),
            "WEIGHT": get_value(customer["weight"]),
            "WEIGHT_UNIT": get_value(customer["weight_unit"]),
            "DEPARTMENT": get_value(customer["department"]),
            "AISLE_LOCATION": get_value(customer["aisle_location"]),
            "COUNTRY_OF_ORIGIN": get_value(customer["country_of_origin"]),
            "BRAND": get_value(customer["brand"]),
            "MODEL": get_value(customer["model"]),
            "COLOR": get_value(customer["color"]),
            "INVENTORY": get_value(customer["inventory"]),
            "START_DATE": get_value(customer["start_date"]),
            "END_DATE": get_value(customer["end_date"]),
            "LANGUAGE": get_value(customer["language"]),
            "CATEGORY_01": get_value(customer["category_01"]),
            "CATEGORY_02": get_value(customer["category_02"]),
            "CATEGORY_03": get_value(customer["category_03"]),
            "MISC_01": get_value(customer["misc_01"]),
            "MISC_02": get_value(customer["misc_02"]),
            "MISC_03": get_value(customer["misc_03"]),
            "DISPLAY_PAGE_1": get_value(customer["display_page_1"]),
            "DISPLAY_PAGE_2": get_value(customer["display_page_2"]),
            "DISPLAY_PAGE_3": get_value(customer["display_page_3"]),
            "DISPLAY_PAGE_4": get_value(customer["display_page_4"]),
            "DISPLAY_PAGE_5": get_value(customer["display_page_5"]),
            "DISPLAY_PAGE_6": get_value(customer["display_page_6"]),
            "DISPLAY_PAGE_7": get_value(customer["display_page_7"]),
            "NFC_DATA": get_value(customer["nfc_data"]),
        }.items():
            if value is not None:
                data[key] = value
        article["data"] = data
        # Set template field based on logic
        template_field = customer.get("template_field", "MISC_03")
        template_value = determine_template(
            customer.get("name", ""), article["data"]
        )
        if template_value:
            article["data"][template_field] = template_value
        articles.append(article)
        #print(f"Added article: {article}")  # DEBUG
        #break

    print(f"number of articles: {len(articles)}")
    logging.info(f"Parsed {len(articles)} articles")
    # print(json.dumps(articles))
    return articles


def discover_plugins():
    """Discover and import all modules in the local plugins/ directory.

    Plugins should live in `ubi_ingest/plugins` and import and register
    themselves with `plugins.base.register()` (see plugins/base.py).
    """
    logging.debug("Discovering plugins...")
    plugins_dir = os.path.join(os.path.dirname(__file__), "plugins")
    if not os.path.isdir(plugins_dir):
        logging.debug("No plugins directory found")
        return
    pkg = os.path.basename(os.path.dirname(__file__))
    for finder, name, ispkg in pkgutil.iter_modules([plugins_dir]):
        try:
            importlib.import_module(f"{pkg}.plugins.{name}")
            logging.info(f"Loaded plugin module: {name}")
        except Exception as e:
            logging.error(f"Failed to load plugin {name}: {e}")


def get_plugins_for_customer(customer):
    """Return list of plugin instances that apply to this customer.

    This dynamically imports the base plugin registry and queries it so the
    daemon does not need a hard dependency at import time.
    """
    logging.info(f"Getting plugins for customer {customer['name']}")
    pkg = os.path.basename(os.path.dirname(__file__))
    base = importlib.import_module(f"{pkg}.plugins.base")
    return base.get_plugins_for_customer(customer)


def push_to_api(customer, data):
    # Unpack the data and push to API
    endpoint = customer["output_endpoint"]

    # Get access token
    acc_token_req = requests.post(
        endpoint + "/common/api/v2/token",
        timeout=30,
        json={
            "username": customer["output_user"],
            "password": customer["output_pass"],
        },
    )

    print("Access token request JSON:", acc_token_req.json())
    logging.debug(f"Access token request response: {acc_token_req.json()}")

    acc_token = acc_token_req.json().get("responseMessage").get("access_token")
    print(f"Access token: {acc_token}")
    logging.debug(f"Access token obtained for {customer['name']}")
    if acc_token_req.status_code != 200:
        print(
            f"Failed to get access token for {customer['name']}: {acc_token_req.status_code}"
        )
        logging.error(
            f"Failed to get access token for {customer['name']}: {acc_token_req.status_code}"
        )
        return

    # Upsert articles
    headers = {"Authorization": f"Bearer {acc_token}"}
    # Break data into chunks of 1000 elements or less
    chunk_size = 1000
    for i in range(0, len(data), chunk_size):
        chunk = data[i : i + chunk_size]
        article_req = requests.post(
            endpoint + "/common/api/v2/common/articles",
            headers=headers,
            params={
                "store": customer["store_name"],
                "company": customer["company_name"],
            },
            timeout=300,
            json=chunk,
        )

        print(
            f"Pushed chunk {i//chunk_size + 1} to {endpoint}/common/api/v2/common/articles: {article_req.status_code}"
        )
        logging.info(
            f"Pushed {len(chunk)} articles (chunk {i//chunk_size + 1}) to {endpoint}: {article_req.status_code}"
        )
        print(f"Response: {article_req.json()}")
        logging.debug(f"Response: {article_req.json()}")


def process_customer(customer):
    print(f"Processing customer {customer['name']}")
    logging.info(f"Processing customer {customer['name']}")
    input_type = customer["input_type"]
    creds = customer["creds"]

    try:
        if input_type == "ftp":
            customer_data, source_file = fetch_ftp(customer["name"], **creds)
        elif input_type == "sftp":
            customer_data, source_file = fetch_sftp(customer["name"], **creds)
        elif input_type == "sql":
            customer_data, source_file = fetch_sql(**creds)
        elif input_type == "local":
            customer_data, source_file = fetch_local(**creds)
        else:
            print(f"Unknown input type: {input_type}")
            logging.error(f"Unknown input type: {input_type}")
            return

        csv_data = customer_data
        if customer.get("input_parser", "csv") != "csv":
            print(
                f"Parsing customer {customer['name']} data with parser {customer['input_parser']}"
            )
            path = os.getenv(f"{customer['name'].upper()}_LOCAL_PATH")
            if not path:
                print(f"No LOCAL_PATH found for {customer['name']}")
                logging.error(f"No LOCAL_PATH found for {customer['name']}")
                return
            # Find latest file in path
            if os.path.isdir(path):
                files = [
                    f
                    for f in os.listdir(path)
                    if os.path.isfile(os.path.join(path, f))
                ]
                if not files:
                    print(f"No files found in {path}")
                    logging.error(f"No files found in {path}")
                    return
                latest = max(
                    files,
                    key=lambda f: os.path.getmtime(os.path.join(path, f)),
                )
                file_path = os.path.join(path, latest)
            else:
                file_path = path
            result = subprocess.run(
                ["python", f'utils/{customer["input_parser"]}.py', file_path],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                csv_data = result.stdout
                source_file = file_path  # for non-csv, the file is the source
            else:
                print(f"Parser error: {result.stderr}")
                logging.error(
                    f"Parser error for {customer['name']}: {result.stderr}"
                )
                return

        # format data for API
        parsed_data = parse_csv_data(csv_data, customer)

        # Allow plugins to transform/modify articles for this customer
        try:
            plugins = get_plugins_for_customer(customer)
            logging.debug(f"Found {len(plugins)} plugins for {customer['name']}")
            for plugin in plugins:
                try:
                    parsed_data = plugin.transform_articles(customer, parsed_data)
                except Exception as e:
                    logging.error(f"Plugin {plugin} failed for {customer['name']}: {e}")
        except Exception as e:
            # If plugin subsystem fails, continue with default behaviour
            logging.debug("No plugins available or plugin system failed")
            logging.debug(f"Plugin error for {customer['name']}: {e}")

        # push data to customer server
        push_to_api(customer, parsed_data)

        # Move file to tmp
        if source_file:
            customer_dir = os.path.join("tmp", customer["name"])
            os.makedirs(customer_dir, exist_ok=True)
            shutil.move(source_file, customer_dir)
            # Keep only 3 most recent files
            files = sorted(
                os.listdir(customer_dir),
                key=lambda f: os.path.getmtime(os.path.join(customer_dir, f)),
                reverse=True,
            )
            for f in files[3:]:
                os.remove(os.path.join(customer_dir, f))

    except Exception as e:
        print(f"Error processing {customer['name']}: {e}")
        logging.error(f"Error processing {customer['name']}: {e}")


def run_daemon(config):
    # discover plugins once at startup
    try:
        discover_plugins()
    except Exception:
        logging.debug("discover_plugins failed or no plugins present")

    def job():
        for customer in config.customers:
            print(f"Starting job with customer {customer['name']}")
            logging.info(f"Starting job with customer {customer['name']}")
            process_customer(customer)

    # schedule.every(1).hours.do(job)  # Run every hour
    schedule.every(1).minutes.do(job)  # Run every minute

    while True:
        print(schedule.get_jobs())
        logging.debug(f"Scheduled jobs: {schedule.get_jobs()}")

        schedule.run_pending()
        time.sleep(60)
