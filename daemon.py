import csv
import io
import time
import json
import logging
import schedule
import requests
import ftplib
import paramiko
import pymysql

def fetch_ftp(host, user, passw):
    # Placeholder: fetch latest CSV file
    ftp = ftplib.FTP(host)
    ftp.login(user, passw)
    # Assume file is 'data.csv'
    data = io.BytesIO()
    ftp.retrbinary('RETR data.csv', data.write)
    ftp.quit()
    data.seek(0)
    return data.read().decode('utf-8')

def fetch_sftp(host, user, passw, key_path=None):
    # Placeholder
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    if key_path:
        ssh.connect(host, username=user, key_filename=key_path)
    else:
        ssh.connect(host, username=user, password=passw)
    sftp = ssh.open_sftp()
    with sftp.open('data.csv', 'r') as f:
        data = f.read().decode('utf-8')
    ssh.close()
    return data

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
        return output.getvalue()
    return ''

def fetch_local(path):
    with open(path, 'r') as f:
        return f.read()

def parse_csv_data(csv_data, customer):
    print("parsing csv data")
    logging.debug("Parsing CSV data")
    reader = csv.reader(io.StringIO(csv_data))
    #print(f"header row: {customer['header_row']}")
    if customer['header_row'] == "YES":
        print("Skipping header row")
        logging.debug("Skipping header row")
        next(reader, None)
    rows = list(reader)
    articles = []
    for row in rows:
        print(f"Processing row: {row}")
        logging.debug(f"Processing row: {row}")
        def get_value(mapping):
            #print(f"getting value of mapping {mapping}")
            #print(f"type of mapping: {type(mapping)}")
            if not mapping:
                return None
            if not mapping.isnumeric():
                return mapping
            #col_str = mapping.split()[1]
            col_idx = int(mapping) - 1
            #print(f"col_str = {mapping}")
            #print(f"col_idx = {col_idx}")
            if col_idx < len(row):
                #print(f"returning value {row[col_idx]}")
                return row[col_idx]
            return None

        article = {
            'articleId': get_value(customer['article_id']),
            'articleName': get_value(customer['article_name']),
            'nfcUrl': get_value(customer['nfc_url']),
            'eans': [[
                get_value(customer['ean1']),
                get_value(customer['ean2']),
                get_value(customer['ean3'])
            ]],
            'data': {
                'STORE_CODE': get_value(customer['store_code']),
                'ITEM_ID': get_value(customer['item_id']),
                'ITEM_NAME': get_value(customer['item_name']),
                'ITEM_DESCRIPTION': get_value(customer['item_description']),
                'BARCODE': get_value(customer['barcode']),
                'SKU': get_value(customer['sku']),
                'LIST_PRICE': get_value(customer['list_price']),
                'SALE_PRICE': get_value(customer['sale_price']),
                'CLEARANCE_PRICE': get_value(customer['clearance_price']),
                'UNIT_PRICE': get_value(customer['unit_price']),
                'PACK_QUANTITY': get_value(customer['pack_quantity']),
                'WEIGHT': get_value(customer['weight']),
                'WEIGHT_UNIT': get_value(customer['weight_unit']),
                'DEPARTMENT': get_value(customer['department']),
                'AISLE_LOCATION': get_value(customer['aisle_location']),
                'COUNTRY_OF_ORIGIN': get_value(customer['country_of_origin']),
                'BRAND': get_value(customer['brand']),
                'MODEL': get_value(customer['model']),
                'COLOR': get_value(customer['color']),
                'INVENTORY': get_value(customer['inventory']),
                'START_DATE': get_value(customer['start_date']),
                'END_DATE': get_value(customer['end_date']),
                'LANGUAGE': get_value(customer['language']),
                'CATEGORY_01': get_value(customer['category_01']),
                'CATEGORY_02': get_value(customer['category_02']),
                'CATEGORY_03': get_value(customer['category_03']),
                'MISC_01': get_value(customer['misc_01']),
                'MISC_02': get_value(customer['misc_02']),
                'MISC_03': get_value(customer['misc_03']),
                'DISPLAY_PAGE_1': get_value(customer['display_page_1']),
                'DISPLAY_PAGE_2': get_value(customer['display_page_2']),
                'DISPLAY_PAGE_3': get_value(customer['display_page_3']),
                'DISPLAY_PAGE_4': get_value(customer['display_page_4']),
                'DISPLAY_PAGE_5': get_value(customer['display_page_5']),
                'DISPLAY_PAGE_6': get_value(customer['display_page_6']),
                'DISPLAY_PAGE_7': get_value(customer['display_page_7']),
                'NFC_DATA': get_value(customer['nfc_data'])
            }
        }
        articles.append(article)

    print(f"number of articles: {len(articles)}")
    logging.info(f"Parsed {len(articles)} articles")
    #print(json.dumps(articles))
    return articles

def push_to_api(customer, data):
    # Unpack the data and push to API
    endpoint = customer['output_endpoint']

    # Get access token
    acc_token_req = requests.post(endpoint+'/common/api/v2/token',
        timeout=30,
        json={
            "username": customer['output_user'],
            "password": customer['output_pass']
        }
    )

    print("Access token request JSON:", acc_token_req.json())
    logging.debug(f"Access token request response: {acc_token_req.json()}")

    acc_token = acc_token_req.json().get('responseMessage').get('access_token')
    print(f"Access token: {acc_token}")
    logging.debug(f"Access token obtained for {customer['name']}")
    if acc_token_req.status_code != 200:
        print(f"Failed to get access token for {customer['name']}: {acc_token_req.status_code}")
        logging.error(f"Failed to get access token for {customer['name']}: {acc_token_req.status_code}")
        return

    exit()
    # Upsert articles
    #TODO: handle pagination if data is too large
    headers = {"Authorization": f"Bearer {acc_token}"}
    article_req = requests.post(
        endpoint + '/common/api/v2/articles',
        headers=headers,
        params={"store": customer['store_name'], "company": customer['company_name']},
        timeout=30,
        json=data
    )

    print(f"Pushed to {endpoint}: {article_req.status_code}")
    logging.info(f"Pushed {len(data)} articles to {endpoint}: {article_req.status_code}")
    print(f"Response: {article_req.json()}")
    logging.debug(f"Response: {article_req.json()}")

def process_customer(customer):
    print(f"Processing customer {customer['name']}")
    logging.info(f"Processing customer {customer['name']}")
    input_type = customer['input_type']
    creds = customer['creds']

    try:
        if input_type == 'ftp':
            customer_data = fetch_ftp(**creds)
        elif input_type == 'sftp':
            customer_data = fetch_sftp(**creds)
        elif input_type == 'sql':
            customer_data = fetch_sql(**creds)
        elif input_type == 'local':
            customer_data = fetch_local(**creds)
        else:
            print(f"Unknown input type: {input_type}")
            logging.error(f"Unknown input type: {input_type}")
            return
        
        if customer['input_parser'] != 'csv':
            print(f"Parsing customer {customer['name']} data with parser {customer['input_parser']}")
            #TODO: execute utils/{customer['input_parser']} to convert to csv
        else:
            csv_data = customer_data  

        parsed_data = parse_csv_data(csv_data, customer)
        push_to_api(customer, parsed_data)
    except Exception as e:
        print(f"Error processing {customer['name']}: {e}")
        logging.error(f"Error processing {customer['name']}: {e}")

def run_daemon(config):
    def job():
        for customer in config.customers:
            print(f"Starting job with customer {customer['name']}")
            logging.info(f"Starting job with customer {customer['name']}")
            process_customer(customer)

    #schedule.every(1).hours.do(job)  # Run every hour
    schedule.every(1).minutes.do(job)  # Run every minute

    while True:
        print(schedule.get_jobs())
        logging.debug(f"Scheduled jobs: {schedule.get_jobs()}")

        schedule.run_pending()
        time.sleep(60)