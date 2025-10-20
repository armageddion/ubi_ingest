import csv
import io
import time
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

def parse_csv_data(csv_data):
    reader = csv.DictReader(io.StringIO(csv_data))
    return list(reader)

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
    acc_token = acc_token_req.json().get('access_token') ## maybe?? 
    if acc_token.response.status_code != 200:
        print(f"Failed to get access token for {customer['name']}: {acc_token.status_code}")
        return

    # Upsert article
    # json is array of articles
    # below is sample article
    article_req = requests.post(
        endpoint + '/common/api/v2/articles',
        params={"store": customer['store_name'], "company": customer['company_name']},
        timeout=30,
        json=[
                {
                    "articleId": "314159",
                    "articleName": "Number Pi",
                    "nfcUrl": "solumesl.com",
                    "eans": [
                    [
                        "314159",
                        "31415926",
                        "31415926535"
                    ]
                    ],
                    "data": {
                    "STORE_CODE": "007",
                    "ITEM_ID": "314159",
                    "ITEM_NAME": "a new pi",
                    "ITEM_DESCRIPTION": "just first few digits of number pi",
                    "BARCODE": "31415926535",
                    "SKU": "31415926535",
                    "LIST_PRICE": "$999.99",
                    "SALE_PRICE": "$949.99",
                    "CLEARANCE_PRICE": "$899.99",
                    "UNIT_PRICE": "$999.99",
                    "PACK_QUANTITY": "1",
                    "WEIGHT": "heavy",
                    "WEIGHT_UNIT": None,
                    "DEPARTMENT": "Science",
                    "AISLE_LOCATION": None,
                    "COUNTRY_OF_ORIGIN": "Egypt",
                    "BRAND": "Ahmes",
                    "MODEL": "Rhind Papyrus",
                    "COLOR": "Fruit",
                    "INVENTORY": "27",
                    "START_DATE": "4/15/25",
                    "END_DATE": "12/31/25",
                    "LANGUAGE": "EN",
                    "CATEGORY_01": None,
                    "CATEGORY_02": None,
                    "CATEGORY_03": None,
                    "MISC_01": "$27.77/mo for 36 months",
                    "MISC_02": None,
                    "MISC_03": None,
                    "DISPLAY_PAGE_1": "REGULAR",
                    "DISPLAY_PAGE_2": "SALE",
                    "DISPLAY_PAGE_3": None,
                    "DISPLAY_PAGE_4": None,
                    "DISPLAY_PAGE_5": None,
                    "DISPLAY_PAGE_6": None,
                    "DISPLAY_PAGE_7": None,
                    "NFC_DATA": "solumesl.com"
                    }
                }
            ]
    )

    print(f"Pushed to {endpoint}: {article_req.status_code}")

def process_customer(customer):
    input_type = customer['input_type']
    creds = customer['creds']
    output_endpoint = customer['output_endpoint']
    
    try:
        if input_type == 'ftp':
            csv_data = fetch_ftp(**creds)
        elif input_type == 'sftp':
            csv_data = fetch_sftp(**creds)
        elif input_type == 'sql':
            csv_data = fetch_sql(**creds)
        elif input_type == 'local':
            csv_data = fetch_local(**creds)
        else:
            print(f"Unknown input type: {input_type}")
            return
        
        parsed_data = parse_csv_data(csv_data)
        push_to_api(customer, parsed_data)
    except Exception as e:
        print(f"Error processing {customer['name']}: {e}")

def run_daemon(config):
    def job():
        for customer in config.customers:
            process_customer(customer)
    
    schedule.every(1).hours.do(job)  # Run every hour
    
    while True:
        schedule.run_pending()
        time.sleep(60)