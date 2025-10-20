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

def parse_csv_data(csv_data):
    reader = csv.DictReader(io.StringIO(csv_data))
    return list(reader)

def push_to_api(endpoint, data):
    response = requests.post(endpoint, json=data)
    print(f"Pushed to {endpoint}: {response.status_code}")

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
        else:
            print(f"Unknown input type: {input_type}")
            return
        
        parsed_data = parse_csv_data(csv_data)
        push_to_api(output_endpoint, parsed_data)
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