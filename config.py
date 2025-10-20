import os

class Config:
    def __init__(self):
        self.customers = []
        customers_str = os.getenv('CUSTOMERS', '')
        if customers_str:
            customer_names = [c.strip() for c in customers_str.split(',')]
            for name in customer_names:
                cust_config = {
                    'name': name,
                    'company_name': os.getenv(f'{name.upper()}_COMPANY_NAME'),
                    'store_name': os.getenv(f'{name.upper()}_STORE_NAME'),
                    'input_type': os.getenv(f'{name.upper()}_INPUT_TYPE'),
                    'output_endpoint': os.getenv(f'{name.upper()}_OUTPUT_ENDPOINT'),
                    'output_user': os.getenv(f'{name.upper()}_OUTPUT_USER'),
                    'output_pass': os.getenv(f'{name.upper()}_OUTPUT_PASS'),
                    'creds': {}
                }
                input_type = cust_config['input_type']
                if input_type in ['ftp', 'ftps']:
                    cust_config['creds'] = {
                        'host': os.getenv(f'{name.upper()}_FTP_HOST'),
                        'user': os.getenv(f'{name.upper()}_FTP_USER'),
                        'pass': os.getenv(f'{name.upper()}_FTP_PASS'),
                    }
                elif input_type == 'sftp':
                    cust_config['creds'] = {
                        'host': os.getenv(f'{name.upper()}_SFTP_HOST'),
                        'user': os.getenv(f'{name.upper()}_SFTP_USER'),
                        'pass': os.getenv(f'{name.upper()}_SFTP_PASS'),
                        'key_path': os.getenv(f'{name.upper()}_SFTP_KEY_PATH'),
                    }
                elif input_type == 'sql':
                    cust_config['creds'] = {
                        'host': os.getenv(f'{name.upper()}_SQL_HOST'),
                        'user': os.getenv(f'{name.upper()}_SQL_USER'),
                        'pass': os.getenv(f'{name.upper()}_SQL_PASS'),
                        'db': os.getenv(f'{name.upper()}_SQL_DB'),
                        'query': os.getenv(f'{name.upper()}_SQL_QUERY'),
                    }
                elif input_type == 'local':
                    cust_config['creds'] = {
                        'path': os.getenv(f'{name.upper()}_LOCAL_PATH'),
                    }
                self.customers.append(cust_config)

        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        self.log_file = os.getenv('LOG_FILE', '/var/log/ubi_ingest.log')