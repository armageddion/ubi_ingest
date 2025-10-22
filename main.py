import argparse
from dotenv import load_dotenv
from daemon import run_daemon
from config import Config

def main():
    parser = argparse.ArgumentParser(description='CSV Ingest Daemon')
    parser.add_argument('--ftp', action='store_true', help='Process FTP customers')
    parser.add_argument('--sftp', action='store_true', help='Process SFTP customers')
    parser.add_argument('--sql', action='store_true', help='Process SQL customers')
    parser.add_argument('--local', action='store_true', help='Process local path customers')
    parser.add_argument('--config', type=str, help='Path to config file')
    args = parser.parse_args()

    # Load environment variables from config file or default .env
    if args.config:
        print(f"Loading config from {args.config}")
        load_dotenv(args.config)
    else:
        print("Loading config from default .env")
        load_dotenv()

    config = Config()
    
    # Filter customers based on args
    enabled_types = []
    if args.ftp:
        enabled_types.append('ftp')
    if args.sftp:
        enabled_types.append('sftp')
    if args.sql:
        enabled_types.append('sql')
    if args.local:
        enabled_types.append('local')
    
    if enabled_types:
        config.customers = [c for c in config.customers if c['input_type'] in enabled_types]

    print(f"Starting daemon with customers:{config.customers}")    
    run_daemon(config)

if __name__ == '__main__':
    main()