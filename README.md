# UBI Ingest Daemon

A Python-based daemon for ingesting CSV data from various sources (FTP, SFTP, SQL databases, or local files), parsing it according to configurable field mappings, and pushing the structured data to a REST API endpoint.

## Features

- Supports multiple input sources: FTP, SFTP, SQL, and local files
- Configurable field mappings for CSV parsing
- Scheduled processing with configurable intervals
- Logging and log rotation
- Extensible with custom parsers (e.g., XML to CSV)

## Installation

### Prerequisites

- Python 3.7 or higher
- Access to input sources (FTP/SFTP servers, SQL databases, or local file paths)
- Permissions to write logs (default: `/var/log/ubi_ingest.log`)

### Setup Virtual Environment

Create and activate a virtual environment to isolate dependencies:

```bash
python -m venv venv
source venv/bin/activate  # On Linux/Mac
# venv\Scripts\activate   # On Windows
```

### Install Requirements

Install the required Python packages:

```bash
pip install -r requirements.txt
```

## Configuration

The daemon is configured via environment variables loaded from a `.env` file. Copy the example file and customize it for your setup.

### Generating a Correct .env File

1. **Copy the example file:**
   ```bash
   cp .env.example .env
   ```

2. **Edit the `.env` file** with your actual values. Key sections include:

   - **Global Settings:**
     - `CUSTOMERS`: Comma-separated list of customer names (e.g., `CUSTOMERS=cust1,cust2`).
     - `LOG_LEVEL`: Logging level (e.g., `INFO`, `DEBUG`).
     - `LOG_FILE`: Path to log file (e.g., `/var/log/ubi_ingest.log`).

   - **Per-Customer Configuration:**
     For each customer (replace `CUST1` with your customer name in uppercase):

     - **Basic Info:**
       - `CUST1_COMPANY_NAME`: Company name for API requests.
       - `CUST1_STORE_NAME`: Store name for API requests.
       - `CUST1_INPUT_TYPE`: Input source type (`ftp`, `sftp`, `sql`, `local`).
       - `CUST1_OUTPUT_ENDPOINT`: API endpoint URL (e.g., `https://api.example.com`).
       - `CUST1_OUTPUT_USER`: API username.
       - `CUST1_OUTPUT_PASS`: API password.
       - `CUST1_HEADER_ROW`: Set to `YES` if the first row is a header row.

     - **Input Credentials:**
       - For FTP: `CUST1_FTP_HOST`, `CUST1_FTP_USER`, `CUST1_FTP_PASS`.
       - For SFTP: `CUST1_SFTP_HOST`, `CUST1_SFTP_USER`, `CUST1_SFTP_PASS`, `CUST1_SFTP_KEY_PATH` (optional key file path).
       - For SQL: `CUST1_SQL_HOST`, `CUST1_SQL_USER`, `CUST1_SQL_PASS`, `CUST1_SQL_DB`, `CUST1_SQL_QUERY`.
       - For Local: `CUST1_LOCAL_PATH` (absolute path to CSV file).

     - **Field Mappings:**
       Map CSV columns to API fields. Use 1-based column numbers (e.g., `3` for column 3) or fixed strings (e.g., `"fixed_value"`).
       - `CUST1_ARTICLE_ID`: Column or value for article ID.
       - `CUST1_ARTICLE_NAME`: Column or value for article name.
       - And so on for all fields (EANs, prices, categories, etc.). See `.env.example` for the full list.

3. **Validation Tips:**
   - Ensure customer names in `CUSTOMERS` match the prefixes (e.g., `CUST1` for customer `cust1`).
   - For column mappings, verify your CSV structure. The first column is `1`.
   - Test credentials and paths manually before running the daemon.
   - For SFTP, ensure host keys are in `~/.ssh/known_hosts` to avoid connection failures (the daemon rejects unknown keys for security).
   - If using custom parsers (e.g., XML), set `CUST1_INPUT_PARSER=xml_to_csv` and ensure `utils/xml_to_csv.py` exists.

Example snippet for a single FTP customer:

```env
CUSTOMERS=mycompany
LOG_LEVEL=INFO
LOG_FILE=/var/log/ubi_ingest.log

MYCOMPANY_COMPANY_NAME=My Company
MYCOMPANY_STORE_NAME=My Store
MYCOMPANY_INPUT_TYPE=ftp
MYCOMPANY_OUTPUT_ENDPOINT=https://api.mycompany.com
MYCOMPANY_OUTPUT_USER=apiuser
MYCOMPANY_OUTPUT_PASS=apipass
MYCOMPANY_HEADER_ROW=YES
MYCOMPANY_ARTICLE_ID=1
MYCOMPANY_ARTICLE_NAME=2
# ... other mappings ...

MYCOMPANY_FTP_HOST=ftp.mycompany.com
MYCOMPANY_FTP_USER=ftpuser
MYCOMPANY_FTP_PASS=ftppass
```

## Running the Daemon

Run the daemon with optional filters:

```bash
python main.py [options]
```

### Options

- `--ftp`: Process only FTP customers.
- `--sftp`: Process only SFTP customers.
- `--sql`: Process only SQL customers.
- `--local`: Process only local file customers.
- `--config <file>`: Specify a custom config file path (default: `.env`).
- `--customer <name>`: Process only the specified customer.

Example: Process only SFTP customers from a custom config:

```bash
python main.py --sftp --config /path/to/myconfig.env
```

The daemon runs indefinitely, processing customers every minute (configurable in code).

## Logrotate Setup

To manage log files, set up log rotation:

1. Copy the provided `logrotate.conf` to `/etc/logrotate.d/`:
   ```bash
   sudo cp logrotate.conf /etc/logrotate.d/ubi_ingest
   ```

2. Adjust the file if your log path differs (default: `/var/log/ubi_ingest.log`).

3. Ensure the logrotate service is running (usually enabled by default on Linux).

This rotates logs daily, keeps 3 old files, compresses them, and creates new logs with appropriate permissions.

## Testing

Run the test suite to verify functionality:

```bash
pytest
```

Tests cover configuration loading, data parsing, and API interactions.

## Security Notes

- Store sensitive credentials (passwords, keys) securely; avoid committing `.env` to version control.
- SFTP connections reject unknown host keys for MITM protection—manually add trusted keys to `~/.ssh/known_hosts`.
- Use HTTPS for API endpoints and strong passwords.

## Troubleshooting

- Check logs at the configured `LOG_FILE` for errors.
- Ensure input files/sources are accessible and correctly formatted.
- For SFTP issues, verify host keys and permissions.
- If custom parsers fail, check `utils/` scripts and error logs.

## Contributing

- Follow the existing code style and add tests for new features.
- Update this README for configuration changes.