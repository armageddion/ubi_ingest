import os
import pytest
from config import Config


def test_config_loading_sftp(monkeypatch):
    monkeypatch.setenv("CUSTOMERS", "cust1")
    monkeypatch.setenv("CUST1_INPUT_TYPE", "sftp")
    monkeypatch.setenv("CUST1_OUTPUT_ENDPOINT", "https://example.com")
    monkeypatch.setenv("CUST1_SFTP_HOST", "sftp.example.com")
    monkeypatch.setenv("CUST1_SFTP_USER", "user")
    monkeypatch.setenv("CUST1_SFTP_PASS", "pass")
    monkeypatch.setenv("CUST1_SFTP_KEY_PATH", "/path/to/key")

    config = Config()
    assert len(config.customers) == 1
    cust = config.customers[0]
    assert cust["name"] == "cust1"
    assert cust["input_type"] == "sftp"
    assert cust["output_endpoint"] == "https://example.com"
    assert cust["creds"]["host"] == "sftp.example.com"
    assert cust["creds"]["user"] == "user"
    assert cust["creds"]["pass"] == "pass"
    assert cust["creds"]["key_path"] == "/path/to/key"


def test_config_loading_sql(monkeypatch):
    monkeypatch.setenv("CUSTOMERS", "cust1")
    monkeypatch.setenv("CUST1_INPUT_TYPE", "sql")
    monkeypatch.setenv("CUST1_OUTPUT_ENDPOINT", "https://example.com")
    monkeypatch.setenv("CUST1_SQL_HOST", "sql.example.com")
    monkeypatch.setenv("CUST1_SQL_USER", "user")
    monkeypatch.setenv("CUST1_SQL_PASS", "pass")
    monkeypatch.setenv("CUST1_SQL_DB", "db")
    monkeypatch.setenv("CUST1_SQL_QUERY", "SELECT * FROM table")

    config = Config()
    assert len(config.customers) == 1
    cust = config.customers[0]
    assert cust["name"] == "cust1"
    assert cust["input_type"] == "sql"
    assert cust["output_endpoint"] == "https://example.com"
    assert cust["creds"]["host"] == "sql.example.com"
    assert cust["creds"]["user"] == "user"
    assert cust["creds"]["pass"] == "pass"
    assert cust["creds"]["db"] == "db"
    assert cust["creds"]["query"] == "SELECT * FROM table"


def test_config_loading_ftps(monkeypatch):
    monkeypatch.setenv("CUSTOMERS", "cust1")
    monkeypatch.setenv("CUST1_INPUT_TYPE", "ftps")
    monkeypatch.setenv("CUST1_OUTPUT_ENDPOINT", "https://example.com")
    monkeypatch.setenv("CUST1_FTP_HOST", "ftps.example.com")
    monkeypatch.setenv("CUST1_FTP_USER", "user")
    monkeypatch.setenv("CUST1_FTP_PASS", "pass")

    config = Config()
    assert len(config.customers) == 1
    cust = config.customers[0]
    assert cust["name"] == "cust1"
    assert cust["input_type"] == "ftps"
    assert cust["creds"]["host"] == "ftps.example.com"
    assert cust["creds"]["user"] == "user"
    assert cust["creds"]["pass"] == "pass"


def test_config_loading_mappings(monkeypatch):
    monkeypatch.setenv("CUSTOMERS", "cust1")
    monkeypatch.setenv("CUST1_INPUT_TYPE", "local")
    monkeypatch.setenv("CUST1_OUTPUT_ENDPOINT", "https://example.com")
    monkeypatch.setenv("CUST1_LOCAL_PATH", "/path")
    monkeypatch.setenv("CUST1_HEADER_ROW", "YES")
    monkeypatch.setenv("CUST1_ARTICLE_ID", "1")
    monkeypatch.setenv("CUST1_ARTICLE_NAME", "2")
    monkeypatch.setenv("CUST1_NFC_URL", "3")
    monkeypatch.setenv("CUST1_EAN_1", "4")
    monkeypatch.setenv("CUST1_STORE_CODE", "5")
    monkeypatch.setenv("CUST1_ITEM_ID", "6")
    monkeypatch.setenv("CUST1_INPUT_PARSER", "xml_to_csv")

    config = Config()
    cust = config.customers[0]
    assert cust["header_row"] == "YES"
    assert cust["article_id"] == "1"
    assert cust["article_name"] == "2"
    assert cust["nfc_url"] == "3"
    assert cust["ean1"] == "4"
    assert cust["store_code"] == "5"
    assert cust["item_id"] == "6"
    assert cust["input_parser"] == "xml_to_csv"


def test_config_log_settings(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("LOG_FILE", "/tmp/custom.log")

    config = Config()
    assert config.log_level == "DEBUG"
    assert config.log_file == "/tmp/custom.log"


def test_config_default_log_settings(monkeypatch):
    config = Config()
    assert config.log_level == "INFO"
    assert config.log_file == "/var/log/ubi_ingest.log"


def test_config_loading_ftp(monkeypatch):
    monkeypatch.setenv("CUSTOMERS", "cust1")
    monkeypatch.setenv("CUST1_INPUT_TYPE", "ftp")
    monkeypatch.setenv("CUST1_OUTPUT_ENDPOINT", "https://example.com")
    monkeypatch.setenv("CUST1_FTP_HOST", "ftp.example.com")
    monkeypatch.setenv("CUST1_FTP_USER", "user")
    monkeypatch.setenv("CUST1_FTP_PASS", "pass")

    config = Config()
    assert len(config.customers) == 1
    cust = config.customers[0]
    assert cust["name"] == "cust1"
    assert cust["input_type"] == "ftp"
    assert cust["output_endpoint"] == "https://example.com"
    assert cust["creds"]["host"] == "ftp.example.com"
    assert cust["creds"]["user"] == "user"
    assert cust["creds"]["pass"] == "pass"


def test_config_loading_local(monkeypatch):
    monkeypatch.setenv("CUSTOMERS", "cust2")
    monkeypatch.setenv("CUST2_INPUT_TYPE", "local")
    monkeypatch.setenv("CUST2_OUTPUT_ENDPOINT", "https://example.com")
    monkeypatch.setenv("CUST2_LOCAL_PATH", "/path/to/file.csv")

    config = Config()
    assert len(config.customers) == 1
    cust = config.customers[0]
    assert cust["name"] == "cust2"
    assert cust["input_type"] == "local"
    assert cust["output_endpoint"] == "https://example.com"
    assert cust["creds"]["path"] == "/path/to/file.csv"


def test_config_loading_specific_customer(monkeypatch):
    monkeypatch.setenv("CUSTOMERS", "cust1,cust2")
    monkeypatch.setenv("CUST1_INPUT_TYPE", "ftp")
    monkeypatch.setenv("CUST1_OUTPUT_ENDPOINT", "https://example1.com")
    monkeypatch.setenv("CUST1_FTP_HOST", "ftp.example1.com")
    monkeypatch.setenv("CUST2_INPUT_TYPE", "local")
    monkeypatch.setenv("CUST2_OUTPUT_ENDPOINT", "https://example2.com")
    monkeypatch.setenv("CUST2_LOCAL_PATH", "/path/to/file2.csv")

    config = Config(customer_name="cust1")
    assert len(config.customers) == 1
    cust = config.customers[0]
    assert cust["name"] == "cust1"
    assert cust["input_type"] == "ftp"
    assert cust["output_endpoint"] == "https://example1.com"
    assert cust["creds"]["host"] == "ftp.example1.com"


def test_config_loading_nonexistent_customer(monkeypatch):
    monkeypatch.setenv("CUSTOMERS", "cust1,cust2")
    monkeypatch.setenv("CUST1_INPUT_TYPE", "ftp")
    monkeypatch.setenv("CUST1_OUTPUT_ENDPOINT", "https://example1.com")

    config = Config(customer_name="cust3")
    assert len(config.customers) == 0
