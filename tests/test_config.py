import os
import pytest
from config import Config

def test_config_loading(monkeypatch):
    monkeypatch.setenv('CUSTOMERS', 'cust1')
    monkeypatch.setenv('CUST1_INPUT_TYPE', 'ftp')
    monkeypatch.setenv('CUST1_OUTPUT_ENDPOINT', 'https://example.com')
    monkeypatch.setenv('CUST1_FTP_HOST', 'ftp.example.com')
    monkeypatch.setenv('CUST1_FTP_USER', 'user')
    monkeypatch.setenv('CUST1_FTP_PASS', 'pass')
    
    config = Config()
    assert len(config.customers) == 1
    cust = config.customers[0]
    assert cust['name'] == 'cust1'
    assert cust['input_type'] == 'ftp'
    assert cust['output_endpoint'] == 'https://example.com'
    assert cust['creds']['host'] == 'ftp.example.com'
    assert cust['creds']['user'] == 'user'
    assert cust['creds']['pass'] == 'pass'