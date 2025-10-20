import pytest
from unittest.mock import patch, MagicMock
from daemon import parse_csv_data, push_to_api, process_customer

def test_parse_csv_data():
    csv_data = "name,age\nAlice,30\nBob,25"
    result = parse_csv_data(csv_data)
    assert result == [{'name': 'Alice', 'age': '30'}, {'name': 'Bob', 'age': '25'}]

@patch('daemon.requests.post')
def test_push_to_api(mock_post):
    mock_post.return_value.status_code = 200
    push_to_api('https://example.com', [{'data': 'test'}])
    mock_post.assert_called_once_with('https://example.com', json=[{'data': 'test'}])

@patch('daemon.fetch_ftp')
@patch('daemon.parse_csv_data')
@patch('daemon.push_to_api')
def test_process_customer_ftp(mock_push, mock_parse, mock_fetch):
    mock_fetch.return_value = "csv data"
    mock_parse.return_value = [{'data': 'parsed'}]
    customer = {
        'name': 'cust1',
        'input_type': 'ftp',
        'creds': {'host': 'ftp.example.com', 'user': 'user', 'pass': 'pass'},
        'output_endpoint': 'https://example.com'
    }
    process_customer(customer)
    mock_fetch.assert_called_once_with(host='ftp.example.com', user='user', passw='pass')
    mock_parse.assert_called_once_with("csv data")
    mock_push.assert_called_once_with('https://example.com', [{'data': 'parsed'}])

# Similar tests for sftp and sql