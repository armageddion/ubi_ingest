import pytest
import sys
import builtins
from unittest.mock import patch, MagicMock
from daemon import parse_csv_data, push_to_api, process_customer
from types import SimpleNamespace

def test_parse_csv_data():
    csv_data = "name,age\nAlice,30\nBob,25"
    customer = {
        'header_row': 'YES',
        'article_id': '1',
        'article_name': '2',
        'nfc_url': '',
        'ean1': '',
        'ean2': '',
        'ean3': '',
        'store_code': '',
        'item_id': '',
        'item_name': '',
        'item_description': '',
        'barcode': '',
        'sku': '',
        'list_price': '',
        'sale_price': '',
        'clearance_price': '',
        'unit_price': '',
        'pack_quantity': '',
        'weight': '',
        'weight_unit': '',
        'department': '',
        'aisle_location': '',
        'country_of_origin': '',
        'brand': '',
        'model': '',
        'color': '',
        'inventory': '',
        'start_date': '',
        'end_date': '',
        'language': '',
        'category_01': '',
        'category_02': '',
        'category_03': '',
        'misc_01': '',
        'misc_02': '',
        'misc_03': '',
        'display_page_1': '',
        'display_page_2': '',
        'display_page_3': '',
        'display_page_4': '',
        'display_page_5': '',
        'display_page_6': '',
        'display_page_7': '',
        'nfc_data': ''
    }
    result = parse_csv_data(csv_data, customer)
    assert len(result) == 2
    assert 'articleId' in result[0]
    assert result[0]['articleId'] == 'Alice'
    assert result[0]['articleName'] == '30'

@patch('builtins.exit')
@patch('daemon.requests.post')
def test_push_to_api(mock_post, mock_exit):
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {'responseMessage': {'access_token': 'tok'}}
    customer = {
        'output_endpoint': 'https://example.com',
        'output_user': 'user',
        'output_pass': 'pass',
        'store_name': 'store',
        'company_name': 'company',
        'name': 'cust'
    }
    data = [{'articleId': '1'}]
    push_to_api(customer, data)
    # Check token request
    mock_post.assert_any_call('https://example.com/common/api/v2/token',
                              timeout=30,
                              json={'username': 'user', 'password': 'pass'})
    # Check article post
    mock_post.assert_any_call('https://example.com/common/api/v2/articles',
                              headers={'Authorization': 'Bearer tok'},
                              params={'store': 'store', 'company': 'company'},
                              timeout=30,
                              json=data)

@patch('daemon.fetch_ftp')
@patch('daemon.parse_csv_data')
@patch('daemon.push_to_api')
def test_process_customer_ftp(mock_push, mock_parse, mock_fetch):
    mock_fetch.return_value = "csv data"
    mock_parse.return_value = [{'data': 'parsed'}]
    customer = {
        'name': 'cust1',
        'input_type': 'ftp',
        'creds': {'host': 'ftp.example.com', 'user': 'user', 'passw': 'pass'},
        'output_endpoint': 'https://example.com'
    }
    process_customer(customer)
    mock_fetch.assert_called_once_with(host='ftp.example.com', user='user', passw='pass')
    mock_parse.assert_called_once_with("csv data", customer)
    mock_push.assert_called_once_with(customer, [{'data': 'parsed'}])

@patch('daemon.fetch_sftp')
@patch('daemon.parse_csv_data')
@patch('daemon.push_to_api')
def test_process_customer_sftp(mock_push, mock_parse, mock_fetch):
    mock_fetch.return_value = "csv data"
    mock_parse.return_value = [{'data': 'parsed'}]
    customer = {
        'name': 'cust1',
        'input_type': 'sftp',
        'creds': {'host': 'sftp.example.com', 'user': 'user', 'passw': 'pass', 'key_path': None},
        'output_endpoint': 'https://example.com'
    }
    process_customer(customer)
    mock_fetch.assert_called_once_with(host='sftp.example.com', user='user', passw='pass', key_path=None)
    mock_parse.assert_called_once_with("csv data", customer)
    mock_push.assert_called_once_with(customer, [{'data': 'parsed'}])

@patch('daemon.fetch_sql')
@patch('daemon.parse_csv_data')
@patch('daemon.push_to_api')
def test_process_customer_sql(mock_push, mock_parse, mock_fetch):
    mock_fetch.return_value = "csv data"
    mock_parse.return_value = [{'data': 'parsed'}]
    customer = {
        'name': 'cust1',
        'input_type': 'sql',
        'creds': {'host': 'sql.example.com', 'user': 'user', 'passw': 'pass', 'db': 'db', 'query': 'SELECT *'},
        'output_endpoint': 'https://example.com'
    }
    process_customer(customer)
    mock_fetch.assert_called_once_with(host='sql.example.com', user='user', passw='pass', db='db', query='SELECT *')
    mock_parse.assert_called_once_with("csv data", customer)
    mock_push.assert_called_once_with(customer, [{'data': 'parsed'}])

@patch('daemon.fetch_local')
@patch('daemon.parse_csv_data')
@patch('daemon.push_to_api')
def test_process_customer_local(mock_push, mock_parse, mock_fetch):
    mock_fetch.return_value = "csv data"
    mock_parse.return_value = [{'data': 'parsed'}]
    customer = {
        'name': 'cust1',
        'input_type': 'local',
        'creds': {'path': '/path/to/file.csv'},
        'output_endpoint': 'https://example.com'
    }
    process_customer(customer)
    mock_fetch.assert_called_once_with(path='/path/to/file.csv')
    mock_parse.assert_called_once_with("csv data", customer)
    mock_push.assert_called_once_with(customer, [{'data': 'parsed'}])

def test_push_to_api_includes_customer_params(monkeypatch):
    monkeypatch.setattr(builtins, 'exit', lambda: None)
    import daemon

    calls = []

    class MockResp:
        def __init__(self, status_code, json_data):
            self.status_code = status_code
            self._json = json_data
        def json(self):
            return self._json

    def mock_post(url, headers=None, params=None, timeout=None, json=None):
        calls.append({'url': url, 'headers': headers, 'params': params, 'json': json})
        if url.endswith('/common/api/v2/token'):
            return MockResp(200, {'responseMessage': {'access_token': 'tok'}})
        if url.endswith('/common/api/v2/articles'):
            return MockResp(201, {'ok': True})
        return MockResp(404, {})

    monkeypatch.setattr(daemon, 'requests', SimpleNamespace(post=mock_post))

    customer = {
        'output_endpoint': 'https://example.com',
        'output_user': 'user',
        'output_pass': 'pass',
        'name': 'cust',
        'store_name': 'MyStore',
        'company_name': 'MyCompany'
    }
    data = [{'articleId': '1'}]

    daemon.push_to_api(customer, data)

    article_calls = [c for c in calls if c['url'].endswith('/common/api/v2/articles')]
    assert len(article_calls) == 1
    assert article_calls[0]['params'] == {'store': 'MyStore', 'company': 'MyCompany'}