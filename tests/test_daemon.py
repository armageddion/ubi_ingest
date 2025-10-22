import pytest
from unittest.mock import patch, MagicMock
from daemon import parse_csv_data, push_to_api, process_customer
from types import SimpleNamespace

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
    mock_parse.assert_called_once_with("csv data")
    mock_push.assert_called_once_with(customer, [{'data': 'parsed'}])

def test_push_to_api_includes_ubi_params(monkeypatch):
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
        # existing code may ignore these, ensure they exist
        'store_name': 'ignored',
        'company_name': 'ignored'
    }
    data = [{'articleId': '1'}]

    daemon.push_to_api(customer, data)

    article_calls = [c for c in calls if c['url'].endswith('/common/api/v2/articles')]
    assert len(article_calls) == 1
    assert article_calls[0]['params'] == {'store': 'UBI', 'company': 'UBI'}

# Similar tests for sftp and sql