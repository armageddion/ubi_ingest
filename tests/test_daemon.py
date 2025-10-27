import pytest
import sys
import builtins
import io
from unittest.mock import patch, MagicMock
from daemon import (
    parse_csv_data,
    push_to_api,
    process_customer,
    fetch_ftp,
    fetch_sftp,
    fetch_sql,
    fetch_local,
    run_daemon,
)
from types import SimpleNamespace


def test_parse_csv_data():
    csv_data = "name,age\nAlice,30\nBob,25"
    customer = {
        "header_row": "YES",
        "article_id": "1",
        "article_name": "2",
        "nfc_url": "",
        "ean1": "",
        "ean2": "",
        "ean3": "",
        "store_code": "",
        "item_id": "",
        "item_name": "",
        "item_description": "",
        "barcode": "",
        "sku": "",
        "list_price": "",
        "sale_price": "",
        "clearance_price": "",
        "unit_price": "",
        "pack_quantity": "",
        "weight": "",
        "weight_unit": "",
        "department": "",
        "aisle_location": "",
        "country_of_origin": "",
        "brand": "",
        "model": "",
        "color": "",
        "inventory": "",
        "start_date": "",
        "end_date": "",
        "language": "",
        "category_01": "",
        "category_02": "",
        "category_03": "",
        "misc_01": "",
        "misc_02": "",
        "misc_03": "",
        "display_page_1": "",
        "display_page_2": "",
        "display_page_3": "",
        "display_page_4": "",
        "display_page_5": "",
        "display_page_6": "",
        "display_page_7": "",
        "nfc_data": "",
    }
    result = parse_csv_data(csv_data, customer)
    assert len(result) == 2
    assert "articleId" in result[0]
    assert result[0]["articleId"] == "Alice"
    assert result[0]["articleName"] == "30"


@patch("builtins.exit")
@patch("daemon.requests.post")
def test_push_to_api(mock_post, mock_exit):
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {
        "responseMessage": {"access_token": "tok"}
    }
    customer = {
        "output_endpoint": "https://example.com",
        "output_user": "user",
        "output_pass": "pass",
        "store_name": "store",
        "company_name": "company",
        "name": "cust",
    }
    data = [{"articleId": "1"}]
    push_to_api(customer, data)
    # Check token request
    mock_post.assert_any_call(
        "https://example.com/common/api/v2/token",
        timeout=30,
        json={"username": "user", "password": "pass"},
    )
    # Check article post
    mock_post.assert_any_call(
        "https://example.com/common/api/v2/common/articles",
        headers={"Authorization": "Bearer tok"},
        params={"store": "store", "company": "company"},
        timeout=300,
        json=data,
    )


@patch("daemon.fetch_ftp")
@patch("daemon.parse_csv_data")
@patch("daemon.push_to_api")
def test_process_customer_ftp(mock_push, mock_parse, mock_fetch):
    mock_fetch.return_value = ("csv data", "/tmp/path")
    mock_parse.return_value = [{"data": "parsed"}]
    customer = {
        "name": "cust1",
        "input_type": "ftp",
        "creds": {"host": "ftp.example.com", "user": "user", "passw": "pass"},
        "output_endpoint": "https://example.com",
    }
    process_customer(customer)
    mock_fetch.assert_called_once_with(
        'cust1', host="ftp.example.com", user="user", passw="pass"
    )
    mock_parse.assert_called_once_with("csv data", customer)
    mock_push.assert_called_once_with(customer, [{"data": "parsed"}])


@patch("daemon.fetch_sftp")
@patch("daemon.parse_csv_data")
@patch("daemon.push_to_api")
def test_process_customer_sftp(mock_push, mock_parse, mock_fetch):
    mock_fetch.return_value = ("csv data", "/tmp/path")
    mock_parse.return_value = [{"data": "parsed"}]
    customer = {
        "name": "cust1",
        "input_type": "sftp",
        "creds": {
            "host": "sftp.example.com",
            "user": "user",
            "passw": "pass",
            "key_path": None,
        },
        "output_endpoint": "https://example.com",
    }
    process_customer(customer)
    mock_fetch.assert_called_once_with(
        'cust1', host="sftp.example.com", user="user", passw="pass", key_path=None
    )
    mock_parse.assert_called_once_with("csv data", customer)
    mock_push.assert_called_once_with(customer, [{"data": "parsed"}])


@patch("daemon.fetch_sql")
@patch("daemon.parse_csv_data")
@patch("daemon.push_to_api")
def test_process_customer_sql(mock_push, mock_parse, mock_fetch):
    mock_fetch.return_value = ("csv data", None)
    mock_parse.return_value = [{"data": "parsed"}]
    customer = {
        "name": "cust1",
        "input_type": "sql",
        "creds": {
            "host": "sql.example.com",
            "user": "user",
            "passw": "pass",
            "db": "db",
            "query": "SELECT *",
        },
        "output_endpoint": "https://example.com",
    }
    process_customer(customer)
    mock_fetch.assert_called_once_with(
        host="sql.example.com",
        user="user",
        passw="pass",
        db="db",
        query="SELECT *",
    )
    mock_parse.assert_called_once_with("csv data", customer)
    mock_push.assert_called_once_with(customer, [{"data": "parsed"}])


@patch("daemon.fetch_local")
@patch("daemon.parse_csv_data")
@patch("daemon.push_to_api")
def test_process_customer_local(mock_push, mock_parse, mock_fetch):
    mock_fetch.return_value = ("csv data", "/path/to/file.csv")
    mock_parse.return_value = [{"data": "parsed"}]
    customer = {
        "name": "cust1",
        "input_type": "local",
        "creds": {"path": "/path/to/file.csv"},
        "output_endpoint": "https://example.com",
    }
    process_customer(customer)
    mock_fetch.assert_called_once_with(path="/path/to/file.csv")
    mock_parse.assert_called_once_with("csv data", customer)
    mock_push.assert_called_once_with(customer, [{"data": "parsed"}])


@patch("daemon.ftplib.FTP")
def test_fetch_ftp(mock_ftp):
    mock_ftp_instance = MagicMock()
    mock_ftp.return_value = mock_ftp_instance
    mock_ftp_instance.nlst.return_value = ["test.csv"]
    mock_ftp_instance.voidcmd.return_value = "213 20231027120000"
    mock_ftp_instance.retrbinary.return_value = None
    mock_ftp_instance.quit.return_value = None

    # Mock the data
    data = io.BytesIO(b"test,csv\ndata")
    mock_ftp_instance.retrbinary = MagicMock(side_effect=lambda cmd, callback: callback(data.getvalue()))

    result = fetch_ftp("test_customer", "host", "user", "pass")
    assert result[0] == "test,csv\ndata"
    mock_ftp.assert_called_once_with("host")
    mock_ftp_instance.login.assert_called_once_with("user", "pass")
    mock_ftp_instance.retrbinary.assert_called_once()
    mock_ftp_instance.quit.assert_called_once()


@patch("daemon.paramiko.SSHClient")
@patch("daemon.os.path.expanduser")
def test_fetch_sftp(mock_expanduser, mock_ssh):
    mock_expanduser.return_value = "/home/user/.ssh/known_hosts"
    mock_ssh_instance = MagicMock()
    mock_ssh.return_value = mock_ssh_instance
    mock_sftp = MagicMock()
    mock_ssh_instance.open_sftp.return_value = mock_sftp
    mock_file = MagicMock()
    mock_file.read.return_value = b"test,csv\ndata"
    mock_sftp.open.return_value.__enter__.return_value = mock_file
    mock_sftp.open.return_value.__exit__.return_value = None
    # Mock listdir_attr to return a file with mtime
    mock_attr = MagicMock()
    mock_attr.filename = "test.csv"
    mock_attr.st_mtime = 123456
    mock_sftp.listdir_attr.return_value = [mock_attr]

    result = fetch_sftp("test_customer", "host", "user", "pass", None)
    assert result[0] == "test,csv\ndata"
    mock_ssh.assert_called_once()
    mock_ssh_instance.load_host_keys.assert_called_once_with("/home/user/.ssh/known_hosts")
    mock_ssh_instance.set_missing_host_key_policy.assert_called_once()
    mock_ssh_instance.connect.assert_called_once_with("host", username="user", password="pass")
    mock_sftp.open.assert_called_once_with("/test.csv", "r")
    mock_file.read.assert_called_once()
    mock_ssh_instance.close.assert_called_once()


@patch("daemon.pymysql.connect")
def test_fetch_sql(mock_connect):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [{"col1": "val1", "col2": "val2"}]

    result = fetch_sql("host", "user", "pass", "db", "SELECT *")
    assert "col1,col2" in result[0]
    assert "val1,val2" in result[0]
    mock_connect.assert_called_once_with(host="host", user="user", password="pass", database="db")
    mock_cursor.execute.assert_called_once_with("SELECT *")
    mock_conn.close.assert_called_once()


def test_fetch_local(tmp_path):
    test_file = tmp_path / "test.csv"
    test_file.write_text("test,csv\ndata")
    result = fetch_local(str(test_file))
    assert result[0] == "test,csv\ndata"


@patch("daemon.time.sleep")
@patch("daemon.schedule.run_pending")
@patch("daemon.schedule.get_jobs")
@patch("daemon.process_customer")
def test_run_daemon(mock_process, mock_get_jobs, mock_run_pending, mock_sleep):
    mock_get_jobs.return_value = ["job1"]

    # Mock run_pending to call process_customer on first call, then raise
    call_count = 0
    def mock_pending():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            for customer in config.customers:
                mock_process(customer)
        else:
            raise KeyboardInterrupt()
    mock_run_pending.side_effect = mock_pending

    config = MagicMock()
    config.customers = [{"name": "cust1"}]

    with pytest.raises(KeyboardInterrupt):
        run_daemon(config)

    mock_process.assert_called_with({"name": "cust1"})


@patch("daemon.fetch_local")
@patch("daemon.parse_csv_data")
@patch("daemon.push_to_api")
def test_process_customer_with_parser(mock_push, mock_parse, mock_fetch):
    import os
    from unittest.mock import patch

    mock_fetch.return_value = ("original csv", "/some/path")
    mock_parse.return_value = [{"data": "parsed"}]

    with patch("daemon.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="parsed csv")
        with patch.dict(os.environ, {"CUST1_LOCAL_PATH": "/path/to/xml"}):
            customer = {
                "name": "cust1",
                "input_type": "local",
                "creds": {"path": "/path/to/file"},
                "input_parser": "xml_to_csv",
                "output_endpoint": "https://example.com",
            }
            process_customer(customer)
            mock_run.assert_called_once_with(
                ["python", "utils/xml_to_csv.py", "/path/to/xml"],
                capture_output=True,
                text=True,
            )
            mock_parse.assert_called_once_with("parsed csv", customer)
            mock_push.assert_called_once()


@patch("daemon.fetch_local")
@patch("daemon.parse_csv_data")
@patch("daemon.push_to_api")
def test_process_customer_parser_error(mock_push, mock_parse, mock_fetch):
    import os
    from unittest.mock import patch

    mock_fetch.return_value = ("original csv", "/some/path")

    with patch("daemon.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=1, stderr="error")
        with patch.dict(os.environ, {"CUST1_LOCAL_PATH": "/path/to/xml"}):
            customer = {
                "name": "cust1",
                "input_type": "local",
                "creds": {"path": "/path/to/file"},
                "input_parser": "xml_to_csv",
                "output_endpoint": "https://example.com",
            }
            process_customer(customer)
            mock_run.assert_called_once()
            mock_parse.assert_not_called()
            mock_push.assert_not_called()


@patch("daemon.parse_csv_data")
@patch("daemon.push_to_api")
def test_process_customer_unknown_input_type(mock_push, mock_parse):
    customer = {
        "name": "cust1",
        "input_type": "unknown",
        "creds": {},
        "output_endpoint": "https://example.com",
    }
    process_customer(customer)
    mock_parse.assert_not_called()
    mock_push.assert_not_called()


@patch("daemon.parse_csv_data")
@patch("daemon.push_to_api")
def test_process_customer_exception(mock_push, mock_parse):
    mock_parse.side_effect = Exception("parse error")
    customer = {
        "name": "cust1",
        "input_type": "local",
        "creds": {"path": "/path"},
        "output_endpoint": "https://example.com",
    }
    process_customer(customer)
    mock_push.assert_not_called()


@patch("builtins.exit")
@patch("daemon.requests.post")
def test_push_to_api_chunking(mock_post, mock_exit):
    mock_post.side_effect = [
        MagicMock(status_code=200, json=MagicMock(return_value={"responseMessage": {"access_token": "tok"}})),
        MagicMock(status_code=201, json=MagicMock(return_value={"ok": True})),
        MagicMock(status_code=201, json=MagicMock(return_value={"ok": True})),
    ]

    customer = {
        "output_endpoint": "https://example.com",
        "output_user": "user",
        "output_pass": "pass",
        "store_name": "store",
        "company_name": "company",
        "name": "cust",
    }
    data = [{"articleId": str(i)} for i in range(1500)]  # More than 1000

    push_to_api(customer, data)

    # Should have 2 article posts (chunks of 1000, then 500)
    article_calls = [call for call in mock_post.call_args_list if "/common/articles" in call[0][0]]
    assert len(article_calls) == 2
    assert len(article_calls[0][1]["json"]) == 1000
    assert len(article_calls[1][1]["json"]) == 500


def test_push_to_api_includes_customer_params(monkeypatch):
    monkeypatch.setattr(builtins, "exit", lambda: None)
    import daemon

    calls = []

    class MockResp:
        def __init__(self, status_code, json_data):
            self.status_code = status_code
            self._json = json_data

        def json(self):
            return self._json

    def mock_post(url, headers=None, params=None, timeout=None, json=None):
        calls.append(
            {"url": url, "headers": headers, "params": params, "json": json}
        )
        if url.endswith("/common/api/v2/token"):
            return MockResp(200, {"responseMessage": {"access_token": "tok"}})
        if url.endswith("/common/api/v2/common/articles"):
            return MockResp(201, {"ok": True})
        return MockResp(404, {})

    monkeypatch.setattr(daemon, "requests", SimpleNamespace(post=mock_post))

    customer = {
        "output_endpoint": "https://example.com",
        "output_user": "user",
        "output_pass": "pass",
        "name": "cust",
        "store_name": "MyStore",
        "company_name": "MyCompany",
    }
    data = [{"articleId": "1"}]

    daemon.push_to_api(customer, data)

    article_calls = [
        c for c in calls if c["url"].endswith("/common/api/v2/common/articles")
    ]
    assert len(article_calls) == 1
    assert article_calls[0]["params"] == {
        "store": "MyStore",
        "company": "MyCompany",
    }
