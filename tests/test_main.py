import pytest
from unittest.mock import patch
from main import main


@patch("main.load_dotenv")
@patch("main.Config")
@patch("main.run_daemon")
def test_main_with_args(mock_run, mock_config, mock_load):
    # Mock sys.argv
    import sys

    sys.argv = ["main.py", "--ftp", "--config", "test.env"]

    # Set up mock config attributes
    mock_config.return_value.log_level = "INFO"
    mock_config.return_value.log_file = "/var/log/ubi_ingest.log"

    main()

    mock_load.assert_called_once_with("test.env")
    mock_config.assert_called_once()
    # Ensure run_daemon was called with the Config instance
    mock_run.assert_called_once_with(mock_config.return_value)


@patch("main.load_dotenv")
@patch("main.Config")
@patch("main.run_daemon")
def test_main_default_config(mock_run, mock_config, mock_load):
    import sys
    sys.argv = ["main.py"]

    mock_config.return_value.log_level = "INFO"
    mock_config.return_value.log_file = "/var/log/ubi_ingest.log"

    main()

    mock_load.assert_called_once_with()
    mock_config.assert_called_once_with(customer_name=None)
    mock_run.assert_called_once_with(mock_config.return_value)


@patch("main.load_dotenv")
@patch("main.Config")
@patch("main.run_daemon")
def test_main_with_customer_filter(mock_run, mock_config, mock_load):
    import sys
    sys.argv = ["main.py", "--customer", "cust1"]

    mock_config.return_value.log_level = "INFO"
    mock_config.return_value.log_file = "/var/log/ubi_ingest.log"

    main()

    mock_config.assert_called_once_with(customer_name="cust1")
    mock_run.assert_called_once_with(mock_config.return_value)


@patch("main.load_dotenv")
@patch("main.Config")
@patch("main.run_daemon")
def test_main_with_type_filters(mock_run, mock_config, mock_load):
    import sys
    sys.argv = ["main.py", "--ftp", "--sftp"]

    mock_config_instance = mock_config.return_value
    mock_config_instance.log_level = "INFO"
    mock_config_instance.log_file = "/var/log/ubi_ingest.log"
    mock_config_instance.customers = [
        {"name": "cust1", "input_type": "ftp"},
        {"name": "cust2", "input_type": "sftp"},
        {"name": "cust3", "input_type": "sql"},
    ]

    main()

    # Check that customers were filtered
    assert mock_config_instance.customers == [
        {"name": "cust1", "input_type": "ftp"},
        {"name": "cust2", "input_type": "sftp"},
    ]
    mock_run.assert_called_once_with(mock_config_instance)


@patch("main.load_dotenv")
@patch("main.logging.basicConfig")
@patch("main.Config")
@patch("main.run_daemon")
def test_main_logging_setup(mock_run, mock_config, mock_logging, mock_load):
    import sys
    sys.argv = ["main.py"]

    mock_config.return_value.log_level = "DEBUG"
    mock_config.return_value.log_file = "/tmp/test.log"

    main()

    mock_logging.assert_called_once_with(
        level=10,  # DEBUG level
        filename="/tmp/test.log",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
