import pytest
from unittest.mock import patch
from main import main

@patch('main.load_dotenv')
@patch('main.Config')
@patch('main.run_daemon')
def test_main_with_args(mock_run, mock_config, mock_load):
    # Mock sys.argv
    import sys
    sys.argv = ['main.py', '--ftp', '--config', 'test.env']
    
    main()
    
    mock_load.assert_called_once_with('test.env')
    mock_config.assert_called_once()
    # Ensure run_daemon was called with the Config instance
    mock_run.assert_called_once_with(mock_config.return_value)