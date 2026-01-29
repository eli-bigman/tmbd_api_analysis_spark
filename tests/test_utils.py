
import pytest
import os
import json
import logging
from src.utils.helpers import load_config, load_json, save_json

# Checking the actual file content: helpers.py has load_config, load_json, save_json, get_all_json_files, setup_logging.
# cleaning/udfs.py has sort_pipe_separated.

def test_load_config(config_path):
    """Test loading configuration from YAML."""
    config = load_config(config_path)
    assert 'api' in config
    assert config['api']['base_url'] == "https://api.themoviedb.org/3"

def test_save_and_load_json(tmp_path):
    """Test saving and loading JSON data."""
    test_data = {"key": "value", "number": 123}
    file_path = tmp_path / "test.json"
    
    # Test Save
    save_json(test_data, str(file_path))
    assert file_path.exists()
    
    # Test Load
    loaded_data = load_json(str(file_path))
    assert loaded_data == test_data

def test_setup_logging(config_path, caplog):
    """Test logging setup."""
    from src.utils.helpers import setup_logging
    
    logger = setup_logging(config_path, module_name="test_logger")
    # Enable propagation so caplog fixture can capture the logs
    logger.propagate = True
    
    with caplog.at_level(logging.INFO):
        logger.info("Test message")
        
    assert "Test message" in caplog.text
