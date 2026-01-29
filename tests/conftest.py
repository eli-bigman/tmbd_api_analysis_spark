
import pytest
import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession

# Add src to path so we can import modules
sys.path.append(str(Path(__file__).parent.parent))

@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for testing.
    Scope is session so we only create it once.
    """
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("TMDB_Analysis_Tests")
             .config("spark.driver.host", "localhost")
             .getOrCreate())
    
    # Set log level to WARN to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    yield spark
    
    spark.stop()

@pytest.fixture(scope="session")
def config_path(tmp_path_factory):
    """
    Create a dummy config file for testing.
    """
    config_dir = tmp_path_factory.mktemp("config")
    config_file = config_dir / "config.yaml"
    
    content = """
    api:
      base_url: "https://api.themoviedb.org/3"
      timeout: 10
      rate_limit_delay: 0.1
      
    paths:
      raw_data: "data/raw_test"
      processed_data: "data/processed_test"
      
    spark:
      driver.memory: "2g"
    
    logging:
      level: "DEBUG"
      log_to_console: true
      log_to_file: false
    """
    
    with open(config_file, "w") as f:
        f.write(content)
        
    return str(config_file)
