"""
Phase 1 Tests: Project Structure & Core Utilities

Tests for Spark utilities, configuration loading, and test fixtures.
"""

import pytest
import sys
from pathlib import Path


# Add src directory to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))


class TestSparkUtils:
    """Test Spark utility functions"""
    
    def test_get_spark_session_creation(self, config_path):
        """Test that SparkSession can be created from config"""
        from src.utils.spark_utils import get_spark_session
        
        spark = get_spark_session(config_path=config_path)
        assert spark is not None
        assert spark.version is not None
        
    def test_spark_session_singleton(self, config_path):
        """Test that SparkSession follows singleton pattern"""
        from src.utils.spark_utils import get_spark_session
        
        session1 = get_spark_session(config_path=config_path)
        session2 = get_spark_session(config_path=config_path)
        
        # Should be the same instance
        assert session1 == session2
        assert session1._jsc == session2._jsc
        
    def test_spark_session_has_correct_app_name(self, config_path):
        """Test that SparkSession has correct application name"""
        from src.utils.spark_utils import get_spark_session
        
        spark = get_spark_session(app_name="TestApp", config_path=config_path)
        assert "TestApp" in spark.sparkContext.appName or spark.sparkContext.appName == "TMDB_Tests"
        
    def test_spark_context_accessible(self, config_path):
        """Test that SparkContext is accessible from session"""
        from src.utils.spark_utils import get_spark_session, get_spark_context
        
        spark = get_spark_session(config_path=config_path)
        sc = get_spark_context(spark)
        
        assert sc is not None
        assert sc.master == spark.sparkContext.master


class TestHelpers:
    """Test helper utility functions"""
    
    def test_load_config(self, config_path):
        """Test configuration file loading"""
        from src.utils.helpers import load_config
        
        config = load_config(config_path)
        
        assert config is not None
        assert isinstance(config, dict)
        assert 'api' in config
        assert 'paths' in config
        assert 'spark' in config
        
    def test_config_has_spark_settings(self, config_path):
        """Test that config contains Spark-specific settings"""
        from src.utils.helpers import load_config
        
        config = load_config(config_path)
        spark_config = config.get('spark', {})
        
        assert 'master' in spark_config
        assert 'app.name' in spark_config
        assert spark_config['master'] == 'local[*]'
        assert spark_config['app.name'] == 'TMDB_Analysis'
        
    def test_load_json(self, tmp_path):
        """Test JSON file loading"""
        from src.utils.helpers import load_json, save_json
        
        # Create temporary JSON file
        test_data = {"test": "data", "number": 42}
        test_file = tmp_path / "test.json"
        save_json(test_data, str(test_file))
        
        # Load it back
        loaded_data = load_json(str(test_file))
        
        assert loaded_data == test_data
        
    def test_save_json_creates_directories(self, tmp_path):
        """Test that save_json creates parent directories"""
        from src.utils.helpers import save_json
        
        test_data = {"nested": "data"}
        test_file = tmp_path / "nested" / "dir" / "test.json"
        
        save_json(test_data, str(test_file))
        
        assert test_file.exists()
        assert test_file.parent.exists()


class TestFixtures:
    """Test that pytest fixtures work correctly"""
    
    def test_spark_fixture(self, spark):
        """Test that spark fixture provides valid session"""
        assert spark is not None
        assert hasattr(spark, 'read')
        assert hasattr(spark, 'createDataFrame')
        
    def test_sample_movie_data_fixture(self, sample_movie_data):
        """Test that sample movie data fixture works"""
        assert sample_movie_data is not None
        assert sample_movie_data.count() > 0
        
        # Check expected columns exist
        columns = sample_movie_data.columns
        assert 'id' in columns
        assert 'title' in columns
        assert 'budget' in columns
        assert 'revenue' in columns
        assert 'genres' in columns
        
    def test_sample_movie_data_schema(self, sample_movie_data):
        """Test that sample movie data has correct schema"""
        schema_dict = {field.name: field.dataType.typeName() 
                      for field in sample_movie_data.schema.fields}
        
        assert schema_dict['id'] == 'integer'
        assert schema_dict['title'] == 'string'
        assert schema_dict['budget'] == 'integer'
        assert schema_dict['revenue'] == 'integer'
        assert schema_dict['vote_average'] == 'double'
        
    def test_sample_json_data_fixture(self, sample_json_data):
        """Test that sample JSON data fixture works"""
        assert sample_json_data is not None
        assert 'id' in sample_json_data
        assert 'title' in sample_json_data
        assert 'genres' in sample_json_data
        assert isinstance(sample_json_data['genres'], list)


class TestSparkOperations:
    """Test basic Spark operations work in test environment"""
    
    def test_dataframe_creation(self, spark):
        """Test creating DataFrame from list"""
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        assert df.count() == 3
        assert len(df.columns) == 2
        
    def test_dataframe_operations(self, sample_movie_data):
        """Test basic DataFrame operations"""
        from pyspark.sql import functions as F
        
        # Filter
        high_budget = sample_movie_data.filter(F.col('budget') > 100000000)
        assert high_budget.count() > 0
        
        # Select
        titles = sample_movie_data.select('title')
        assert titles.count() == sample_movie_data.count()
        
        # Sort
        sorted_df = sample_movie_data.orderBy(F.col('revenue').desc())
        first_row = sorted_df.first()
        assert first_row is not None


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
