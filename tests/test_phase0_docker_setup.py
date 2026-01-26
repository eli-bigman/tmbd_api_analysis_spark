"""
Phase 0 Tests: Docker Environment Setup

Tests to verify Docker environment is properly configured.
"""

import pytest
import os
import sys
from pathlib import Path


# Add src directory to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))


class TestDockerSetup:
    """Test Docker environment setup"""
    
    def test_spark_session_creation(self):
        """Test SparkSession can be created in Docker environment"""
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder.appName("test").getOrCreate()
        assert spark is not None
        assert spark.version is not None
        
        # Cleanup
        spark.stop()
    
    def test_spark_version(self):
        """Test Spark version is accessible"""
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder.appName("test").getOrCreate()
        version = spark.version
        
        assert version is not None
        assert len(version) > 0
        
        spark.stop()


class TestEnvironmentVariables:
    """Test environment variables and config"""
    
    def test_env_file_exists(self):
        """Test .env file exists in project root"""
        env_path = project_root / ".env"
        
        # Either .env should exist or .env.example should exist
        assert env_path.exists() or (project_root / ".env.example").exists(), \
            "Either .env or .env.example should exist"
    
    def test_dotenv_loading(self):
        """Test python-dotenv package is available"""
        try:
            from dotenv import load_dotenv
            assert True
        except ImportError:
            pytest.fail("python-dotenv package not installed")
    
    def test_env_variables_loaded(self):
        """Test environment variables can be loaded"""
        from dotenv import load_dotenv
        
        # Try to load from .env file
        env_path = project_root / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            # Just verify the function works, API key might not be set yet
            assert True
        else:
            pytest.skip(".env file not present")


class TestVolumeMounts:
    """Test Docker volume mounts (only runs in Docker)"""
    
    def test_required_directories_exist(self):
        """Test all required directories are accessible"""
        required_dirs = [
            project_root / "data",
            project_root / "src",
            project_root / "notebooks",
            project_root / "config",
            project_root / "tests",
        ]
        
        for directory in required_dirs:
            assert directory.exists(), f"Directory {directory} should exist"
    
    def test_config_file_accessible(self):
        """Test config.yaml is accessible"""
        config_path = project_root / "config" / "config.yaml"
        assert config_path.exists(), "config/config.yaml should exist"
    
    def test_data_directories_writable(self):
        """Test data directories are writable"""
        test_file = project_root / "data" / "raw" / ".test_write"
        
        try:
            test_file.write_text("test")
            assert test_file.exists()
            test_file.unlink()  # Clean up
        except Exception as e:
            pytest.fail(f"Data directory not writable: {e}")


class TestDependencies:
    """Test required Python packages are installed"""
    
    def test_pyspark_available(self):
        """Test PySpark is installed"""
        try:
            import pyspark
            assert True
        except ImportError:
            pytest.fail("PySpark not installed")
    
    def test_pytest_available(self):
        """Test pytest is installed"""
        # If this test runs, pytest is available :)
        assert True
    
    def test_yaml_available(self):
        """Test PyYAML is installed"""
        try:
            import yaml
            assert True
        except ImportError:
            pytest.fail("PyYAML not installed")
    
    def test_requests_available(self):
        """Test requests is installed"""
        try:
            import requests
            assert True
        except ImportError:
            pytest.fail("requests not installed")
    
    def test_tqdm_available(self):
        """Test tqdm is installed"""
        try:
            import tqdm
            assert True
        except ImportError:
            pytest.fail("tqdm not installed")


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
