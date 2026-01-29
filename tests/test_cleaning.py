

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, StructType

from src.cleaning.cleaner import SparkMovieDataCleaner

# Use collections.namedtuple for easy UDF input mocking if Row is not enough, 
# but Row is usually sufficient for Spark.

class TestCleaner:

    """Test SparkMovieDataCleaner class."""
    
    @pytest.fixture
    def sample_data(self, spark):
        """Create sample raw movie data."""
        return spark.createDataFrame([
            {
                "id": 1,
                "title": "Test Movie",
                "adult": False,
                "revenue": 1000000,
                "budget": 500000,
                "genres": [{"id": 28, "name": "Action"}],
                "belongs_to_collection": {"name": "Test Collection"},
                "release_date": "2023-01-01",
                "status": "Released"
            },
            {
                "id": 2,
                "title": "Bad Movie",
                "adult": False,
                "revenue": 0,
                "budget": 0,
                "genres": [],
                "belongs_to_collection": None,
                "release_date": "2023-01-02",
                "status": "Released"
            }
        ])

    def test_drop_irrelevant_columns(self, spark, sample_data):
        cleaner = SparkMovieDataCleaner(spark)
        # Add a column that should be dropped
        df = sample_data.withColumn("imdb_id", F.lit("tt1234567"))
        
        cleaned = cleaner.drop_irrelevant_columns(df)
        assert "imdb_id" not in cleaned.columns
        assert "adult" not in cleaned.columns # This is in the drop list
        assert "title" in cleaned.columns

    def test_flatten_nested_columns(self, spark, sample_data):
        cleaner = SparkMovieDataCleaner(spark)
        flattened = cleaner.flatten_nested_columns(sample_data)
        
        rows = flattened.collect()
        assert rows[0]["collection_name"] == "Test Collection"
        assert rows[0]["genres"] == "Action"
        assert rows[1]["collection_name"] is None

    def test_clean_datatypes(self, spark, sample_data):
        cleaner = SparkMovieDataCleaner(spark)
        cleaned = cleaner.clean_datatypes(sample_data)
        
        # Check type conversion
        schema = dict(cleaned.dtypes)
        assert schema['revenue'] == 'bigint' # long
        assert schema['budget'] == 'bigint' # long
        
        # Check MUSD calculation
        rows = cleaned.collect()
        assert rows[0]['revenue_musd'] == 1.0 # 1000000 / 1000000
        
        # Check zero handling (should be null)
        assert rows[1]['revenue'] is None
        assert rows[1]['budget'] is None
    
    def test_filter_data(self, spark):
        cleaner = SparkMovieDataCleaner(spark)
        
        data = [
            {"id": 1, "title": "Good", "status": "Released"},
            {"id": 2, "title": "Rumored", "status": "Rumored"}, # Should be removed
            {"id": 1, "title": "Duplicate", "status": "Released"}, # Duplicate ID
            {"id": None, "title": "No ID", "status": "Released"} # Missing ID
        ]
        df = spark.createDataFrame(data)
        
        filtered = cleaner.filter_data(df)
        assert filtered.count() == 1
        assert filtered.first()['id'] == 1
