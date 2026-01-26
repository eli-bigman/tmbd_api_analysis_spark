"""
Pytest configuration and fixtures for TMDB Spark tests.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import sys
from pathlib import Path


# Add src directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))


@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for testing.
    
    This fixture creates a Spark session once per test session and tears it down
    after all tests complete.
    
    Yields:
        SparkSession: Configured Spark session for tests
    """
    spark = SparkSession.builder \
        .appName("TMDB_Tests") \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", 4) \
        .getOrCreate()
    
    # Set log level to reduce noise during tests
    spark.sparkContext.setLogLevel("WARN")
    
    yield spark
    
    # Cleanup
    spark.stop()


@pytest.fixture(scope="function")
def sample_movie_data(spark):
    """
    Create sample movie data for testing.
    
    Args:
        spark: SparkSession fixture
        
    Returns:
        DataFrame: Sample movie data matching expected schema
    """
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("release_date", StringType(), True),
        StructField("budget", IntegerType(), True),
        StructField("revenue", IntegerType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("vote_count", IntegerType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("runtime", IntegerType(), True),
        StructField("genres", StringType(), True),
        StructField("cast", StringType(), True),
        StructField("director", StringType(), True),
        StructField("collection_name", StringType(), True),
    ])
    
    data = [
        (1, "Test Movie 1", "2020-01-15", 100000000, 500000000, 7.5, 1000, 85.5, 120, 
         "Action|Science Fiction", "Actor A|Actor B|Actor C", "Director X", "Test Collection"),
        (2, "Test Movie 2", "2019-05-20", 50000000, 200000000, 6.8, 500, 62.3, 110,
         "Drama|Romance", "Actor D|Actor E", "Director Y", None),
        (3, "Test Movie 3", "2021-03-10", 150000000, 800000000, 8.2, 2000, 92.1, 150,
         "Action|Adventure", "Actor A|Actor F", "Director X", "Test Collection"),
        (4, "Test Movie 4", "2018-11-05", 30000000, 100000000, 5.5, 200, 45.7, 95,
         "Comedy", "Actor G", "Director Z", None),
        (5, "Test Movie 5", "2022-07-22", 200000000, 1000000000, 9.0, 3000, 98.5, 180,
         "Science Fiction|Thriller", "Actor A|Actor H|Actor I", "Director X", None),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="function")
def sample_json_data():
    """
    Create sample raw JSON data structure (as would come from TMDB API).
    
    Returns:
        dict: Sample movie data in API format
    """
    return {
        "id": 603,
        "title": "The Matrix",
        "release_date": "1999-03-30",
        "budget": 63000000,
        "revenue": 463517383,
        "vote_average": 8.2,
        "vote_count": 22000,
        "popularity": 78.5,
        "runtime": 136,
        "genres": [
            {"id": 28, "name": "Action"},
            {"id": 878, "name": "Science Fiction"}
        ],
        "production_companies": [
            {"id": 79, "name": "Village Roadshow Pictures"},
            {"id": 372, "name": "Warner Bros. Pictures"}
        ],
        "belongs_to_collection": {
            "id": 2344,
            "name": "The Matrix Collection"
        },
        "credits": {
            "cast": [
                {"name": "Keanu Reeves", "character": "Neo"},
                {"name": "Laurence Fishburne", "character": "Morpheus"},
                {"name": "Carrie-Anne Moss", "character": "Trinity"}
            ],
            "crew": [
                {"name": "Lana Wachowski", "job": "Director"},
                {"name": "Lilly Wachowski", "job": "Director"}
            ]
        }
    }


@pytest.fixture(scope="session")
def config_path():
    """
    Return path to test configuration file.
    
    Returns:
        str: Path to config.yaml
    """
    project_root = Path(__file__).parent.parent
    return str(project_root / "config" / "config.yaml")
