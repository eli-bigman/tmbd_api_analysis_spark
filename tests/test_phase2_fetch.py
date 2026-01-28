"""
Phase 2: Data Fetching Tests

Validates that TMDB API fetching works correctly in Docker environment.
Tests the TMDBFetcher class initialization and basic fetching functionality.
"""
import pytest
import os
from pathlib import Path
from dotenv import load_dotenv


def test_tmdb_fetcher_initialization():
    """Test TMDB fetcher can be initialized with API credentials"""
    from src.fetch.fetch_tmdb_api import TMDBFetcher
    
    # Should not raise any errors
    fetcher = TMDBFetcher()
    
    # At least one credential should be present
    assert fetcher.api_key is not None or fetcher.api_token is not None
    
    # Verify configuration loaded
    assert fetcher.base_url is not None
    assert fetcher.timeout > 0
    assert fetcher.rate_limit >= 0
    assert fetcher.raw_data_path.exists()


def test_environment_variables():
    """Test that .env file is loaded and contains TMDB API credentials"""
    load_dotenv()
    
    api_key = os.getenv('TMDB_API_KEY')
    api_token = os.getenv('TMDB_API_TOKEN')
    
    # At least one should be set
    assert api_key is not None or api_token is not None, \
        "Either TMDB_API_KEY or TMDB_API_TOKEN must be set in .env file"


def test_fetch_single_movie():
    """Test fetching a single movie from TMDB API"""
    from src.fetch.fetch_tmdb_api import TMDBFetcher
    
    fetcher = TMDBFetcher()
    
    # Use a known movie ID (The Matrix: 603)
    # Set skip_existing=False to force fetch for testing
    data = fetcher.fetch_movie(603, skip_existing=False)
    
    # Verify data structure
    assert data is not None, "Failed to fetch movie data"
    assert 'id' in data
    assert 'title' in data
    assert data['id'] == 603
    
    # Verify credits and keywords were appended
    assert 'credits' in data, "Credits not included in response"
    assert 'keywords' in data, "Keywords not included in response"
    
    # Verify JSON file was saved
    json_file = fetcher.raw_data_path / "603.json"
    assert json_file.exists(), "JSON file was not saved"


def test_raw_data_path_exists():
    """Test that raw data directory is created"""
    from src.fetch.fetch_tmdb_api import TMDBFetcher
    
    fetcher = TMDBFetcher()
    assert fetcher.raw_data_path.exists()
    assert fetcher.raw_data_path.is_dir()


def test_fetch_with_skip_existing():
    """Test that skip_existing parameter works correctly"""
    from src.fetch.fetch_tmdb_api import TMDBFetcher
    
    fetcher = TMDBFetcher()
    
    # First fetch should return data
    data1 = fetcher.fetch_movie(603, skip_existing=False)
    assert data1 is not None
    
    # Second fetch with skip_existing=True should return None
    data2 = fetcher.fetch_movie(603, skip_existing=True)
    assert data2 is None, "Should skip existing file"
    
    # Fetch with skip_existing=False should return data again
    data3 = fetcher.fetch_movie(603, skip_existing=False)
    assert data3 is not None


def test_fetch_multiple_movies():
    """Test fetching multiple movies"""
    from src.fetch.fetch_tmdb_api import TMDBFetcher
    
    fetcher = TMDBFetcher()
    
    # Test with a small list of movie IDs
    movie_ids = [603, 550, 13]  # The Matrix, Fight Club, Forrest Gump
    
    # Count should be number of movies fetched (may be 0 if all exist)
    count = fetcher.fetch_movies(movie_ids, skip_existing=False)
    assert count >= 0
    assert count <= len(movie_ids)
    
    # Verify files were created
    for movie_id in movie_ids:
        json_file = fetcher.raw_data_path / f"{movie_id}.json"
        assert json_file.exists(), f"JSON file for movie {movie_id} not created"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
