
import pytest
from unittest.mock import patch, MagicMock, mock_open
import requests
from src.fetch.fetch_tmdb_api import TMDBFetcher

class TestTMDBFetcher:
    """Test TMDBFetcher class."""
    
    @pytest.fixture
    def mock_env(self):
        """Mock environment variables."""
        with patch.dict('os.environ', {'TMDB_API_KEY': 'test_key', 'TMDB_API_TOKEN': 'test_token'}):
            yield

    @pytest.fixture
    def mock_config(self):
        """Mock configuration loader."""
        config = {
            'api': {
                'base_url': 'http://test.api',
                'timeout': 5,
                'rate_limit_delay': 0
            },
            'paths': {
                'raw_data': 'test_data'
            }
        }
        with patch('src.fetch.fetch_tmdb_api.load_config', return_value=config):
            yield config

    def test_init_raises_without_env(self):
        """Test initialization fails without API key/token."""
        # Patch load_dotenv to prevent reloading from .env file
        with patch('src.fetch.fetch_tmdb_api.load_dotenv'):
            with patch.dict('os.environ', {}, clear=True):
                 with pytest.raises(ValueError):
                    # Use a plausible path, though load_config shouldn't be reached
                    TMDBFetcher(config_path="config/config.yaml")

    def test_fetch_movie_success(self, mock_env, mock_config, tmp_path):
        """Test successful movie fetch."""
        
        # Mock pathlib Path in the class to control where it writes or check calls
        with patch('src.fetch.fetch_tmdb_api.Path') as MockPath:
            # Setup path mock
            mock_path_instance = MagicMock()
            MockPath.return_value = mock_path_instance
            mock_path_instance.__truediv__.return_value = mock_path_instance
            mock_path_instance.exists.return_value = False # File doesn't exist
            
            # Mock requests
            with patch('requests.get') as mock_get:
                mock_response = MagicMock()
                mock_response.json.return_value = {"id": 1, "title": "Test"}
                mock_response.status_code = 200
                mock_get.return_value = mock_response
                
                # Mock save_json helper
                with patch('src.fetch.fetch_tmdb_api.save_json') as mock_save:
                    
                    # mock_config fixture handles load_config, so path string is just for show
                    fetcher = TMDBFetcher(config_path="config/config.yaml")
                    result = fetcher.fetch_movie(1)
                    
                    assert result == {"id": 1, "title": "Test"}
                    mock_get.assert_called_once()
                    mock_save.assert_called_once()

    def test_fetch_movie_skip_existing(self, mock_env, mock_config):
        """Test skipping existing files."""
        with patch('src.fetch.fetch_tmdb_api.Path') as MockPath:
            mock_path_instance = MagicMock()
            MockPath.return_value = mock_path_instance
            mock_path_instance.__truediv__.return_value = mock_path_instance
            mock_path_instance.exists.return_value = True # File exists
            
            fetcher = TMDBFetcher(config_path="config/config.yaml")
            result = fetcher.fetch_movie(1, skip_existing=True)
            
            assert result is None

    def test_fetch_movie_failure(self, mock_env, mock_config):
        """Test handling of API errors."""
        with patch('src.fetch.fetch_tmdb_api.Path') as MockPath:
            mock_path_instance = MagicMock()
            MockPath.return_value = mock_path_instance
            mock_path_instance.__truediv__.return_value = mock_path_instance
            
            with patch('requests.get') as mock_get:
                mock_get.side_effect = requests.exceptions.RequestException("API Error")
                
                fetcher = TMDBFetcher(config_path="config/config.yaml")
                result = fetcher.fetch_movie(1)
                
                assert result is None

    def test_fetch_movies_batch(self, mock_env, mock_config):
        """Test batch fetching."""
        with patch('src.fetch.fetch_tmdb_api.TMDBFetcher.fetch_movie') as mock_fetch_single:
            mock_fetch_single.side_effect = [{}, None, {}] # 2 successes, 1 fail/skip
            
            fetcher = TMDBFetcher(config_path="config/config.yaml")
            count = fetcher.fetch_movies([1, 2, 3])
            
            assert count == 2
            assert mock_fetch_single.call_count == 3
