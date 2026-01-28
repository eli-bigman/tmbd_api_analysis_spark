"""
Fetch movie data from TMDB API and save as raw JSON files.
"""
import os
import time
import requests
from pathlib import Path
from typing import List, Optional
from dotenv import load_dotenv
from tqdm import tqdm

import sys
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.helpers import load_config, save_json, setup_logging

# Setup logger for this module
logger = setup_logging(module_name='fetch')


class TMDBFetcher:
    """Fetch movie data from TMDB API."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize TMDB fetcher.
        
        Args:
            config_path: Path to configuration file
        """
        load_dotenv()
        self.api_key = os.getenv("TMDB_API_KEY")
        self.api_token = os.getenv("TMDB_API_TOKEN")
        
        if not self.api_key and not self.api_token:
            raise ValueError("TMDB_API_KEY or TMDB_API_TOKEN must be set in .env file")
        
        self.config = load_config(config_path)
        self.base_url = self.config['api']['base_url']
        self.timeout = self.config['api']['timeout']
        self.rate_limit = self.config['api']['rate_limit_delay']
        self.raw_data_path = Path(self.config['paths']['raw_data'])
        self.raw_data_path.mkdir(parents=True, exist_ok=True)
    
    def fetch_movie(self, movie_id: int, skip_existing: bool = True) -> Optional[dict]:
        """
        Fetch single movie data with credits and keywords.
        
        Args:
            movie_id: TMDB movie ID
            skip_existing: Skip if JSON already exists
            
        Returns:
            Movie data dictionary or None if failed
        """
        output_file = self.raw_data_path / f"{movie_id}.json"
        
        # Skip if already exists
        if skip_existing and output_file.exists():
            return None
        
        url = f"{self.base_url}/movie/{movie_id}"
        params = {
            "api_key": self.api_key,
            "append_to_response": "credits,keywords"
        }
        
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            
            # Save raw JSON
            save_json(data, str(output_file))
            
            # Rate limiting
            time.sleep(self.rate_limit)
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching movie {movie_id}: {e}")
            return None
    
    def fetch_movies(self, movie_ids: List[int], skip_existing: bool = True) -> int:
        """
        Fetch multiple movies.
        
        Args:
            movie_ids: List of TMDB movie IDs
            skip_existing: Skip already downloaded movies
            
        Returns:
            Number of movies successfully fetched
        """
        fetched_count = 0
        
        for movie_id in tqdm(movie_ids, desc="Fetching movies"):
            result = self.fetch_movie(movie_id, skip_existing=skip_existing)
            if result is not None:
                fetched_count += 1
        
        return fetched_count


def main():
    """Main execution function."""

    movie_ids = [0]
    if not movie_ids:
        logger.error("No movie ids found in config under 'data_collection.movie_id'. Nothing to fetch.")
        return
    fetcher = TMDBFetcher()
    logger.info("="*60)
    logger.info(f"Starting fetch for {len(movie_ids)} movies...")
    count = fetcher.fetch_movies(movie_ids)
    logger.info(f"Successfully fetched {count} new movies")
    logger.info(f"Raw data saved to: {fetcher.raw_data_path}")
    logger.info("="*60)


if __name__ == "__main__":
    main()
