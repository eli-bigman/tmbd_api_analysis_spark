"""
Spark-based advanced filtering and search functions for movie data.

This module provides functions for filtering movies by genre, actor, director,
and executing complex multi-criteria searches using PySpark.
"""

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from typing import Optional, List, Union
import logging

logger = logging.getLogger(__name__)


class SparkMovieFilters:
    """
    Handles advanced filtering and search operations for movie data using PySpark.
    """
    
    def __init__(self, spark=None):
        """
        Initialize the movie filters.
        
        Args:
            spark: SparkSession instance (optional)
        """
        self.spark = spark
    
    def filter_by_genres(
        self,
        df: DataFrame,
        genres: Union[str, List[str]],
        match_all: bool = True
    ) -> DataFrame:
        """
        Filter movies by genre(s).
        
        Genres in the dataset are pipe-separated strings (e.g., "Action|Adventure|Sci-Fi").
        
        Args:
            df: Spark DataFrame with movie data
            genres: Single genre string or list of genres to filter by
            match_all: If True, movie must have ALL genres. If False, ANY genre matches
            
        Returns:
            Filtered DataFrame
            
        Example:
            >>> filters.filter_by_genres(df, ["Action", "Adventure"], match_all=True)
            >>> filters.filter_by_genres(df, "Comedy", match_all=False)
        """
        # Convert single genre to list
        if isinstance(genres, str):
            genres = [genres]
        
        # Create filter condition
        if match_all:
            # Movie must have ALL genres
            condition = F.lit(True)
            for genre in genres:
                condition = condition & F.col('genres').contains(genre)
        else:
            # Movie must have ANY genre
            condition = F.lit(False)
            for genre in genres:
                condition = condition | F.col('genres').contains(genre)
        
        return df.filter(condition)
    
    def filter_by_actor(
        self,
        df: DataFrame,
        actor_name: str,
        case_sensitive: bool = False
    ) -> DataFrame:
        """
        Filter movies where actor appears in cast.
        
        Cast in the dataset is a pipe-separated string of actor names.
        
        Args:
            df: Spark DataFrame with movie data
            actor_name: Actor name to search for (supports partial matching)
            case_sensitive: If True, perform case-sensitive search
            
        Returns:
            Filtered DataFrame
            
        Example:
            >>> filters.filter_by_actor(df, "Bruce Willis")
            >>> filters.filter_by_actor(df, "willis", case_sensitive=False)
        """
        if 'cast' not in df.columns:
            logger.warning("'cast' column not found in DataFrame")
            return df.filter(F.lit(False))  # Return empty DataFrame
        
        if case_sensitive:
            condition = F.col('cast').contains(actor_name)
        else:
            # Case-insensitive search: convert both to lowercase
            condition = F.lower(F.col('cast')).contains(actor_name.lower())
        
        return df.filter(condition & F.col('cast').isNotNull())
    
    def filter_by_director(
        self,
        df: DataFrame,
        director_name: str,
        case_sensitive: bool = False
    ) -> DataFrame:
        """
        Filter movies by director.
        
        Args:
            df: Spark DataFrame with movie data
            director_name: Director name to search for (supports partial matching)
            case_sensitive: If True, perform case-sensitive search
            
        Returns:
            Filtered DataFrame
            
        Example:
            >>> filters.filter_by_director(df, "Quentin Tarantino")
            >>> filters.filter_by_director(df, "tarantino", case_sensitive=False)
        """
        if 'director' not in df.columns:
            logger.warning("'director' column not found in DataFrame")
            return df.filter(F.lit(False))  # Return empty DataFrame
        
        if case_sensitive:
            condition = F.col('director').contains(director_name)
        else:
            # Case-insensitive search: convert both to lowercase
            condition = F.lower(F.col('director')).contains(director_name.lower())
        
        return df.filter(condition & F.col('director').isNotNull())
    
    def filter_by_year_range(
        self,
        df: DataFrame,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
    ) -> DataFrame:
        """
        Filter movies by release year range.
        
        Args:
            df: Spark DataFrame with movie data
            start_year: Minimum release year (inclusive)
            end_year: Maximum release year (inclusive)
            
        Returns:
            Filtered DataFrame
        """
        result = df
        
        if start_year is not None:
            result = result.filter(F.col('release_year') >= start_year)
        
        if end_year is not None:
            result = result.filter(F.col('release_year') <= end_year)
        
        return result
    
    def search_movies(
        self,
        df: DataFrame,
        genres: Optional[Union[str, List[str]]] = None,
        actors: Optional[Union[str, List[str]]] = None,
        directors: Optional[Union[str, List[str]]] = None,
        min_rating: Optional[float] = None,
        min_votes: Optional[int] = None,
        sort_by: str = 'vote_average',
        ascending: bool = False,
        top_n: Optional[int] = None
    ) -> DataFrame:
        """
        Advanced multi-criteria search for movies.
        
        This is a flexible search builder that allows combining multiple filters.
        
        Args:
            df: Spark DataFrame with movie data
            genres: Genre(s) to filter by (all must match)
            actors: Actor name(s) to filter by (any must match)
            directors: Director name(s) to filter by (any must match)
            min_rating: Minimum vote_average threshold
            min_votes: Minimum vote_count threshold
            sort_by: Column to sort results by
            ascending: Sort order (False = descending/highest first)
            top_n: Limit results to top N movies
            
        Returns:
            Filtered and sorted DataFrame
            
        Example:
            >>> filters.search_movies(df, genres=["Action", "Sci-Fi"], actors="Keanu Reeves",
            ...                       sort_by='revenue_musd', ascending=False, top_n=10)
        """
        result = df
        
        # Apply genre filter (ALL must match)
        if genres is not None:
            result = self.filter_by_genres(result, genres, match_all=True)
        
        # Apply actor filter (ANY must match)
        if actors is not None:
            if isinstance(actors, str):
                actors = [actors]
            
            actor_condition = F.lit(False)
            for actor in actors:
                actor_condition = actor_condition | F.lower(F.col('cast')).contains(actor.lower())
            
            result = result.filter(actor_condition & F.col('cast').isNotNull())
        
        # Apply director filter (ANY must match)
        if directors is not None:
            if isinstance(directors, str):
                directors = [directors]
            
            director_condition = F.lit(False)
            for director in directors:
                director_condition = director_condition | F.lower(F.col('director')).contains(director.lower())
            
            result = result.filter(director_condition & F.col('director').isNotNull())
        
        # Apply rating filter
        if min_rating is not None:
            result = result.filter(F.col('vote_average') >= min_rating)
        
        # Apply vote count filter
        if min_votes is not None:
            result = result.filter(F.col('vote_count') >= min_votes)
        
        # Sort results
        if sort_by in result.columns:
            if ascending:
                result = result.orderBy(F.col(sort_by).asc())
            else:
                result = result.orderBy(F.col(sort_by).desc())
        
        # Limit results
        if top_n is not None:
            result = result.limit(top_n)
        
        return result
    
    # ==================== Pre-defined Search Queries ====================
    
    def search_scifi_action_bruce_willis(self, df: DataFrame) -> DataFrame:
        """
        Search Query 1: Find best-rated Science Fiction Action movies starring Bruce Willis.
        
        Criteria:
        - Genres: Must have BOTH "Science Fiction" AND "Action"
        - Actor: Bruce Willis
        - Sort: By rating (vote_average) descending (highest to lowest)
        
        Args:
            df: Spark DataFrame with movie data
            
        Returns:
            Filtered and sorted DataFrame with relevant columns
        """
        results = self.search_movies(
            df,
            genres=["Science Fiction", "Action"],
            actors="Bruce Willis",
            sort_by='vote_average',
            ascending=False
        )
        
        # Select relevant columns for display
        display_cols = ['title', 'release_year', 'vote_average', 'vote_count',
                        'genres', 'director', 'revenue_musd']
        available_cols = [col for col in display_cols if col in results.columns]
        
        return results.select(*available_cols)
    
    def search_uma_tarantino(self, df: DataFrame) -> DataFrame:
        """
        Search Query 2: Find movies starring Uma Thurman, directed by Quentin Tarantino.
        
        Criteria:
        - Actor: Uma Thurman
        - Director: Quentin Tarantino
        - Sort: By runtime ascending (shortest to longest)
        
        Args:
            df: Spark DataFrame with movie data
            
        Returns:
            Filtered and sorted DataFrame with relevant columns
        """
        results = self.search_movies(
            df,
            actors="Uma Thurman",
            directors="Quentin Tarantino",
            sort_by='runtime',
            ascending=True
        )
        
        # Select relevant columns for display
        display_cols = ['title', 'release_year', 'runtime', 'vote_average',
                        'genres', 'revenue_musd', 'budget_musd']
        available_cols = [col for col in display_cols if col in results.columns]
        
        return results.select(*available_cols)
