"""
Data cleaning module for TMDB movie data.

Provides Spark-based cleaning and preprocessing functionality.
"""

from .cleaner import SparkMovieDataCleaner
from .udfs import (
    extract_collection_name,
    extract_names_from_array,
    extract_keywords_from_struct,
    extract_top_cast,
    get_cast_size,
    extract_director,
    get_crew_size,
    sort_pipe_separated
)

__all__ = [
    'SparkMovieDataCleaner',
    'extract_collection_name',
    'extract_names_from_array',
    'extract_keywords_from_struct',
    'extract_top_cast',
    'get_cast_size',
    'extract_director',
    'get_crew_size',
    'sort_pipe_separated'
]
