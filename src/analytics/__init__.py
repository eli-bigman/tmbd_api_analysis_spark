"""
Analytics module for TMDB movie data.

Provides KPI calculations, filtering, aggregations, and performance analysis functionality.
"""

from .kpi_calculator import SparkKPICalculator
from .filters import SparkMovieFilters
from .aggregations import SparkMovieAggregations

__all__ = [
    'SparkKPICalculator',
    'SparkMovieFilters',
    'SparkMovieAggregations'
]
