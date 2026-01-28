"""
Analytics module for TMDB movie data.

Provides KPI calculations, filtering, aggregations, EDA, and performance analysis functionality.
"""

from .kpi_calculator import SparkKPICalculator
from .filters import SparkMovieFilters
from .aggregations import SparkMovieAggregations
from .eda import SparkEDA

__all__ = [
    'SparkKPICalculator',
    'SparkMovieFilters',
    'SparkMovieAggregations',
    'SparkEDA'
]
