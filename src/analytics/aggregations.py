"""
Spark-based aggregation functions for movie franchise and director analysis.

This module provides functions for analyzing movie franchises and directors,
including comparisons between franchise and standalone movies.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class SparkMovieAggregations:
    """
    Handles aggregation analysis for franchises and directors using PySpark.
    """
    
    def __init__(self, spark=None):
        """
        Initialize the aggregations analyzer.
        
        Args:
            spark: SparkSession instance (optional)
        """
        self.spark = spark
    
    # ==================== Franchise vs Standalone Comparison ====================
    
    def compare_franchise_vs_standalone(self, df: DataFrame) -> DataFrame:
        """
        Compare movie franchises vs standalone movies across key metrics.
        
        Compares:
        - Mean Revenue
        - Median ROI
        - Mean Budget
        - Mean Popularity
        - Mean Rating
        
        Args:
            df: Spark DataFrame with movie data
            
        Returns:
            DataFrame with comparison metrics
        """
        # Add franchise flag
        df_flagged = df.withColumn(
            'is_franchise',
            F.when(F.col('collection_name').isNotNull(), 'Franchise').otherwise('Standalone')
        )
        
        # Calculate profit and ROI if needed
        df_calc = df_flagged.withColumn(
            'profit_musd',
            F.col('revenue_musd') - F.col('budget_musd')
        ).withColumn(
            'roi',
            F.when(
                F.col('budget_musd') > 0,
                ((F.col('revenue_musd') - F.col('budget_musd')) / F.col('budget_musd') * 100)
            ).otherwise(None)
        )
        
        # Group by franchise flag and calculate aggregates
        result = df_calc.groupBy('is_franchise').agg(
            F.count('*').alias('movie_count'),
            F.mean('revenue_musd').alias('mean_revenue_musd'),
            F.expr('percentile_approx(roi, 0.5)').alias('median_roi'),
            F.mean('budget_musd').alias('mean_budget_musd'),
            F.mean('popularity').alias('mean_popularity'),
            F.mean('vote_average').alias('mean_rating')
        ).orderBy(F.col('is_franchise').desc())
        
        return result
    
    # ==================== Most Successful Franchises ====================
    
    def get_top_franchises(
        self,
        df: DataFrame,
        top_n: int = 10,
        sort_by: str = 'total_revenue_musd'
    ) -> DataFrame:
        """
        Find the most successful movie franchises.
        
        Calculates:
        - Total number of movies in franchise
        - Total & Mean Budget
        - Total & Mean Revenue
        - Mean Rating
        
        Args:
            df: Spark DataFrame with movie data
            top_n: Number of top franchises to return
            sort_by: Column to sort by ('total_revenue', 'mean_revenue', 'mean_rating', 'movie_count')
            
        Returns:
            DataFrame with franchise statistics
        """
        # Filter only franchise movies
        franchise_df = df.filter(F.col('collection_name').isNotNull())
        
        # Group by franchise and calculate aggregates
        franchise_stats = franchise_df.groupBy('collection_name').agg(
            F.count('*').alias('movie_count'),
            F.sum('budget_musd').alias('total_budget_musd'),
            F.mean('budget_musd').alias('mean_budget_musd'),
            F.sum('revenue_musd').alias('total_revenue_musd'),
            F.mean('revenue_musd').alias('mean_revenue_musd'),
            F.mean('vote_average').alias('mean_rating'),
            F.mean('vote_count').alias('mean_vote_count')
        )
        
        # Optimize: Sort and Limit FIRST to avoid global window warning
        df_sorted = franchise_stats.orderBy(F.col(sort_by).desc()).limit(top_n)
        
        # Add rank column to the limited set
        # usage of Window over literal 1 is safe here because dataset is small (top_n)
        window_spec = Window.orderBy(F.col(sort_by).desc())
        result = df_sorted.withColumn('rank', F.row_number().over(window_spec))
        
        # Reorder columns for better display
        return result.select(
            'rank',
            'collection_name',
            'movie_count',
            'total_budget_musd',
            'mean_budget_musd',
            'total_revenue_musd',
            'mean_revenue_musd',
            'mean_rating',
            'mean_vote_count'
        )
    
    # ==================== Most Successful Directors ====================
    
    def get_top_directors(
        self,
        df: DataFrame,
        top_n: int = 10,
        sort_by: str = 'total_revenue_musd',
        min_movies: int = 1
    ) -> DataFrame:
        """
        Find the most successful directors.
        
        Calculates:
        - Total Number of Movies Directed
        - Total Revenue
        - Mean Revenue
        - Mean Rating
        
        Args:
            df: Spark DataFrame with movie data
            top_n: Number of top directors to return
            sort_by: Column to sort by ('total_revenue', 'mean_revenue', 'mean_rating', 'movie_count')
            min_movies: Minimum number of movies to be included in analysis
            
        Returns:
            DataFrame with director statistics
        """
        # Filter movies with director info
        director_df = df.filter(F.col('director').isNotNull())
        
        # Group by director and calculate aggregates
        director_stats = director_df.groupBy('director').agg(
            F.count('*').alias('movie_count'),
            F.sum('budget_musd').alias('total_budget_musd'),
            F.mean('budget_musd').alias('mean_budget_musd'),
            F.sum('revenue_musd').alias('total_revenue_musd'),
            F.mean('revenue_musd').alias('mean_revenue_musd'),
            F.mean('vote_average').alias('mean_rating'),
            F.mean('vote_count').alias('mean_vote_count')
        )
        
        # Filter by minimum movies
        filtered = director_stats.filter(F.col('movie_count') >= min_movies)
        
        # Optimize: Sort and Limit FIRST
        df_sorted = filtered.orderBy(F.col(sort_by).desc()).limit(top_n)
        
        # Add rank column
        window_spec = Window.orderBy(F.col(sort_by).desc())
        result = df_sorted.withColumn('rank', F.row_number().over(window_spec))
        
        # Reorder columns for better display
        return result.select(
            'rank',
            'director',
            'movie_count',
            'total_budget_musd',
            'mean_budget_musd',
            'total_revenue_musd',
            'mean_revenue_musd',
            'mean_rating',
            'mean_vote_count'
        )
    
    # ==================== Specific Analyses ====================
    
    def get_franchise_details(self, df: DataFrame, franchise_name: str) -> DataFrame:
        """
        Get detailed statistics for a specific franchise.
        
        Args:
            df: Spark DataFrame with movie data
            franchise_name: Name of the franchise to analyze
            
        Returns:
            DataFrame with all movies in the franchise and key metrics
        """
        # Filter for specific franchise
        franchise_movies = df.filter(
            F.col('collection_name').contains(franchise_name)
        )
        
        # Select relevant columns
        result = franchise_movies.select(
            'title',
            'release_year',
            'budget_musd',
            'revenue_musd',
            'vote_average',
            'vote_count',
            'popularity',
            'director',
            'collection_name'
        ).orderBy('release_year')
        
        return result
    
    def get_director_details(self, df: DataFrame, director_name: str) -> DataFrame:
        """
        Get detailed statistics for a specific director.
        
        Args:
            df: Spark DataFrame with movie data
            director_name: Name of the director to analyze
            
        Returns:
            DataFrame with all movies by the director and key metrics
        """
        # Filter for specific director (case-insensitive, partial match)
        director_movies = df.filter(
            F.lower(F.col('director')).contains(director_name.lower())
        )
        
        # Select relevant columns
        result = director_movies.select(
            'title',
            'release_year',
            'budget_musd',
            'revenue_musd',
            'vote_average',
            'vote_count',
            'popularity',
            'genres',
            'collection_name'
        ).orderBy('release_year')
        
        return result
