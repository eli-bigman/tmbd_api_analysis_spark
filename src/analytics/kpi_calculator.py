"""
Spark-based KPI calculator functions for movie performance analysis.

This module provides ranking functions for various movie performance metrics
including revenue, budget, profit, ROI, ratings, and popularity using PySpark.
"""

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


class SparkKPICalculator:
    """
    Handles KPI calculations and movie ranking using PySpark.
    """
    
    def __init__(self, spark=None):
        """
        Initialize the KPI calculator.
        
        Args:
            spark: SparkSession instance (optional)
        """
        self.spark = spark
    
    def rank_movies(
        self,
        df: DataFrame,
        metric: str,
        ascending: bool = False,
        top_n: int = 10,
        filter_condition: Optional[Column] = None,
        display_columns: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Core ranking function for movies by any metric.
        
        This is the main function used by all other KPI functions.
        
        Args:
            df: Spark DataFrame with movie data
            metric: Column name to rank by (e.g., 'revenue_musd', 'roi')
            ascending: If True, rank lowest to highest. If False, rank highest to lowest
            top_n: Number of top movies to return
            filter_condition: Column expression for filtering (e.g., F.col('budget_musd') >= 10)
            display_columns: List of columns to display. If None, uses default set
            
        Returns:
            DataFrame with ranked movies
            
        Example:
            >>> calculator.rank_movies(df, 'revenue_musd', top_n=20)
            >>> calculator.rank_movies(df, 'roi', filter_condition=F.col('budget_musd') >= 10)
        """
        # Create working copy
        df_filtered = df
        
        # Calculate derived columns if missing
        if 'release_year' not in df_filtered.columns and 'release_date' in df_filtered.columns:
            df_filtered = df_filtered.withColumn('release_year', F.year(F.col('release_date')))
        
        # Calculate profit if needed
        if metric == 'profit_musd' and 'profit_musd' not in df_filtered.columns:
            if 'revenue_musd' in df_filtered.columns and 'budget_musd' in df_filtered.columns:
                df_filtered = df_filtered.withColumn(
                    'profit_musd',
                    F.col('revenue_musd') - F.col('budget_musd')
                )
        
        # Calculate ROI if needed
        if metric == 'roi' and 'roi' not in df_filtered.columns:
            if 'revenue_musd' in df_filtered.columns and 'budget_musd' in df_filtered.columns:
                # ROI = (Revenue - Budget) / Budget * 100
                # Avoid division by zero
                df_filtered = df_filtered.withColumn(
                    'roi',
                    F.when(
                        F.col('budget_musd') > 0,
                        ((F.col('revenue_musd') - F.col('budget_musd')) / F.col('budget_musd') * 100)
                    ).otherwise(None)
                )
        
        # Apply filter if provided
        if filter_condition is not None:
            df_filtered = df_filtered.filter(filter_condition)
        
        # Remove rows where metric is null
        if metric in df_filtered.columns:
            df_filtered = df_filtered.filter(F.col(metric).isNotNull())
        else:
            # If metric doesn't exist, return empty DataFrame
            logger.warning(f"Metric '{metric}' not found in DataFrame")
            return self.spark.createDataFrame([], schema="rank INT, title STRING")
        
        # Sort by metric
        if ascending:
            df_sorted = df_filtered.orderBy(F.col(metric).asc())
        else:
            df_sorted = df_filtered.orderBy(F.col(metric).desc())
        
        # Select top N
        df_top = df_sorted.limit(top_n)
        
        # Add rank column using row_number
        window_spec = Window.orderBy(F.lit(1))  # Maintain the order from sorting
        df_top = df_top.withColumn('rank', F.row_number().over(window_spec))
        
        # Determine display columns
        if display_columns is None:
            # Default columns based on metric
            base_cols = ['rank', 'title', metric]
            optional_cols = []
            
            # Add relevant context columns based on metric
            if metric in ['revenue_musd', 'budget_musd', 'profit_musd']:
                optional_cols = ['release_year', 'budget_musd', 'revenue_musd']
            elif metric == 'roi':
                optional_cols = ['release_year', 'budget_musd', 'revenue_musd', 'roi']
            elif metric in ['vote_average', 'vote_count']:
                optional_cols = ['release_year', 'vote_average', 'vote_count']
            elif metric == 'popularity':
                optional_cols = ['release_year', 'popularity', 'vote_average']
            else:
                optional_cols = ['release_year']
            
            # Build final column list (avoid duplicates)
            display_columns = base_cols + [
                col for col in optional_cols 
                if col not in base_cols and col in df_top.columns
            ]
        
        # Select only the display columns
        result = df_top.select(*display_columns)
        
        return result
    
    # ==================== Revenue Rankings ====================
    
    def get_top_by_revenue(self, df: DataFrame, top_n: int = 10) -> DataFrame:
        """
        Get movies with highest revenue.
        
        Args:
            df: Spark DataFrame with movie data
            top_n: Number of top movies to return
            
        Returns:
            DataFrame with top revenue movies
        """
        return self.rank_movies(df, 'revenue_musd', ascending=False, top_n=top_n)
    
    def get_bottom_by_revenue(self, df: DataFrame, top_n: int = 10) -> DataFrame:
        """
        Get movies with lowest revenue.
        
        Args:
            df: Spark DataFrame with movie data
            top_n: Number of bottom movies to return
            
        Returns:
            DataFrame with lowest revenue movies
        """
        return self.rank_movies(df, 'revenue_musd', ascending=True, top_n=top_n)
    
    # ==================== Budget Rankings ====================
    
    def get_top_by_budget(self, df: DataFrame, top_n: int = 10) -> DataFrame:
        """
        Get movies with highest budget.
        
        Args:
            df: Spark DataFrame with movie data
            top_n: Number of top movies to return
            
        Returns:
            DataFrame with highest budget movies
        """
        return self.rank_movies(df, 'budget_musd', ascending=False, top_n=top_n)
    
    def get_bottom_by_budget(self, df: DataFrame, top_n: int = 10) -> DataFrame:
        """
        Get movies with lowest budget.
        
        Args:
            df: Spark DataFrame with movie data
            top_n: Number of bottom movies to return
            
        Returns:
            DataFrame with lowest budget movies
        """
        return self.rank_movies(df, 'budget_musd', ascending=True, top_n=top_n)
    
    # ==================== Profit Rankings ====================
    
    def get_top_by_profit(self, df: DataFrame, top_n: int = 10) -> DataFrame:
        """
        Get movies with highest profit (Revenue - Budget).
        
        Args:
            df: Spark DataFrame with movie data
            top_n: Number of top movies to return
            
        Returns:
            DataFrame with highest profit movies
        """
        return self.rank_movies(df, 'profit_musd', ascending=False, top_n=top_n)
    
    def get_bottom_by_profit(self, df: DataFrame, top_n: int = 10) -> DataFrame:
        """
        Get movies with lowest profit (Revenue - Budget).
        
        Args:
            df: Spark DataFrame with movie data
            top_n: Number of bottom movies to return
            
        Returns:
            DataFrame with lowest profit movies
        """
        return self.rank_movies(df, 'profit_musd', ascending=True, top_n=top_n)
    
    # ==================== ROI Rankings ====================
    
    def get_top_by_roi(self, df: DataFrame, top_n: int = 10) -> DataFrame:
        """
        Get movies with highest ROI (Return on Investment).
        
        Only includes movies with budget >= $10M to filter out low-budget outliers.
        ROI = (Revenue - Budget) / Budget * 100
        
        Args:
            df: Spark DataFrame with movie data
            top_n: Number of top movies to return
            
        Returns:
            DataFrame with highest ROI movies (budget >= $10M)
        """
        filter_condition = F.col('budget_musd') >= 10
        return self.rank_movies(df, 'roi', ascending=False, top_n=top_n, filter_condition=filter_condition)
    
    def get_bottom_by_roi(self, df: DataFrame, top_n: int = 10) -> DataFrame:
        """
        Get movies with lowest ROI (Return on Investment).
        
        Only includes movies with budget >= $10M to filter out low-budget outliers.
        ROI = (Revenue - Budget) / Budget * 100
        
        Args:
            df: Spark DataFrame with movie data
            top_n: Number of bottom movies to return
            
        Returns:
            DataFrame with lowest ROI movies (budget >= $10M)
        """
        filter_condition = F.col('budget_musd') >= 10
        return self.rank_movies(df, 'roi', ascending=True, top_n=top_n, filter_condition=filter_condition)
    
    # ==================== Vote Rankings ====================
    
    def get_most_voted(self, df: DataFrame, top_n: int = 10) -> DataFrame:
        """
        Get movies with most votes.
        
        Args:
            df: Spark DataFrame with movie data
            top_n: Number of top movies to return
            
        Returns:
            DataFrame with most voted movies
        """
        return self.rank_movies(df, 'vote_count', ascending=False, top_n=top_n)
    
    # ==================== Rating Rankings ====================
    
    def get_top_rated(self, df: DataFrame, top_n: int = 10) -> DataFrame:
        """
        Get highest rated movies.
        
        Only includes movies with at least 10 votes to ensure rating reliability.
        
        Args:
            df: Spark DataFrame with movie data
            top_n: Number of top movies to return
            
        Returns:
            DataFrame with highest rated movies (vote_count >= 10)
        """
        filter_condition = F.col('vote_count') >= 10
        return self.rank_movies(df, 'vote_average', ascending=False, top_n=top_n, filter_condition=filter_condition)
    
    def get_bottom_rated(self, df: DataFrame, top_n: int = 10) -> DataFrame:
        """
        Get lowest rated movies.
        
        Only includes movies with at least 10 votes to ensure rating reliability.
        
        Args:
            df: Spark DataFrame with movie data
            top_n: Number of bottom movies to return
            
        Returns:
            DataFrame with lowest rated movies (vote_count >= 10)
        """
        filter_condition = F.col('vote_count') >= 10
        return self.rank_movies(df, 'vote_average', ascending=True, top_n=top_n, filter_condition=filter_condition)
    
    # ==================== Popularity Rankings ====================
    
    def get_most_popular(self, df: DataFrame, top_n: int = 10) -> DataFrame:
        """
        Get most popular movies based on TMDB popularity score.
        
        Args:
            df: Spark DataFrame with movie data
            top_n: Number of top movies to return
            
        Returns:
            DataFrame with most popular movies
        """
        return self.rank_movies(df, 'popularity', ascending=False, top_n=top_n)
