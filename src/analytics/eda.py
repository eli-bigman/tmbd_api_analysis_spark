"""
Spark-based Exploratory Data Analysis (EDA) module for movie data.

This module provides comprehensive EDA functionality including:
- Dataset overview and profiling
- Missing value analysis
- Statistical summaries
- Distribution analysis
- Categorical data analysis
- Temporal trends
- Correlation analysis
- Data quality reporting
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import logging

# Suppress warnings
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

logger = logging.getLogger(__name__)


class SparkEDA:
    """Handles comprehensive Exploratory Data Analysis for movie data using PySpark."""
    
    def __init__(self, spark: SparkSession = None):
        """
        Initialize the EDA analyzer.
        
        Args:
            spark: SparkSession instance (optional)
        """
        self.spark = spark
        
    def dataset_overview(self, df: DataFrame) -> dict:
        """
        Get basic dataset overview statistics.
        
        Args:
            df: Spark DataFrame with movie data
            
        Returns:
            Dictionary with overview statistics
        """
        print("="*80)
        print("1. DATASET OVERVIEW")
        print("="*80)
        
        row_count = df.count()
        col_count = len(df.columns)
        
        print(f"\nDataset Shape:")
        print(f"  Number of rows: {row_count:,}")
        print(f"  Number of columns: {col_count}")
        
        print(f"\nColumn Names:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i:2d}. {col}")
            
        return {
            'row_count': row_count,
            'col_count': col_count,
            'columns': df.columns
        }
    
    def missing_value_analysis(self, df: DataFrame, visualize: bool = True):
        """
        Analyze and visualize missing values in the dataset.
        
        Args:
            df: Spark DataFrame with movie data
            visualize: Whether to create visualization
            
        Returns:
            DataFrame with missing value statistics
        """
        print("\n" + "="*80)
        print("2. MISSING VALUE ANALYSIS")
        print("="*80)
        
        # Calculate null counts
        null_counts = []
        total_rows = df.count()
        
        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_percentage = (null_count / total_rows) * 100
            null_counts.append({
                'column': col,
                'null_count': null_count,
                'null_percentage': round(null_percentage, 2)
            })
        
        # Create Spark DataFrame
        null_df = self.spark.createDataFrame(null_counts)
        null_df_sorted = null_df.orderBy(F.col('null_percentage').desc())
        
        print("\nMissing Values by Column (sorted by percentage):")
        null_df_sorted.show(30, truncate=False)
        
        # Visualize if requested
        if visualize:
            null_pd = null_df_sorted.toPandas()
            null_pd_filtered = null_pd[null_pd['null_percentage'] > 0]
            
            if len(null_pd_filtered) > 0:
                plt.figure(figsize=(12, 6))
                sns.barplot(data=null_pd_filtered, x='null_percentage', y='column', palette='Reds_r')
                plt.title('Missing Values by Column (%)', fontsize=14, fontweight='bold')
                plt.xlabel('Percentage Missing (%)')
                plt.ylabel('Column Name')
                plt.tight_layout()
                plt.show()
            else:
                print("\n✓ No missing values found in dataset!")
        
        return null_df_sorted
    
    def statistical_summary(self, df: DataFrame, numerical_cols: list = None):
        """
        Generate statistical summary for numerical columns.
        
        Args:
            df: Spark DataFrame with movie data
            numerical_cols: List of numerical columns to analyze
        """
        print("\n" + "="*80)
        print("3. STATISTICAL SUMMARY OF NUMERICAL COLUMNS")
        print("="*80)
        
        if numerical_cols is None:
            numerical_cols = ['budget_musd', 'revenue_musd', 'runtime', 'vote_average', 
                            'vote_count', 'popularity', 'cast_size', 'crew_size']
        
        # Filter to only existing columns
        existing_cols = [col for col in numerical_cols if col in df.columns]
        
        print("\nDescriptive Statistics:")
        df.select(existing_cols).summary().show()
        
        print("\nAdditional Statistics:")
        for col in existing_cols:
            stats = df.select(
                F.count(col).alias('count'),
                F.mean(col).alias('mean'),
                F.stddev(col).alias('std'),
                F.min(col).alias('min'),
                F.expr(f'percentile({col}, 0.25)').alias('25%'),
                F.expr(f'percentile({col}, 0.50)').alias('median'),
                F.expr(f'percentile({col}, 0.75)').alias('75%'),
                F.max(col).alias('max')
            ).collect()[0]
            
            print(f"\n{col}:")
            print(f"  Count: {stats['count']:,}")
            print(f"  Mean: {stats['mean']:.2f}")
            print(f"  Std: {stats['std']:.2f}")
            print(f"  Min: {stats['min']:.2f}")
            print(f"  25%: {stats['25%']:.2f}")
            print(f"  Median: {stats['median']:.2f}")
            print(f"  75%: {stats['75%']:.2f}")
            print(f"  Max: {stats['max']:.2f}")
    
    def plot_distributions(self, df: DataFrame, columns: list = None):
        """
        Plot distribution histograms for numerical columns.
        
        Args:
            df: Spark DataFrame with movie data
            columns: List of columns to plot
        """
        print("\n" + "="*80)
        print("4. DATA DISTRIBUTIONS")
        print("="*80)
        
        if columns is None:
            columns = ['budget_musd', 'revenue_musd', 'runtime', 'vote_average', 
                     'vote_count', 'popularity', 'cast_size', 'crew_size']
        
        # Add profit column
        df_with_profit = df.withColumn('profit_musd', F.col('revenue_musd') - F.col('budget_musd'))
        plot_cols = columns + ['profit_musd']
        
        # Create subplots
        fig, axes = plt.subplots(3, 3, figsize=(16, 12))
        axes = axes.flatten()
        
        for idx, col in enumerate(plot_cols):
            if idx < len(axes) and col in df_with_profit.columns:
                data = df_with_profit.select(col).filter(F.col(col).isNotNull()).toPandas()
                
                if len(data) > 0:
                    axes[idx].hist(data[col].dropna(), bins=30, color='skyblue', 
                                 edgecolor='black', alpha=0.7)
                    axes[idx].set_title(f'{col.replace("_", " ").title()} Distribution', 
                                      fontweight='bold')
                    axes[idx].set_xlabel(col.replace('_', ' ').title())
                    axes[idx].set_ylabel('Frequency')
                    axes[idx].grid(alpha=0.3)
        
        plt.tight_layout()
        plt.show()
    
    def categorical_analysis(self, df: DataFrame):
        """
        Analyze categorical variables including genres, languages, and franchises.
        
        Args:
            df: Spark DataFrame with movie data
        """
        print("\n" + "="*80)
        print("5. CATEGORICAL DATA ANALYSIS")
        print("="*80)
        
        # Genre distribution
        print("\n5.1 Top 15 Genres (by movie count):")
        genre_counts = df.select(F.explode(F.split(F.col('genres'), '\\|')).alias('genre')) \
            .groupBy('genre') \
            .count() \
            .orderBy(F.col('count').desc()) \
            .limit(15)
        
        genre_counts.show(truncate=False)
        
        # Visualize genres
        genre_pd = genre_counts.toPandas()
        plt.figure(figsize=(12, 6))
        sns.barplot(data=genre_pd, x='count', y='genre', palette='viridis')
        plt.title('Top 15 Movie Genres', fontsize=14, fontweight='bold')
        plt.xlabel('Number of Movies')
        plt.ylabel('Genre')
        plt.tight_layout()
        plt.show()
    
    def temporal_analysis(self, df: DataFrame):
        """
        Analyze temporal trends in movie releases.
        
        Args:
            df: Spark DataFrame with movie data
        """
        print("\n" + "="*80)
        print("6. TEMPORAL ANALYSIS")
        print("="*80)
        
        print("\n6.1 Movies Released per Year (last 20 years):")
        movies_by_year = df.groupBy('release_year') \
            .count() \
            .orderBy('release_year', ascending=False) \
            .limit(20)
        
        movies_by_year.show(truncate=False)
        
        # Visualize trend
        year_pd = movies_by_year.orderBy('release_year').toPandas()
        plt.figure(figsize=(14, 6))
        plt.plot(year_pd['release_year'], year_pd['count'], marker='o', linewidth=2, markersize=6)
        plt.title('Movies Released per Year', fontsize=14, fontweight='bold')
        plt.xlabel('Year')
        plt.ylabel('Number of Movies')
        plt.grid(alpha=0.3)
        plt.tight_layout()
        plt.show()
    
    def correlation_analysis(self, df: DataFrame, columns: list = None):
        """
        Analyze correlations between numerical features.
        
        Args:
            df: Spark DataFrame with movie data
            columns: List of columns to analyze
        """
        print("\n" + "="*80)
        print("7. CORRELATION ANALYSIS")
        print("="*80)
        
        if columns is None:
            columns = ['budget_musd', 'revenue_musd', 'runtime', 'vote_average', 
                     'vote_count', 'popularity']
        
        # Filter to existing columns
        existing_cols = [col for col in columns if col in df.columns]
        
        # Convert to pandas for correlation
        corr_data = df.select(existing_cols).toPandas()
        correlation_matrix = corr_data.corr()
        
        print("\nCorrelation Matrix:")
        print(correlation_matrix.round(3))
        
        # Visualize
        plt.figure(figsize=(10, 8))
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0, 
                   square=True, linewidths=1, cbar_kws={"shrink": 0.8},
                   fmt='.3f', vmin=-1, vmax=1)
        plt.title('Feature Correlation Matrix', fontsize=14, fontweight='bold')
        plt.tight_layout()
        plt.show()
        
        return correlation_matrix
    
    def data_quality_report(self, df: DataFrame, null_counts_data: list):
        """
        Generate comprehensive data quality report.
        
        Args:
            df: Spark DataFrame with movie data
            null_counts_data: List of null count dictionaries from missing_value_analysis
        """
        print("\n" + "="*80)
        print("8. DATA QUALITY REPORT")
        print("="*80)
        
        total_rows = df.count()
        total_cols = len(df.columns)
        total_cells = total_rows * total_cols
        total_nulls = sum([row['null_count'] for row in null_counts_data])
        completeness = ((total_cells - total_nulls) / total_cells) * 100
        
        print(f"\nData Completeness: {completeness:.2f}%")
        print(f"Total cells: {total_cells:,}")
        print(f"Cells with data: {(total_cells - total_nulls):,}")
        print(f"Missing cells: {total_nulls:,}")
        
        # Financial data quality
        valid_financial = df.filter(
            (F.col('budget_musd').isNotNull()) & 
            (F.col('revenue_musd').isNotNull()) &
            (F.col('budget_musd') > 0) &
            (F.col('revenue_musd') > 0)
        ).count()
        
        print(f"\nMovies with valid financial data: {valid_financial:,} ({(valid_financial/total_rows)*100:.1f}%)")
        
        # Rating data quality
        rated_movies = df.filter(
            (F.col('vote_count') >= 10) &
            (F.col('vote_average').isNotNull())
        ).count()
        
        print(f"Movies with reliable ratings (≥10 votes): {rated_movies:,} ({(rated_movies/total_rows)*100:.1f}%)")
    
    def run_full_eda(self, df: DataFrame):
        """
        Run complete EDA pipeline on the dataset.
        
        Args:
            df: Spark DataFrame with movie data
        """
        print("="*80)
        print("EXPLORATORY DATA ANALYSIS")
        print("="*80)
        
        # 1. Dataset Overview
        overview = self.dataset_overview(df)
        
        # 2. Missing Value Analysis
        null_df = self.missing_value_analysis(df, visualize=True)
        null_counts_list = null_df.collect()
        null_counts_data = [row.asDict() for row in null_counts_list]
        
        # 3. Statistical Summary
        self.statistical_summary(df)
        
        # 4. Distributions
        self.plot_distributions(df)
        
        # 5. Categorical Analysis (Genres only)
        self.categorical_analysis(df)
        
        print("\n" + "="*80)
        print("EDA COMPLETE")
        print("="*80)
