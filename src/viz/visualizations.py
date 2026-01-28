"""
Visualization functions for movie data analysis.

This module provides functions for creating visualizations of movie performance,
trends, and comparisons using Matplotlib and Seaborn.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Optional, Tuple
import logging

logger = logging.getLogger(__name__)

# Set default style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)


class MovieVisualizer:
    """
    Handles visualization of movie data using Matplotlib and Seaborn.
    
    Converts Spark DataFrames to Pandas for plotting.
    """
    
    def __init__(self, spark=None):
        """
        Initialize the visualizer.
        
        Args:
            spark: SparkSession instance (optional)
        """
        self.spark = spark
    
    def plot_revenue_vs_budget(
        self,
        df: DataFrame,
        figsize: Tuple[int, int] = (12, 8),
        save_path: Optional[str] = None
    ) -> plt.Figure:
        """
        Plot Revenue vs Budget Trends with scatter plot and trend line.
        
        Args:
            df: Spark DataFrame with movie data
            figsize: Figure size (width, height)
            save_path: Optional path to save the figure
            
        Returns:
            Matplotlib figure object
        """
        # Prepare data in Spark
        plot_df = df.select('title', 'budget_musd', 'revenue_musd', 'release_year') \
                    .filter((F.col('budget_musd').isNotNull()) & 
                           (F.col('revenue_musd').isNotNull()) &
                           (F.col('budget_musd') > 0) &
                           (F.col('revenue_musd') > 0))
        
        # Convert to pandas for plotting
        pdf = plot_df.toPandas()
        
        # Create figure
        fig, ax = plt.subplots(figsize=figsize)
        
        # Scatter plot
        scatter = ax.scatter(pdf['budget_musd'], pdf['revenue_musd'], 
                           alpha=0.6, s=100, c=pdf['release_year'], 
                           cmap='viridis', edgecolors='black', linewidth=0.5)
        
        # Add diagonal line (break-even line)
        max_val = max(pdf['budget_musd'].max(), pdf['revenue_musd'].max())
        ax.plot([0, max_val], [0, max_val], 'r--', linewidth=2, 
               label='Break-even line', alpha=0.7)
        
        # Add trend line
        z = np.polyfit(pdf['budget_musd'], pdf['revenue_musd'], 1)
        p = np.poly1d(z)
        ax.plot(pdf['budget_musd'], p(pdf['budget_musd']), 
               "g-", linewidth=2, label=f'Trend line', alpha=0.7)
        
        # Labels and title
        ax.set_xlabel('Budget (Million USD)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Revenue (Million USD)', fontsize=12, fontweight='bold')
        ax.set_title('Revenue vs Budget Trends', fontsize=14, fontweight='bold', pad=20)
        ax.legend(fontsize=10)
        ax.grid(True, alpha=0.3)
        
        # Add colorbar
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('Release Year', fontsize=10)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Saved plot to {save_path}")
        
        return fig
    
    def plot_roi_by_genre(
        self,
        df: DataFrame,
        top_n_genres: int = 10,
        figsize: Tuple[int, int] = (14, 8),
        save_path: Optional[str] = None
    ) -> plt.Figure:
        """
        Plot ROI Distribution by Genre using box plots.
        
        Args:
            df: Spark DataFrame with movie data
            top_n_genres: Number of top genres to display
            figsize: Figure size (width, height)
            save_path: Optional path to save the figure
            
        Returns:
            Matplotlib figure object
        """
        # Calculate ROI and explode genres (split pipe-separated values)
        df_roi = df.withColumn(
            'roi',
            F.when(
                F.col('budget_musd') > 0,
                ((F.col('revenue_musd') - F.col('budget_musd')) / F.col('budget_musd') * 100)
            ).otherwise(None)
        ).filter(F.col('roi').isNotNull())
        
        # Split genres into array and explode
        df_exploded = df_roi.withColumn('genre', F.explode(F.split(F.col('genres'), '\\|'))) \
                            .select('genre', 'roi')
        
        # Get top genres by count
        top_genres = df_exploded.groupBy('genre').count() \
                                .orderBy(F.col('count').desc()) \
                                .limit(top_n_genres) \
                                .select('genre') \
                                .rdd.flatMap(lambda x: x).collect()
        
        # Filter for top genres
        df_filtered = df_exploded.filter(F.col('genre').isin(top_genres))
        
        # Convert to pandas
        pdf = df_filtered.toPandas()
        
        # Create figure
        fig, ax = plt.subplots(figsize=figsize)
        
        # Box plot
        sns.boxplot(data=pdf, x='genre', y='roi', ax=ax, palette='Set2')
        
        # Rotate x labels
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
        
        # Labels and title
        ax.set_xlabel('Genre', fontsize=12, fontweight='bold')
        ax.set_ylabel('ROI (%)', fontsize=12, fontweight='bold')
        ax.set_title(f'ROI Distribution by Top {top_n_genres} Genres', 
                    fontsize=14, fontweight='bold', pad=20)
        ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Saved plot to {save_path}")
        
        return fig
    
    def plot_popularity_vs_rating(
        self,
        df: DataFrame,
        min_votes: int = 10,
        figsize: Tuple[int, int] = (12, 8),
        save_path: Optional[str] = None
    ) -> plt.Figure:
        """
        Plot Popularity vs Rating scatter plot.
        
        Args:
            df: Spark DataFrame with movie data
            min_votes: Minimum vote count to include
            figsize: Figure size (width, height)
            save_path: Optional path to save the figure
            
        Returns:
            Matplotlib figure object
        """
        # Filter and prepare data
        plot_df = df.select('title', 'popularity', 'vote_average', 'vote_count', 'revenue_musd') \
                    .filter((F.col('popularity').isNotNull()) & 
                           (F.col('vote_average').isNotNull()) &
                           (F.col('vote_count') >= min_votes))
        
        # Convert to pandas
        pdf = plot_df.toPandas()
        
        # Create figure
        fig, ax = plt.subplots(figsize=figsize)
        
        # Scatter plot with size based on revenue
        sizes = (pdf['revenue_musd'] / pdf['revenue_musd'].max() * 500).fillna(50)
        scatter = ax.scatter(pdf['vote_average'], pdf['popularity'], 
                           alpha=0.6, s=sizes, c=pdf['vote_count'], 
                           cmap='plasma', edgecolors='black', linewidth=0.5)
        
        # Labels and title
        ax.set_xlabel('Rating (Vote Average)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Popularity Score', fontsize=12, fontweight='bold')
        ax.set_title('Popularity vs Rating (size = revenue, color = vote count)', 
                    fontsize=14, fontweight='bold', pad=20)
        ax.grid(True, alpha=0.3)
        
        # Add colorbar
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('Vote Count', fontsize=10)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Saved plot to {save_path}")
        
        return fig
    
    def plot_yearly_box_office_trends(
        self,
        df: DataFrame,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        figsize: Tuple[int, int] = (14, 8),
        save_path: Optional[str] = None
    ) -> plt.Figure:
        """
        Plot Yearly Trends in Box Office Performance.
        
        Shows mean and total revenue by year.
        
        Args:
            df: Spark DataFrame with movie data
            start_year: Starting year (optional)
            end_year: Ending year (optional)
            figsize: Figure size (width, height)
            save_path: Optional path to save the figure
            
        Returns:
            Matplotlib figure object
        """
        # Ensure release_year exists
        df_year = df.withColumn('release_year', F.year(F.col('release_date'))) \
                    .filter(F.col('release_year').isNotNull() & 
                           F.col('revenue_musd').isNotNull())
        
        # Filter by year range if specified
        if start_year:
            df_year = df_year.filter(F.col('release_year') >= start_year)
        if end_year:
            df_year = df_year.filter(F.col('release_year') <= end_year)
        
        # Aggregate by year
        yearly_stats = df_year.groupBy('release_year').agg(
            F.count('*').alias('movie_count'),
            F.mean('revenue_musd').alias('mean_revenue'),
            F.sum('revenue_musd').alias('total_revenue'),
            F.mean('budget_musd').alias('mean_budget'),
            F.mean('vote_average').alias('mean_rating')
        ).orderBy('release_year')
        
        # Convert to pandas
        pdf = yearly_stats.toPandas()
        
        # Create figure with two y-axes
        fig, ax1 = plt.subplots(figsize=figsize)
        ax2 = ax1.twinx()
        
        # Plot mean revenue (bar)
        ax1.bar(pdf['release_year'], pdf['mean_revenue'], 
               alpha=0.7, color='steelblue', label='Mean Revenue')
        
        # Plot total revenue (line)
        ax2.plot(pdf['release_year'], pdf['total_revenue'], 
                color='darkred', marker='o', linewidth=2, 
                markersize=6, label='Total Revenue')
        
        # Labels and title
        ax1.set_xlabel('Release Year', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Mean Revenue (Million USD)', fontsize=12, fontweight='bold', color='steelblue')
        ax2.set_ylabel('Total Revenue (Million USD)', fontsize=12, fontweight='bold', color='darkred')
        ax1.set_title('Yearly Trends in Box Office Performance', 
                     fontsize=14, fontweight='bold', pad=20)
        
        # Legends
        ax1.legend(loc='upper left', fontsize=10)
        ax2.legend(loc='upper right', fontsize=10)
        
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis='y', labelcolor='steelblue')
        ax2.tick_params(axis='y', labelcolor='darkred')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Saved plot to {save_path}")
        
        return fig
    
    def plot_franchise_vs_standalone(
        self,
        df: DataFrame,
        figsize: Tuple[int, int] = (14, 8),
        save_path: Optional[str] = None
    ) -> plt.Figure:
        """
        Plot comparison of Franchise vs. Standalone movies.
        
        Shows mean revenue, budget, rating, and popularity.
        
        Args:
            df: Spark DataFrame with movie data
            figsize: Figure size (width, height)
            save_path: Optional path to save the figure
            
        Returns:
            Matplotlib figure object
        """
        # Add franchise flag
        df_flagged = df.withColumn(
            'movie_type',
            F.when(F.col('collection_name').isNotNull(), 'Franchise').otherwise('Standalone')
        )
        
        # Calculate ROI
        df_calc = df_flagged.withColumn(
            'roi',
            F.when(
                F.col('budget_musd') > 0,
                ((F.col('revenue_musd') - F.col('budget_musd')) / F.col('budget_musd') * 100)
            ).otherwise(None)
        )
        
        # Aggregate by type
        comparison = df_calc.groupBy('movie_type').agg(
            F.count('*').alias('count'),
            F.mean('revenue_musd').alias('mean_revenue'),
            F.mean('budget_musd').alias('mean_budget'),
            F.mean('vote_average').alias('mean_rating'),
            F.mean('popularity').alias('mean_popularity'),
            F.mean('roi').alias('mean_roi')
        ).orderBy(F.col('movie_type').desc())
        
        # Convert to pandas
        pdf = comparison.toPandas()
        
        # Create subplots
        fig, axes = plt.subplots(2, 3, figsize=figsize)
        fig.suptitle('Franchise vs Standalone Movies Comparison', 
                    fontsize=16, fontweight='bold', y=1.02)
        
        metrics = [
            ('mean_revenue', 'Mean Revenue (M USD)', 'Blues'),
            ('mean_budget', 'Mean Budget (M USD)', 'Greens'),
            ('mean_rating', 'Mean Rating', 'Oranges'),
            ('mean_popularity', 'Mean Popularity', 'Purples'),
            ('mean_roi', 'Mean ROI (%)', 'Reds'),
            ('count', 'Movie Count', 'Greys')
        ]
        
        for idx, (metric, title, palette) in enumerate(metrics):
            row = idx // 3
            col = idx % 3
            ax = axes[row, col]
            
            sns.barplot(data=pdf, x='movie_type', y=metric, ax=ax, palette=palette)
            ax.set_title(title, fontsize=11, fontweight='bold')
            ax.set_xlabel('')
            ax.set_ylabel('')
            ax.grid(True, alpha=0.3, axis='y')
            
            # Add value labels on bars
            for container in ax.containers:
                ax.bar_label(container, fmt='%.1f', padding=3)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Saved plot to {save_path}")
        
        return fig


# Add numpy import for trend line
import numpy as np
