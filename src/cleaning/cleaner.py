"""
Spark-based movie data cleaning and preprocessing module.

This module handles cleaning raw TMDB API data using PySpark:
- Dropping irrelevant columns
- Flattening nested JSON structures
- Converting datatypes
- Filtering invalid records
- Feature engineering
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType
import logging

from .udfs import (
    extract_collection_name, extract_names_from_array, extract_keywords_from_struct,
    extract_top_cast, get_cast_size, extract_director, get_crew_size, sort_pipe_separated
)

logger = logging.getLogger(__name__)


class SparkMovieDataCleaner:
    """
    Handles data cleaning and preprocessing for TMDB movie data using PySpark.
    """
    
    def __init__(self, spark: SparkSession = None, config=None):
        """
        Initialize the Spark cleaner.
        
        Args:
            spark (SparkSession, optional): Spark session instance
            config (dict, optional): Configuration dictionary
        """
        self.spark = spark
        self.config = config or {}
    
    def load_raw_data(self, raw_data_path: str) -> DataFrame:
        """
        Load raw JSON files from the specified path using Spark.
        
        Args:
            raw_data_path (str): Path to raw data directory
            
        Returns:
            DataFrame: Spark DataFrame with raw data
        """
        logger.info("Loading raw data...")
        logger.info(f"Raw data path: {raw_data_path}")
        
        # Read JSON files with mult

# multiiLine option for TMDB API format
        df = self.spark.read.option("multiLine", "true").json(f"{raw_data_path}/*.json")
        
        count = df.count()
        logger.info(f"Loaded {count} movies")
        logger.info(f"Initial columns: {len(df.columns)}")
        
        return df
    
    def drop_irrelevant_columns(self, df: DataFrame) -> DataFrame:
        """
        Drop columns that are not needed for analysis.
        
        Args:
            df (DataFrame): Input Spark DataFrame
            
        Returns:
            DataFrame: DataFrame with irrelevant columns removed
        """
        logger.info("Dropping irrelevant columns...")
        
        cols_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage', 
                        'backdrop_path', 'poster_path', 'origin_country']
        
        # Only drop columns that exist
        existing_cols_to_drop = [col for col in cols_to_drop if col in df.columns]
        
        df_clean = df.drop(*existing_cols_to_drop)
        
        logger.info(f"Dropped columns: {existing_cols_to_drop}")
        logger.info(f"Remaining columns: {len(df_clean.columns)}")
        
        return df_clean
    
    def flatten_nested_columns(self, df: DataFrame) -> DataFrame:
        """
        Flatten nested JSON columns into pipe-separated strings.
        
        Extracts data from: belongs_to_collection, genres, production_countries,
        production_companies, spoken_languages, keywords
        
        Args:
            df (DataFrame): Input Spark DataFrame
            
        Returns:
            DataFrame: DataFrame with flattened columns
        """
        logger.info("Flattening nested JSON columns...")
        
        # Extract collection name
        if 'belongs_to_collection' in df.columns:
            df = df.withColumn('collection_name', extract_collection_name(F.col('belongs_to_collection')))
        
        # Extract genres
        if 'genres' in df.columns:
            df = df.withColumn('genres', extract_names_from_array(F.col('genres')))
        
        # Extract production countries
        if 'production_countries' in df.columns:
            df = df.withColumn('production_countries', extract_names_from_array(F.col('production_countries')))
        
        # Extract production companies
        if 'production_companies' in df.columns:
            df = df.withColumn('production_companies', extract_names_from_array(F.col('production_companies')))
        
        # Extract spoken languages
        if 'spoken_languages' in df.columns:
            df = df.withColumn('spoken_languages', extract_names_from_array(F.col('spoken_languages')))
        
        # Extract keywords (note: keywords has different structure)
        if 'keywords' in df.columns:
            df = df.withColumn('keywords', extract_keywords_from_struct(F.col('keywords')))
        
        logger.info("Nested columns flattened successfully")
        
        return df
    
    def clean_datatypes(self, df: DataFrame) -> DataFrame:
        """
        Convert columns to appropriate datatypes and handle invalid values.
        
        Args:
            df (DataFrame): Input Spark DataFrame
            
        Returns:
            DataFrame: DataFrame with cleaned datatypes
        """
        logger.info("Cleaning datatypes...")
        
        # Convert numeric columns
        numeric_cols = {
            'budget': 'long',
            'id': 'long',
            'popularity': 'double',
            'revenue': 'long',
            'vote_count': 'long',
            'vote_average': 'double',
            'runtime': 'long'
        }
        
        for col_name, dtype in numeric_cols.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(dtype))
        
        # Convert release_date to date type
        if 'release_date' in df.columns:
            df = df.withColumn('release_date', F.to_date(F.col('release_date')))
        
        # Replace zero values with null in budget/revenue/runtime (zeros are unrealistic)
        for col_name in ['budget', 'revenue', 'runtime']:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    F.when(F.col(col_name) == 0, None).otherwise(F.col(col_name))
                )
        
        # Create million USD columns
        if 'budget' in df.columns:
            df = df.withColumn('budget_musd', F.col('budget') / 1_000_000)
        
        if 'revenue' in df.columns:
            df = df.withColumn('revenue_musd', F.col('revenue') / 1_000_000)
        
        # Handle text placeholders
        placeholders = ['No Data', 'No Overview', 'n/a', 'nan']
        for col_name in ['overview', 'tagline']:
            if col_name in df.columns:
                # Replace placeholders with null
                condition = F.col(col_name).isin(placeholders)
                df = df.withColumn(
                    col_name,
                    F.when(condition, None).otherwise(F.col(col_name))
                )
        
        logger.info("Datatypes cleaned.")
        
        return df
    
    def filter_data(self, df: DataFrame) -> DataFrame:
        """
        Filter out invalid or incomplete records.
        
        Removes:
        - Duplicates
        - Rows with missing ID/title
        - Non-released movies
        
        Args:
            df (DataFrame): Input Spark DataFrame
            
        Returns:
            DataFrame: Filtered DataFrame
        """
        logger.info("Filtering data...")
        initial_count = df.count()
        
        # Drop duplicates based on ID
        df = df.dropDuplicates(['id'])
        
        # Drop rows with missing ID or title
        df = df.filter(F.col('id').isNotNull() & F.col('title').isNotNull())
        
        # Filter by status (only keep 'Released' movies)
        if 'status' in df.columns:
            df = df.filter(F.col('status') == 'Released')
            df = df.drop('status')
        
        final_count = df.count()
        rows_removed = initial_count - final_count
        
        logger.info(f"Rows removed: {rows_removed}")
        logger.info(f"Final count: {final_count}")
        
        return df
    
    def engineer_features(self, df: DataFrame) -> DataFrame:
        """
        Create new features from existing data.
        
        Adds:
        - cast, cast_size, director, crew_size (from credits)
        - release_year (from release_date)
        
        Args:
            df (DataFrame): Input Spark DataFrame
            
        Returns:
            DataFrame: DataFrame with engineered features
        """
        logger.info("Performing feature engineering...")
        
        # Extract cast and crew information from credits
        if 'credits' in df.columns:
            logger.info("Extracting cast and crew information from the 'credits' column.")
            
            df = df.withColumn('cast', extract_top_cast(F.col('credits')))
            df = df.withColumn('cast_size', get_cast_size(F.col('credits')))
            df = df.withColumn('director', extract_director(F.col('credits')))
            df = df.withColumn('crew_size', get_crew_size(F.col('credits')))
            
            logger.info("Cast and crew information successfully extracted.")
        else:
            logger.warning("The 'credits' column was not found. Skipping cast/crew extraction.")
        
        # Extract release year
        if 'release_date' in df.columns:
            logger.info("Extracting the release year from the 'release_date' column.")
            df = df.withColumn('release_year', F.year(F.col('release_date')))
            logger.info("Release year successfully extracted.")
        
        # Handle 'nan' string values in text columns
        text_cols = ['tagline', 'title', 'collection_name']
        for col_name in text_cols:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    F.when(F.col(col_name) == 'nan', None).otherwise(F.col(col_name))
                )
        
        logger.info("Feature engineering complete.")
        
        return df
    
    def sort_genres(self, df: DataFrame) -> DataFrame:
        """
        Sort genres alphabetically within each cell.
        
        Args:
            df (DataFrame): Input Spark DataFrame
            
        Returns:
            DataFrame: DataFrame with sorted genres
        """
        logger.info("Sorting genres alphabetically...")
        
        if 'genres' in df.columns:
            df = df.withColumn('genres', sort_pipe_separated(F.col('genres')))
        
        logger.info("Genres sorted.")
        
        return df
    
    def finalize_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Reorder columns to desired format.
        
        Args:
            df (DataFrame): Input Spark DataFrame
            
        Returns:
            DataFrame: Finalized DataFrame with ordered columns
        """
        logger.info("Finalizing dataframe...")
        
        desired_order = [
            'id', 'title', 'tagline', 'release_date', 'genres', 'collection_name',
            'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
            'production_countries', 'vote_count', 'vote_average', 'popularity',
            'runtime', 'overview', 'spoken_languages',
            'cast', 'cast_size', 'director', 'crew_size', 'release_year', 'keywords'
        ]
        
        # Select only existing columns in desired order
        final_cols = [col for col in desired_order if col in df.columns]
        df_final = df.select(*final_cols)
        
        logger.info(f"Final columns: {df_final.columns}")
        logger.info(f"Final column count: {len(df_final.columns)}")
        
        return df_final
    
    def clean_all(self, df: DataFrame) -> DataFrame:
        """
        Run the complete cleaning pipeline on raw data.
        
        Args:
            df (DataFrame): Raw Spark DataFrame
            
        Returns:
            DataFrame: Cleaned and processed Spark DataFrame
        """
        logger.info("=" * 60)
        logger.info("Starting complete data cleaning pipeline")
        logger.info("=" * 60)
        
        # Step 1: Drop irrelevant columns
        df = self.drop_irrelevant_columns(df)
        
        # Step 2: Flatten nested columns
        df = self.flatten_nested_columns(df)
        
        # Step 3: Clean datatypes
        df = self.clean_datatypes(df)
        
        # Step 4: Filter data
        df = self.filter_data(df)
        
        # Step 5: Engineer features
        df = self.engineer_features(df)
        
        # Step 6: Sort genres
        df = self.sort_genres(df)
        
        # Step 7: Finalize
        df = self.finalize_dataframe(df)
        
        logger.info("=" * 60)
        logger.info("Data cleaning completed successfully")
        logger.info("=" * 60)
        
        return df
    
    def save_cleaned_data(self, df: DataFrame, output_path: str):
        """
        Save cleaned data to Parquet and CSV formats.
        
        Args:
            df (DataFrame): Cleaned Spark DataFrame
            output_path (str): Output directory path
        """
        logger.info(f"Saving cleaned data to {output_path}...")
        
        # Save as Parquet (efficient for Spark)
        parquet_path = f"{output_path}/movies_cleaned.parquet"
        df.write.mode('overwrite').parquet(parquet_path)
        logger.info(f"Saved to {parquet_path}")
        
        # Save as CSV (single file for compatibility)
        csv_path = f"{output_path}/movies_cleaned.csv"
        df.coalesce(1).write.mode('overwrite') \
          .option('header', 'true') \
          .csv(csv_path)
        logger.info(f"Saved to {csv_path}")
        
        logger.info("Data saved successfully.")
