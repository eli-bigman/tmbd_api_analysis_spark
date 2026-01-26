"""
Spark session management utilities for TMDB Analysis project.
"""

from pyspark.sql import SparkSession
from typing import Optional, Dict, Any
import logging
from .helpers import load_config


logger = logging.getLogger(__name__)


def get_spark_session(
    app_name: str = "TMDB_Analysis",
    config_path: str = "config/config.yaml",
    config_overrides: Optional[Dict[str, Any]] = None
) -> SparkSession:
    """
    Create or get existing SparkSession with configuration from YAML file.
    
    This function implements a singleton pattern - if a SparkSession already exists,
    it returns that session rather than creating a new one.
    
    Args:
        app_name: Name of the Spark application
        config_path: Path to config.yaml file containing Spark settings
        config_overrides: Optional dictionary to override config file settings
        
    Returns:
        SparkSession: Configured Spark session instance
        
    Example:
        >>> spark = get_spark_session()
        >>> df = spark.read.json("data/raw/movies.json")
    """
    # Check if session already exists
    existing_session = SparkSession.getActiveSession()
    if existing_session is not None:
        logger.info(f"Returning existing SparkSession: {existing_session.sparkContext.appName}")
        return existing_session
    
    # Load configuration from YAML
    try:
        config = load_config(config_path)
        spark_config = config.get('spark', {})
    except Exception as e:
        logger.warning(f"Failed to load config from {config_path}: {e}")
        logger.warning("Using default Spark configuration")
        spark_config = {}
    
    # Apply config overrides if provided
    if config_overrides:
        spark_config.update(config_overrides)
    
    # Build SparkSession
    builder = SparkSession.builder
    
    # Set app name
    builder = builder.appName(app_name)
    
    # Apply Spark configurations
    for key, value in spark_config.items():
        # Convert config key format to spark.key.format
        if not key.startswith('spark.'):
            config_key = f"spark.{key}"
        else:
            config_key = key
        
        builder = builder.config(config_key, value)
        logger.debug(f"Set Spark config: {config_key} = {value}")
    
    # Create session
    spark = builder.getOrCreate()
    
    logger.info(f"Created new SparkSession: {app_name}")
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Spark master: {spark.sparkContext.master}")
    
    return spark


def stop_spark_session() -> None:
    """
    Stop the active SparkSession if one exists.
    
    This function gracefully shuts down the Spark session and releases resources.
    Safe to call even if no session exists.
    
    Example:
        >>> stop_spark_session()
    """
    session = SparkSession.getActiveSession()
    if session is not None:
        logger.info(f"Stopping SparkSession: {session.sparkContext.appName}")
        session.stop()
    else:
        logger.info("No active SparkSession to stop")


def get_spark_context(spark: Optional[SparkSession] = None):
    """
    Get SparkContext from SparkSession.
    
    Args:
        spark: SparkSession instance. If None, gets the active session.
        
    Returns:
        SparkContext instance
        
    Raises:
        RuntimeError: If no active SparkSession exists
    """
    if spark is None:
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active SparkSession. Call get_spark_session() first.")
    
    return spark.sparkContext
