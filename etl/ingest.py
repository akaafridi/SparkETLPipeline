"""
ETL Ingestion Module: Handles reading, transforming, and writing data using Spark.
"""
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def initialize_spark():
    """Initialize and return a SparkSession for local processing."""
    logger.info("Initializing Spark session")
    spark = (
        SparkSession.builder
        .appName("SparkETLDemo")
        .master("local[*]")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    # Set log level for Spark
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized successfully")
    return spark

def ingest(input_path=None, output_path=None):
    """
    Ingest data from CSV, transform it, and write to Parquet.
    
    Args:
        input_path (str, optional): Path to input CSV. Defaults to data/labels.csv.
        output_path (str, optional): Path to output directory. Defaults to data/output/parquet.
    
    Returns:
        tuple: (spark_session, output_path) for further processing
    """
    # Default paths
    if input_path is None:
        input_path = "data/labels.csv"
    if output_path is None:
        output_path = "data/output/parquet"
    
    # Ensure input file exists
    if not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        raise FileNotFoundError(f"The input file {input_path} does not exist.")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Initialize Spark
    spark = initialize_spark()
    
    try:
        # Read CSV data
        logger.info(f"Reading CSV from {input_path}")
        df = spark.read.option("header", "true").csv(input_path)
        
        # Log initial data stats
        total_rows = df.count()
        logger.info(f"Initial row count: {total_rows}")
        
        # Filter out null ImageID records
        logger.info("Filtering out null ImageID records")
        filtered_df = df.filter(col("ImageID").isNotNull())
        
        # Log filtered data stats
        filtered_rows = filtered_df.count()
        logger.info(f"Filtered row count: {filtered_rows}")
        logger.info(f"Removed {total_rows - filtered_rows} rows with null ImageID")
        
        # Write to Parquet
        logger.info(f"Writing Parquet to {output_path}")
        filtered_df.write.mode("overwrite").parquet(output_path)
        
        logger.info("ETL process completed successfully")
        return spark, output_path
    
    except Exception as e:
        logger.error(f"Error during ETL process: {str(e)}")
        raise

if __name__ == "__main__":
    # When run directly, this script will perform the ETL process
    ingest()
